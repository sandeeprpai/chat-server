const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
// Set this in Render before using /admin/clients.
// Example: ADMIN_TOKEN=change-me-to-a-long-random-value
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "";

// In-memory maps only (zero database, zero disk storage)
const clients = new Map();       // userId -> Set<WebSocket>
const pendingQueue = new Map();  // recipientId -> Array<JSON string>

function nowIso(ms) {
  return new Date(ms).toISOString();
}

function clientIp(req) {
  const forwarded = req.headers["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded.length > 0) {
    return forwarded.split(",")[0].trim();
  }
  return req.socket.remoteAddress || null;
}

function log(event, details) {
  console.log(JSON.stringify({ event, at: nowIso(Date.now()), ...details }));
}

function isAdminAuthorized(req) {
  if (!ADMIN_TOKEN) return false;
  return req.headers.authorization === `Bearer ${ADMIN_TOKEN}`;
}

function sendJson(res, status, body) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body, null, 2));
}

const server = http.createServer((req, res) => {
  let url;
  try {
    url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  } catch (_) {
    res.writeHead(400);
    res.end("Bad Request");
    return;
  }

  // Simple HTTP health check endpoint for Render / uptime checks
  if (url.pathname === "/" || url.pathname === "/health") {
    sendJson(res, 200, { ok: true, service: "sancil-chat-relay" });
    return;
  }

  // Owner-only list of currently connected sockets.
  // This is the closest thing to "connected devices" the relay can know:
  // one WebSocket normally means one app session/device.
  if (url.pathname === "/admin/clients") {
    if (req.method !== "GET") {
      sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
      return;
    }
    if (!ADMIN_TOKEN) {
      sendJson(res, 503, { ok: false, error: "ADMIN_TOKEN is not configured" });
      return;
    }
    if (!isAdminAuthorized(req)) {
      sendJson(res, 401, { ok: false, error: "Unauthorized" });
      return;
    }

    const connectedUsers = [];
    for (const [userId, sockets] of clients.entries()) {
      connectedUsers.push({
        userId,
        socketCount: sockets.size,
        sockets: Array.from(sockets).map((ws) => ({
          connectedAt: nowIso(ws.connectedAt),
          lastSeenAt: nowIso(ws.lastSeenAt),
          ip: ws.ip || null,
          userAgent: ws.userAgent || null
        }))
      });
    }
    connectedUsers.sort((a, b) => a.userId.localeCompare(b.userId));

    let queuedMessages = 0;
    const queueDetails = [];
    for (const [recipientId, queue] of pendingQueue.entries()) {
      queuedMessages += queue.length;
      queueDetails.push({
        recipientId,
        count: queue.length,
        types: queue.slice(0, 10).map((raw) => {
          try {
            return JSON.parse(raw).type || "chat";
          } catch (_) {
            return "invalid";
          }
        })
      });
    }

    sendJson(res, 200, {
      ok: true,
      generatedAt: nowIso(Date.now()),
      connectedSockets: wss.clients.size,
      connectedUsers,
      pendingQueue: {
        users: pendingQueue.size,
        messages: queuedMessages,
        details: queueDetails
      }
    });
    return;
  }

  res.writeHead(404);
  res.end("Not Found");
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  if (parsedUrl.pathname !== "/chat") {
    socket.destroy();
    return;
  }

  const userId = (parsedUrl.searchParams.get("userId") || "").trim();
  if (!userId) {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.userId = userId;
    ws.isAlive = true;
    ws.connectedAt = Date.now();
    ws.lastSeenAt = ws.connectedAt;
    ws.ip = clientIp(req);
    ws.userAgent = typeof req.headers["user-agent"] === "string"
      ? req.headers["user-agent"]
      : null;
    wss.emit("connection", ws, req);
  });
});

function registerClient(userId, ws) {
  if (!clients.has(userId)) {
    clients.set(userId, new Set());
  }
  clients.get(userId).add(ws);
}

function unregisterClient(userId, ws) {
  const set = clients.get(userId);
  if (!set) return;
  set.delete(ws);
  if (set.size === 0) {
    clients.delete(userId);
  }
}

function deliverOrQueue(toId, rawPayload) {
  const targets = clients.get(toId);
  let delivered = false;

  if (targets && targets.size > 0) {
    for (const sock of targets) {
      if (sock.readyState === sock.OPEN) {
        sock.send(rawPayload);
        delivered = true;
      }
    }
  }

  // Temporary RAM buffer only while recipient is offline (capped at 100 per user)
  if (!delivered) {
    if (!pendingQueue.has(toId)) {
      pendingQueue.set(toId, []);
    }
    const queue = pendingQueue.get(toId);
    queue.push(rawPayload);
    if (queue.length > 100) {
      queue.shift();
    }
  }

  return delivered;
}

function flushPending(userId, ws) {
  const queue = pendingQueue.get(userId);
  if (!queue || queue.length === 0) return;

  while (queue.length > 0 && ws.readyState === ws.OPEN) {
    const item = queue.shift();
    ws.send(item);
  }

  if (queue.length === 0) {
    pendingQueue.delete(userId);
  }
}

wss.on("connection", (ws) => {
  const userId = ws.userId;
  registerClient(userId, ws);
  flushPending(userId, ws);

  log("connected", {
    userId,
    ip: ws.ip,
    socketCount: clients.get(userId).size
  });

  ws.on("pong", () => {
    ws.isAlive = true;
    ws.lastSeenAt = Date.now();
  });

  ws.on("message", (data) => {
    ws.lastSeenAt = Date.now();

    let payload;
    try {
      payload = JSON.parse(data.toString());
    } catch (_) {
      return;
    }

    const type = typeof payload.type === "string" ? payload.type : "chat";
    const toId = typeof payload.to === "string" ? payload.to.trim() : "";
    if (!toId) {
      log("dropped_no_to", { from: userId, type });
      return;
    }

    // Ensure sender ID matches authenticated connection
    payload.from = userId;
    if (!payload.timestamp) {
      payload.timestamp = Date.now();
    }

    const delivered = deliverOrQueue(toId, JSON.stringify(payload));

    // Log relationship/call/admin-relevant signals, not normal chat text.
    if (type !== "chat") {
      log("relayed_signal", {
        from: userId,
        to: toId,
        type,
        delivered,
        queued: !delivered
      });
    }
  });

  ws.on("close", (code, reason) => {
    unregisterClient(userId, ws);
    log("disconnected", {
      userId,
      code,
      reason: String(reason || ""),
      socketCount: clients.has(userId) ? clients.get(userId).size : 0
    });
  });

  ws.on("error", (error) => {
    unregisterClient(userId, ws);
    log("socket_error", {
      userId,
      error: error && error.message ? error.message : String(error)
    });
  });
});

// Keep NAT/proxy WebSocket connections alive every 25 seconds
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      unregisterClient(ws.userId, ws);
      log("heartbeat_timeout", { userId: ws.userId });
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

wss.on("close", () => {
  clearInterval(heartbeat);
});

server.listen(PORT, () => {
  console.log(
    `Chat relay listening on port ${PORT}` +
    (ADMIN_TOKEN ? " (admin endpoint enabled)" : " (admin endpoint disabled: set ADMIN_TOKEN)")
  );
});
