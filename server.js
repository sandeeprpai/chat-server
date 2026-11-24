const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
// Bump this when deploying so /health confirms which server version Render is running.
const RELAY_VERSION = 4;
// Set this in Render before using /admin/clients.
// Example: ADMIN_TOKEN=change-me-to-a-long-random-value
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "";

// In-memory maps only (zero database, zero disk storage)
const clients = new Map();       // userId -> Set<WebSocket>
const pendingQueue = new Map();  // recipientId -> Array<JSON string>
const blockEvents = [];          // recent relay-observed block/unblock signals
const signalEvents = [];         // recent non-chat signals, for owner diagnostics
const MAX_BLOCK_EVENTS = 500;
const MAX_SIGNAL_EVENTS = 500;

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

function isValidUserId(id) {
  return typeof id === "string" && /^[a-z0-9_]{3,24}$/.test(id.trim().toLowerCase());
}

function recordBlockEvent(action, blocker, blocked, source) {
  blockEvents.push({
    at: Date.now(),
    action,
    blocker,
    blocked,
    source
  });
  if (blockEvents.length > MAX_BLOCK_EVENTS) {
    blockEvents.shift();
  }
}

function recordSignalEvent(details) {
  signalEvents.push({ at: Date.now(), ...details });
  if (signalEvents.length > MAX_SIGNAL_EVENTS) {
    signalEvents.shift();
  }
}

function currentBlockState() {
  // Last event wins for each blocker/blocked pair.
  const latest = new Map();
  for (const event of blockEvents) {
    latest.set(`${event.blocker}|${event.blocked}`, event);
  }
  const active = [];
  for (const event of latest.values()) {
    if (event.action === "blocked") {
      active.push({
        blocker: event.blocker,
        blocked: event.blocked,
        since: nowIso(event.at),
        source: event.source
      });
    }
  }
  active.sort((a, b) => a.blocker.localeCompare(b.blocker) || a.blocked.localeCompare(b.blocked));
  return active;
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += chunk;
      if (raw.length > 4096) {
        reject(new Error("Request body too large"));
        req.destroy();
      }
    });
    req.on("end", () => {
      try {
        resolve(raw.length ? JSON.parse(raw) : {});
      } catch (_) {
        reject(new Error("Invalid JSON body"));
      }
    });
    req.on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
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
    sendJson(res, 200, {
      ok: true,
      service: "sancil-chat-relay",
      version: RELAY_VERSION,
      features: ["admin_clients", "admin_blocks", "admin_unblock", "admin_signals", "signal_logs", "flush_logs", "socket_dedupe"]
    });
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

  // Owner-only view of block signals the relay has observed since the last restart.
  if (url.pathname === "/admin/blocks") {
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

    sendJson(res, 200, {
      ok: true,
      generatedAt: nowIso(Date.now()),
      note: "Relay-observed only; blocks made while offline are not visible until relayed.",
      currentBlocks: currentBlockState(),
      events: blockEvents.slice().reverse().map((event) => ({
        at: nowIso(event.at),
        action: event.action,
        blocker: event.blocker,
        blocked: event.blocked,
        source: event.source
      }))
    });
    return;
  }

  // Owner override. Sends an admin_unblock signal to both ids so both local databases
  // clear their block flags for each other. Works immediately if both are connected;
  // otherwise the signal is queued in RAM until they reconnect.
  if (url.pathname === "/admin/unblock") {
    if (req.method !== "POST") {
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

    let body;
    try {
      body = await readJsonBody(req);
    } catch (error) {
      sendJson(res, 400, { ok: false, error: error.message });
      return;
    }

    const userId = isValidUserId(body.userId) ? body.userId.trim().toLowerCase() : "";
    const peerId = isValidUserId(body.peerId) ? body.peerId.trim().toLowerCase() : "";
    if (!userId || !peerId || userId === peerId) {
      sendJson(res, 400, {
        ok: false,
        error: "Provide two different valid userId values (3-24 lowercase letters, numbers or underscore)"
      });
      return;
    }

    const sendTo = [
      { targetId: userId, otherId: peerId },
      { targetId: peerId, otherId: userId }
    ];
    const sentTo = [];
    for (const item of sendTo) {
      const raw = JSON.stringify({
        type: "admin_unblock",
        from: "admin",
        to: item.targetId,
        peerId: item.otherId,
        timestamp: Date.now()
      });
      const delivered = deliverOrQueue(item.targetId, raw);
      log("admin_unblock_sent", {
        targetId: item.targetId,
        peerId: item.otherId,
        delivered,
        queued: !delivered
      });
      sentTo.push({ userId: item.targetId, peerId: item.otherId, delivered, queued: !delivered });
    }

    recordBlockEvent("unblocked_by_admin", peerId, userId, "admin");
    recordBlockEvent("unblocked_by_admin", userId, peerId, "admin");

    sendJson(res, 200, {
      ok: true,
      message: "Unblock signal sent to both users",
      sentTo
    });
    return;
  }

  // Owner-only history of relationship/call signals, without chat message text.
  if (url.pathname === "/admin/signals") {
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

    sendJson(res, 200, {
      ok: true,
      generatedAt: nowIso(Date.now()),
      note: "Recent non-chat signals since server restart; chat text is never stored.",
      signals: signalEvents.slice().reverse().map((event) => ({
        at: nowIso(event.at),
        type: event.type,
        from: event.from,
        to: event.to,
        delivered: event.delivered,
        queued: event.queued
      }))
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
  let set = clients.get(userId);

  // One user id should have exactly one live socket. If an older socket is still
  // registered (stale app process or an old APK), close it so messages always go
  // to the newest connection.
  if (set && set.size > 0) {
    for (const old of set) {
      if (old !== ws && old.readyState === old.OPEN) {
        log("superseded_socket", { userId });
        old.close(4000, "Superseded by newer connection");
      }
    }
    set.clear();
  } else {
    set = new Set();
    clients.set(userId, set);
  }

  set.add(ws);
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

  // Stale signals from an earlier session can arrive after a reinstall/reconnect and
  // cancel a brand-new friend request. Log what was flushed so this is diagnosable.
  log("flushed_pending", {
    userId,
    count: queue.length,
    types: queue.slice(0, 20).map((raw) => {
      try {
        return JSON.parse(raw).type || "chat";
      } catch (_) {
        return "invalid";
      }
    })
  });

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

    if (type === "blocked_by_peer" || type === "unblocked_by_peer") {
      recordBlockEvent(
        type === "blocked_by_peer" ? "blocked" : "unblocked",
        userId,
        toId,
        "signal"
      );
    }

    const delivered = deliverOrQueue(toId, JSON.stringify(payload));

    // Log relationship/call/admin-relevant signals, not normal chat text.
    if (type !== "chat") {
      const signalDetails = {
        from: userId,
        to: toId,
        type,
        delivered,
        queued: !delivered
      };
      recordSignalEvent(signalDetails);
      log("relayed_signal", signalDetails);
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
