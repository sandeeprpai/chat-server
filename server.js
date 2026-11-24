const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;

// In-memory maps only (zero database, zero disk storage)
const clients = new Map();       // userId -> Set<WebSocket>
const pendingQueue = new Map();  // recipientId -> Array<JSON string>

const server = http.createServer((req, res) => {
  // Simple HTTP health check endpoint for Render / uptime checks
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, service: "sancil-chat-relay" }));
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

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (data) => {
    let payload;
    try {
      payload = JSON.parse(data.toString());
    } catch (_) {
      return;
    }

    const toId = typeof payload.to === "string" ? payload.to.trim() : "";
    if (!toId) return;

    // Ensure sender ID matches authenticated connection
    payload.from = userId;
    if (!payload.timestamp) {
      payload.timestamp = Date.now();
    }

    deliverOrQueue(toId, JSON.stringify(payload));
  });

  ws.on("close", () => {
    unregisterClient(userId, ws);
  });

  ws.on("error", () => {
    unregisterClient(userId, ws);
  });
});

// Keep NAT/proxy WebSocket connections alive every 25 seconds
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      unregisterClient(ws.userId, ws);
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
  console.log(`Chat relay listening on port ${PORT}`);
});
