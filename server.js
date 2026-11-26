const http = require("http");
const https = require("https");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
// Bump this when deploying so /health confirms which server version Render is running.
const RELAY_VERSION = 8;
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
// Chats are retried by the sender until the recipient confirms delivery. Keep enough
// distinct messages for a long offline period instead of dropping the oldest at 100.
const MAX_PENDING_MESSAGES = 500;

// S3-compatible object storage is used only for album photos/videos and the album manifest.
// Backblaze B2 is the default (free without a card); Cloudflare R2 env vars still work as a
// fallback. Chats, presence and calls stay on the relay/WebRTC path. Credentials live on the
// server; the APK only ever receives short-lived signed URLs.
const LEGACY_R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || "";
const LEGACY_R2_ENDPOINT = LEGACY_R2_ACCOUNT_ID
  ? `https://${LEGACY_R2_ACCOUNT_ID}.r2.cloudflarestorage.com`
  : "";
const STORAGE_BUCKET = process.env.B2_BUCKET || process.env.R2_BUCKET || "sancil-albums";
const STORAGE_ACCESS_KEY_ID = process.env.B2_KEY_ID || process.env.R2_ACCESS_KEY_ID || "";
const STORAGE_SECRET_ACCESS_KEY = process.env.B2_APPLICATION_KEY || process.env.R2_SECRET_ACCESS_KEY || "";
const STORAGE_ENDPOINT = (process.env.B2_ENDPOINT || process.env.R2_ENDPOINT || LEGACY_R2_ENDPOINT)
  .replace(/\/+$/, "");

function inferStorageRegion(endpoint) {
  try {
    const host = new URL(endpoint).hostname;
    const b2Match = host.match(/^s3\.([a-z0-9-]+)\.backblazeb2\.com$/i);
    if (b2Match) return b2Match[1];
  } catch (_) {
  }
  return "auto";
}

const STORAGE_REGION = process.env.B2_REGION || process.env.R2_REGION
  || inferStorageRegion(STORAGE_ENDPOINT);
const MAX_ALBUM_UPLOAD_BYTES = parseInt(process.env.MAX_ALBUM_UPLOAD_BYTES || "52428800", 10);
const MAX_ALBUM_MANIFEST_BYTES = 1024 * 1024;
const CLOUD_CONFIGURED = Boolean(STORAGE_ENDPOINT && STORAGE_ACCESS_KEY_ID
  && STORAGE_SECRET_ACCESS_KEY && STORAGE_BUCKET);

function storageProviderName() {
  try {
    const host = new URL(STORAGE_ENDPOINT).hostname.toLowerCase();
    if (host.includes("backblazeb2.com")) return "Backblaze B2";
    if (host.includes("r2.cloudflarestorage.com")) return "Cloudflare R2";
  } catch (_) {
  }
  return "S3-compatible storage";
}

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

// Both devices derive the same folder from the two chat participants. Length prefixes keep
// pairs unambiguous even when user ids contain underscores.
function chatAlbumScope(userId, peerId) {
  const a = String(userId || "").trim().toLowerCase();
  const b = String(peerId || "").trim().toLowerCase();
  if (!isValidUserId(a) || !isValidUserId(b)) return "";
  const pair = [a, b].sort();
  return `chat_${pair[0].length}_${pair[0]}_${pair[1].length}_${pair[1]}`;
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

function readJsonBody(req, maxBytes = 4096) {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += chunk;
      if (raw.length > maxBytes) {
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

// --- S3-compatible Signature V4 helpers (no extra npm dependencies) ---

function sha256Hex(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function hmacSha256(key, value) {
  return crypto.createHmac("sha256", key).update(value).digest();
}

function s3SigningKey(secret, dateStamp) {
  let key = hmacSha256(`AWS4${secret}`, dateStamp);
  key = hmacSha256(key, STORAGE_REGION);
  key = hmacSha256(key, "s3");
  return hmacSha256(key, "aws4_request");
}

// RFC3986 encoding: encodeURIComponent leaves a few characters unescaped that SigV4 requires.
function s3Encode(value) {
  return encodeURIComponent(String(value))
    .replace(/[!'()*]/g, (ch) => "%" + ch.charCodeAt(0).toString(16).toUpperCase());
}

function s3CanonicalUri(key) {
  const safeKey = String(key || "").replace(/^\/+/, "");
  return "/" + s3Encode(STORAGE_BUCKET) + "/" + safeKey.split("/").map(s3Encode).join("/");
}

function s3Host() {
  return new URL(STORAGE_ENDPOINT).host;
}

function s3Url(key) {
  return new URL(STORAGE_ENDPOINT + s3CanonicalUri(key));
}

function s3Timestamp() {
  const now = new Date();
  return {
    amzDate: now.toISOString().replace(/[:-]|\.\d{3}/g, ""),
    dateStamp: now.toISOString().slice(0, 10).replace(/-/g, "")
  };
}

function s3PresignUrl(method, key, expiresSeconds, contentType) {
  const { amzDate, dateStamp } = s3Timestamp();
  const host = s3Host();
  const headers = contentType ? { "content-type": contentType, host } : { host };
  const signedHeaders = Object.keys(headers).sort();
  const canonicalHeaders = signedHeaders
    .map((name) => `${name}:${String(headers[name]).trim()}\n`)
    .join("");
  const credential = `${STORAGE_ACCESS_KEY_ID}/${dateStamp}/${STORAGE_REGION}/s3/aws4_request`;
  const params = [
    ["X-Amz-Algorithm", "AWS4-HMAC-SHA256"],
    ["X-Amz-Credential", credential],
    ["X-Amz-Date", amzDate],
    ["X-Amz-Expires", String(expiresSeconds)],
    ["X-Amz-SignedHeaders", signedHeaders.join(";")]
  ];
  params.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  const canonicalQuery = params
    .map(([name, value]) => `${s3Encode(name)}=${s3Encode(value)}`)
    .join("&");
  const canonicalRequest = [
    method,
    s3CanonicalUri(key),
    canonicalQuery,
    canonicalHeaders,
    signedHeaders.join(";"),
    "UNSIGNED-PAYLOAD"
  ].join("\n");
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    `${dateStamp}/${STORAGE_REGION}/s3/aws4_request`,
    sha256Hex(canonicalRequest)
  ].join("\n");
  const signature = crypto
    .createHmac("sha256", s3SigningKey(STORAGE_SECRET_ACCESS_KEY, dateStamp))
    .update(stringToSign)
    .digest("hex");
  return `${STORAGE_ENDPOINT}${s3CanonicalUri(key)}?${canonicalQuery}&X-Amz-Signature=${signature}`;
}

function s3SignedHeaders(method, key, body, contentType) {
  const { amzDate, dateStamp } = s3Timestamp();
  const payloadHash = sha256Hex(body || "");
  const headers = {
    "content-type": contentType || "application/octet-stream",
    host: s3Host(),
    "x-amz-content-sha256": payloadHash,
    "x-amz-date": amzDate
  };
  const signedHeaders = Object.keys(headers).sort();
  const canonicalHeaders = signedHeaders
    .map((name) => `${name}:${String(headers[name]).trim()}\n`)
    .join("");
  const canonicalRequest = [
    method,
    s3CanonicalUri(key),
    "",
    canonicalHeaders,
    signedHeaders.join(";"),
    payloadHash
  ].join("\n");
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    `${dateStamp}/${STORAGE_REGION}/s3/aws4_request`,
    sha256Hex(canonicalRequest)
  ].join("\n");
  const signature = crypto
    .createHmac("sha256", s3SigningKey(STORAGE_SECRET_ACCESS_KEY, dateStamp))
    .update(stringToSign)
    .digest("hex");
  headers.Authorization =
    `AWS4-HMAC-SHA256 Credential=${STORAGE_ACCESS_KEY_ID}/${dateStamp}/${STORAGE_REGION}/s3/aws4_request, ` +
    `SignedHeaders=${signedHeaders.join(";")}, Signature=${signature}`;
  return headers;
}

function s3Request(method, key, body, contentType) {
  return new Promise((resolve, reject) => {
    const url = s3Url(key);
    const payload = body ? Buffer.from(body) : Buffer.alloc(0);
    const headers = s3SignedHeaders(method, key, payload, contentType);
    headers["content-length"] = String(payload.length);
    const transport = url.protocol === "http:" ? http : https;
    const req = transport.request(url, { method, headers }, (res) => {
      let raw = "";
      res.setEncoding("utf8");
      res.on("data", (chunk) => {
        raw += chunk;
        if (raw.length > 4 * 1024 * 1024) {
          req.destroy(new Error("Object storage response too large"));
        }
      });
      res.on("end", () => resolve({ status: res.statusCode || 0, body: raw }));
    });
    req.on("error", reject);
    if (payload.length > 0) {
      req.write(payload);
    }
    req.end();
  });
}

function safeAlbumName(name) {
  return String(name || "album")
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80) || "album";
}

function isValidAlbumId(id) {
  return typeof id === "string" && /^[A-Za-z0-9_-]{4,80}$/.test(id);
}

function isValidAlbumKey(scope, key) {
  return typeof key === "string" && key.startsWith(`albums/${scope}/`)
    && !key.includes("..") && key.length <= 512;
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
      features: ["admin_clients", "admin_blocks", "admin_unblock", "admin_signals", "signal_logs", "flush_logs", "socket_dedupe", "chat_retry_dedupe", "album_cloud_b2", "album_chat_scoped"]
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

  // Album cloud status. The app uses this to show "Local only" instead of failing silently
  // when object storage has not been configured on the server yet.
  if (url.pathname === "/api/cloud/status") {
    if (req.method !== "GET") {
      sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
      return;
    }
    sendJson(res, 200, {
      ok: true,
      enabled: CLOUD_CONFIGURED,
      provider: storageProviderName(),
      maxUploadBytes: Number.isFinite(MAX_ALBUM_UPLOAD_BYTES) ? MAX_ALBUM_UPLOAD_BYTES : 52428800
    });
    return;
  }

  // Short-lived signed PUT URL for one album photo/video. The APK uploads the bytes
  // directly to object storage, so media never passes through the relay process.
  if (url.pathname === "/api/album/upload-url") {
    if (req.method !== "POST") {
      sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
      return;
    }
    if (!CLOUD_CONFIGURED) {
      sendJson(res, 503, { ok: false, error: "Album cloud storage is not configured on the server" });
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
    const scope = chatAlbumScope(userId, peerId);
    const albumId = isValidAlbumId(body.albumId) ? body.albumId : "";
    const mime = typeof body.mime === "string" ? body.mime.toLowerCase() : "";
    const size = Number(body.size);
    const name = typeof body.name === "string" ? body.name : "media";
    if (!scope || !albumId || !(mime.startsWith("image/") || mime.startsWith("video/"))
        || !Number.isFinite(size) || size <= 0 || size > MAX_ALBUM_UPLOAD_BYTES) {
      sendJson(res, 400, { ok: false, error: "Invalid album upload request" });
      return;
    }
    const timestamp = Date.now();
    const key = `albums/${scope}/${albumId}/${timestamp}-${crypto.randomUUID()}-${safeAlbumName(name)}`;
    // Sign only the host header: the app then sends no Content-Type, which keeps the
    // presigned PUT compatible with Backblaze B2 and R2.
    const uploadUrl = s3PresignUrl("PUT", key, 900);
    log("album_upload_url", { userId, peerId, scope, albumId, size, key });
    sendJson(res, 200, { ok: true, key, uploadUrl, mime, expiresIn: 900 });
    return;
  }

  // Short-lived signed GET URL for viewing/downloading an album item.
  if (url.pathname === "/api/album/download-url") {
    if (req.method !== "POST") {
      sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
      return;
    }
    if (!CLOUD_CONFIGURED) {
      sendJson(res, 503, { ok: false, error: "Album cloud storage is not configured on the server" });
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
    const scope = chatAlbumScope(userId, peerId);
    const key = body.key;
    if (!scope || !isValidAlbumKey(scope, key)) {
      sendJson(res, 400, { ok: false, error: "Invalid album key for this chat" });
      return;
    }
    sendJson(res, 200, { ok: true, key, downloadUrl: s3PresignUrl("GET", key, 900), expiresIn: 900 });
    return;
  }

  // Cloud album manifest: album names + item metadata. Media bytes stay in storage objects.
  if (url.pathname === "/api/album/manifest") {
    if (!CLOUD_CONFIGURED) {
      sendJson(res, 503, { ok: false, error: "Album cloud storage is not configured on the server" });
      return;
    }
    if (req.method === "GET") {
      const userId = (url.searchParams.get("userId") || "").trim().toLowerCase();
      const peerId = (url.searchParams.get("peerId") || "").trim().toLowerCase();
      const scope = chatAlbumScope(userId, peerId);
      if (!scope) {
        sendJson(res, 400, { ok: false, error: "Invalid chat participants" });
        return;
      }
      const key = `albums/${scope}/manifest.json`;
      try {
        const result = await s3Request("GET", key, null, "application/json");
        if (result.status === 404) {
          sendJson(res, 200, { ok: true, userId, peerId, scope, manifest: null });
        } else if (result.status >= 200 && result.status < 300) {
          sendJson(res, 200, { ok: true, userId, peerId, scope,
            manifest: JSON.parse(result.body || "{}") });
        } else {
          sendJson(res, 502, { ok: false, error: `Storage read failed (${result.status})` });
        }
      } catch (error) {
        sendJson(res, 502, { ok: false, error: error.message });
      }
      return;
    }
    if (req.method === "POST") {
      let body;
      try {
        body = await readJsonBody(req, MAX_ALBUM_MANIFEST_BYTES);
      } catch (error) {
        sendJson(res, 400, { ok: false, error: error.message });
        return;
      }
      const userId = isValidUserId(body.userId) ? body.userId.trim().toLowerCase() : "";
      const peerId = isValidUserId(body.peerId) ? body.peerId.trim().toLowerCase() : "";
      const scope = chatAlbumScope(userId, peerId);
      if (!scope) {
        sendJson(res, 400, { ok: false, error: "Invalid chat participants" });
        return;
      }
      if (!body.manifest || typeof body.manifest !== "object") {
        sendJson(res, 400, { ok: false, error: "Missing manifest" });
        return;
      }
      const serialized = JSON.stringify(body.manifest);
      if (Buffer.byteLength(serialized) > MAX_ALBUM_MANIFEST_BYTES) {
        sendJson(res, 413, { ok: false, error: "Album manifest is too large" });
        return;
      }
      const key = `albums/${scope}/manifest.json`;
      try {
        const result = await s3Request("PUT", key, serialized, "application/json");
        if (result.status >= 200 && result.status < 300) {
          sendJson(res, 200, { ok: true, userId, peerId, scope, updatedAt: Date.now() });
        } else {
          sendJson(res, 502, { ok: false, error: `Storage write failed (${result.status})` });
        }
      } catch (error) {
        sendJson(res, 502, { ok: false, error: error.message });
      }
      return;
    }
    sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
    return;
  }

  // Remove an album item from storage when it is removed from every local album.
  if (url.pathname === "/api/album/delete") {
    if (req.method !== "POST") {
      sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
      return;
    }
    if (!CLOUD_CONFIGURED) {
      sendJson(res, 503, { ok: false, error: "Album cloud storage is not configured on the server" });
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
    const scope = chatAlbumScope(userId, peerId);
    const key = body.key;
    if (!scope || !isValidAlbumKey(scope, key)) {
      sendJson(res, 400, { ok: false, error: "Invalid album key for this chat" });
      return;
    }
    try {
      const result = await s3Request("DELETE", key, null, "application/octet-stream");
      if (result.status >= 200 && result.status < 300 || result.status === 404) {
        sendJson(res, 200, { ok: true, key });
      } else {
        sendJson(res, 502, { ok: false, error: `Storage delete failed (${result.status})` });
      }
    } catch (error) {
      sendJson(res, 502, { ok: false, error: error.message });
    }
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

function queueKey(payload) {
  if (!payload || !payload.messageId) return null;
  return `${payload.from}|${payload.to}|${payload.messageId}`;
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

  // Temporary RAM buffer only while the recipient is offline. A sender retries chats until
  // it gets an end-to-end delivery receipt, so replace an existing queued copy by messageId
  // instead of letting retries fill the buffer and push out other conversations.
  if (!delivered) {
    if (!pendingQueue.has(toId)) {
      pendingQueue.set(toId, []);
    }
    const queue = pendingQueue.get(toId);
    let parsed = null;
    try {
      parsed = JSON.parse(rawPayload);
    } catch (_) {
      // Keep unparseable payloads as-is; they have no dedupe key.
    }
    const key = queueKey(parsed);
    if (key) {
      for (let i = queue.length - 1; i >= 0; i--) {
        try {
          if (queueKey(JSON.parse(queue[i])) === key) {
            queue.splice(i, 1);
            break;
          }
        } catch (_) {
          // Ignore corrupt entries while scanning.
        }
      }
    }
    queue.push(rawPayload);
    if (queue.length > MAX_PENDING_MESSAGES) {
      queue.splice(0, queue.length - MAX_PENDING_MESSAGES);
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
    (ADMIN_TOKEN ? " (admin endpoint enabled)" : " (admin endpoint disabled: set ADMIN_TOKEN)") +
    (CLOUD_CONFIGURED
      ? ` (album cloud enabled: ${storageProviderName()})`
      : " (album cloud disabled: set B2_KEY_ID, B2_APPLICATION_KEY, B2_BUCKET, B2_ENDPOINT)")
  );
});
