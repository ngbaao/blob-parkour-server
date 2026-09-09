// Blob Parkour — real multiplayer backend
// ---------------------------------------------------------------------
// This is a genuine WebSocket server: real connections, a real in-memory
// room registry (capacity-checked at 20 players per room), a real
// friends graph persisted to disk in data.json, and a real live co-op
// relay (join_level / state / peer_state / peer_left_level / emote /
// peer_emote below) used for in-level co-op abilities like standing on
// a teammate's head, and for the emote wheel (quick emoji pop-ups).
// Nothing here is mocked — every player, friend, request, invite,
// co-op position update, and emote comes from an actual connected
// client, and the wire protocol below matches exactly what main.js sends and expects
// (see the "SOCIAL / MULTIPLAYER" section near the top of main.js).
//
// The co-op relay is intentionally a dumb broadcaster: it does not run
// physics or validate positions, it just forwards each player's own
// reported state to roommates on the same level. All collision/landing
// logic (and any anti-cheese clamping) happens client-side in main.js,
// the same trust model already used for the rest of this game.
//
// This process does not run itself on the internet. Start it somewhere
// reachable by you and your friends:
//   - Locally, for friends on the same network:  node server.js
//   - On a host like Render, Railway, or Fly.io, for internet play
//   - On your own VPS behind a reverse proxy (nginx + TLS) for wss://
//
// Setup:
//   1. npm install
//   2. node server.js          (defaults to port 8080; override with PORT env var)
//   3. In main.js, set MULTIPLAYER_SERVER_URL to this server's wss:// (or
//      ws:// for local testing) URL.
// ---------------------------------------------------------------------

const { WebSocketServer } = require('ws');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const DATA_FILE = path.join(__dirname, 'data.json');
const ROOM_CAP = 20;

// The one reserved admin name. Case-insensitive: "Saw6970", "SAW6970", etc.
// all refer to this same reserved slot. Whichever persisted player id first
// claims it "owns" it forever after (ownership recorded in data.adminId);
// every other id is permanently blocked from ever taking this name, even
// while the owner is offline.
const ADMIN_NAME = 'saw6970';
const norm = s => String(s || '').trim().toLowerCase();

// ---- Persistence: a real on-disk friends/players graph ----
function loadData() {
  try {
    const d = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    if (!d.players) d.players = {};
    if (d.adminId === undefined) d.adminId = null;
    return d;
  }
  catch (e) { return { players: {}, adminId: null }; }
}
function saveData() { fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2)); }
let data = loadData();

// Validates an ISO 3166-1 alpha-2 country code (e.g. 'AU', 'RU'). Anything
// else is dropped rather than trusted, since this value came from the client.
function normalizeCountry(cc) {
  return typeof cc === 'string' && /^[A-Za-z]{2}$/.test(cc) ? cc.toUpperCase() : null;
}

// True if `name` is already in use by some OTHER persisted player besides
// `excludeId`. Case-insensitive, so "Blob", "blob" and "BLOB" all collide.
function nameTaken(name, excludeId) {
  const n = norm(name);
  if (!n) return false;
  return Object.entries(data.players).some(([id, p]) => id !== excludeId && norm(p.name) === n);
}

// Builds a name that's guaranteed free, based on whatever the player already
// had (or "Player") plus a short numeric suffix, so a collision never just
// silently reuses someone else's name.
function pickFallbackName(id, base) {
  const root = String(base || (data.players[id] && data.players[id].name) || 'Player').trim().slice(0, 12) || 'Player';
  if (!nameTaken(root, id)) return root;
  let n = 2;
  while (nameTaken(root + n, id)) n++;
  return (root + n).slice(0, 16);
}

// Decides what name `id` is actually allowed to end up with when it asks
// for `requestedName`. Enforces two rules:
//   1. Every name is unique (case-insensitive) across all players.
//   2. "saw6970" is reserved: the first id ever to claim it becomes the
//      permanent admin; nobody else can ever take that name afterward.
// Returns { name, isAdmin, rejected } — `rejected` is true when the request
// had to be overridden (name was taken/reserved), so the caller can tell
// the client its requested name didn't stick.
function resolveRequestedName(id, requestedName, fallbackBase) {
  const requested = String(requestedName || '').trim().slice(0, 16);
  const wantsAdminName = norm(requested) === ADMIN_NAME;

  if (wantsAdminName) {
    if (!data.adminId) {
      // Nobody owns the admin name yet — this id claims it permanently.
      data.adminId = id;
      return { name: ADMIN_NAME, isAdmin: true, rejected: false };
    }
    if (data.adminId === id) {
      // The rightful admin, reclaiming their own name.
      return { name: ADMIN_NAME, isAdmin: true, rejected: false };
    }
    // Someone else trying to take/impersonate the admin name — blocked.
    return { name: pickFallbackName(id, fallbackBase), isAdmin: false, rejected: true };
  }

  // If the current admin renames away from "saw6970", they give it up
  // (the name becomes claimable again by whoever asks for it next).
  if (data.adminId === id) data.adminId = null;

  if (requested && nameTaken(requested, id)) {
    return { name: pickFallbackName(id, fallbackBase), isAdmin: false, rejected: true };
  }
  return { name: requested || pickFallbackName(id, fallbackBase), isAdmin: false, rejected: false };
}

function ensurePlayer(id, name, country) {
  if (!data.players[id]) {
    data.players[id] = { name: name || 'Player', friends: [], incomingRequests: [], pendingInvites: [], country: normalizeCountry(country), createdAt: Date.now(), isAdmin: false };
  }
  const p = data.players[id];
  if (name) p.name = name;
  const nc = normalizeCountry(country);
  if (nc) p.country = nc;
  if (!p.friends) p.friends = [];
  if (!p.incomingRequests) p.incomingRequests = [];
  if (!p.pendingInvites) p.pendingInvites = [];
  if (p.country === undefined) p.country = null;
  if (p.isAdmin === undefined) p.isAdmin = (data.adminId === id);
  return p;
}

// ---- Runtime state (rebuilt from live connections, not persisted) ----
const connections = new Map(); // id -> ws
const playerRoom = new Map();  // id -> roomId
const rooms = new Map();       // roomId -> Set<id>
const playerLevel = new Map(); // id -> levelIndex the player is currently playing (co-op sync)

const isOnline = id => connections.has(id);
function send(id, msg) {
  const ws = connections.get(id);
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

// Tell every online friend of `id` that their online state changed.
function broadcastPresence(id, online) {
  const p = data.players[id];
  if (!p) return;
  p.friends.forEach(fid => {
    if (isOnline(fid)) send(fid, { type: 'presence', id, name: p.name, country: p.country || null, online });
  });
}

function assignRoom(id) {
  let chosen = null;
  for (const [rid, members] of rooms.entries()) {
    if (members.size < ROOM_CAP) { chosen = rid; break; }
  }
  if (!chosen) { chosen = 'srv-' + Math.random().toString(36).slice(2, 7); rooms.set(chosen, new Set()); }
  rooms.get(chosen).add(id);
  playerRoom.set(id, chosen);
  return chosen;
}
function leaveRoom(id) {
  const rid = playerRoom.get(id);
  if (rid && rooms.has(rid)) {
    rooms.get(rid).delete(id);
    if (rooms.get(rid).size === 0) rooms.delete(rid);
  }
  playerRoom.delete(id);
}
const roomCount = rid => (rooms.has(rid) ? rooms.get(rid).size : 0);

// Tells co-op peers in the same room+level that this player is gone, and
// forgets their level so a later 'state' from a stale race is dropped.
function notifyLeftLevel(id) {
  if (!playerLevel.has(id)) return;
  playerLevel.delete(id);
  const roomId = playerRoom.get(id);
  if (roomId && rooms.has(roomId)) {
    rooms.get(roomId).forEach(pid => { if (pid !== id) send(pid, { type: 'peer_left_level', id }); });
  }
}
const shortId = () => Math.random().toString(36).slice(2) + Date.now().toString(36);

// A plain http.Server is needed alongside the WebSocket server: `ws`
// only handles the special "Upgrade: websocket" handshake used by real
// game clients. Ordinary HTTP requests — like the periodic GET pings
// from an uptime service (cron-job.org, UptimeRobot, etc.) used to stop
// this free-tier instance from spinning down, or Render's own health
// checks — have nothing to answer them otherwise, and error out instead
// of getting a clean response. This handler just answers any plain HTTP
// request with 200 OK; it doesn't need to do anything else, since real
// gameplay traffic never goes through it.
const http = require('http');
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Blob Parkour multiplayer server is running.\n');
});
const wss = new WebSocketServer({ server: httpServer });
httpServer.listen(PORT);
console.log('Blob Parkour multiplayer server listening on port ' + PORT);

wss.on('connection', ws => {
  let myId = null;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    // ---- hello: register/reconnect, join a room, send initial state ----
    if (msg.type === 'hello') {
      myId = String(msg.id || '').slice(0, 64);
      if (!myId) return;
      const requestedName = String(msg.name || 'Player').slice(0, 16);
      const country = normalizeCountry(msg.country);
      connections.set(myId, ws);

      // Resolve the name through the uniqueness + admin-reservation rules
      // BEFORE persisting anything, so a taken/reserved name never lands
      // in data.players even for a moment.
      const resolved = resolveRequestedName(myId, requestedName, requestedName);
      const p = ensurePlayer(myId, resolved.name, country);
      p.isAdmin = resolved.isAdmin;
      saveData();

      const roomId = assignRoom(myId);
      send(myId, {
        type: 'welcome',
        roomId,
        roomCount: roomCount(roomId),
        name: p.name,
        isAdmin: resolved.isAdmin,
        nameRejected: resolved.rejected,
        friends: p.friends.map(fid => ({ id: fid, name: data.players[fid] ? data.players[fid].name : '(deleted)', country: data.players[fid] ? (data.players[fid].country || null) : null, online: isOnline(fid) })),
        incomingRequests: p.incomingRequests.map(r => ({ id: r.id, fromId: r.fromId, fromName: r.fromName, fromCountry: r.fromCountry || null, ts: r.ts }))
      });

      if (p.pendingInvites.length) {
        p.pendingInvites.forEach(inv => send(myId, { type: 'server_invite', fromName: inv.fromName, fromCountry: inv.fromCountry || null }));
        p.pendingInvites = [];
        saveData();
      }

      broadcastPresence(myId, true);
      return;
    }

    if (!myId) return; // must say hello first

    // ---- live country updates (e.g. geolocation resolves after 'hello' was sent) ----
    if (msg.type === 'set_country') {
      const country = normalizeCountry(msg.country);
      if (country) {
        ensurePlayer(myId).country = country;
        saveData();
        broadcastPresence(myId, true);
      }
      return;
    }

    // ---- name changes ----
    if (msg.type === 'set_name') {
      const requestedName = String(msg.name || '').trim().slice(0, 16);
      if (requestedName) {
        const resolved = resolveRequestedName(myId, requestedName, requestedName);
        const p = ensurePlayer(myId);
        p.name = resolved.name;
        p.isAdmin = resolved.isAdmin;
        saveData();
        send(myId, { type: 'name_set', name: p.name, isAdmin: resolved.isAdmin, rejected: resolved.rejected });
        broadcastPresence(myId, true);
      }
      return;
    }

    // ---- player search ----
    if (msg.type === 'search') {
      const q = String(msg.query || '').toLowerCase();
      const me = ensurePlayer(myId);
      const results = !q ? [] : Object.entries(data.players)
        .filter(([id, p]) => id !== myId && p.name && p.name.toLowerCase().includes(q))
        .slice(0, 8)
        .map(([id, p]) => ({ id, name: p.name, country: p.country || null, online: isOnline(id), isFriend: me.friends.includes(id) }));
      send(myId, { type: 'search_results', results });
      return;
    }

    // ---- friend request send ----
    if (msg.type === 'friend_request') {
      const targetId = String(msg.targetId || '');
      const target = data.players[targetId];
      const me = ensurePlayer(myId);
      if (target && targetId !== myId && !me.friends.includes(targetId)) {
        const already = target.incomingRequests.some(r => r.fromId === myId);
        if (!already) {
          const reqEntry = { id: shortId(), fromId: myId, fromName: me.name, fromCountry: me.country || null, ts: Date.now() };
          target.incomingRequests.push(reqEntry);
          saveData();
          if (isOnline(targetId)) send(targetId, { type: 'friend_request', id: reqEntry.id, fromId: myId, fromName: me.name, fromCountry: me.country || null });
        }
      }
      return;
    }

    // ---- friend request accept/decline ----
    if (msg.type === 'respond_request') {
      const me = ensurePlayer(myId);
      me.incomingRequests = me.incomingRequests.filter(r => r.id !== msg.reqId);
      if (msg.accept && msg.fromId) {
        const fromId = String(msg.fromId);
        if (!me.friends.includes(fromId)) me.friends.push(fromId);
        const them = ensurePlayer(fromId, msg.fromName);
        if (!them.friends.includes(myId)) them.friends.push(myId);
        saveData();
        // Tell the original requester they now have a new friend
        if (isOnline(fromId)) send(fromId, { type: 'friend_accepted', id: myId, name: me.name, country: me.country || null });
        broadcastPresence(myId, true);
      } else {
        saveData();
      }
      return;
    }

    // ---- server invites ----
    if (msg.type === 'server_invite') {
      const me = ensurePlayer(myId);
      const targetId = String(msg.targetId || '');
      if (isOnline(targetId)) send(targetId, { type: 'server_invite', fromName: me.name, fromCountry: me.country || null });
      else if (data.players[targetId]) { data.players[targetId].pendingInvites.push({ fromId: myId, fromName: me.name, fromCountry: me.country || null }); saveData(); }
      return;
    }
    if (msg.type === 'server_invite_by_name') {
      const me = ensurePlayer(myId);
      const targetName = String(msg.name || '').toLowerCase();
      const entry = Object.entries(data.players).find(([id, p]) => id !== myId && p.name.toLowerCase() === targetName);
      if (!entry) { send(myId, { type: 'invite_sent', ok: false, name: msg.name }); return; }
      const [targetId, targetP] = entry;
      if (isOnline(targetId)) send(targetId, { type: 'server_invite', fromName: me.name, fromCountry: me.country || null });
      else { targetP.pendingInvites.push({ fromId: myId, fromName: me.name, fromCountry: me.country || null }); saveData(); }
      send(myId, { type: 'invite_sent', ok: true, name: targetP.name });
      return;
    }

    // ---- CO-OP: live in-level state relay ----------------------------
    // These messages carry no game logic and are trusted only as far as
    // "draw/collide this other blob roughly here" — the server does not
    // simulate physics, it just relays each sender's own reported state
    // to the other players currently in the same room *and* the same
    // level, so head-standing and any future co-op abilities only ever
    // apply between players who are actually looking at the same level.

    // Announce (or update) which level this player is on. Sent once on
    // entering PLAYING and again whenever the level changes (new level,
    // restart, respawn-triggered reload, etc). Also tells the room who
    // else is already on that level, so a late joiner immediately knows
    // who they can co-op with.
    if (msg.type === 'join_level') {
      const levelIndex = Number.isInteger(msg.levelIndex) ? msg.levelIndex : null;
      if (levelIndex === null) return;
      playerLevel.set(myId, levelIndex);
      const roomId = playerRoom.get(myId);
      if (!roomId || !rooms.has(roomId)) return;
      const me = data.players[myId];
      const peers = [];
      rooms.get(roomId).forEach(pid => {
        if (pid === myId) return;
        if (playerLevel.get(pid) !== levelIndex) return;
        const p = data.players[pid];
        peers.push({ id: pid, name: p ? p.name : 'Player', country: p ? (p.country || null) : null });
        // Tell that already-present peer about the new arrival too.
        send(pid, { type: 'peer_joined_level', id: myId, name: me ? me.name : 'Player', country: me ? (me.country || null) : null, levelIndex });
      });
      send(myId, { type: 'level_peers', levelIndex, peers });
      return;
    }

    // A player left PLAYING (back to menu, level change, etc) without
    // closing the socket — stop treating them as co-op-able immediately
    // rather than waiting for a stale timeout.
    if (msg.type === 'leave_level') {
      playerLevel.delete(myId);
      const roomId = playerRoom.get(myId);
      if (roomId && rooms.has(roomId)) {
        rooms.get(roomId).forEach(pid => { if (pid !== myId) send(pid, { type: 'peer_left_level', id: myId }); });
      }
      return;
    }

    // Live per-frame position/animation update. Fired at a client-side
    // throttle (see main.js), never trusted for anything beyond rendering
    // and head-standing collision on the receiving end.
    if (msg.type === 'state') {
      const roomId = playerRoom.get(myId);
      if (!roomId || !rooms.has(roomId)) return;
      const levelIndex = playerLevel.get(myId);
      if (levelIndex === undefined) return;
      const out = {
        type: 'peer_state',
        id: myId,
        x: msg.x, y: msg.y, vx: msg.vx, vy: msg.vy,
        facing: msg.facing, onGround: !!msg.onGround, alive: msg.alive !== false,
        legPhase: msg.legPhase, isSpinning: !!msg.isSpinning, color: msg.color
      };
      rooms.get(roomId).forEach(pid => {
        if (pid === myId) return;
        if (playerLevel.get(pid) !== levelIndex) return;
        send(pid, out);
      });
      return;
    }

    // Emote wheel: a quick, ephemeral emoji pop-up above the sender's
    // blob. Relayed the exact same way as 'state' above — a dumb
    // broadcast to roommates on the same room *and* level, with zero
    // game-logic significance. The server does not validate which emote
    // was picked (that's a small fixed enum defined client-side); it
    // only caps the id to a short string so a malformed/hostile client
    // can't smuggle arbitrary payloads through this channel.
    if (msg.type === 'emote') {
      const roomId = playerRoom.get(myId);
      if (!roomId || !rooms.has(roomId)) return;
      const levelIndex = playerLevel.get(myId);
      if (levelIndex === undefined) return;
      const emoteId = String(msg.emoteId || '').slice(0, 32);
      if (!emoteId) return;
      const out = { type: 'peer_emote', id: myId, emoteId };
      rooms.get(roomId).forEach(pid => {
        if (pid === myId) return;
        if (playerLevel.get(pid) !== levelIndex) return;
        send(pid, out);
      });
      return;
    }

    // ---- account deletion ----
    if (msg.type === 'delete_account') {
      const me = data.players[myId];
      if (me) {
        me.friends.forEach(fid => {
          const f = data.players[fid];
          if (f) f.friends = f.friends.filter(id => id !== myId);
        });
        Object.values(data.players).forEach(p => {
          p.incomingRequests = (p.incomingRequests || []).filter(r => r.fromId !== myId);
          p.pendingInvites = (p.pendingInvites || []).filter(inv => inv.fromId !== myId);
        });
        if (data.adminId === myId) data.adminId = null; // free up "saw6970" again
        broadcastPresence(myId, false);
        delete data.players[myId];
        saveData();
      }
      notifyLeftLevel(myId);
      leaveRoom(myId);
      ws.send(JSON.stringify({ type: 'account_deleted' }));
      connections.delete(myId);
      ws.close();
      return;
    }
  });

  ws.on('close', () => {
    if (myId) {
      broadcastPresence(myId, false);
      notifyLeftLevel(myId);
      leaveRoom(myId);
      connections.delete(myId);
    }
  });
});
