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
const http = require('http');
const url = require('url');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const DATA_FILE = path.join(__dirname, 'data.json');
const ROOM_CAP = 20;

// ---- Admin grant ----------------------------------------------------
// A single admin slot, tied to one specific in-game name (ADMIN_NAME) AND
// a secret only the real owner knows (ADMIN_SECRET). Both are required —
// the name alone is never enough, because names are visible to everyone
// and would otherwise let anyone claim admin just by typing them. Set
// ADMIN_SECRET as an environment variable on your host (never commit it
// to the repo); see the README for how to send it once from the browser
// console to claim the slot. If ADMIN_SECRET is unset, admin can never be
// granted, which is the safe default for a fresh checkout.
const ADMIN_NAME = (process.env.ADMIN_NAME || 'saw6970').toLowerCase();
const ADMIN_SECRET = process.env.ADMIN_SECRET || null;

// ---- Persistence: a real on-disk friends/players graph ----
function loadData() {
  try { return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
  catch (e) { return { players: {} }; }
}
function saveData() { fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2)); }
let data = loadData();

// Validates an ISO 3166-1 alpha-2 country code (e.g. 'AU', 'RU'). Anything
// else is dropped rather than trusted, since this value came from the client.
function normalizeCountry(cc) {
  return typeof cc === 'string' && /^[A-Za-z]{2}$/.test(cc) ? cc.toUpperCase() : null;
}

function ensurePlayer(id, name, country) {
  if (!data.players[id]) {
    data.players[id] = { name: name || 'Player', friends: [], incomingRequests: [], pendingInvites: [], country: normalizeCountry(country), admin: false, createdAt: Date.now() };
  }
  const p = data.players[id];
  if (name) p.name = name;
  const nc = normalizeCountry(country);
  if (nc) p.country = nc;
  if (!p.friends) p.friends = [];
  if (!p.incomingRequests) p.incomingRequests = [];
  if (!p.pendingInvites) p.pendingInvites = [];
  if (p.country === undefined) p.country = null;
  if (p.admin === undefined) p.admin = false;
  return p;
}

// ---- Name monopoly: a name belongs to exactly one id, first claimed,
// held forever (or until that account is deleted). Rebuilt from the
// persisted players on every boot so a restart can't un-claim names.
// Lookups are case-insensitive so 'Saw6970' and 'saw6970' collide.
const claimedNames = new Map(); // lowercased name -> id
function rebuildNameRegistry() {
  claimedNames.clear();
  for (const [id, p] of Object.entries(data.players)) {
    if (p.name) claimedNames.set(p.name.toLowerCase(), id);
  }
}
// Returns true if `name` is free for `id` to hold (either unclaimed, or
// already claimed by this same id — e.g. re-sending the same name on
// reconnect isn't a conflict).
function nameAvailableFor(name, id) {
  const key = name.toLowerCase();
  const holder = claimedNames.get(key);
  return !holder || holder === id;
}
function claimName(name, id, previousName) {
  if (previousName && previousName.toLowerCase() !== name.toLowerCase()) {
    // Only release the old key if we're still its holder (guards against
    // a stale release after two rapid renames).
    if (claimedNames.get(previousName.toLowerCase()) === id) claimedNames.delete(previousName.toLowerCase());
  }
  claimedNames.set(name.toLowerCase(), id);
}
function releaseName(name, id) {
  if (name && claimedNames.get(name.toLowerCase()) === id) claimedNames.delete(name.toLowerCase());
}
rebuildNameRegistry();

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
// Fully deletes a player: unfriends them from everyone, strips their
// pending incoming requests from other players' lists, releases their
// claimed name back into the pool, removes them from data.json, and (if
// they're currently connected) notifies and disconnects their live
// socket. This is the ONE place that knows how to delete a player —
// both the player's own "delete my account" button (via the
// 'delete_account' message) and the admin panel's delete button call
// this same function, so the two can never drift out of sync with each
// other or leave a player half-deleted.
function deletePlayer(id) {
  const me = data.players[id];
  if (!me) return false;
  me.friends.forEach(fid => {
    const f = data.players[fid];
    if (f) f.friends = f.friends.filter(fid2 => fid2 !== id);
  });
  Object.values(data.players).forEach(p => { p.incomingRequests = (p.incomingRequests || []).filter(r => r.fromId !== id); });
  releaseName(me.name, id);
  delete data.players[id];
  saveData();

  broadcastPresence(id, false);
  notifyLeftLevel(id);
  leaveRoom(id);
  const sock = connections.get(id);
  if (sock) {
    try { sock.send(JSON.stringify({ type: 'account_deleted' })); } catch (e) {}
    try { sock.close(); } catch (e) {}
  }
  connections.delete(id);
  return true;
}

const shortId = () => Math.random().toString(36).slice(2) + Date.now().toString(36);

// ---- Admin panel: view every player, delete any by name ----
// Reachable only over plain HTTP GET/POST on the SAME server/port as the
// game's WebSocket connections (no second service, no second port to
// expose). Every request must supply the exact ADMIN_SECRET as a query
// parameter (?secret=...) or the request is refused before any player
// data is read or returned — matching the same fail-closed rule already
// used for granting admin in-game: if ADMIN_SECRET isn't set on this
// server at all, the panel refuses every request, full stop.
const crypto = require('crypto');
function secretMatches(candidate) {
  if (!ADMIN_SECRET || typeof candidate !== 'string') return false;
  const a = Buffer.from(candidate);
  const b = Buffer.from(ADMIN_SECRET);
  // timingSafeEqual throws if lengths differ, so check that first — but a
  // length mismatch is itself just "no match", not a special case to leak.
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
}

function renderAdminPage(secret) {
  const rows = Object.entries(data.players)
    .sort((a, b) => (b[1].createdAt || 0) - (a[1].createdAt || 0))
    .map(([id, p]) => {
      const online = isOnline(id);
      return '<tr>' +
        '<td>' + escapeHtml(p.name) + (p.admin ? ' <span class="admin-tag">ADMIN</span>' : '') + '</td>' +
        '<td class="muted">' + escapeHtml(id) + '</td>' +
        '<td>' + (online ? '<span class="online">online</span>' : '<span class="muted">offline</span>') + '</td>' +
        '<td>' + (p.friends ? p.friends.length : 0) + '</td>' +
        '<td class="muted">' + (p.createdAt ? new Date(p.createdAt).toLocaleString() : '—') + '</td>' +
        '<td><form method="POST" action="/admin/delete?secret=' + encodeURIComponent(secret) + '" onsubmit="return confirm(\'Permanently delete \\\'' + escapeHtml(p.name).replace(/'/g, "\\'") + '\\\'? This cannot be undone.\');">' +
        '<input type="hidden" name="id" value="' + escapeHtml(id) + '">' +
        '<button type="submit" class="delete-btn">Delete</button></form></td>' +
        '</tr>';
    }).join('\n');

  return '<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Blob Parkour — Admin</title>' +
    '<style>' +
    'body{font-family:system-ui,sans-serif;background:#051620;color:#e2e8f0;padding:24px;max-width:900px;margin:0 auto;}' +
    'h1{color:#ffcc00;font-size:20px;}' +
    'table{width:100%;border-collapse:collapse;margin-top:16px;}' +
    'th,td{text-align:left;padding:8px 10px;border-bottom:1px solid #1e293b;font-size:14px;}' +
    'th{color:#94a3b8;font-weight:600;font-size:12px;text-transform:uppercase;}' +
    '.muted{color:#64748b;font-size:12px;}' +
    '.online{color:#4ade80;}' +
    '.admin-tag{background:#facc15;color:#051620;font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;margin-left:6px;}' +
    '.delete-btn{background:#3f1d1d;color:#fecaca;border:1px solid #f87171;padding:5px 12px;border-radius:5px;cursor:pointer;font-size:13px;}' +
    '.delete-btn:hover{background:#f87171;color:#1e0a0a;}' +
    '.count{color:#94a3b8;font-size:13px;margin-top:4px;}' +
    '</style></head><body>' +
    '<h1>Blob Parkour — Players</h1>' +
    '<div class="count">' + Object.keys(data.players).length + ' total accounts</div>' +
    '<table><thead><tr><th>Name</th><th>ID</th><th>Status</th><th>Friends</th><th>Created</th><th></th></tr></thead>' +
    '<tbody>' + (rows || '<tr><td colspan="6" class="muted">No players yet.</td></tr>') + '</tbody></table>' +
    '</body></html>';
}

function handleHttpRequest(req, res) {
  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;

  if (pathname === '/admin' && req.method === 'GET') {
    if (!secretMatches(parsed.query.secret)) {
      res.writeHead(403, { 'Content-Type': 'text/plain' });
      res.end('Forbidden');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(renderAdminPage(parsed.query.secret));
    return;
  }

  if (pathname === '/admin/delete' && req.method === 'POST') {
    if (!secretMatches(parsed.query.secret)) {
      res.writeHead(403, { 'Content-Type': 'text/plain' });
      res.end('Forbidden');
      return;
    }
    let body = '';
    req.on('data', chunk => {
      body += chunk;
      if (body.length > 10000) req.destroy(); // guard against an absurdly large body
    });
    req.on('end', () => {
      const params = new url.URLSearchParams(body);
      const id = params.get('id');
      if (id) deletePlayer(id);
      res.writeHead(302, { Location: '/admin?secret=' + encodeURIComponent(parsed.query.secret) });
      res.end();
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
}

const httpServer = http.createServer(handleHttpRequest);
const wss = new WebSocketServer({ server: httpServer });
httpServer.listen(PORT, () => {
  console.log('Blob Parkour multiplayer server listening on port ' + PORT);
});

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

      // Existing player reconnecting keeps their currently-held name as the
      // fallback if their requested name turns out to be unavailable;
      // a brand-new id falls back to 'Player' (never silently steals a slot).
      const existing = data.players[myId];
      const previousName = existing ? existing.name : null;
      let nameRejected = false;
      let nameToUse = requestedName;
      if (!nameAvailableFor(requestedName, myId)) {
        nameRejected = true;
        nameToUse = previousName || 'Player';
      }

      const p = ensurePlayer(myId, nameToUse, country);
      claimName(nameToUse, myId, previousName);

      // ---- admin grant: name match + secret match + slot still open ----
      // Requires ALL of:
      //   - the requester actually ended up holding the admin name (not
      //     rejected in favor of someone else who has it) — checked via
      //     nameToUse/p.name, the *resolved* name, never the raw requested
      //     string, so someone who doesn't own "saw6970" can't get admin
      //     just by typing it while the real owner is connected elsewhere
      //   - the correct, server-only secret was supplied
      //   - no one holds admin yet (first legitimate claim only, ever)
      if (
        ADMIN_SECRET &&
        !p.admin &&
        !nameRejected &&
        p.name.toLowerCase() === ADMIN_NAME &&
        typeof msg.adminSecret === 'string' &&
        msg.adminSecret === ADMIN_SECRET &&
        !Object.values(data.players).some(pl => pl.admin)
      ) {
        p.admin = true;
      }

      saveData();

      const roomId = assignRoom(myId);
      send(myId, {
        type: 'welcome',
        roomId,
        roomCount: roomCount(roomId),
        name: p.name,
        admin: !!p.admin,
        nameRejected,
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
      const name = String(msg.name || '').trim().slice(0, 16);
      if (name) {
        if (!nameAvailableFor(name, myId)) {
          send(myId, { type: 'name_rejected', name });
          return;
        }
        const me = ensurePlayer(myId);
        const previousName = me.name;
        me.name = name;
        claimName(name, myId, previousName);
        saveData();
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
      else if (data.players[targetId]) { data.players[targetId].pendingInvites.push({ fromName: me.name, fromCountry: me.country || null }); saveData(); }
      return;
    }
    if (msg.type === 'server_invite_by_name') {
      const me = ensurePlayer(myId);
      const targetName = String(msg.name || '').toLowerCase();
      const entry = Object.entries(data.players).find(([id, p]) => id !== myId && p.name.toLowerCase() === targetName);
      if (!entry) { send(myId, { type: 'invite_sent', ok: false, name: msg.name }); return; }
      const [targetId, targetP] = entry;
      if (isOnline(targetId)) send(targetId, { type: 'server_invite', fromName: me.name, fromCountry: me.country || null });
      else { targetP.pendingInvites.push({ fromName: me.name, fromCountry: me.country || null }); saveData(); }
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

    // ---- account deletion (self) ----
    if (msg.type === 'delete_account') {
      deletePlayer(myId); // this socket IS connections.get(myId) right now, so deletePlayer's own
                           // notify+close logic handles messaging and closing this exact ws already
      return;
    }

    // ---- admin panel: get all players ----
    if (msg.type === 'admin_get_players') {
      const me = data.players[myId];
      if (!me || !me.admin) {
        send(myId, { type: 'admin_error', message: 'Admin access required' });
        return;
      }
      const players = Object.entries(data.players).map(([id, p]) => ({
        id,
        name: p.name,
        admin: p.admin,
        online: isOnline(id),
        friends: p.friends || [],
        createdAt: p.createdAt
      }));
      send(myId, { type: 'admin_player_list', players });
      return;
    }

    // ---- admin panel: delete player by ID ----
    if (msg.type === 'admin_delete_player') {
      const me = data.players[myId];
      if (!me || !me.admin) {
        send(myId, { type: 'admin_error', message: 'Admin access required' });
        return;
      }
      const targetId = String(msg.id || '');
      if (!targetId) {
        send(myId, { type: 'admin_error', message: 'Invalid player ID' });
        return;
      }
      if (targetId === myId) {
        send(myId, { type: 'admin_error', message: 'Cannot delete your own account' });
        return;
      }
      const deleted = deletePlayer(targetId);
      if (deleted) {
        send(myId, { type: 'admin_delete_success', id: targetId });
      } else {
        send(myId, { type: 'admin_error', message: 'Player not found' });
      }
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
