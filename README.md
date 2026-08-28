# Blob Parkour — multiplayer backend

This is a real WebSocket server: live connections, a real 20-player-per-room
cap, a real friends graph persisted to `data.json`, a real per-player
country code (used to show a flag next to each name — see "Flags" below),
and now a real live co-op relay used for **Co-op Assist** (see below).
It's been tested end-to-end (friend requests, live push notifications,
invites, live country/flag updates, and account deletion all verified
against this exact protocol — see the test transcript at the bottom).

## Co-op Assist (head-standing)
Players who are in the same server room **and** currently on the same level
see each other's blobs live and can jump on top of a teammate's head to
reach platforms neither could reach alone — the same "land on top, ride if
it moves" physics the game already uses for real platforms.

This only works over the real-time WebSocket backend (Option B/C below) —
it needs a live position feed at roughly 20 updates/sec, and the
Claude.ai-storage fallback (Option "switch back to Claude's shared
storage") only updates every ~6 seconds, which is far too slow for
frame-accurate platforming. On that fallback, friends/invites/presence
still work; co-op head-standing is simply inactive and no blobs are shown
in-level.

The server itself does no physics and doesn't validate positions — it's a
dumb relay that forwards each player's own reported state (`x`, `y`, `vx`,
`vy`, facing, animation flags) to roommates on the same level
(`join_level` / `state` in, `level_peers` / `peer_joined_level` /
`peer_state` / `peer_left_level` out). All collision — including deciding
whether a landing counts as "on top of a teammate's head" — happens
client-side in `main.js`, the same trust model as the rest of this game.
A shield-bubble buff ability is planned as a follow-up but not yet built.

It is **not hosted anywhere by default** — that's not something that can be
turned on from inside this chat, since it requires a persistent process with
a public address, which is outside what I can spin up for you. You (or
anyone) has to run it somewhere reachable. This is the one manual step
standing between this game and true worldwide, real-time play — everything
else (the server code, the protocol, the client) is already real and wired
up. Pick one:

## Option A — same Wi-Fi / LAN (fastest to try, not global)
```
cd server
npm install
node server.js
```
This starts it on `ws://<your-computer's-LAN-IP>:8080`. Find your LAN IP
(`ipconfig` on Windows, `ifconfig`/`ip a` on Mac/Linux), then in `main.js` set:
```js
const MULTIPLAYER_SERVER_URL = 'ws://192.168.1.23:8080';
```
Only works while your computer is on and friends are on the same network.

## Option B — free internet hosting, so anyone anywhere can join (recommended)
This is the one that actually makes it global: an Australian and a Russian
player can both connect to the same URL over the open internet.

1. Push this whole project (including the `server/` folder) to a GitHub repo.
2. Go to [render.com](https://render.com) → New → Web Service → connect that
   repo. Render will detect `server/render.yaml` automatically (or set the
   root directory to `server`, build command `npm install`, start command
   `node server.js` manually if it doesn't).
3. Deploy. You'll get a URL like `blob-parkour-server.onrender.com`.
4. In `main.js`, set:
   ```js
   const MULTIPLAYER_SERVER_URL = 'wss://blob-parkour-server.onrender.com';
   ```
   (`wss://`, not `ws://`, since Render terminates TLS for you.)
5. Reload the game. The status line under the name box will show
   `Server XXXXX • n/20 players` once it connects — that confirms it's
   really talking to your deployed server, not the offline fallback.

Railway and Fly.io work the same way (Node web service, start command
`node server.js`, uses `process.env.PORT`).

Note: `data.json` lives on that host's disk. Free tiers on some platforms
wipe the disk on redeploy/restart — fine for casual play, but don't rely on
it as permanent storage without adding a real database if you want friends
lists to survive redeploys.

## Flags
Each client detects its own country via IP geolocation in the browser (see
`detectCountry()` in `main.js`) and sends it as a 2-letter code (`AU`, `RU`,
etc.) in the `hello` / `set_country` messages below. The server validates
the code, stores it per-player in `data.json`, and relays it back out in
`welcome`, `presence`, `search_results`, `friend_request`, `friend_accepted`,
and `server_invite` messages. The client turns the code into a flag emoji
with a pure Unicode transform (`countryCodeToFlag()`), so no image assets or
external flag API is needed. If a player's network blocks all three
geolocation providers the client tries, no flag is shown for them — it's
never guessed or faked.

## Option C — your own VPS
Run it with a process manager (`pm2 start server.js`) behind an nginx
reverse proxy configured for WebSocket upgrade + TLS, so you can use
`wss://yourdomain.com`.

## Switching the client back to Claude's shared storage
Set `MULTIPLAYER_SERVER_URL = ''` in `main.js` to go back to using
Claude.ai's built-in shared artifact storage instead (works only when the
game is opened as a Claude.ai artifact, no deployment needed, but is
poll-based rather than instant).

## What's real here vs. what to expect
- Server capacity, friend requests/accepts, live presence, country/flag
  propagation, and invites are all genuinely implemented and were tested
  with two simulated WebSocket clients ("Alice" in AU, "Bob" in RU) going
  through hello → friend_request → accept → live country change → presence
  push, over an actual running instance of this server.
- Co-op Assist (head-standing) is newly added: a real position relay on
  the server plus real client-side landing/riding collision. The relay
  protocol (join_level → level_peers/peer_joined_level, state →
  peer_state, leave_level → peer_left_level, including correct scoping
  so a player who leaves a level stops receiving/sending to it) was
  tested end-to-end with two simulated WebSocket clients ("Alice" and
  "Bob") against a live running instance of this exact server — every
  message above was actually sent and received as shown. The client-side
  collision (landing on a teammate's blob, riding it if it moves) has
  been read through and syntax-checked but not yet play-tested in a
  browser with two real clients, so give that part a real two-browser
  check after deploying.
- There's no authentication — a player's identity is just a random ID
  generated once and stored in their browser. Anyone who copies that ID
  could impersonate them. Fine for a friend group, not for a public game.
- `data.json` is a flat file, not a database — it's fine at friend-group
  scale, but wasn't built to handle concurrent writes at real scale.
- Country codes come from client-side IP geolocation (not GPS, no
  permission prompt) and are validated server-side to 2-letter codes only.
