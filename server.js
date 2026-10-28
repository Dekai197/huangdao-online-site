'use strict';
// 荒岛突围 relay server: serves the game page and forwards each player's presence to the others in the same room.
// It runs no game logic. Every message is size-limited and rate-limited, and room names / field names are validated.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const PAGE = path.join(__dirname, 'public', 'index.html');
const MAX_MSG = 8 * 1024;          // bytes per WebSocket message
const MAX_PRESENCE = 6 * 1024;     // bytes of one player's merged state in one room
const MAX_CLIENTS = 400;
const MAX_ROOM_PLAYERS = 16;
const MAX_ROOMS_PER_CLIENT = 4;    // lobby + a few game rooms
const RATE_PER_SEC = 40, RATE_BURST = 80;
const ROOM_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const KEY_RE = /^[A-Za-z_][A-Za-z0-9_]{0,31}$/;
const BAD_KEYS = new Set(['__proto__', 'constructor', 'prototype']);

let page = null, three = null;
function loadPage() {
  try { page = fs.readFileSync(PAGE); } catch (e) { console.error('cannot read', PAGE, e.message); }
  try { three = fs.readFileSync(require.resolve('three/build/three.min.js')); } catch (e) { console.error('three.js not installed; the page will fall back to a CDN or 2D'); }
}
loadPage();

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
  // the 3D view's library is served from node_modules, so the site does not depend on an outside CDN
  if (url === '/vendor/three.min.js' && (req.method === 'GET' || req.method === 'HEAD')) {
    if (!three) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8', 'cache-control': 'public, max-age=86400', 'x-content-type-options': 'nosniff' });
    res.end(req.method === 'HEAD' ? undefined : three);
    return;
  }
  if ((url === '/' || url === '/index.html') && (req.method === 'GET' || req.method === 'HEAD')) {
    if (!page) { res.writeHead(500); res.end('page missing'); return; }
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache', 'x-content-type-options': 'nosniff' });
    res.end(req.method === 'HEAD' ? undefined : page);
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain' }); res.end('not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MSG });
const rooms = new Map();    // room name -> Map(peer id -> client)
const clients = new Map();  // peer id -> client

function send(c, obj) { if (c.ws.readyState === 1) c.ws.send(JSON.stringify(obj)); }
function broadcast(room, except, obj) {
  const r = rooms.get(room); if (!r) return;
  const msg = JSON.stringify(obj);
  for (const o of r.values()) if (o !== except && o.ws.readyState === 1) o.ws.send(msg);
}
function join(c, name) {
  if (c.rooms.has(name)) { sendSnap(c, name); return; }
  if (c.rooms.size >= MAX_ROOMS_PER_CLIENT) { send(c, { t: 'err', room: name, code: 'too_many_rooms' }); return; }
  let r = rooms.get(name);
  if (!r) { r = new Map(); rooms.set(name, r); }
  if (name !== '' && r.size >= MAX_ROOM_PLAYERS) { send(c, { t: 'err', room: name, code: 'room_full' }); return; }
  c.rooms.set(name, Object.create(null));
  r.set(c.peer, c);
  sendSnap(c, name);
}
function sendSnap(c, name) {
  const r = rooms.get(name); if (!r) return;
  const peers = [];
  for (const o of r.values()) if (o !== c) peers.push({ peer: o.peer, presence: o.rooms.get(name) });
  send(c, { t: 'snap', room: name, peers });
}
function leave(c, name) {
  if (!c.rooms.has(name)) return;
  c.rooms.delete(name);
  const r = rooms.get(name); if (!r) return;
  r.delete(c.peer);
  broadcast(name, null, { t: 'bye', room: name, peer: c.peer });
  if (!r.size) rooms.delete(name);
}
function presence(c, name, patch) {
  const cur = c.rooms.get(name);
  if (!cur || !patch || typeof patch !== 'object' || Array.isArray(patch)) return;
  const keys = Object.keys(patch);
  if (keys.length > 40) return;
  for (const k of keys) if (!KEY_RE.test(k) || BAD_KEYS.has(k)) return;
  const next = Object.assign(Object.create(null), cur);
  for (const k of keys) { if (patch[k] === null) delete next[k]; else next[k] = patch[k]; }
  if (Buffer.byteLength(JSON.stringify(next)) > MAX_PRESENCE) return;
  c.rooms.set(name, next);
  broadcast(name, c, { t: 'p', room: name, peer: c.peer, patch });
}

wss.on('connection', (ws, req) => {
  if (clients.size >= MAX_CLIENTS) { ws.close(1013, 'server busy'); return; }
  // a page keeps its id across reconnects, so a dropped connection does not turn the player into someone new
  const q = new URLSearchParams((req.url || '').split('?')[1] || '');
  let peer = q.get('id');
  if (!/^[0-9a-f]{16}$/.test(peer || '')) peer = crypto.randomBytes(8).toString('hex');
  const old = clients.get(peer);
  let carried = null;
  if (old) { carried = old.rooms; old.rooms = new Map(); clients.delete(peer); try { old.ws.terminate(); } catch (_) {} }
  const c = { ws, peer, rooms: new Map(), tokens: RATE_BURST, last: Date.now(), alive: true, strikes: 0 };
  clients.set(peer, c);
  if (carried) for (const [name, st] of carried) { const r = rooms.get(name); if (r) { r.set(peer, c); c.rooms.set(name, st); } }
  send(c, { t: 'hello', peer });
  join(c, '');

  ws.on('pong', () => { c.alive = true; });
  ws.on('message', (data, isBinary) => {
    const now = Date.now();
    c.tokens = Math.min(RATE_BURST, c.tokens + (now - c.last) / 1000 * RATE_PER_SEC); c.last = now;
    if (c.tokens < 1) { if (++c.strikes > 200) ws.close(1008, 'too many messages'); return; }
    c.tokens -= 1;
    if (isBinary) return;
    let m; try { m = JSON.parse(data.toString()); } catch (_) { return; }
    if (!m || typeof m !== 'object') return;
    const room = typeof m.room === 'string' ? m.room : null;
    if (room === null || (room !== '' && !ROOM_RE.test(room))) return;
    if (m.t === 'join' && room !== '') join(c, room);
    else if (m.t === 'leave' && room !== '') leave(c, room);
    else if (m.t === 'p') presence(c, room, m.patch);
  });
  ws.on('close', () => {
    if (clients.get(peer) !== c) return; // replaced by a reconnect
    clients.delete(peer);
    for (const name of [...c.rooms.keys()]) leave(c, name);
  });
  ws.on('error', () => {});
});

// drop connections that stopped answering (phone locked, network gone)
setInterval(() => {
  for (const c of clients.values()) {
    if (!c.alive) { try { c.ws.terminate(); } catch (_) {} continue; }
    c.alive = false; try { c.ws.ping(); } catch (_) {}
  }
}, 30000).unref();

process.on('SIGHUP', loadPage);
server.listen(PORT, () => console.log(`荒岛突围 server on http://localhost:${PORT}`));
