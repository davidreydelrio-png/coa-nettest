// Call of API - network connectivity test server. Zero dependencies (Node >= 18).
// Serves the test page, an HTTP ping, a Server-Sent Events stream and a WebSocket echo at /ws.
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000;
const PAGE = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const sockets = new Set();

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/' || url.pathname === '/index.html') { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' }); return res.end(PAGE); }
  if (url.pathname === '/api/ping') { res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }); return res.end(JSON.stringify({ ok: true, t: Date.now(), clients: sockets.size })); }
  if (url.pathname === '/api/echo' && req.method === 'POST') { let b = ''; req.on('data', d => { b += d; if (b.length > 1e5) req.destroy(); }); req.on('end', () => { res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(b || '{}'); }); return; }
  if (url.pathname === '/api/sse') {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store', connection: 'keep-alive', 'x-accel-buffering': 'no' });
    let n = 0; const pad = 'x'.repeat(400);
    const iv = setInterval(() => { n++; res.write('data: ' + JSON.stringify({ n, t: Date.now(), pad }) + '\n\n'); if (n >= 60) { clearInterval(iv); res.end(); } }, 50);
    req.on('close', () => clearInterval(iv)); return;
  }
  res.writeHead(404); res.end('not found');
});

// ---- minimal RFC 6455 WebSocket (text frames, ping/pong, close) ----
function wsSend(sock, str) {
  const data = Buffer.from(str); let head;
  if (data.length < 126) head = Buffer.from([0x81, data.length]);
  else if (data.length < 65536) { head = Buffer.alloc(4); head[0] = 0x81; head[1] = 126; head.writeUInt16BE(data.length, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x81; head[1] = 127; head.writeBigUInt64BE(BigInt(data.length), 2); }
  if (!sock.destroyed) sock.write(Buffer.concat([head, data]));
}
function wsControl(sock, op, payload) { if (!sock.destroyed) sock.write(Buffer.concat([Buffer.from([0x80 | op, payload.length]), payload])); }

server.on('upgrade', (req, sock) => {
  if (new URL(req.url, 'http://x').pathname !== '/ws' || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const key = req.headers['sec-websocket-key'];
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true); sockets.add(sock);
  let buf = Buffer.alloc(0), burst = null;
  const onMsg = txt => {
    let m; try { m = JSON.parse(txt); } catch (e) { return; }
    if (m.type === 'ping') wsSend(sock, JSON.stringify({ type: 'pong', id: m.id, t: m.t, st: Date.now() }));
    else if (m.type === 'burst') {            // simulate game snapshots: ~1.5 KB at 30 Hz for N seconds
      clearInterval(burst); let n = 0; const total = Math.min(600, (m.seconds || 5) * 30), pad = 'y'.repeat(1400);
      burst = setInterval(() => { n++; wsSend(sock, JSON.stringify({ type: 'snap', n, total, st: Date.now(), pad })); if (n >= total) clearInterval(burst); }, 1000 / 30);
    } else if (m.type === 'input') wsSend(sock, JSON.stringify({ type: 'ack', id: m.id }));
  };
  sock.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const op = buf[0] & 0x0f, masked = buf[1] & 0x80; let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > 1e6) { sock.destroy(); return; }
      const need = off + (masked ? 4 : 0) + len; if (buf.length < need) return;
      let payload = buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const mk = buf.subarray(off, off + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= mk[i & 3]; }
      buf = buf.subarray(need);
      if (op === 0x1) onMsg(payload.toString('utf8'));
      else if (op === 0x8) { wsControl(sock, 0x8, Buffer.alloc(0)); sock.end(); }
      else if (op === 0x9) wsControl(sock, 0xA, payload.subarray(0, 125));
    }
  });
  const done = () => { clearInterval(burst); sockets.delete(sock); };
  sock.on('close', done); sock.on('error', done);
});

server.listen(PORT, () => console.log('coa-nettest listening on ' + PORT));
