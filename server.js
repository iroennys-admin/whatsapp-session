import express from 'express';
import { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, makeCacheableSignalKeyStore, DisconnectReason } from '@whiskeysockets/baileys';
import qrcode from 'qrcode';
import pino from 'pino';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createServer } from 'http';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const sessions = {};
let sid = 0;

// ── QR session ──
app.post('/api/start', async (req, res) => {
  const id = ++sid;
  sessions[id] = { state: 'starting' };
  startSocket(id).catch(e => { sessions[id].state = 'error'; sessions[id].error = e.message; });
  res.json({ id });
});

// SSE stream for QR + status
app.get('/api/stream/:id', (req, res) => {
  const s = sessions[req.params.id];
  if (!s) return res.status(404).end();
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  const send = (data) => res.write(`data: ${JSON.stringify(data)}\n\n`);
  let lastQr = null;
  const iv = setInterval(() => {
    if (s.qr && s.qr !== lastQr) { lastQr = s.qr; send({ type: 'qr', qr: s.qr }); }
    if (s.state === 'connected') { send({ type: 'connected' }); cleanup(); }
    if (s.state === 'error') { send({ type: 'error', error: s.error }); cleanup(); }
    if (s.state === 'closed') { send({ type: 'error', error: 'Connection closed unexpectedly' }); cleanup(); }
    if (s.state === 'loggedOut') { send({ type: 'error', error: 'Session logged out' }); cleanup(); }
  }, 500);
  const cleanup = () => { clearInterval(iv); res.end(); };
  req.on('close', cleanup);
});

// ── Pairing code ──
app.post('/api/pair', async (req, res) => {
  let num = req.body.phone;
  if (!num) return res.status(400).json({ error: 'phone required' });
  const id = ++sid;
  sessions[id] = { state: 'pairing' };
  try {
    const code = await startPair(id, num.replace(/\D/g, ''));
    sessions[id].pairingCode = code;
    // keep socket alive for connection
    res.json({ id, pairingCode: code });
  } catch (e) {
    sessions[id].state = 'error';
    res.status(500).json({ error: e.message });
  }
});

// ── Download creds ──
app.get('/api/creds/:id', (req, res) => {
  const dir = path.join(__dirname, `session-${req.params.id}`);
  const credsFile = path.join(dir, 'creds.json');
  if (!fs.existsSync(credsFile)) return res.status(404).json({ error: 'not ready yet' });
  res.download(credsFile, 'creds.json');
});

// ── Baileys ──
async function startSocket(id) {
  const authDir = `session-${id}`;
  const { state, saveCreds } = await useMultiFileAuthState(authDir);
  let version;
  try { ({ version } = await fetchLatestBaileysVersion()); } catch { version = [2, 3000, 1033959288]; }
  const logger = pino({ level: 'silent' });

  const sock = makeWASocket({
    version,
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
    printQRInTerminal: false,
    logger,
    syncFullHistory: false,
    markOnlineOnConnect: false,
  });

  sessions[id].sock = sock;
  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (up) => {
    if (up.qr) sessions[id].qr = await qrcode.toDataURL(up.qr);
    if (up.connection === 'open') sessions[id].state = 'connected';
    if (up.connection === 'close') {
      const reason = up.lastDisconnect?.error?.output?.statusCode;
      sessions[id].state = reason === DisconnectReason.loggedOut ? 'loggedOut' : 'closed';
    }
  });
}

async function startPair(id, phone) {
  const { state, saveCreds } = await useMultiFileAuthState(`session-${id}`);
  let version;
  try { ({ version } = await fetchLatestBaileysVersion()); } catch { version = [2, 3000, 1033959288]; }
  const logger = pino({ level: 'silent' });

  // ponytail: mismo defaultQueryTimeoutMs:undefined que GataBot para evitar
  // que el pairing request expire antes de que el websocket termine de conectar
  const sock = makeWASocket({
    version,
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
    printQRInTerminal: false,
    logger,
    syncFullHistory: false,
    markOnlineOnConnect: false,
    defaultQueryTimeoutMs: undefined,
    keepAliveIntervalMs: 55000,
    maxIdleTimeMs: 60000,
  });

  sessions[id].sock = sock;
  sessions[id].state = 'pairing';
  sock.ev.on('creds.update', saveCreds);
  sock.ev.on('connection.update', (up) => {
    if (up.connection === 'open') sessions[id].state = 'connected';
    if (up.connection === 'close') {
      sessions[id].state = up.lastDisconnect?.error?.output?.statusCode === DisconnectReason.loggedOut ? 'loggedOut' : 'closed';
    }
  });

  // Pequeña pausa para que el websocket termine de establecer
  await new Promise(r => setTimeout(r, 500));

  const code = await sock.requestPairingCode(phone);
  return code.match(/.{1,4}/g)?.join('-') || code;
}

createServer(app).listen(PORT, () => console.log(`→ http://localhost:${PORT}`));
