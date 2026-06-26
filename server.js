// server.js — сигналинг-сервер для VoiceRoom (WebRTC mesh)
// Запуск:
//   npm install ws
//   node server.js
// По умолчанию слушает порт из переменной PORT, либо 3000.

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX_PATH = path.join(__dirname, 'index.html');

// ── статика: раздаём index.html на любой GET-запрос (кроме /ws) ──
const server = http.createServer((req, res) => {
  if (req.url.startsWith('/ws')) return; // ws-сервер обработает это отдельно
  fs.readFile(INDEX_PATH, (err, data) => {
    if (err) {
      res.writeHead(500);
      res.end('index.html not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, path: '/ws' });

// room -> Map(id -> { ws, name })
const rooms = new Map();

function getRoom(room) {
  if (!rooms.has(room)) rooms.set(room, new Map());
  return rooms.get(room);
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch {}
  }
}

function broadcast(room, obj, exceptId) {
  const members = getRoom(room);
  for (const [id, client] of members) {
    if (id !== exceptId) send(client.ws, obj);
  }
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const room = url.searchParams.get('room') || 'default';
  const name = url.searchParams.get('name') || 'Гость';
  const id = url.searchParams.get('id');

  if (!id) {
    ws.close();
    return;
  }

  const members = getRoom(room);
  members.set(id, { ws, name });

  // Сообщаем новому участнику, кто уже в комнате
  const peerList = [...members.entries()]
    .filter(([pid]) => pid !== id)
    .map(([pid, c]) => ({ id: pid, name: c.name }));

  send(ws, { type: 'room-info', myId: id, peers: peerList });

  // Сообщаем остальным, что кто-то присоединился
  broadcast(room, { type: 'user-joined', from: id, name }, id);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'offer':
      case 'answer':
      case 'ice': {
        const target = members.get(msg.to);
        if (target) send(target.ws, { ...msg });
        break;
      }
      case 'meta': {
        // транслируем состояние (микрофон/говорит) всем остальным в комнате
        broadcast(room, { ...msg }, id);
        break;
      }
      default:
        break;
    }
  });

  ws.on('close', () => {
    members.delete(id);
    broadcast(room, { type: 'user-left', from: id }, id);
    if (members.size === 0) rooms.delete(room);
  });

  ws.on('error', () => {
    try { ws.close(); } catch {}
  });
});

server.listen(PORT, () => {
  console.log(`VoiceRoom сервер запущен: http://localhost:${PORT}`);
});
