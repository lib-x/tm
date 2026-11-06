// Application demo: opaque application payloads over one transport-independent TMP link.
// No command execution, OS clipboard access, or desktop capture is performed.
import net from 'node:net';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { createReadStream } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { create } from '@bufbuild/protobuf';
import {
  Session, OpenSchema, ErrorCode, WouldBlock, readPackets, SerialPacketWriter,
  nodeWritableSink, WebSocketByteSource, webSocketSink,
} from '../dist/index.js';

const ROUTES = ['terminal', 'events', 'file', 'clipboard', 'screenshot'];
const CHUNK = 1024;
const DEADLINE_MS = 15000;
const text = (value) => Buffer.from(value, 'utf8');
const fixtures = (file) => [
  text("printf '你好 TMP\\n'\r\n"),
  text('{"type":"started","seq":1}\n{"type":"output","seq":2,"text":"你好"}\n'),
  file ?? Buffer.from(Array.from({ length: 262144 }, (_, i) => i % 251)),
  text('TMP clipboard: 你好 🌍\n'),
  Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=', 'base64'),
];

function check(condition, reason) {
  if (!condition) throw new Error(reason);
}

// This single owner pumps on every packet, including WINDOW packets with no events.
// Waiting for credit inside a DATA callback would prevent WINDOW from being read.
async function drive(role, source, sink, { close, log, file, output, signal }) {
  const session = new Session(role, undefined, ['bytes.v1']);
  const writer = new SerialPacketWriter(sink, { onFailure: close });
  const abort = () => { session.close(); close(); };
  signal?.addEventListener('abort', abort, { once: true });
  const streams = new Map();
  const seen = new Set();
  const payloads = fixtures(file);
  const returnedFile = output ? [] : undefined; // Optional file roundtrip, capped by CLI input limit.
  let completed = 0;
  let sentGoAway = false;
  let gotGoAway = false;
  const send = (packet) => writer.write(packet);

  async function pump() {
    let progress;
    do {
      progress = false;
      for (const [id, state] of streams) {
        if (role === 'initiator') {
          if (!state.accepted || state.sentFin) continue;
          const end = Math.min(state.sent + CHUNK, state.payload.length);
          const bytes = state.payload.subarray(state.sent, end);
          let packet;
          try { packet = session.send(id, bytes, end === state.payload.length); }
          catch (error) { if (error instanceof WouldBlock) continue; throw error; }
          await send(packet);
          state.sent = end;
          state.sentFin = end === state.payload.length;
        } else {
          const chunk = state.queue[0];
          if (!chunk) continue;
          const end = Math.min(chunk.offset + Math.min(CHUNK, session.peerSettings.maxData), chunk.bytes.length);
          const bytes = chunk.bytes.subarray(chunk.offset, end);
          const fin = chunk.fin && end === chunk.bytes.length;
          let packet;
          try { packet = session.send(id, bytes, fin); }
          catch (error) { if (error instanceof WouldBlock) continue; throw error; }
          await send(packet);
          // Return credit only once the echo has been handed to the carrier.
          // Retaining credit bounds queued data to the advertised receive window.
          if (bytes.length) await writer.writeAll(session.consume(id, bytes.length));
          chunk.offset = end;
          if (end === chunk.bytes.length) state.queue.shift();
          if (fin) { streams.delete(id); completed++; }
        }
        progress = true;
      }
    } while (progress);
  }

  try {
    if (role === 'initiator') await send(session.start());
    for await (const packet of readPackets(source)) {
      const result = session.receive(packet);
      await writer.writeAll(result.outbound);
      for (const event of result.events) {
        if (event.type === 'ready' && role === 'initiator') {
          for (const [index, route] of ROUTES.entries()) {
            const opened = session.open(create(OpenSchema, { profile: 'bytes.v1', route }));
            streams.set(opened.id, { route, payload: payloads[index], sent: 0, received: 0, accepted: false, sentFin: false });
            await send(opened.packet);
          }
        } else if (event.type === 'open') {
          const { profile, route } = event.open;
          if (role !== 'responder' || profile !== 'bytes.v1' || !ROUTES.includes(route) || seen.has(route)) {
            await writer.writeAll(session.reset(event.id, ErrorCode.REFUSED, 'unknown or duplicate demo route'));
            continue;
          }
          // Production hosts authorize the authenticated peer before this accept.
          seen.add(route);
          streams.set(event.id, { route, queue: [] });
          await send(session.accept(event.id));
        } else if (event.type === 'accepted') {
          check(role === 'initiator' && streams.has(event.id), 'unexpected ACK');
          streams.get(event.id).accepted = true;
        } else if (event.type === 'data') {
          const state = streams.get(event.id);
          check(state, 'data on unknown demo stream');
          if (role === 'responder') {
            state.queue.push({ bytes: event.bytes, offset: 0, fin: event.fin });
          } else {
            check(state.received + event.bytes.length <= state.sent, `echo precedes request: ${state.route}`);
            const expected = state.payload.subarray(state.received, state.received + event.bytes.length);
            check(Buffer.from(event.bytes).equals(expected), `echo mismatch: ${state.route}`);
            state.received += event.bytes.length;
            if (returnedFile && state.route === 'file') returnedFile.push(Buffer.from(event.bytes));
            if (event.bytes.length) await writer.writeAll(session.consume(event.id, event.bytes.length));
            if (event.fin) {
              check(state.sentFin && state.received === state.payload.length, `early FIN: ${state.route}`);
              log(`PASS ${state.route} ${state.received} bytes`);
              streams.delete(event.id);
              completed++;
            }
          }
        } else if (event.type === 'reset') {
          throw new Error(`stream ${event.id} reset: ${event.reason}`);
        } else if (event.type === 'goaway') {
          check(event.code === ErrorCode.CANCELLED, `peer GOAWAY error: ${event.reason}`);
          check(completed === ROUTES.length && streams.size === 0, 'GOAWAY before all five streams completed');
          if (!sentGoAway) {
            await writer.writeAll(session.goaway());
            sentGoAway = true;
          }
          gotGoAway = true;
        }
      }
      await pump();
      if (role === 'initiator' && completed === ROUTES.length && !sentGoAway) {
        await writer.writeAll(session.goaway());
        sentGoAway = true;
      }
      if (gotGoAway) break;
    }
    check(gotGoAway, 'carrier EOF before reciprocal GOAWAY');
    await writer.finish();
    if (role === 'initiator') {
      if (output) {
        const data = Buffer.concat(returnedFile);
        // wx avoids accidentally overwriting an existing file.
        await writeFile(output, data, { flag: 'wx' });
        log(`FILE ${output} sha256=${createHash('sha256').update(data).digest('hex')}`);
      }
      log('PASS all 5 streams');
    }
  } finally {
    signal?.removeEventListener('abort', abort);
    session.close();
  }
}

function address(value) {
  const url = new URL(`tcp://${value}`);
  check(url.port && /^\d+$/.test(url.port), 'expected host:port');
  return { host: url.hostname.replace(/^\[|\]$/g, ''), port: Number(url.port) };
}

async function tcp(mode, target, options) {
  const endpoint = address(target);
  let server;
  let socket;
  const timer = setTimeout(() => {
    socket?.destroy(new Error('demo deadline exceeded'));
    if (server && !socket) server.emit('error', new Error('demo deadline exceeded waiting for peer'));
    server?.close();
  }, DEADLINE_MS);
  try {
    if (mode === 'server') {
      server = net.createServer({ allowHalfOpen: true });
      const accepted = once(server, 'connection');
      accepted.catch(() => {}); // Also observed when listen itself fails.
      server.listen(endpoint.port, endpoint.host);
      await once(server, 'listening');
      const bound = server.address();
      console.log(`LISTEN ${net.isIPv6(bound.address) ? `[${bound.address}]` : bound.address}:${bound.port}`);
      [socket] = await accepted;
      server.close();
    } else {
      socket = net.connect({ ...endpoint, allowHalfOpen: true });
      await once(socket, 'connect');
    }
    socket.setNoDelay(true);
    socket.on('error', () => {}); // Async iterator/write callbacks report transport errors.
    await drive(mode === 'server' ? 'responder' : 'initiator', socket, nodeWritableSink(socket), {
      ...options, close: () => socket.destroy(),
    });
    socket.end();
  } finally {
    clearTimeout(timer);
    socket?.destroy();
    server?.close();
  }
}

async function websocket(mode, target, options) {
  // Optional demo-only dependency; the SDK itself does not require ws.
  const { WebSocket, WebSocketServer } = await import('ws');
  const endpoint = address(target);
  let server;
  let ws;
  let rejectDeadline;
  const timeout = new Promise((_, reject) => { rejectDeadline = reject; });
  const timer = setTimeout(() => rejectDeadline(new Error('demo deadline exceeded')), DEADLINE_MS);
  async function run() {
    if (mode === 'ws-server') {
      server = new WebSocketServer({ ...endpoint, maxPayload: 131076, perMessageDeflate: false });
      const connected = once(server, 'connection');
      connected.catch(() => {});
      await once(server, 'listening');
      const bound = server.address();
      console.log(`LISTEN ${net.isIPv6(bound.address) ? `[${bound.address}]` : bound.address}:${bound.port}`);
      [ws] = await connected;
      server.close();
    } else {
      ws = new WebSocket(`ws://${target}`, { maxPayload: 131076, perMessageDeflate: false });
      await once(ws, 'open');
    }
    const source = new WebSocketByteSource({ maxMessageBytes: 131076, maxBufferedBytes: 262144 });
    ws.on('message', (data, binary) => {
      try { source.push(data, binary); }
      catch { ws.terminate(); }
    });
    ws.on('error', (error) => source.end(error));
    ws.on('close', (code) => source.end(code === 1000 ? undefined : new Error(`WS closed: ${code}`)));
    const sink = webSocketSink((bytes) => new Promise((resolve, reject) => {
      ws.send(bytes, { binary: true }, (error) => error ? reject(error) : resolve());
    }), { bufferedAmount: () => ws.bufferedAmount, maxBufferedBytes: 262144 });
    await drive(mode === 'ws-server' ? 'responder' : 'initiator', source, sink, {
      ...options, close: () => ws.terminate(),
    });
    ws.close(1000);
    await once(ws, 'close');
  }
  try { await Promise.race([run(), timeout]); }
  finally { clearTimeout(timer); ws?.terminate(); server?.close(); }
}

async function stdio(mode, options) {
  const controller = new AbortController();
  const close = () => { process.stdin.destroy(); process.stdout.destroy(); };
  const timer = setTimeout(() => {
    // A full stdout pipe can leave a write pending after stdin has closed.
    // Abort the Session and both directions, then terminate this dedicated CLI
    // instead of waiting for an uncancellable stdio write to drain.
    controller.abort();
    console.error('demo deadline exceeded (15s); stdio carrier closed');
    process.exit(1);
  }, DEADLINE_MS);
  try {
    await drive(mode === 'stdio-server' ? 'responder' : 'initiator', process.stdin, nodeWritableSink(process.stdout), {
      ...options, close, signal: controller.signal, log: (message) => console.error(message),
    });
  } finally { clearTimeout(timer); process.stdin.destroy(); }
}

async function ssh(host, command, options) {
  check(host && !host.startsWith('-'), 'ssh-client requires a host');
  // Authentication and host key validation use the user's existing OpenSSH config.
  // The remote command is explicitly supplied by the operator and interpreted by SSH.
  const child = spawn('ssh', ['-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', host, command], {
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const exit = once(child, 'exit');
  exit.catch(() => {}); // The receive path also reports a failed spawn.
  // Observe spawn failures even if the read side never becomes readable.
  child.on('error', (error) => child.stdout.destroy(error));
  const timer = setTimeout(() => {
    child.stdout.destroy(new Error('demo deadline exceeded'));
    child.kill();
  }, DEADLINE_MS);
  try {
    await drive('initiator', child.stdout, nodeWritableSink(child.stdin), { ...options, close: () => child.kill() });
    child.stdin.end();
    const [code, signal] = await exit;
    check(code === 0, `SSH exited ${code ?? signal}`);
  } finally { clearTimeout(timer); child.kill(); }
}

async function readInput(path) {
  const chunks = [];
  let size = 0;
  for await (const chunk of createReadStream(path)) {
    size += chunk.length;
    check(size <= 8 * 1024 * 1024, 'demo file limit is 8 MiB');
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function main() {
  const args = process.argv.slice(2);
  const mode = args.shift();
  check(['server', 'client', 'stdio-server', 'stdio-client', 'ws-server', 'ws-client', 'ssh-client'].includes(mode),
    'usage: node examples/mux.mjs server|client|stdio-server|stdio-client|ws-server|ws-client|ssh-client [host:port|ssh-host] [remote-command] [--file input --output new-file]');
  if (mode === 'ssh-client') check(args[0] && !args[0].startsWith('-'), 'ssh-client requires a host');
  if (mode.startsWith('stdio-')) check(!args[0] || args[0].startsWith('--'), 'stdio mode has no address argument');
  const target = args[0] && !args[0].startsWith('--') ? args.shift() : '127.0.0.1:7000';
  const command = mode === 'ssh-client'
    ? (args[0] && !args[0].startsWith('--') ? args.shift() : 'mux-demo stdio-server') : undefined;
  const options = { log: console.log };
  while (args.length) {
    const flag = args.shift();
    check(['--file', '--output'].includes(flag) && args.length > 0, 'expected --file input or --output new-file');
    check(['client', 'ws-client', 'ssh-client', 'stdio-client'].includes(mode), 'file options require a client');
    const value = args.shift();
    if (flag === '--file') {
      // A bounded in-memory input keeps this demo small; production senders stream from disk.
      options.file = await readInput(value);
    } else options.output = value;
  }
  if (mode.startsWith('stdio-')) await stdio(mode, options);
  else if (mode.startsWith('ws-')) await websocket(mode, target, options);
  else if (mode === 'ssh-client') await ssh(target, command, options);
  else await tcp(mode, target, options);
}

main().catch((error) => { console.error(error.message); process.exitCode = 1; });
