// Run: npm run example (Node >=22). Uses a real loopback TCP connection.
import net from 'node:net';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { create } from '@bufbuild/protobuf';
import { OpenSchema, Session, runTransport, nodeWritableSink } from '../dist/index.js';

const utf8 = new TextEncoder();
const decoder = new TextDecoder();
let responderDone;
const server = net.createServer((socket) => {
  const session = new Session('responder');
  responderDone = runTransport(session, socket, nodeWritableSink(socket), async (event, { send }) => {
    if (event.type === 'open') {
      assert.equal(event.open.profile, 'bytes.v1');
      assert.equal(event.open.route, 'echo'); // Host authorization/routing belongs here.
      await send(session.accept(event.id));
    } else if (event.type === 'data') {
      assert.equal(decoder.decode(event.bytes), '你好 TMP');
      assert.equal(event.fin, true);
      await send(session.consume(event.id, event.bytes.length));
      await send(session.send(event.id, utf8.encode('ok'), true));
    } else if (event.type === 'goaway') {
      await send(session.goaway());
      socket.end();
    } else if (event.type === 'reset') throw new Error(`stream failed: ${event.reason}`);
  }).catch((error) => { socket.destroy(); throw error; });
  // The main flow awaits this promise after connecting.
  responderDone.catch(() => {});
});
server.listen(0, '127.0.0.1');
await once(server, 'listening');
const socket = net.connect(server.address().port, '127.0.0.1');
try {
  await once(socket, 'connect');
  const session = new Session('initiator');
  await runTransport(session, socket, nodeWritableSink(socket), async (event, { send }) => {
    if (event.type === 'ready') {
      const opened = session.open(create(OpenSchema, { profile: 'bytes.v1', route: 'echo' }));
      await send(opened.packet);
    } else if (event.type === 'accepted') {
      await send(session.send(event.id, utf8.encode('你好 TMP'), true));
    } else if (event.type === 'data') {
      assert.equal(decoder.decode(event.bytes), 'ok');
      assert.equal(event.fin, true);
      await send(session.consume(event.id, event.bytes.length));
      await send(session.goaway());
    } else if (event.type === 'goaway') socket.end();
    else if (event.type === 'reset') throw new Error(`stream failed: ${event.reason}`);
  });
  await responderDone;
  console.log('TCP echo: 你好 TMP -> ok (FIN, consume and reciprocal GOAWAY completed)');
} finally {
  socket.destroy();
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
}
