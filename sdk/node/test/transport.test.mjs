import test from 'node:test';
import assert from 'node:assert/strict';
import { Writable, PassThrough } from 'node:stream';
import { setImmediate as turn } from 'node:timers/promises';
import { SerialPacketWriter, readPackets, nodeWritableSink, writableStreamSink, runTransport,
  WebSocketByteSource, webSocketSink, Session, encode, decode, ProtocolError, StateError } from '../dist/index.js';
import { p, pair, connect, utf8 } from './helpers.mjs';

async function* source(values) { yield* values; }

test('readPackets accepts async iterables, split/coalesced frames and respects consumer backpressure', async () => {
  const frames = [encode(p(1, 'openAck')), encode(p(0, 'ping', { token: 9007199254740993n }))];
  let pulls = 0;
  async function* input() {
    pulls++; yield frames[0].subarray(0, 2);
    pulls++; yield Buffer.concat([frames[0].subarray(2), frames[1]]);
    pulls++; yield frames[0];
  }
  const reader = readPackets(input(), { framesPerTurn: 1 });
  assert.equal((await reader.next()).value.body.case, 'openAck');
  assert.equal(pulls, 2);
  assert.equal((await reader.next()).value.body.value.token, 9007199254740993n);
  assert.equal(pulls, 2);
  assert.equal((await reader.next()).value.body.case, 'openAck');
  assert.equal(pulls, 3);
  assert.equal((await reader.next()).done, true);
});

test('readPackets supports WHATWG ReadableStream and rejects truncated/nonbinary input', async () => {
  const frame = encode(p(1, 'openAck'));
  const input = new ReadableStream({ start(controller) { controller.enqueue(frame.subarray(0, 1)); controller.enqueue(frame.subarray(1)); controller.close(); } });
  const values = [];
  for await (const value of readPackets(input)) values.push(value);
  assert.equal(values[0].body.case, 'openAck');
  await assert.rejects(async () => { for await (const _ of readPackets(source([frame.subarray(0, 5)]))) {} }, /truncated/);
  await assert.rejects(async () => { for await (const _ of readPackets(source(['text']))) {} }, /nonbinary/);
  await assert.rejects(async () => { for await (const _ of readPackets(source([]), { framesPerTurn: 0 })) {} }, RangeError);
});

test('serial writer writes complete frames in invocation order and snapshots input before await', async () => {
  const output = [];
  let inside = 0;
  const writer = new SerialPacketWriter(async (frame) => {
    assert.equal(++inside, 1);
    await turn();
    output.push(decode(frame));
    inside--;
  });
  const first = p(1, 'data', { payload: utf8('owned') });
  const a = writer.write(first);
  first.body.value.payload.fill(0);
  const b = writer.write(p(0, 'ping', { token: 1n }));
  const c = writer.write(p(1, 'openAck'));
  assert.equal(writer.pendingPackets, 3);
  await Promise.all([a, b, c]);
  assert.deepEqual(output.map((v) => v.body.case), ['data', 'ping', 'openAck']);
  assert.deepEqual(output[0].body.value.payload, utf8('owned'));
  assert.equal(writer.pendingBytes, 0);
  await writer.finish();
  await assert.rejects(writer.write(p(1, 'openAck')), /closed/);
});

test('write failure is terminal, queued packets never write and failure observer runs once', async () => {
  const error = new Error('write failed');
  let calls = 0;
  let failures = 0;
  const writer = new SerialPacketWriter(async () => { calls++; await turn(); throw error; }, {
    onFailure: (failure) => { assert.equal(failure, error); failures++; },
  });
  const outcomes = await Promise.allSettled([writer.write(p(1, 'openAck')), writer.write(p(3, 'openAck'))]);
  assert.deepEqual(outcomes.map((r) => r.status), ['rejected', 'rejected']);
  assert.equal(calls, 1);
  assert.equal(failures, 1);
  assert.equal(writer.pendingPackets, 0);
  await assert.rejects(writer.write(p(5, 'openAck')), (e) => e === error);
  await assert.rejects(writer.finish(), (e) => e === error);
});

test('bounded writer rejects queue overflow and poisons already queued writes', async () => {
  const writer = new SerialPacketWriter(async () => assert.fail('poisoned before first write'), { maxQueuedPackets: 1 });
  const outcomes = await Promise.allSettled([writer.write(p(1, 'openAck')), writer.write(p(3, 'openAck'))]);
  assert.equal(outcomes.every((r) => r.status === 'rejected'), true);
  const bytes = new SerialPacketWriter(() => {}, { maxQueuedBytes: 5 });
  await assert.rejects(bytes.write(p(1, 'openAck')), /limit/);
  assert.throws(() => new SerialPacketWriter(() => {}, { maxQueuedPackets: 0 }), RangeError);
});

test('Node Writable sink awaits callbacks; WritableStream sink propagates ready/write failure', async () => {
  const written = [];
  const node = new Writable({ write(chunk, _encoding, done) { setImmediate(() => { written.push(Buffer.from(chunk)); done(); }); } });
  const nodeSink = nodeWritableSink(node);
  await nodeSink(utf8('node'));
  assert.equal(written[0].toString(), 'node');
  const web = new WritableStream({ write(chunk) { written.push(Buffer.from(chunk)); } });
  const writer = web.getWriter();
  await writableStreamSink(writer)(utf8('web'));
  await writer.close();
  assert.equal(written[1].toString(), 'web');
  const bad = new Writable({ write(_chunk, _encoding, done) { done(new Error('node sink failure')); } });
  await assert.rejects(nodeWritableSink(bad)(utf8('data')), /node sink failure/);
  await turn();
  const closed = new PassThrough(); closed.destroy();
  await assert.rejects(nodeWritableSink(closed)(utf8('data')), /closed/);
});

test('runTransport sends receive.outbound before events and notifies EOF failures', async () => {
  const session = new Session('responder');
  const peer = new Session('initiator');
  const order = [];
  const hello = peer.start();
  const open = p(1, 'open', { profile: 'bytes.v1', route: 'echo' });
  await runTransport(session, source([encode(hello), encode(open)]), async (frame) => {
    order.push(decode(frame).body.case);
  }, async (event, context) => {
    order.push(event.type);
    if (event.type === 'open') await context.send(session.accept(event.id));
    if (event.type === 'reset') assert.equal(event.code, 7);
  });
  assert.deepEqual(order, ['welcome', 'ready', 'open', 'openAck', 'reset']);
  assert.equal(session.failed, true);
});

test('runTransport starts initiator and closes on read/framing/write/host failures', async () => {
  for (const mode of ['read', 'framing', 'write', 'host']) {
    const session = new Session('initiator');
    const responder = new Session('responder');
    const welcome = responder.receive(new Session('initiator').start()).outbound[0];
    async function* input() {
      if (mode === 'read') throw new Error('read failed');
      if (mode === 'framing') { yield Uint8Array.of(0); return; }
      yield encode(welcome);
    }
    const sent = [];
    await assert.rejects(runTransport(session, input(), async (frame) => {
      if (mode === 'write') throw new Error('write failed');
      sent.push(decode(frame));
    }, () => { if (mode === 'host') throw new Error('host failed'); }));
    assert.equal(session.failed, true);
    if (mode !== 'write') assert.equal(sent[0].body.case, 'hello');
  }
});

test('write failure during an active stream emits INTERNAL Reset for application', async () => {
  const [a, b] = pair();
  connect(a, b);
  const events = [];
  await assert.rejects(runTransport(a, source([encode(p(0, 'ping', { token: 1n }))]),
    () => { throw new Error('broken output'); }, (event) => events.push(event)), /broken output/);
  assert.deepEqual(events.map((e) => [e.type, e.id, e.code]), [['reset', 1, 7]]);
});

test('WebSocket binary messages concatenate into bytes and may split or combine TMP frames', async () => {
  const source = new WebSocketByteSource();
  const frame = encode(p(0, 'ping', { token: (1n << 63n) + 2n }));
  source.push(frame.subarray(0, 3).buffer.slice(0, 3));
  const rest = frame.subarray(3);
  source.push(new DataView(rest.buffer, rest.byteOffset, rest.byteLength), true);
  source.push(Buffer.concat([frame, frame]), true);
  source.end();
  const got = [];
  for await (const value of readPackets(source)) got.push(value.body.value.token);
  assert.deepEqual(got, [(1n << 63n) + 2n, (1n << 63n) + 2n, (1n << 63n) + 2n]);
  assert.equal(source.bufferedBytes, 0);
});

test('WebSocket message, buffer, message count limits and text rejection fail source', async () => {
  for (const mode of ['text', 'text-buffer', 'message', 'buffer', 'count', 'blob']) {
    let failures = 0;
    const source = new WebSocketByteSource({ maxMessageBytes: 10, maxBufferedBytes: 12,
      maxBufferedMessages: 1, onFailure: () => failures++ });
    if (mode === 'text') assert.throws(() => source.push('hello'), /binary/);
    if (mode === 'text-buffer') assert.throws(() => source.push(Buffer.from('hello'), false), /binary/);
    if (mode === 'message') assert.throws(() => source.push(new Uint8Array(11)), /message limit/);
    if (mode === 'buffer') { source.push(new Uint8Array(10)); assert.throws(() => source.push(new Uint8Array(3)), /buffer limit/); }
    if (mode === 'count') { source.push(new Uint8Array(1)); assert.throws(() => source.push(new Uint8Array(1)), /buffer limit/); }
    if (mode === 'blob') assert.throws(() => source.push(new Blob(['x'])), /binary/);
    assert.equal(failures, 1);
    assert.equal(source.bufferedBytes, 0);
    await assert.rejects(async () => { for await (const _ of source) {} });
    assert.throws(() => source.push(Uint8Array.of(1)), /closed/);
  }
});

test('WebSocket wakeup, one-reader rule, partial EOF and errors propagate', async () => {
  const source = new WebSocketByteSource();
  const iterator = source[Symbol.asyncIterator]();
  const pending = iterator.next();
  source.push(Uint8Array.of(1));
  assert.deepEqual((await pending).value, Uint8Array.of(1));
  const second = source[Symbol.asyncIterator]();
  await assert.rejects(second.next(), /one reader/);
  source.end();
  assert.equal((await iterator.next()).done, true);
  const partial = new WebSocketByteSource();
  partial.push(Uint8Array.of(0)); partial.end();
  await assert.rejects(async () => { for await (const _ of readPackets(partial)) {} }, /truncated/);
  const errored = new WebSocketByteSource();
  const waiting = errored[Symbol.asyncIterator]().next();
  errored.end(new Error('WS failed'));
  await assert.rejects(waiting, /WS failed/);
});

test('WebSocket sink sends binary bytes through serial writer', async () => {
  const frames = [];
  const writer = new SerialPacketWriter(webSocketSink(async (data) => { frames.push(data); }));
  await writer.write(p(1, 'openAck'));
  await writer.finish();
  assert.ok(frames[0] instanceof Uint8Array);
  assert.equal(decode(frames[0]).body.case, 'openAck');
});

test('writeAll batches cannot interleave and default queue can flush all default pending refusals', async () => {
  const ids = [];
  const writer = new SerialPacketWriter(async (frame) => { await turn(); ids.push(decode(frame).streamId); });
  await Promise.all([writer.writeAll([p(1, 'openAck'), p(3, 'openAck')]), writer.write(p(5, 'openAck'))]);
  assert.deepEqual(ids, [1, 3, 5]);
  const batch = new SerialPacketWriter(() => {});
  await batch.writeAll(Array.from({ length: 257 }, (_, i) => p(i * 2 + 1, 'reset')));
  await batch.finish();
});

test('WebSocket send limits reject messages and carrier buffers before sending', async () => {
  let calls = 0;
  const sink = webSocketSink(() => { calls++; }, { maxMessageBytes: 10, maxBufferedBytes: 12, bufferedAmount: () => 8 });
  await sink(new Uint8Array(4));
  assert.equal(calls, 1);
  await assert.rejects(sink(new Uint8Array(5)), /buffer limit/);
  await assert.rejects(sink(new Uint8Array(11)), /message limit/);
  assert.equal(calls, 1);
});

test('external context send failure terminates a blocked receive pump immediately', { timeout: 1500 }, async () => {
  const [session, peer] = pair();
  const id = connect(session, peer);
  const input = new WebSocketByteSource();
  const events = [];
  let expose;
  let failureCalls = 0;
  const ready = new Promise((resolve) => { expose = resolve; });
  const task = runTransport(session, input, () => { throw new Error('external output failed'); }, (event, context) => {
    events.push(event);
    if (event.type === 'pong') expose(context);
  }, { onFailure: () => { failureCalls++; } });
  const rejected = assert.rejects(task, /external output failed/);
  input.push(encode(p(0, 'pong', { token: 1n })));
  try {
    const context = await ready;
    await assert.rejects(context.send(session.send(id, utf8('data'))), /external output failed/);
    assert.equal(session.failed, true);
    assert.equal(failureCalls, 1);
    assert.equal(session.activeStreams, 0);
    assert.deepEqual(events.filter((event) => event.type === 'reset').map((event) => [event.id, event.code]), [[id, 7]]);
    assert.throws(() => session.ping(2n), StateError);
    await rejected;
  } finally { input.end(); }
});
