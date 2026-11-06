import test from 'node:test';
import assert from 'node:assert/strict';
import { create } from '@bufbuild/protobuf';
import { Session, DEFAULT_SETTINGS as defaults, SettingsSchema, StateError, ProtocolError, WouldBlock,
  ErrorCode, encode } from '../dist/index.js';
import { pair, connect, p, opening, utf8, wire } from './helpers.mjs';

test('handshake negotiates intersection and snapshots immutable settings/profiles', () => {
  const config = { ...defaults };
  const profiles = ['bytes.v1', 'custom.v1'];
  const a = new Session('initiator', config, profiles);
  config.maxData = 1;
  profiles.push('later.v1');
  const hello = a.start();
  assert.equal(hello.body.value.settings.maxData, defaults.maxData);
  assert.deepEqual(hello.body.value.profiles, ['bytes.v1', 'custom.v1']);
  const b = new Session('responder', defaults, ['other.v1', 'bytes.v1']);
  const result = b.receive(wire(hello));
  assert.deepEqual(result.events, [{ type: 'ready', profiles: ['bytes.v1'] }]);
  assert.equal(b.state, 'ready');
  const welcome = result.outbound[0];
  const ready = a.receive(wire(welcome));
  assert.equal(a.state, 'ready');
  assert.deepEqual(a.negotiatedProfiles, ['bytes.v1']);
  assert.equal(ready.events[0].type, 'ready');
  welcome.body.value.settings.maxData = 1;
  welcome.body.value.profiles.length = 0;
  assert.equal(a.peerSettings.maxData, defaults.maxData);
  assert.throws(() => { a.peerSettings.maxData = 1; }, TypeError);
  assert.throws(() => { a.negotiatedProfiles.push('evil.v1'); }, TypeError);
  assert.throws(() => { a.peerSettings = defaults; }, TypeError);
  assert.throws(() => { a.negotiatedProfiles = []; }, TypeError);
  assert.throws(() => { a.localSettings.maxStreams = 1; }, TypeError);
});

test('role-specific start, pre-handshake operations, empty profile intersection', () => {
  const a = new Session('initiator', defaults, []);
  const b = new Session('responder');
  assert.throws(() => b.start(), StateError);
  assert.throws(() => a.open(opening()), StateError);
  const response = b.receive(a.start());
  assert.throws(() => a.start(), StateError);
  a.receive(response.outbound[0]);
  assert.deepEqual(a.negotiatedProfiles, []);
  assert.throws(() => a.open(opening()), ProtocolError);
  assert.equal(a.failed, false);
});

test('constructor validates all settings, roles and profiles', () => {
  for (const [key, values] of Object.entries({ maxData: [0, 1023, 65537, NaN, 1.1],
    streamWindow: [0, 4095, 16777217], connectionWindow: [65535, 268435457], maxStreams: [0, 4097] })) {
    for (const value of values) assert.throws(() => new Session('initiator', { ...defaults, [key]: value }));
  }
  for (const profiles of [[''], ['Bytes.v1'], ['a', 'a'], ['a'.repeat(65)], Array.from({ length: 33 }, (_, i) => `p${i}`)]) {
    assert.throws(() => new Session('initiator', defaults, profiles));
  }
  assert.throws(() => new Session('bad'));
});

test('invalid handshake sequence and settings fail terminally', () => {
  for (const value of [p(0, 'ping', { token: 1n }), p(0, 'welcome', { version: 1, settings: defaults }),
    p(0, 'hello', { version: 2, settings: defaults }), p(0, 'hello', { version: 1 }),
    p(0, 'hello', { version: 1, settings: { ...defaults, maxData: 0 } }),
    p(0, 'hello', { version: 1, settings: defaults, profiles: ['x', 'x'] })]) {
    const session = new Session('responder');
    assert.throws(() => session.receive(value), ProtocolError);
    assert.equal(session.failed, true);
    assert.throws(() => session.receive(value), StateError);
    assert.throws(() => session.start(), StateError);
  }
  const a = new Session('initiator');
  a.start();
  assert.throws(() => a.receive(p(0, 'welcome', { version: 1, settings: defaults, profiles: ['injected.v1'] })));
  const [c] = pair();
  assert.throws(() => c.receive(p(0, 'hello', { version: 1, settings: defaults })), /duplicate handshake/);
});

test('bidirectional parity, ACK, snapshot ownership and half close', () => {
  const [a, b] = pair();
  const meta = utf8('meta');
  const item = a.open(opening({ metadata: meta }));
  meta.fill(0);
  const event = b.receive(item.packet).events[0];
  item.packet.body.value.metadata.fill(0);
  assert.deepEqual(event.open.metadata, utf8('meta'));
  assert.throws(() => a.send(item.id, utf8('no optimistic')), StateError);
  const ack = b.accept(item.id);
  assert.throws(() => b.accept(item.id), StateError);
  assert.deepEqual(a.receive(ack).events, [{ type: 'accepted', id: 1 }]);
  const input = utf8('你好 TMP');
  const data = a.send(1, input, true);
  input.fill(0);
  const received = b.receive(data).events[0];
  data.body.value.payload.fill(0);
  assert.deepEqual(received.bytes, utf8('你好 TMP'));
  assert.equal(received.fin, true);
  assert.throws(() => a.send(1, utf8('after fin')), StateError);
  a.receive(b.send(1, utf8('ok'), true));
  assert.equal(a.activeStreams, 1);
  assert.equal(b.activeStreams, 1);
  const aWindows = a.consume(1, 2);
  assert.deepEqual(aWindows.map((v) => [v.streamId, v.body.value.credit]), [[1, 2], [0, 2]]);
  assert.equal(a.activeStreams, 0);
  const bWindows = b.consume(1, 10);
  assert.equal(b.activeStreams, 0);
  for (const value of aWindows) b.receive(value);
  for (const value of bWindows) a.receive(value);
  const reverse = b.open(opening());
  assert.equal(reverse.id, 2);
  a.receive(reverse.packet);
  b.receive(a.accept(2));
  assert.equal(a.open(opening()).id, 3);
  assert.equal(b.open(opening()).id, 4);
});

test('local argument errors are atomic; caller can retry valid operations', () => {
  const [a, b] = pair();
  for (const request of [opening({ class: 3 }), opening({ route: '你'.repeat(86) }),
    opening({ route: '\ud800' }), opening({ metadata: new Uint8Array(4097) }), opening({ profile: 'other.v1' })]) {
    assert.throws(() => a.open(request));
    assert.equal(a.activeStreams, 0);
    assert.equal(a.failed, false);
  }
  const id = connect(a, b);
  assert.equal(id, 1);
  assert.throws(() => a.send(id, new Uint8Array()), /empty/);
  assert.throws(() => a.send(id, new Uint8Array(defaults.maxData + 1)));
  assert.throws(() => a.consume(id, 1));
  assert.throws(() => a.consume(id, -1));
  assert.throws(() => a.reset(id, 9));
  assert.throws(() => a.goaway(0, 'x'.repeat(131072)));
  assert.equal(a.draining, false);
  assert.deepEqual(a.consume(id, 0), []);
  assert.doesNotThrow(() => b.receive(a.send(id, utf8('valid'))));
});

test('stream credit blocks atomically and consumption replenishes both windows', () => {
  const tiny = { ...defaults, maxData: 4096, streamWindow: 4096 };
  const [a, b] = pair(tiny, tiny);
  const id = connect(a, b);
  b.receive(a.send(id, new Uint8Array(4096)));
  assert.throws(() => a.send(id, utf8('x'), true), WouldBlock);
  assert.throws(() => b.consume(id, 4097));
  const windows = b.consume(id, 4096);
  for (const value of windows) a.receive(value);
  assert.doesNotThrow(() => b.receive(a.send(id, utf8('x'), true)));
});

test('connection credit is shared across streams and flow violations terminate', () => {
  const tiny = { ...defaults, maxData: 65536, streamWindow: 65536, connectionWindow: 65536 };
  const [a, b] = pair(tiny, tiny);
  const id = connect(a, b);
  const other = connect(a, b);
  b.receive(a.send(id, new Uint8Array(65536)));
  assert.throws(() => a.send(other, utf8('x')), WouldBlock);
  const windows = b.consume(id, 1);
  a.receive(windows[1]); // only connection credit is needed on the other stream
  b.receive(a.send(other, utf8('x')));
  assert.throws(() => b.receive(p(other, 'data', { payload: utf8('x') })), (error) => error.code === ErrorCode.FLOW_CONTROL);
  assert.equal(b.failed, true);
});

test('credit must be positive and cannot exceed initial windows', () => {
  for (const value of [p(0, 'window', { credit: 0 }), p(0, 'window', { credit: 1 }),
    p(1, 'window', { credit: 1 }), p(0, 'window', { credit: 2 ** 32 })]) {
    const [a, b] = pair();
    connect(a, b);
    assert.throws(() => a.receive(value), ProtocolError);
  }
  const [a, b] = pair({ ...defaults, maxData: 1024 });
  connect(a, b);
  assert.throws(() => a.receive(p(1, 'data', { payload: new Uint8Array(1025) })), /max_data/);
  const [c, d] = pair({ ...defaults, streamWindow: 4096 });
  connect(c, d);
  assert.throws(() => c.receive(p(1, 'data', { payload: new Uint8Array(4097) })), /stream receive/);
});

test('RESET returns unread connection credit after RESET, never a dead stream WINDOW', () => {
  const [a, b] = pair();
  const id = connect(a, b);
  b.receive(a.send(id, utf8('abc')));
  const result = b.reset(id, ErrorCode.CANCELLED, 'cancel');
  assert.deepEqual(result.map((v) => [v.streamId, v.body.case]), [[1, 'reset'], [0, 'window']]);
  assert.equal(result[1].body.value.credit, 3);
  assert.equal(b.activeStreams, 0);
  assert.equal(a.receive(result[0]).events[0].type, 'reset');
  a.receive(result[1]);
  assert.equal(a.activeStreams, 0);
  const id2 = connect(a, b);
  a.receive(b.send(id2, utf8('unread')));
  const incoming = a.receive(p(id2, 'reset', { code: 0 }));
  assert.deepEqual(incoming.outbound.map((v) => [v.streamId, v.body.value.credit]), [[0, 6]]);
});

test('historical control is ignored; late DATA refunds connection only; future IDs fail', () => {
  const [a, b] = pair();
  const id = connect(a, b);
  a.reset(id);
  for (const kind of ['openAck', 'reset', 'window']) {
    assert.deepEqual(a.receive(p(id, kind, kind === 'window' ? { credit: 1 } : {})), { outbound: [], events: [] });
  }
  const late = a.receive(p(id, 'data', { payload: utf8('late'), fin: true }));
  assert.deepEqual(late.outbound.map((v) => [v.streamId, v.body.value.credit]), [[0, 4]]);
  assert.deepEqual(late.events, []);
  for (const kind of ['openAck', 'reset', 'window', 'data']) {
    const [s] = pair();
    const value = kind === 'window' ? { credit: 1 } : kind === 'data' ? { payload: utf8('x') } : {};
    assert.throws(() => s.receive(p(3, kind, value)), /future/);
  }
});

test('late historical DATA still checks connection credit before refund', () => {
  const settings = { ...defaults, maxData: 65536, streamWindow: 65536, connectionWindow: 65536 };
  const [a, b] = pair(settings, settings);
  const closed = connect(a, b);
  a.reset(closed);
  const active = connect(a, b);
  a.receive(p(active, 'data', { payload: new Uint8Array(65536) }));
  assert.throws(() => a.receive(p(closed, 'data', { payload: utf8('x') })), /connection receive/);
});

test('pending DATA, duplicate ACK, remote ACK and post-FIN DATA are terminal', () => {
  for (const scenario of ['pending', 'duplicate', 'remote-ack', 'fin']) {
    const [a, b] = pair();
    const opened = a.open(opening());
    b.receive(opened.packet);
    if (scenario === 'pending') assert.throws(() => b.receive(p(1, 'data', { fin: true })));
    else if (scenario === 'remote-ack') assert.throws(() => b.receive(p(1, 'openAck')));
    else {
      const ack = b.accept(1);
      a.receive(ack);
      if (scenario === 'duplicate') assert.throws(() => a.receive(ack));
      else { b.receive(a.send(1, new Uint8Array(), true)); assert.throws(() => b.receive(p(1, 'data', { fin: true }))); }
    }
  }
});

test('incoming limits are directional, count pending/closed unread, refusals advance high water', () => {
  const [a, b] = pair(defaults, { ...defaults, maxStreams: 1 });
  const one = a.open(opening());
  b.receive(one.packet);
  assert.throws(() => a.open(opening()), /max_streams/);
  const over = b.receive(p(3, 'open', { profile: 'bytes.v1' }));
  assert.equal(over.outbound[0].body.value.code, ErrorCode.RESOURCE_EXHAUSTED);
  b.receive(p(3, 'reset'));
  assert.throws(() => b.receive(p(3, 'open', { profile: 'bytes.v1' })), /high-water/);
  const [c, d] = pair();
  const unsupported = d.receive(p(1, 'open', { profile: 'unknown.v1' }));
  assert.equal(unsupported.outbound[0].body.value.code, ErrorCode.UNSUPPORTED);
  assert.deepEqual(d.receive(p(1, 'openAck')).events, []);
  const reverse = d.open(opening());
  assert.equal(reverse.id, 2);
  assert.equal(c.receive(reverse.packet).events[0].type, 'open');
});

test('GOAWAY locally rejects remote pending before frame and forbids late accept', () => {
  const [a, b] = pair();
  const accepted = connect(a, b);
  const pending = a.open(opening());
  b.receive(pending.packet);
  const output = b.goaway();
  assert.deepEqual(output.map((v) => [v.streamId, v.body.case]), [[3, 'reset'], [0, 'goAway']]);
  assert.equal(output[1].body.value.lastStreamId, accepted);
  assert.equal(b.activeStreams, 1);
  assert.throws(() => b.accept(3), /draining/);
  assert.throws(() => b.open(opening()), /draining/);
  for (const value of output) a.receive(value);
  b.receive(a.send(accepted, utf8('still open')));
});

test('received GOAWAY cancels ALL local pending regardless of cutoff and resets remote pending', () => {
  const [a, b] = pair();
  const pending = a.open(opening());
  b.receive(pending.packet);
  const reverse = b.open(opening());
  a.receive(reverse.packet);
  const result = a.receive(p(0, 'goAway', { lastStreamId: 1 }));
  assert.deepEqual(result.events.filter((e) => e.type === 'reset').map((e) => e.id), [1, 2]);
  assert.deepEqual(result.outbound.map((v) => [v.streamId, v.body.case, v.body.value.code]), [[2, 'reset', ErrorCode.REFUSED]]);
  assert.equal(a.activeStreams, 0);
  assert.throws(() => a.accept(2), /draining/);
  assert.deepEqual(a.receive(b.accept(1)).events, []); // ACK already in flight is historical
});

test('GOAWAY / delayed peer accept fork: in-flight ACK allowed, later accept refused', () => {
  for (const ackInFlight of [false, true]) {
    const [a, b] = pair();
    const item = a.open(opening());
    b.receive(item.packet);
    const ack = ackInFlight ? b.accept(item.id) : undefined;
    const goaway = a.goaway();
    const result = b.receive(goaway.at(-1));
    if (ack) {
      assert.equal(b.activeStreams, 1);
      assert.equal(result.events.filter((e) => e.type === 'reset').length, 0);
      assert.equal(a.receive(ack).events[0].type, 'accepted');
    } else {
      assert.equal(b.activeStreams, 0);
      assert.equal(result.events.find((e) => e.type === 'reset').id, item.id);
      assert.throws(() => b.accept(item.id), /draining/);
      a.receive(result.outbound[0]);
      assert.equal(a.activeStreams, 0);
    }
  }
});

test('GOAWAY cannot revoke ACK even after normal close or reset; cutoffs cannot increase', () => {
  for (const close of ['reset', 'fin']) {
    const [a, b] = pair();
    const id = connect(a, b);
    if (close === 'reset') a.reset(id);
    else { b.receive(a.send(id, new Uint8Array(), true)); a.receive(b.send(id, new Uint8Array(), true)); }
    assert.equal(a.activeStreams, 0);
    assert.throws(() => a.receive(p(0, 'goAway', { lastStreamId: 0 })), /revokes an ACK/);
  }
  const [a] = pair();
  a.open(opening()); a.open(opening());
  a.receive(p(0, 'goAway', { lastStreamId: 1 }));
  assert.throws(() => a.receive(p(0, 'goAway', { lastStreamId: 3 })), /increased/);
  for (const last of [2, 3]) {
    const [s] = pair(); s.open(opening());
    assert.throws(() => s.receive(p(0, 'goAway', { lastStreamId: last })));
  }
});

test('draining rejects new peer OPEN while advancing historical high water', () => {
  const [a, b] = pair();
  b.goaway();
  const result = b.receive(a.open(opening()).packet);
  assert.equal(result.outbound[0].body.value.code, ErrorCode.REFUSED);
  assert.deepEqual(b.receive(p(1, 'reset')).events, []);
  assert.throws(() => b.receive(p(1, 'open', { profile: 'bytes.v1' })), /high-water/);
});

test('PING/PONG preserve all uint64 bits above JS safe integer range', () => {
  const [a, b] = pair();
  for (const token of [0n, (1n << 53n) + 123n, (1n << 64n) - 1n]) {
    const pong = b.receive(wire(a.ping(token))).outbound[0];
    assert.deepEqual(a.receive(wire(pong)).events, [{ type: 'pong', token }]);
  }
  for (const token of [Number.MAX_SAFE_INTEGER + 1, '9007199254740993', -1n, 1n << 64n]) assert.throws(() => a.ping(token));
  a.goaway();
  assert.doesNotThrow(() => a.ping(5n));
});

test('transport close emits INTERNAL failures for every active stream, and is idempotent', () => {
  const [a, b] = pair();
  connect(a, b); a.open(opening());
  const events = a.close();
  assert.deepEqual(events.map((e) => [e.type, e.id, e.code]), [['reset', 1, ErrorCode.INTERNAL], ['reset', 3, ErrorCode.INTERNAL]]);
  assert.equal(a.failed, true);
  assert.equal(a.activeStreams, 0);
  assert.deepEqual(a.close(), []);
  assert.throws(() => a.ping(0n), StateError);
  assert.throws(() => a.goaway(), StateError);
});

test('scope, unknown enums, invalid generated models fail receive validation', () => {
  const malformed = [p(1, 'ping', { token: 0n }), p(0, 'data', { fin: true }),
    p(0, 'openAck'), p(0, 'reset'), p(1, 'goAway'), p(0, 'open', { profile: 'bytes.v1' }),
    p(1, 'reset', { code: 99 }), p(2, 'open', { profile: 'bytes.v1', class: 3 }),
    p(2, 'open', { profile: 'bytes.v1', route: '\udfff' }), p(1, 'data'),
    { streamId: 0, body: { case: 'newBody', value: {} } }, { streamId: 0, body: { case: undefined } },
    p(2 ** 32, 'reset'), p(-1, 'reset')];
  for (const value of malformed) {
    const [session] = pair();
    assert.throws(() => session.receive(value), ProtocolError);
    assert.equal(session.failed, true);
  }
  const [a] = pair();
  assert.throws(() => a.receive(p(1, 'open', { profile: 'bytes.v1' })), /parity/);
});

test('Reset/GoAway reasons enforce 256 UTF-8 byte boundary on local and receive paths', () => {
  const valid = ['a'.repeat(256), '你'.repeat(85) + 'a'];
  const invalid = ['a'.repeat(257), '你'.repeat(85) + 'ab'];
  for (const reason of valid) {
    assert.equal(utf8(reason).length, 256);
    const [a, b] = pair();
    const id = connect(a, b);
    const output = a.reset(id, 0, reason);
    assert.equal(output[0].body.value.reason, reason);
    assert.equal(b.receive(wire(output[0])).events[0].reason, reason);
    const goaway = b.goaway(0, reason);
    assert.equal(a.receive(wire(goaway.at(-1))).events[0].reason, reason);
  }
  for (const reason of invalid) {
    assert.equal(utf8(reason).length, 257);
    const [a, b] = pair();
    const id = connect(a, b);
    a.receive(b.send(id, utf8('unread')));
    assert.throws(() => a.reset(id, 0, reason), /reason is too long/);
    assert.throws(() => a.goaway(0, reason), /reason is too long/);
    assert.equal(a.state, 'ready');
    assert.equal(a.draining, false);
    assert.equal(a.activeStreams, 1);
    assert.equal(a.consume(id, 6)[0].body.value.credit, 6); // outstanding is unchanged
    b.receive(a.send(id, utf8('still open')));
    for (const kind of ['reset', 'goAway']) {
      const [session, other] = pair(); connect(session, other);
      assert.throws(() => session.receive(p(kind === 'reset' ? 1 : 0, kind, { reason })), /reason is too long/);
      assert.equal(session.failed, true);
    }
  }
});
