import test from 'node:test';
import assert from 'node:assert/strict';
import { create, toBinary } from '@bufbuild/protobuf';
import { encode, decode, IncrementalDecoder, MAX_PACKET, ProtocolError, StateError,
  PacketSchema, DEFAULT_SETTINGS } from '../dist/index.js';
import { p, raw, utf8 } from './helpers.mjs';

function varint(value) {
  const bytes = [];
  do { let part = Number(value & 127n); value >>= 7n; if (value) part |= 128; bytes.push(part); } while (value);
  return Buffer.from(bytes);
}
function ld(tag, contents) { return Buffer.concat([varint(BigInt(tag * 8 + 2)), varint(BigInt(contents.length)), contents]); }
function framed(contents) { const head = Buffer.alloc(4); head.writeUInt32BE(contents.length); return Buffer.concat([head, contents]); }

test('encode/decode golden fixed64 Ping retains unsigned uint64 precision', () => {
  const token = 0x20000000000001n;
  const data = encode(p(0, 'ping', { token }));
  assert.equal(Buffer.from(data).toString('hex'), '0000000c8a0109090100000000002000');
  assert.equal(decode(data).body.value.token, token);
  assert.equal(typeof decode(data).body.value.token, 'bigint');
  const view = Buffer.concat([Buffer.from([99]), data, Buffer.from([99])]).subarray(1, 1 + data.length);
  assert.equal(decode(view).body.value.token, token);
});

test('strict top-level scan rejects unknown tags, wrong wire, duplicate id/body and uint32 overflow', () => {
  const invalid = [
    '10016a00', // unknown top tag
    'a20100', // unknown body 20
    '080108016a00', // repeated stream_id
    '6a006a00', // repeated same body
    '6a007200', // different bodies
    '0a006a00', // stream id wrong wire
    '6801', // body wrong wire
    '08ffffffff1f6a00', // stream id > uint32
    '0801', // no body
    '00', // invalid tag
    '808080808080808080808000', // invalid tag varint
    '6affffffff7f', // excessive length
    '6a01', // truncated body
    '6a02ffff', // malformed inner
  ];
  for (const hex of invalid) assert.throws(() => decode(raw(hex)), ProtocolError, hex);
  assert.equal(decode(raw('6a000801')).streamId, 1); // reversed legal field order
  assert.equal(decode(raw('08ffffffff0f6a00')).streamId, 0xffffffff);
});

test('body unknown fields are ignored; singular duplicates use last value; nested messages merge', () => {
  const value = decode(raw('0801720a0a01610a016210011807'));
  assert.equal(value.body.value.fin, true);
  assert.deepEqual(value.body.value.payload, utf8('b'));
  // Two settings occurrences merge disjoint fields. No wire defaults are substituted.
  const first = Buffer.from([0x08, 0x80, 0x08, 0x10, 0x80, 0x20]); // max_data=1024, stream_window=4096
  const second = Buffer.from([0x18, 0x80, 0x80, 0x04, 0x20, 0x01]); // conn_window=65536, max_streams=1
  const hello = Buffer.concat([Buffer.from([8, 1]), ld(2, first), ld(2, second)]);
  const decoded = decode(framed(ld(10, hello)));
  assert.deepEqual({ ...decoded.body.value.settings }, { $typeName: 'tmp.v1.Settings',
    maxData: 1024, streamWindow: 4096, connectionWindow: 65536, maxStreams: 1 });
});

test('known strings require valid UTF-8; embedded malformed wire does not escape scanning', () => {
  for (const contents of [
    Buffer.concat([Buffer.from([8, 1]), ld(12, Buffer.from([10, 1, 255]))]), // open profile
    Buffer.concat([Buffer.from([8, 1]), ld(16, Buffer.from([18, 2, 0xc0, 0x80]))]), // reset reason
    ld(19, Buffer.from([26, 1, 0xff])), // goaway reason
    ld(10, Buffer.from([8, 1, 26, 1, 0xff])), // hello profile
    Buffer.concat([Buffer.from([8, 1]), ld(14, Buffer.from([8, 0]))]), // payload uses wrong wire
    Buffer.concat([Buffer.from([8, 1]), ld(13, Buffer.from([0x0b, 0x14]))]), // mismatched unknown group
  ]) assert.throws(() => decode(framed(contents)), ProtocolError);
  const value = decode(framed(Buffer.concat([Buffer.from([8, 1]), ld(16, ld(2, Buffer.from('\ufeffok')))])));
  assert.equal(value.body.value.reason, '\ufeffok');
  assert.doesNotThrow(() => decode(raw('08016a040b10010c'))); // valid unknown group inside OpenAck
});

test('unknown or illegal enums are rejected after decoding, defaults remain legal', () => {
  assert.throws(() => decode(raw('08018201020863')), ProtocolError); // reset code 99
  assert.throws(() => decode(raw('080162050a01611803')), ProtocolError); // class 3
  assert.throws(() => decode(raw('52020801')), /settings/);
  assert.equal(decode(raw('08016a00')).body.case, 'openAck');
});

test('incremental decoder supports every split, byte fragments and many coalesced frames', () => {
  const value = encode(p(1, 'data', { payload: utf8('你好 TMP'), fin: true }));
  for (let split = 0; split <= value.length; split++) {
    const got = [];
    const decoder = new IncrementalDecoder((packet) => got.push(packet));
    decoder.feed(value.subarray(0, split));
    decoder.feed(value.subarray(split));
    decoder.finish();
    assert.equal(got.length, 1);
    assert.deepEqual(got[0].body.value.payload, utf8('你好 TMP'));
  }
  let count = 0;
  const decoder = new IncrementalDecoder(() => { count++; });
  for (const byte of value) decoder.feed(Uint8Array.of(byte));
  decoder.feed(Buffer.concat(Array.from({ length: 10000 }, () => value)));
  assert.equal(count, 10001);
  assert.equal(decoder.bufferedBytes, 0);
  decoder.finish();
});

test('incremental retention bounded by MAX_PACKET+4, oversize rejected before body allocation', () => {
  const header = Buffer.alloc(4); header.writeUInt32BE(MAX_PACKET);
  const decoder = new IncrementalDecoder(() => {});
  decoder.feed(header);
  decoder.feed(new Uint8Array(MAX_PACKET - 1));
  assert.equal(decoder.bufferedBytes, MAX_PACKET + 3);
  assert.equal(decoder.bytesNeeded, 1);
  assert.throws(() => decoder.finish(), /truncated/);
  assert.equal(decoder.bufferedBytes, 0);
  for (const length of [0, MAX_PACKET + 1, 0xffffffff]) {
    const decoder = new IncrementalDecoder(() => assert.fail('must not deliver'));
    header.writeUInt32BE(length);
    assert.throws(() => decoder.feed(header), /length/);
    assert.equal(decoder.bufferedBytes, 0);
    assert.throws(() => decoder.feed(Uint8Array.of(0)), StateError);
    assert.throws(() => decoder.finish(), StateError);
  }
});

test('EOF detects any partial frame, malformed decoder is terminal and completed decoder cannot reuse', () => {
  const data = encode(p(1, 'openAck'));
  for (let cut = 1; cut < data.length; cut++) {
    const decoder = new IncrementalDecoder(() => {});
    decoder.feed(data.subarray(0, cut));
    assert.throws(() => decoder.finish(), /truncated/);
    assert.throws(() => decoder.feed(data), StateError);
  }
  const decoder = new IncrementalDecoder(() => {});
  decoder.finish();
  assert.throws(() => decoder.feed(data), StateError);
  assert.throws(() => decoder.finish(), StateError);
  const broken = new IncrementalDecoder(() => {});
  assert.throws(() => broken.feed(raw('6a006a00')));
  assert.throws(() => broken.feed(data), StateError);
});

test('callback errors and reentrancy fail decoder, async callbacks are rejected', () => {
  const frame = encode(p(1, 'openAck'));
  const error = new Error('host failed');
  const decoder = new IncrementalDecoder(() => { throw error; });
  assert.throws(() => decoder.feed(frame), (e) => e === error);
  assert.throws(() => decoder.feed(frame), StateError);
  const reentrant = new IncrementalDecoder(() => reentrant.feed(frame));
  assert.throws(() => reentrant.feed(frame), /reenter/);
  assert.throws(() => reentrant.finish(), StateError);
  const asynchronous = new IncrementalDecoder(async () => {});
  assert.throws(() => asynchronous.feed(frame), /synchronous/);
});

test('standalone framing rejects zero, oversize, truncated, trailing, invalid parameters', () => {
  for (const data of [Buffer.alloc(0), Buffer.alloc(3), Buffer.alloc(4), Buffer.from('00020001', 'hex'),
    Buffer.concat([encode(p(1, 'openAck')), Uint8Array.of(0)])]) assert.throws(() => decode(data), ProtocolError);
  assert.throws(() => encode(p(1, 'reset', { reason: 'x'.repeat(MAX_PACKET) })), /too long/);
  assert.throws(() => encode(p(0, 'ping', { token: 9007199254740994 })), /bigint/);
});

test('decoded bytes are owned even if the wire buffer subsequently changes', () => {
  for (const value of [p(1, 'data', { payload: utf8('owned') }), p(1, 'open', { profile: 'bytes.v1', metadata: utf8('owned') })]) {
    const frame = encode(value);
    const decoded = decode(frame);
    frame.fill(0);
    const bytes = decoded.body.case === 'data' ? decoded.body.value.payload : decoded.body.value.metadata;
    assert.deepEqual(bytes, utf8('owned'));
  }
});

test('wire rejects 257-byte reasons for Reset and GoAway while allowing 256 bytes', () => {
  for (const kind of ['reset', 'goAway']) {
    for (const reason of ['x'.repeat(256), '你'.repeat(85) + 'a']) {
      const value = p(kind === 'reset' ? 1 : 0, kind, { reason });
      assert.equal(decode(encode(value)).body.value.reason, reason);
    }
    for (const reason of ['x'.repeat(257), '你'.repeat(85) + 'ab']) {
      const value = p(kind === 'reset' ? 1 : 0, kind, { reason });
      assert.throws(() => encode(value), /reason is too long/);
      assert.throws(() => decode(framed(toBinary(PacketSchema, value))), /reason is too long/);
    }
  }
});
