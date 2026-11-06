#!/usr/bin/env node
import { create } from '@bufbuild/protobuf';
import { deepStrictEqual } from 'node:assert';
import { pathToFileURL } from 'node:url';
import { PacketSchema, OpenSchema, SettingsSchema } from './gen/tmp/v1/mux_pb.js';
import type { Packet } from './gen/tmp/v1/mux_pb.js';
import { decode, encode, MAX_PACKET } from './framing.js';
import { DEFAULT_SETTINGS } from './validation.js';
import { Session } from './session.js';
import type { Role } from './session.js';
import { nodeWritableSink } from './transport.js';

const helloBytes = new TextEncoder().encode('你好 TMP');
const okBytes = new TextEncoder().encode('ok');
const MAX_HEX_LINE = (MAX_PACKET + 4) * 2;

export function transcript(): Packet[] {
  const settings = create(SettingsSchema, DEFAULT_SETTINGS);
  return [
    create(PacketSchema, { body: { case: 'hello', value: { version: 1, settings, profiles: ['bytes.v1'] } } }),
    create(PacketSchema, { body: { case: 'welcome', value: { version: 1, settings, profiles: ['bytes.v1'] } } }),
    create(PacketSchema, { streamId: 1, body: { case: 'open', value: { profile: 'bytes.v1', route: 'echo', class: 0 } } }),
    create(PacketSchema, { streamId: 1, body: { case: 'openAck', value: {} } }),
    create(PacketSchema, { streamId: 1, body: { case: 'data', value: { payload: helloBytes, fin: true } } }),
    create(PacketSchema, { streamId: 1, body: { case: 'data', value: { payload: okBytes, fin: true } } }),
    create(PacketSchema, { streamId: 1, body: { case: 'window', value: { credit: 10 } } }),
    create(PacketSchema, { body: { case: 'window', value: { credit: 10 } } }),
    create(PacketSchema, { streamId: 1, body: { case: 'window', value: { credit: 2 } } }),
    create(PacketSchema, { body: { case: 'window', value: { credit: 2 } } }),
  ];
}

/** Bounded ASCII hex line input, accepts LF and CRLF on all platforms. */
async function* hexInput(): AsyncGenerator<Packet> {
  const line = new Uint8Array(MAX_HEX_LINE + 1);
  let used = 0;
  const parse = () => {
    let length = used;
    if (line[length - 1] === 13) length--;
    if (length === 0 || length % 2 !== 0) throw new Error('empty or odd-length hex line');
    const data = new Uint8Array(length / 2);
    const nibble = (c: number): number => {
      if (c >= 48 && c <= 57) return c - 48;
      if (c >= 65 && c <= 70) return c - 55;
      if (c >= 97 && c <= 102) return c - 87;
      throw new Error('invalid hex character');
    };
    for (let i = 0; i < length; i += 2) data[i / 2] = (nibble(line[i]!) << 4) | nibble(line[i + 1]!);
    used = 0;
    return decode(data);
  };
  for await (const chunk of process.stdin) {
    const data: Uint8Array = typeof chunk === 'string' ? Buffer.from(chunk) : chunk;
    for (const byte of data) {
      if (byte === 10) yield parse();
      else {
        if (used === line.length) throw new Error('hex line limit exceeded');
        line[used++] = byte;
      }
    }
  }
  if (used) yield parse();
}

const stdout = nodeWritableSink(process.stdout);
async function writePackets(values: Iterable<Packet>): Promise<void> {
  for (const value of values) await stdout(Buffer.from(Buffer.from(encode(value)).toString('hex') + '\n', 'ascii'));
}

async function verify(): Promise<void> {
  const expected = transcript();
  let index = 0;
  for await (const value of hexInput()) {
    if (index >= expected.length) throw new Error('too many transcript frames');
    // Compare decoded fields, not encoding order or serialized bytes.
    deepStrictEqual(value.streamId, expected[index]!.streamId, `frame ${index + 1} streamId`);
    deepStrictEqual(value.body, expected[index]!.body, `frame ${index + 1} body`);
    index++;
  }
  if (index !== expected.length) throw new Error(`expected 10 transcript frames; received ${index}`);
}

async function endpoint(role: Role): Promise<void> {
  const session = new Session(role);
  let opened = false;
  let accepted = false;
  let receivedData = false;
  if (role === 'initiator') await writePackets([session.start()]);
  for await (const value of hexInput()) {
    const result = session.receive(value);
    await writePackets(result.outbound);
    for (const event of result.events) {
      switch (event.type) {
        case 'ready':
          if (role === 'initiator') {
            const openedStream = session.open(create(OpenSchema, { profile: 'bytes.v1', route: 'echo', class: 0 }));
            deepStrictEqual(openedStream.id, 1);
            opened = true;
            await writePackets([openedStream.packet]);
          }
          break;
        case 'open':
          if (role !== 'responder' || opened) throw new Error('unexpected Open event');
          deepStrictEqual(event.id, 1);
          deepStrictEqual(event.open.profile, 'bytes.v1');
          deepStrictEqual(event.open.route, 'echo');
          deepStrictEqual(event.open.class, 0);
          deepStrictEqual(event.open.metadata.length, 0);
          opened = accepted = true;
          await writePackets([session.accept(event.id)]);
          break;
        case 'accepted':
          if (role !== 'initiator' || !opened || accepted) throw new Error('unexpected Accepted event');
          deepStrictEqual(event.id, 1);
          accepted = true;
          await writePackets([session.send(event.id, helloBytes, true)]);
          break;
        case 'data':
          if (!accepted || receivedData) throw new Error('unexpected Data event');
          deepStrictEqual(event.id, 1);
          deepStrictEqual(event.bytes, role === 'initiator' ? okBytes : helloBytes);
          deepStrictEqual(event.fin, true);
          receivedData = true;
          await writePackets(session.consume(event.id, event.bytes.length));
          if (role === 'responder') await writePackets([session.send(event.id, okBytes, true)]);
          else await writePackets(session.goaway());
          break;
        case 'goaway':
          if (!receivedData || session.activeStreams !== 0) throw new Error('GOAWAY arrived before completed exchange');
          if (role === 'responder') await writePackets(session.goaway());
          session.close();
          return;
        case 'reset': throw new Error(`unexpected Reset: ${event.id} ${event.reason}`);
        case 'pong': throw new Error('unexpected Pong');
      }
    }
  }
  session.close();
  throw new Error('EOF before reciprocal GOAWAY');
}

export async function main(args: readonly string[]): Promise<void> {
  if (args.length === 1 && args[0] === 'emit') await writePackets(transcript());
  else if (args.length === 1 && args[0] === 'verify') await verify();
  else if (args.length === 2 && args[0] === 'endpoint' && (args[1] === 'initiator' || args[1] === 'responder')) {
    await endpoint(args[1]);
  } else throw new Error('usage: tmp-conformance emit | verify | endpoint initiator|responder');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    process.stderr.write(`tmp-conformance: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  });
}
