import { fromBinary, toBinary, ScalarType } from '@bufbuild/protobuf';
import type { DescMessage } from '@bufbuild/protobuf';
import { PacketSchema } from './gen/tmp/v1/mux_pb.js';
import type { Packet } from './gen/tmp/v1/mux_pb.js';
import { ProtocolError, StateError } from './errors.js';
import { check, MAX_PACKET, UINT32_MAX, validatePacket } from './validation.js';

export { MAX_PACKET } from './validation.js';
const utf8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

class Reader {
  pos = 0;
  constructor(readonly data: Uint8Array) {}
  varint(): bigint {
    let value = 0n;
    for (let shift = 0n; shift < 70n; shift += 7n) {
      check(this.pos < this.data.length, 'truncated varint');
      const byte = this.data[this.pos++]!;
      if (shift === 63n) check(byte <= 1, 'varint exceeds uint64');
      value |= BigInt(byte & 127) << shift;
      if (!(byte & 128)) return value;
    }
    throw new ProtocolError('unterminated varint');
  }
  take(n: number): Uint8Array {
    check(n >= 0 && n <= this.data.length - this.pos, 'truncated field');
    const result = this.data.subarray(this.pos, this.pos + n);
    this.pos += n;
    return result;
  }
  lengthDelimited(): Uint8Array {
    const n = this.varint();
    check(n <= BigInt(UINT32_MAX), 'length exceeds uint32');
    return this.take(Number(n));
  }
  tag(): [number, number] {
    const value = this.varint();
    check(value <= BigInt(UINT32_MAX) && value >= 8n, 'invalid protobuf tag');
    return [Number(value >> 3n), Number(value & 7n)];
  }
  skip(field: number, wire: number, depth = 0): void {
    check(depth < 64, 'unknown group nesting limit');
    switch (wire) {
      case 0: this.varint(); return;
      case 1: this.take(8); return;
      case 2: this.lengthDelimited(); return;
      case 3:
        while (this.pos < this.data.length) {
          const [nested, kind] = this.tag();
          if (kind === 4) {
            check(nested === field, 'mismatched end group');
            return;
          }
          this.skip(nested, kind, depth + 1);
        }
        throw new ProtocolError('unterminated group');
      case 5: this.take(4); return;
      default: throw new ProtocolError('invalid wire type');
    }
  }
}

/** The generated decoder is intentionally preceded by a strict wire scan. */
function scan(data: Uint8Array, schema: DescMessage, top = false): Map<string, string | string[]> {
  const strings = new Map<string, string | string[]>();
  const merge = (key: string, value: string | string[]) => {
    const previous = strings.get(key);
    if (Array.isArray(value) && Array.isArray(previous)) previous.push(...value);
    else strings.set(key, value);
  };
  const reader = new Reader(data);
  let streamSeen = false;
  let bodySeen = false;
  while (reader.pos < data.length) {
    const [number, wire] = reader.tag();
    const field = schema.fields.find((f) => f.number === number);
    if (top) {
      check(number === 1 || (number >= 10 && number <= 19), 'unknown Packet tag');
      if (number === 1) {
        check(!streamSeen && wire === 0, 'duplicate stream_id or incorrect wire type');
        streamSeen = true;
      } else {
        check(!bodySeen && wire === 2, 'multiple Packet bodies or incorrect wire type');
        bodySeen = true;
      }
    }
    if (!field) {
      reader.skip(number, wire);
      continue;
    }
    const scalar = field.scalar;
    const expected = field.message !== undefined || scalar === ScalarType.STRING || scalar === ScalarType.BYTES
      ? 2 : scalar === ScalarType.FIXED64 ? 1 : 0;
    check(wire === expected, `incorrect wire type for ${field.name}`);
    if (wire === 2) {
      const value = reader.lengthDelimited();
      if (field.message) {
        for (const [key, text] of scan(value, field.message)) merge(key, text);
      } else if (scalar === ScalarType.STRING) {
        const text = utf8.decode(value);
        merge(field.localName, field.fieldKind === 'list' ? [text] : text);
      }
    } else if (wire === 1) reader.take(8);
    else {
      const value = reader.varint();
      if (scalar === ScalarType.UINT32) check(value <= BigInt(UINT32_MAX), 'uint32 overflow');
    }
  }
  if (top) check(bodySeen, 'Packet body is required');
  return strings;
}

function decodePayload(payload: Uint8Array): Packet {
  try {
    const strings = scan(payload, PacketSchema, true);
    const packet = fromBinary(PacketSchema, payload, { readUnknownFields: false });
    // All v1 string fields are direct body members. Buf 2.2.5 strips a leading
    // UTF-8 BOM; restore the strict scanner's exact strings without global hooks.
    Object.assign(packet.body.value!, Object.fromEntries(strings));
    validatePacket(packet);
    // fromBinary bytes fields are views into its input; own the delivered bytes.
    if (packet.body.case === 'data') packet.body.value.payload = Uint8Array.from(packet.body.value.payload);
    if (packet.body.case === 'open') packet.body.value.metadata = Uint8Array.from(packet.body.value.metadata);
    return packet;
  } catch (error) {
    if (error instanceof ProtocolError) throw error;
    throw new ProtocolError(`invalid protobuf: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/** Encode exactly one u32be-prefixed Packet. */
export function encode(packet: Packet): Uint8Array {
  validatePacket(packet);
  const payload = toBinary(PacketSchema, packet, { writeUnknownFields: false });
  check(payload.length > 0 && payload.length <= MAX_PACKET, 'packet length outside 1..MAX_PACKET');
  const frame = new Uint8Array(payload.length + 4);
  new DataView(frame.buffer).setUint32(0, payload.length, false);
  frame.set(payload, 4);
  return frame;
}

/** Decode exactly one complete frame; trailing bytes are an error. */
export function decode(frame: Uint8Array): Packet {
  check(frame instanceof Uint8Array && frame.length >= 4, 'truncated frame prefix');
  const n = new DataView(frame.buffer, frame.byteOffset, 4).getUint32(0, false);
  check(n > 0 && n <= MAX_PACKET, 'packet length outside 1..MAX_PACKET');
  check(frame.length === n + 4, 'truncated frame or trailing bytes');
  return decodePayload(frame.subarray(4));
}

/** Retains only one partial frame and synchronously delivers each packet. */
export class IncrementalDecoder {
  private readonly header = new Uint8Array(4);
  private headerUsed = 0;
  private payload: Uint8Array | undefined;
  private used = 0;
  private status: 'open' | 'finished' | 'failed' = 'open';
  private busy = false;

  constructor(private readonly onPacket: (packet: Packet) => void) {
    if (typeof onPacket !== 'function') throw new TypeError('onPacket must be a callback');
  }

  get bufferedBytes(): number { return this.headerUsed + this.used; }
  get bytesNeeded(): number { return this.payload ? this.payload.length - this.used : 4 - this.headerUsed; }

  feed(chunk: Uint8Array): void {
    if (this.busy) throw new StateError('decoder callbacks must not reenter the decoder');
    if (this.status !== 'open') throw new StateError(`decoder is ${this.status}`);
    this.busy = true;
    try {
      check(chunk instanceof Uint8Array, 'transport input must be Uint8Array');
      let offset = 0;
      while (offset < chunk.length) {
        if (!this.payload) {
          const n = Math.min(4 - this.headerUsed, chunk.length - offset);
          this.header.set(chunk.subarray(offset, offset + n), this.headerUsed);
          this.headerUsed += n;
          offset += n;
          if (this.headerUsed !== 4) continue;
          const length = new DataView(this.header.buffer).getUint32(0, false);
          check(length > 0 && length <= MAX_PACKET, 'packet length outside 1..MAX_PACKET');
          this.payload = new Uint8Array(length);
        }
        const n = Math.min(this.payload.length - this.used, chunk.length - offset);
        this.payload.set(chunk.subarray(offset, offset + n), this.used);
        this.used += n;
        offset += n;
        if (this.used === this.payload.length) {
          const packet = decodePayload(this.payload);
          this.payload = undefined;
          this.headerUsed = this.used = 0;
          const returned: unknown = this.onPacket(packet);
          if (returned && typeof (returned as PromiseLike<unknown>).then === 'function') {
            void Promise.resolve(returned).catch(() => {});
            throw new ProtocolError('decoder callback must be synchronous; use readPackets for async delivery');
          }
        }
      }
    } catch (error) {
      this.status = 'failed';
      this.payload = undefined;
      this.headerUsed = this.used = 0;
      throw error;
    } finally { this.busy = false; }
  }

  finish(): void {
    if (this.busy) throw new StateError('decoder is busy');
    if (this.status !== 'open') throw new StateError(`decoder is ${this.status}`);
    if (this.headerUsed !== 0 || this.payload) {
      this.status = 'failed';
      this.payload = undefined;
      this.headerUsed = this.used = 0;
      throw new ProtocolError('truncated frame at EOF');
    }
    this.status = 'finished';
  }
}
