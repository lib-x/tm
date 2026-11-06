import type { Open, Packet, Settings } from './gen/tmp/v1/mux_pb.js';
import { ProtocolError } from './errors.js';

export type SettingsInput = Pick<Settings, 'maxData' | 'streamWindow' | 'connectionWindow' | 'maxStreams'>;
export const DEFAULT_SETTINGS: Readonly<SettingsInput> = Object.freeze({
  maxData: 16384, streamWindow: 65536, connectionWindow: 1048576, maxStreams: 256,
});
export const MAX_PACKET = 131072;
export const UINT32_MAX = 0xffffffff;
export const UINT64_MAX = (1n << 64n) - 1n;
const utf8 = new TextEncoder();

export function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new ProtocolError(message);
}

export function uint32(value: unknown, name: string): asserts value is number {
  check(typeof value === 'number' && Number.isInteger(value) && value >= 0 && value <= UINT32_MAX,
    `${name} must be a uint32`);
}

export function text(value: unknown, name: string, max = MAX_PACKET): asserts value is string {
  check(typeof value === 'string', `${name} must be a string`);
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      const next = value.charCodeAt(++i);
      check(next >= 0xdc00 && next <= 0xdfff, `${name} contains an unpaired surrogate`);
    } else check(c < 0xdc00 || c > 0xdfff, `${name} contains an unpaired surrogate`);
  }
  check(utf8.encode(value).length <= max, `${name} is too long`);
}

export function bytes(value: unknown, name: string, max = MAX_PACKET): asserts value is Uint8Array {
  check(value instanceof Uint8Array && value.byteLength <= max, `${name} must be bounded Uint8Array`);
}

export function token(value: unknown): asserts value is bigint {
  check(typeof value === 'bigint' && value >= 0n && value <= UINT64_MAX, 'token must be uint64 bigint');
}

export function errorFields(code: unknown, reason: unknown): void {
  check(typeof code === 'number' && Number.isInteger(code) && code >= 0 && code <= 7, 'unknown error code');
  text(reason, 'reason', 256);
}

export function validateSettings(value: SettingsInput): void {
  check(value !== null && typeof value === 'object', 'settings are required');
  const ranges = [
    ['maxData', 1024, 65536], ['streamWindow', 4096, 16777216],
    ['connectionWindow', 65536, 268435456], ['maxStreams', 1, 4096],
  ] as const;
  for (const [key, min, max] of ranges) {
    uint32(value[key], key);
    check(value[key] >= min && value[key] <= max, `${key} outside protocol range`);
  }
}

export function copySettings(value: SettingsInput): SettingsInput {
  validateSettings(value);
  return { maxData: value.maxData, streamWindow: value.streamWindow,
    connectionWindow: value.connectionWindow, maxStreams: value.maxStreams };
}

export function validateProfiles(value: readonly string[]): void {
  check(Array.isArray(value) && value.length <= 32, 'profiles must contain at most 32 entries');
  const seen = new Set<string>();
  for (const profile of value) {
    check(typeof profile === 'string' && /^[a-z0-9][a-z0-9._/-]{0,63}$/.test(profile), 'invalid profile');
    check(!seen.has(profile), 'duplicate profile');
    seen.add(profile);
  }
}

export function validateOpen(value: Open): void {
  check(value !== null && typeof value === 'object', 'open is required');
  validateProfiles([value.profile]);
  text(value.route, 'route', 256);
  bytes(value.metadata, 'metadata', 4096);
  check(Number.isInteger(value.class) && value.class >= 0 && value.class <= 2, 'unknown class');
}

/** Validate even generated messages: create() is not an input validator. */
export function validatePacket(packet: Packet): void {
  check(packet !== null && typeof packet === 'object', 'packet is required');
  uint32(packet.streamId, 'streamId');
  const body = packet.body;
  check(body !== null && typeof body === 'object' && body.case !== undefined &&
    body.value !== null && typeof body.value === 'object', 'packet body is required');
  switch (body.case) {
    case 'hello': case 'welcome':
      check(packet.streamId === 0, 'handshake must use stream 0');
      check(body.value.version === 1, 'unsupported protocol version');
      check(body.value.settings !== undefined, 'handshake settings are required');
      validateSettings(body.value.settings);
      validateProfiles(body.value.profiles);
      break;
    case 'open':
      check(packet.streamId !== 0, 'open requires a stream');
      validateOpen(body.value);
      break;
    case 'openAck':
      check(packet.streamId !== 0, 'ack requires a stream');
      break;
    case 'data':
      check(packet.streamId !== 0, 'data requires a stream');
      bytes(body.value.payload, 'payload', 65536);
      check(typeof body.value.fin === 'boolean', 'fin must be boolean');
      check(body.value.payload.length > 0 || body.value.fin, 'empty data requires FIN');
      break;
    case 'window':
      uint32(body.value.credit, 'credit');
      check(body.value.credit > 0, 'window credit must be positive');
      break;
    case 'reset':
      check(packet.streamId !== 0, 'reset requires a stream');
      errorFields(body.value.code, body.value.reason);
      break;
    case 'ping': case 'pong':
      check(packet.streamId === 0, 'ping/pong must use stream 0');
      token(body.value.token);
      break;
    case 'goAway':
      check(packet.streamId === 0, 'goaway must use stream 0');
      uint32(body.value.lastStreamId, 'lastStreamId');
      errorFields(body.value.code, body.value.reason);
      break;
    default: throw new ProtocolError('unknown packet body');
  }
}
