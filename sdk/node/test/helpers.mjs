import { create } from '@bufbuild/protobuf';
import { Session, PacketSchema, OpenSchema, DEFAULT_SETTINGS, encode, decode } from '../dist/index.js';

export const utf8 = (text) => new TextEncoder().encode(text);
export const p = (streamId, kind, value = {}) => create(PacketSchema, { streamId, body: { case: kind, value } });
export const opening = (changes = {}) => create(OpenSchema, { profile: 'bytes.v1', route: 'echo', class: 0, ...changes });
export const wire = (packet) => decode(encode(packet));
export function pair(local = DEFAULT_SETTINGS, peer = DEFAULT_SETTINGS) {
  const a = new Session('initiator', local);
  const b = new Session('responder', peer);
  const response = b.receive(wire(a.start()));
  a.receive(wire(response.outbound[0]));
  return [a, b];
}
export function connect(a, b) {
  const { id, packet } = a.open(opening());
  b.receive(wire(packet));
  a.receive(wire(b.accept(id)));
  return id;
}
export function raw(hex) {
  const bytes = Buffer.from(hex.replace(/\s/g, ''), 'hex');
  const result = Buffer.alloc(bytes.length + 4);
  result.writeUInt32BE(bytes.length);
  bytes.copy(result, 4);
  return result;
}
