import { create } from '@bufbuild/protobuf';
import { Class, ErrorCode, OpenSchema, PacketSchema, SettingsSchema } from './gen/tmp/v1/mux_pb.js';
import type { Open, Packet } from './gen/tmp/v1/mux_pb.js';
import { ProtocolError, StateError, WouldBlock } from './errors.js';
import { encode } from './framing.js';
import { bytes, check, copySettings, DEFAULT_SETTINGS, errorFields, token, uint32,
  UINT32_MAX, validateOpen, validatePacket, validateProfiles } from './validation.js';
import type { SettingsInput } from './validation.js';

export type Role = 'initiator' | 'responder';
export type SessionState = 'new' | 'waiting' | 'ready' | 'failed';
export type Event =
  | { type: 'ready'; profiles: readonly string[] }
  | { type: 'open'; id: number; open: Open }
  | { type: 'accepted'; id: number }
  | { type: 'data'; id: number; bytes: Uint8Array; fin: boolean }
  | { type: 'reset'; id: number; code: ErrorCode; reason: string }
  | { type: 'pong'; token: bigint }
  | { type: 'goaway'; last: number; code: ErrorCode; reason: string };
export interface ReceiveResult { outbound: Packet[]; events: Event[] }
interface Stream {
  localInitiated: boolean;
  accepted: boolean;
  sendCredit: number;
  outstanding: number;
  localFin: boolean;
  remoteFin: boolean;
}

function packet(id: number, body: Packet['body']): Packet {
  const result = create(PacketSchema, { streamId: id, body });
  // Validate size before any local operation commits state.
  encode(result);
  return result;
}
function window(id: number, credit: number): Packet {
  return packet(id, { case: 'window', value: { $typeName: 'tmp.v1.Window', credit } });
}
function refused(id: number, code = ErrorCode.REFUSED, reason = 'link is draining'): Packet {
  return packet(id, { case: 'reset', value: { $typeName: 'tmp.v1.Reset', code, reason } });
}

/** Single-owner SansIO state machine. Send every returned packet in order. */
export class Session {
  readonly role: Role;
  private readonly local: Readonly<SettingsInput>;
  private readonly supported: readonly string[];
  private peer: Readonly<SettingsInput> | undefined;
  private negotiated: readonly string[] = Object.freeze([]);
  private currentState: SessionState = 'new';
  private isDraining = false;
  private readonly streams = new Map<number, Stream>();
  private connSendCredit = 0;
  private connOutstanding = 0;
  private highestLocal = 0;
  private highestRemote = 0;
  private highestAcceptedRemote = 0;
  private highestAcked = 0;
  private peerGoawayLast: number | undefined;

  constructor(role: Role, localSettings: SettingsInput = DEFAULT_SETTINGS,
    profiles: readonly string[] = ['bytes.v1']) {
    check(role === 'initiator' || role === 'responder', 'invalid session role');
    validateProfiles(profiles);
    this.role = role;
    this.local = Object.freeze(copySettings(localSettings));
    this.supported = Object.freeze([...profiles]);
  }

  get state(): SessionState { return this.currentState; }
  get failed(): boolean { return this.currentState === 'failed'; }
  get draining(): boolean { return this.isDraining; }
  get localSettings(): Readonly<SettingsInput> { return this.local; }
  get peerSettings(): Readonly<SettingsInput> | undefined { return this.peer; }
  get negotiatedProfiles(): readonly string[] { return this.negotiated; }
  get profiles(): readonly string[] { return this.negotiated; }
  get activeStreams(): number { return this.streams.size; }

  private usable(): void {
    if (this.failed) throw new StateError('session has failed');
  }
  private ready(): void {
    this.usable();
    if (this.currentState !== 'ready') throw new StateError('session is not ready');
  }
  private active(id: number): Stream {
    uint32(id, 'streamId');
    const stream = this.streams.get(id);
    if (!stream) throw new StateError('stream is not active');
    return stream;
  }
  private localId(id: number): boolean { return id % 2 === (this.role === 'initiator' ? 1 : 0); }
  private historical(id: number): boolean {
    return id > 0 && id <= (this.localId(id) ? this.highestLocal : this.highestRemote);
  }
  private count(local: boolean): number {
    let count = 0;
    for (const stream of this.streams.values()) if (stream.localInitiated === local) count++;
    return count;
  }
  private newStream(localInitiated: boolean): Stream {
    return { localInitiated, accepted: false, sendCredit: this.peer!.streamWindow,
      outstanding: 0, localFin: false, remoteFin: false };
  }
  private reap(id: number, stream: Stream): void {
    if (stream.localFin && stream.remoteFin && stream.outstanding === 0) this.streams.delete(id);
  }
  private discard(id: number, stream: Stream): Packet[] {
    this.streams.delete(id);
    this.connOutstanding -= stream.outstanding;
    return stream.outstanding ? [window(0, stream.outstanding)] : [];
  }

  start(): Packet {
    this.usable();
    if (this.role !== 'initiator' || this.currentState !== 'new') throw new StateError('start requires initiator/new');
    const result = packet(0, { case: 'hello', value: {
      $typeName: 'tmp.v1.Hello', version: 1, settings: create(SettingsSchema, this.local), profiles: [...this.supported],
    } });
    this.currentState = 'waiting';
    return result;
  }

  open(value: Open): { id: number; packet: Packet } {
    this.ready();
    if (this.isDraining) throw new StateError('session is draining');
    validateOpen(value);
    check(this.negotiated.includes(value.profile), 'profile was not negotiated');
    if (this.count(true) >= this.peer!.maxStreams) throw new StateError('peer max_streams reached');
    const id = this.highestLocal === 0 ? (this.role === 'initiator' ? 1 : 2) : this.highestLocal + 2;
    if (id > UINT32_MAX) throw new StateError('stream IDs exhausted; drain and reconnect');
    const copy = create(OpenSchema, { profile: value.profile, route: value.route,
      class: value.class, metadata: Uint8Array.from(value.metadata) });
    const result = packet(id, { case: 'open', value: copy });
    this.streams.set(id, this.newStream(true));
    this.highestLocal = id;
    return { id, packet: result };
  }

  accept(id: number): Packet {
    this.ready();
    if (this.isDraining) throw new StateError('session is draining');
    const stream = this.active(id);
    if (stream.localInitiated || stream.accepted) throw new StateError('accept requires remote pending stream');
    const result = packet(id, { case: 'openAck', value: { $typeName: 'tmp.v1.OpenAck' } });
    stream.accepted = true;
    this.highestAcceptedRemote = Math.max(this.highestAcceptedRemote, id);
    return result;
  }

  send(id: number, payload: Uint8Array, fin = false): Packet {
    this.ready();
    bytes(payload, 'payload', this.peer!.maxData);
    check(typeof fin === 'boolean' && (payload.length > 0 || fin), 'empty data requires FIN');
    const stream = this.active(id);
    if (!stream.accepted || stream.localFin) throw new StateError('send requires ACK and an open send direction');
    if (payload.length > stream.sendCredit || payload.length > this.connSendCredit) throw new WouldBlock();
    const result = packet(id, { case: 'data', value: {
      $typeName: 'tmp.v1.Data', payload: Uint8Array.from(payload), fin,
    } });
    stream.sendCredit -= payload.length;
    this.connSendCredit -= payload.length;
    stream.localFin = fin;
    this.reap(id, stream);
    return result;
  }

  consume(id: number, n: number): Packet[] {
    this.ready();
    const stream = this.active(id);
    uint32(n, 'consume count');
    check(n <= stream.outstanding, 'consume exceeds outstanding bytes');
    if (n === 0) return [];
    const result = [window(id, n), window(0, n)];
    stream.outstanding -= n;
    this.connOutstanding -= n;
    this.reap(id, stream);
    return result;
  }

  reset(id: number, code = ErrorCode.CANCELLED, reason = ''): Packet[] {
    this.ready();
    errorFields(code, reason);
    const stream = this.active(id);
    const result = refused(id, code, reason);
    return [result, ...this.discard(id, stream)];
  }

  goaway(code = ErrorCode.CANCELLED, reason = ''): Packet[] {
    this.ready();
    errorFields(code, reason);
    const last = packet(0, { case: 'goAway', value: {
      $typeName: 'tmp.v1.GoAway', lastStreamId: this.highestAcceptedRemote, code, reason,
    } });
    const outbound: Packet[] = [];
    for (const [id, stream] of this.streams) {
      if (!stream.localInitiated && !stream.accepted) {
        outbound.push(refused(id), ...this.discard(id, stream));
      }
    }
    this.isDraining = true;
    outbound.push(last);
    return outbound;
  }

  ping(value: bigint): Packet {
    this.ready();
    token(value);
    return packet(0, { case: 'ping', value: { $typeName: 'tmp.v1.Ping', token: value } });
  }

  /** Call on EOF, framing failure, read/write failure, or host timeout. Idempotent. */
  close(): Event[] {
    this.currentState = 'failed';
    const events: Event[] = [];
    for (const id of this.streams.keys()) events.push({ type: 'reset', id,
      code: ErrorCode.INTERNAL, reason: 'transport closed' });
    this.streams.clear();
    this.connOutstanding = 0;
    return events;
  }

  receive(value: Packet): ReceiveResult {
    this.usable();
    try {
      validatePacket(value);
      return this.receiveValid(value);
    } catch (error) {
      this.currentState = 'failed';
      if (error instanceof ProtocolError) throw error;
      throw new ProtocolError(error instanceof Error ? error.message : String(error));
    }
  }

  private receiveValid(value: Packet): ReceiveResult {
    const outbound: Packet[] = [];
    const events: Event[] = [];
    const id = value.streamId;
    const body = value.body;
    if (this.currentState !== 'ready') {
      const responder = this.role === 'responder' && this.currentState === 'new' && body.case === 'hello';
      const initiator = this.role === 'initiator' && this.currentState === 'waiting' && body.case === 'welcome';
      check(responder || initiator, 'invalid handshake order');
      if (body.case !== 'hello' && body.case !== 'welcome') throw new ProtocolError('expected handshake');
      const hello = body.value;
      if (initiator) check(hello.profiles.every((p) => this.supported.includes(p)), 'Welcome added an unoffered profile');
      this.peer = Object.freeze(copySettings(hello.settings!));
      this.negotiated = Object.freeze(hello.profiles.filter((p) => this.supported.includes(p)));
      this.connSendCredit = this.peer.connectionWindow;
      if (responder) outbound.push(packet(0, { case: 'welcome', value: {
        $typeName: 'tmp.v1.Welcome', version: 1, settings: create(SettingsSchema, this.local), profiles: [...this.negotiated],
      } }));
      this.currentState = 'ready';
      events.push({ type: 'ready', profiles: [...this.negotiated] });
      return { outbound, events };
    }
    switch (body.case) {
      case 'hello': case 'welcome': throw new ProtocolError('duplicate handshake');
      case 'open': {
        check(!this.localId(id) && id > this.highestRemote, 'OPEN id parity or high-water violation');
        this.highestRemote = id;
        if (this.isDraining) outbound.push(refused(id));
        else if (!this.negotiated.includes(body.value.profile)) outbound.push(refused(id, ErrorCode.UNSUPPORTED, 'profile not negotiated'));
        else if (this.count(false) >= this.local.maxStreams) outbound.push(refused(id, ErrorCode.RESOURCE_EXHAUSTED, 'max_streams reached'));
        else {
          this.streams.set(id, this.newStream(false));
          events.push({ type: 'open', id, open: create(OpenSchema, {
            profile: body.value.profile, route: body.value.route, class: body.value.class,
            metadata: Uint8Array.from(body.value.metadata),
          }) });
        }
        break;
      }
      case 'ping':
        outbound.push(packet(0, { case: 'pong', value: { $typeName: 'tmp.v1.Pong', token: body.value.token } }));
        break;
      case 'pong': events.push({ type: 'pong', token: body.value.token }); break;
      case 'goAway': {
        const last = body.value.lastStreamId;
        check((last === 0 || this.localId(last)) && last <= this.highestLocal && last >= this.highestAcked,
          'GOAWAY cutoff has wrong parity, is future, or revokes an ACK');
        check(this.peerGoawayLast === undefined || last <= this.peerGoawayLast, 'GOAWAY cutoff increased');
        this.peerGoawayLast = last;
        this.isDraining = true;
        for (const [pendingId, stream] of this.streams) {
          if (stream.accepted) continue;
          if (!stream.localInitiated) outbound.push(refused(pendingId));
          outbound.push(...this.discard(pendingId, stream));
          events.push({ type: 'reset', id: pendingId, code: ErrorCode.REFUSED, reason: 'link is draining' });
        }
        events.push({ type: 'goaway', last, code: body.value.code, reason: body.value.reason });
        break;
      }
      case 'window': {
        const n = body.value.credit;
        if (id === 0) {
          this.flow(this.connSendCredit + n <= this.peer!.connectionWindow, 'connection WINDOW overflow');
          this.connSendCredit += n;
        } else {
          const stream = this.streams.get(id);
          if (!stream) check(this.historical(id), 'WINDOW for future stream');
          else {
            this.flow(stream.sendCredit + n <= this.peer!.streamWindow, 'stream WINDOW overflow');
            stream.sendCredit += n;
          }
        }
        break;
      }
      case 'openAck': {
        const stream = this.streams.get(id);
        if (!stream) check(this.historical(id), 'ACK for future stream');
        else {
          check(stream.localInitiated && !stream.accepted, 'unexpected or duplicate ACK');
          stream.accepted = true;
          this.highestAcked = Math.max(this.highestAcked, id);
          events.push({ type: 'accepted', id });
        }
        break;
      }
      case 'reset': {
        const stream = this.streams.get(id);
        if (!stream) check(this.historical(id), 'RESET for future stream');
        else {
          outbound.push(...this.discard(id, stream));
          events.push({ type: 'reset', id, code: body.value.code, reason: body.value.reason });
        }
        break;
      }
      case 'data': {
        const n = body.value.payload.length;
        this.flow(n <= this.local.maxData && this.connOutstanding + n <= this.local.connectionWindow,
          'DATA exceeds max_data or connection receive credit');
        const stream = this.streams.get(id);
        if (!stream) {
          check(this.historical(id), 'DATA for future stream');
          if (n) outbound.push(window(0, n));
        } else {
          check(stream.accepted && !stream.remoteFin, 'DATA before ACK or after FIN');
          this.flow(stream.outstanding + n <= this.local.streamWindow, 'DATA exceeds stream receive credit');
          stream.outstanding += n;
          this.connOutstanding += n;
          stream.remoteFin = body.value.fin;
          events.push({ type: 'data', id, bytes: Uint8Array.from(body.value.payload), fin: body.value.fin });
          this.reap(id, stream);
        }
        break;
      }
      default: throw new ProtocolError('unknown body');
    }
    return { outbound, events };
  }

  private flow(condition: boolean, message: string): void {
    if (!condition) throw new ProtocolError(message, ErrorCode.FLOW_CONTROL);
  }
}

export { Class, ErrorCode };
