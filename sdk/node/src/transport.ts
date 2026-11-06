import { setImmediate as yieldTurn } from 'node:timers/promises';
import type { Writable } from 'node:stream';
import type { Packet } from './gen/tmp/v1/mux_pb.js';
import { IncrementalDecoder, encode, MAX_PACKET } from './framing.js';
import { StateError, ProtocolError } from './errors.js';
import type { Event, Session } from './session.js';

export type ByteSource = AsyncIterable<Uint8Array> | ReadableStream<Uint8Array>;
export type ByteSink = (bytes: Uint8Array) => void | Promise<void>;

function limit(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive safe integer`);
  return value;
}

async function* chunks(source: ByteSource): AsyncGenerator<Uint8Array> {
  if (Symbol.asyncIterator in source) {
    yield* source as AsyncIterable<Uint8Array>;
    return;
  }
  const reader = (source as ReadableStream<Uint8Array>).getReader();
  let done = false;
  try {
    while (true) {
      const item = await reader.read();
      if (item.done) { done = true; break; }
      yield item.value;
    }
  } finally {
    try { if (!done) await reader.cancel(); }
    finally { reader.releaseLock(); }
  }
}

/** Pulls one Packet at a time without accumulating a decoded packet array. */
export async function* readPackets(source: ByteSource,
  options: { framesPerTurn?: number } = {}): AsyncGenerator<Packet> {
  const budget = limit(options.framesPerTurn ?? 32, 'framesPerTurn');
  let pending: Packet | undefined;
  const decoder = new IncrementalDecoder((value) => { pending = value; });
  let processed = 0;
  for await (const chunk of chunks(source)) {
    if (!(chunk instanceof Uint8Array)) throw new ProtocolError('byte source yielded nonbinary data');
    let offset = 0;
    while (offset < chunk.length) {
      const count = Math.min(decoder.bytesNeeded, chunk.length - offset);
      decoder.feed(chunk.subarray(offset, offset + count));
      offset += count;
      if (pending) {
        const value = pending;
        pending = undefined;
        yield value;
        if (++processed === budget) { processed = 0; await yieldTurn(); }
      }
    }
  }
  decoder.finish();
}

export interface WriterOptions {
  maxQueuedBytes?: number;
  maxQueuedPackets?: number;
  onFailure?: (error: unknown) => void;
}

/** Serializes writes in call order; any failure poisons the entire queue. */
export class SerialPacketWriter {
  private tail: Promise<void> = Promise.resolve();
  private error: unknown;
  private poisoned = false;
  private closed = false;
  private queuedBytes = 0;
  private queuedPackets = 0;
  private readonly maxBytes: number;
  private readonly maxPackets: number;

  constructor(private readonly sink: ByteSink, private readonly options: WriterOptions = {}) {
    this.maxBytes = limit(options.maxQueuedBytes ?? 4 * (MAX_PACKET + 4), 'maxQueuedBytes');
    this.maxPackets = limit(options.maxQueuedPackets ?? 512, 'maxQueuedPackets');
  }

  get pendingBytes(): number { return this.queuedBytes; }
  get pendingPackets(): number { return this.queuedPackets; }

  private fail(error: unknown): void {
    if (this.poisoned) return;
    this.poisoned = true;
    this.error = error;
    // Failure observers cannot repair or replace a failed transport write.
    try { this.options.onFailure?.(error); } catch { /* preserve the write error */ }
  }

  write(value: Packet): Promise<void> {
    if (this.closed || this.poisoned) return Promise.reject(this.poisoned ? this.error : new StateError('writer is closed'));
    let data: Uint8Array;
    try { data = encode(value); }
    catch (error) { this.fail(error); return Promise.reject(error); }
    if (this.queuedPackets >= this.maxPackets || data.length > this.maxBytes - this.queuedBytes) {
      const error = new StateError('transport send queue limit exceeded');
      this.fail(error);
      return Promise.reject(error);
    }
    this.queuedBytes += data.length;
    this.queuedPackets++;
    const result = this.tail.then(async () => {
      if (this.poisoned) throw this.error;
      try { await this.sink(data); }
      catch (error) { this.fail(error); throw error; }
    }).finally(() => {
      this.queuedBytes -= data.length;
      this.queuedPackets--;
    });
    // Keep a handled chain; each caller still receives the original rejection.
    this.tail = result.catch(() => {});
    return result;
  }

  writeAll(values: Iterable<Packet>): Promise<void> {
    const pending: Promise<void>[] = [];
    try {
      // Enqueue the whole bounded batch before another caller can interleave.
      for (const value of values) {
        pending.push(this.write(value));
        if (this.poisoned || this.closed) break;
      }
    } catch (error) {
      this.fail(error);
      pending.push(Promise.reject(error));
    }
    return Promise.all(pending).then(() => {});
  }

  async finish(): Promise<void> {
    this.closed = true;
    await this.tail;
    if (this.poisoned) throw this.error;
  }
}

/** Node Writable callback completion provides backpressure for SSH/socket streams. */
export function nodeWritableSink(writable: Writable): ByteSink {
  return (data) => new Promise<void>((resolve, reject) => {
    if (writable.destroyed || writable.writableEnded) { reject(new StateError('writable is closed')); return; }
    const cleanup = () => { writable.off('error', onError); writable.off('close', onClose); };
    const onError = (error: Error) => { cleanup(); reject(error); };
    const onClose = () => { cleanup(); reject(new StateError('writable closed during write')); };
    writable.once('error', onError);
    writable.once('close', onClose);
    writable.write(data, (error) => {
      // Node emits error after invoking this callback; keep the listener through that turn.
      if (error) { reject(error); queueMicrotask(cleanup); }
      else { cleanup(); resolve(); }
    });
  });
}

/** Keep the returned adapter for the lifetime of one WHATWG writer lock. */
export function writableStreamSink(writer: WritableStreamDefaultWriter<Uint8Array>): ByteSink {
  return async (data) => { await writer.ready; await writer.write(data); };
}

export interface TransportContext {
  session: Session;
  send(packets: Packet | Iterable<Packet>): Promise<void>;
}

/** Explicit receive pump. The host owns dialing, authentication, timers and EOF. */
export async function runTransport(session: Session, source: ByteSource, sink: ByteSink,
  onEvent: (event: Event, context: TransportContext) => void | Promise<void>,
  options: WriterOptions & { framesPerTurn?: number } = {}): Promise<void> {
  let failure: unknown;
  let didFail = false;
  let notified = false;
  let notifications: Promise<void> = Promise.resolve();
  let interruptRead: ((reason: unknown) => void) | undefined;
  const reader = readPackets(source, options);

  // Close synchronously even when a send originates outside the receive pump.
  // Notifications run once; the failure signal wakes a blocked iterator.next().
  const notifyClosed = () => {
    if (notified) return;
    notified = true;
    const events = session.close();
    notifications = (async () => {
      for (const event of events) {
        try { await onEvent(event, context); }
        catch (error) { if (!didFail) { failure = error; didFail = true; } }
      }
    })();
  };
  const terminate = (error: unknown) => {
    if (didFail) return;
    didFail = true;
    failure = error;
    notifyClosed();
    interruptRead?.(error);
    try { options.onFailure?.(error); } catch { /* retain original transport failure */ }
  };
  const writer = new SerialPacketWriter(sink, { ...options, onFailure: terminate });
  const context: TransportContext = {
    session,
    send: async (values) => {
      if (session.failed) throw new StateError('session is closed');
      if (Symbol.iterator in values) await writer.writeAll(values as Iterable<Packet>);
      else await writer.write(values as Packet);
    },
  };
  try {
    if (session.role === 'initiator' && session.state === 'new') await context.send(session.start());
    while (true) {
      const item = await new Promise<IteratorResult<Packet>>((resolve, reject) => {
        if (didFail) { reject(failure); return; }
        interruptRead = reject;
        reader.next().then((value) => { interruptRead = undefined; resolve(value); },
          (error) => { interruptRead = undefined; reject(error); });
      });
      if (item.done) break;
      const result = session.receive(item.value);
      await writer.writeAll(result.outbound);
      for (const event of result.events) await onEvent(event, context);
    }
    await writer.finish();
  } catch (error) { terminate(error); }
  finally {
    notifyClosed();
    await notifications;
    // Generic iterators may not cancel a pending next(). The host's onFailure
    // must close its carrier; do not block termination on such an iterator.
    void reader.return(undefined).catch(() => {});
  }
  if (didFail) throw failure;
}

export interface WebSocketSourceOptions {
  maxMessageBytes?: number;
  maxBufferedBytes?: number;
  maxBufferedMessages?: number;
  onFailure?: (error: unknown) => void;
}

/** Host calls push(data,isBinary) from WS message, end() from close/error.
 * Set browser WebSocket.binaryType='arraybuffer'; Blob conversion is host-owned.
 */
export class WebSocketByteSource implements AsyncIterable<Uint8Array> {
  private readonly queue: Uint8Array[] = [];
  private buffered = 0;
  private ended = false;
  private failure: unknown;
  private hasFailure = false;
  private wake: (() => void) | undefined;
  private reading = false;
  private readonly maxMessage: number;
  private readonly maxBuffered: number;
  private readonly maxMessages: number;

  constructor(private readonly options: WebSocketSourceOptions = {}) {
    this.maxMessage = limit(options.maxMessageBytes ?? 1024 * 1024, 'maxMessageBytes');
    this.maxBuffered = limit(options.maxBufferedBytes ?? 4 * 1024 * 1024, 'maxBufferedBytes');
    this.maxMessages = limit(options.maxBufferedMessages ?? 256, 'maxBufferedMessages');
  }
  get bufferedBytes(): number { return this.buffered; }

  push(data: unknown, isBinary = true): void {
    if (this.ended) throw new StateError('WebSocket byte source is closed');
    try {
      if (!isBinary || !(data instanceof ArrayBuffer || ArrayBuffer.isView(data))) {
        throw new ProtocolError('WebSocket requires binary ArrayBuffer or typed array messages');
      }
      const view = data instanceof ArrayBuffer ? new Uint8Array(data)
        : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
      if (view.length > this.maxMessage) throw new ProtocolError('WebSocket message limit exceeded');
      if (view.length === 0) return;
      if (view.length > this.maxBuffered - this.buffered || this.queue.length >= this.maxMessages) {
        throw new ProtocolError('WebSocket receive buffer limit exceeded');
      }
      this.queue.push(Uint8Array.from(view));
      this.buffered += view.length;
      this.wake?.();
      this.wake = undefined;
    } catch (error) { this.end(error); throw error; }
  }

  end(error?: unknown): void {
    if (this.ended) return;
    this.ended = true;
    if (error !== undefined) {
      this.hasFailure = true;
      this.failure = error;
      this.queue.length = 0;
      this.buffered = 0;
      try { this.options.onFailure?.(error); } catch { /* preserve source failure */ }
    }
    this.wake?.();
    this.wake = undefined;
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<Uint8Array> {
    if (this.reading) throw new StateError('WebSocket byte source has one reader');
    this.reading = true;
    try {
      while (true) {
        if (this.hasFailure) throw this.failure;
        const value = this.queue.shift();
        if (value) { this.buffered -= value.length; yield value; }
        else if (this.ended) return;
        else await new Promise<void>((resolve) => { this.wake = resolve; });
      }
    } finally { this.end(); this.queue.length = 0; this.buffered = 0; }
  }
}

/** SerialPacketWriter supplies queue bounds; each WS message is binary bytes.
 * Node ws hosts wrap its send(bytes,{binary:true},callback) in a Promise.
 * Browsers should additionally cap WebSocket.bufferedAmount in this callback.
 */
export function webSocketSink(sendBinary: ByteSink, options: {
  maxMessageBytes?: number;
  maxBufferedBytes?: number;
  bufferedAmount?: () => number;
} = {}): ByteSink {
  const maxMessage = limit(options.maxMessageBytes ?? MAX_PACKET + 4, 'maxMessageBytes');
  const maxBuffered = limit(options.maxBufferedBytes ?? 4 * 1024 * 1024, 'maxBufferedBytes');
  return async (data) => {
    if (!(data instanceof Uint8Array) || data.length > maxMessage) throw new ProtocolError('WebSocket send message limit exceeded');
    if (options.bufferedAmount) {
      const buffered = options.bufferedAmount();
      if (!Number.isSafeInteger(buffered) || buffered < 0 || data.length > maxBuffered - buffered) {
        throw new StateError('WebSocket send buffer limit exceeded');
      }
    }
    await sendBinary(data);
  };
}
