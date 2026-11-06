import { ErrorCode } from './gen/tmp/v1/mux_pb.js';

export class ProtocolError extends Error {
  constructor(message: string, readonly code: ErrorCode = ErrorCode.PROTOCOL) {
    super(message);
    this.name = 'ProtocolError';
  }
}

export class WouldBlock extends Error {
  constructor() {
    super('insufficient stream or connection send credit');
    this.name = 'WouldBlock';
  }
}

export class StateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'StateError';
  }
}
