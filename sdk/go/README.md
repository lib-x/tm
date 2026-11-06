# TMP Go SDK

Module: `github.com/lib-x/tm/sdk/go`, package `tm`, SDK version 0.1.0,
Go 1.24+. Wire protocol: `tmp.pb.v1` (version 1).

The SDK implements a single-owner Sans-I/O session, strict protobuf packet
framing, and synchronous `io.Reader` / `io.Writer` helpers. It creates no
connections, timers, queues, retries, or background goroutines. Authentication,
authorization, timeouts, bounded scheduling and transport lifecycle belong to
the host. Profiles and routes are opaque application identifiers.

```go
import (
    tm "github.com/lib-x/tm/sdk/go"
)

s, err := tm.NewSession(tm.Initiator, tm.DefaultSettings(), []string{"bytes.v1"})
if err != nil { return err }
hello, err := s.Start()
if err != nil { return err }
if err := tm.WritePacket(writer, hello); err != nil {
    notify(s.Close())
    return err
}
// Read frames with tm.ReadPacket(reader), apply s.Receive(packet), transmit
// every result.Outbound in order, then handle result.Events.
```

See [`example_test.go`](example_test.go) for a runnable, complete in-memory
handshake and bidirectional exchange; `go test` checks its output. For an actual
byte transport, initialize `tm.Transport{Session: s, Reader: reader, Writer:
writer}` and call `Receive()` one packet at a time. This helper already sends
`Result.Outbound`; do not send those frames again. It closes the session and
returns failure events on read, protocol, or automatic response write errors.
`Transport.Write(packets...)` validates its entire batch before writing, sends
explicit operation results, and marks the session failed on transport errors;
call `Session.Close()` to retrieve failure events.
The transport helper leaves ownership of the underlying reader/writer to the host.

## Session ownership and operations

Serialize **all** session operations and transport writes. Sessions and decoders
are not safe for concurrent use or reentry. Transmit every returned frame in
method order; do not retry after a partial transport write.

- `NewSession(role, settings, profiles)` validates arguments and copies settings.
  `DefaultSettings()` returns fresh validated defaults. `Start()` emits Hello
  only for a new initiator. A responder starts by receiving Hello.
- `Receive(packet)` returns ordered outbound frames and application events.
  Send outbound first, then process events. `Ready` means negotiation finished;
  `Opened` requires host authorization before `Accept(id)` or `Reset(id, ...)`.
- `Open(*pb.Open)` returns `(id, packet, error)` and creates a pending stream.
  Both roles can open streams; `Accept(id)` emits ACK for a remote pending stream.
- `Send(id, payload, fin)` requires acceptance and copies payload. It never
  splits or queues data. Use `PeerSettings().MaxData` to split writes and retain
  unsent bytes when `errors.Is(err, tm.ErrWouldBlock)`. The same error denotes
  the peer's concurrent stream limit. Resume after WINDOW or stream reclamation.
- `Consume(id, n)` immediately returns stream WINDOW then connection WINDOW.
  Delivered bytes still occupy receive credit until consumed. `n=0` is a no-op
  for a known stream. Overconsumption and invalid local arguments do not mutate
  state. Both FINs retain unread bytes until the outstanding count reaches zero.
- `Reset(id, code, reason)` deletes the stream and returns RESET followed by a
  connection WINDOW refund if unread bytes exist. It never emits a destroyed
  stream's WINDOW. `StreamReset` events report remote cancellation. Reset and
  GoAway reasons must be valid UTF-8 and at most 256 encoded bytes; oversized
  incoming reasons fail the session, including RESET for historical streams.
- `GoAway(code, reason)` returns `[]*pb.Packet`: RESET(REFUSED) for every remote
  pending stream, followed by GOAWAY. These reset packets identify locally
  cancelled streams. Draining forbids both Open and Accept. Receiving GOAWAY
  cancels all pending streams with reset events and returns RESET for remote
  pending streams; accepted streams continue. Cutoffs cannot revoke any stream
  historically ACKed by the peer, including already closed streams.
- `Ping(token)` is allowed while ready or draining. PING receives an automatic
  PONG; `ReceivedPong` carries the full `uint64` token.
- `PeerSettings()` and `NegotiatedProfiles()` return defensive copies.
  `PeerSettings()` is nil before handshake. `IsReady()`, `Draining()` and
  `Failed()` expose state. `Profiles()` also returns the negotiated profile set.
- Protocol errors are terminal. `Close()` is permitted after failure, marks all
  active streams failed, returns `StreamReset(INTERNAL)` events, and emits no
  wire packets. Repeated close produces no duplicate events. A transport EOF
  closes the entire session. `Receive` errors leave stream notifications for
  `Close()`; callers must notify their applications.

Returned packets and events are caller-owned. Mutating byte slices, settings,
metadata, or profile lists cannot alter the session's bookkeeping. Unknown body
fields are protobuf-compatible; unknown Packet top-level fields are rejected.

## Framing and transport adapters

`Encode(packet)` / `Decode(frame)` operate on one complete `u32be length ||
protobuf Packet` frame. Length must be 1..131072. Decoding scans all top-level
tags **before** generated protobuf unmarshalling, rejecting unknown tags,
duplicate stream IDs or bodies, wrong wire types and overflowing stream IDs.

```go
var decoder tm.Decoder // zero value ready
err := decoder.Feed(chunk, func(packet *pb.Packet) error {
    result, err := s.Receive(packet)
    if err != nil { return err }
    if err := sendInOrder(result.Outbound); err != nil { return err }
    return handleEvents(result.Events)
})
// At EOF: decoder.Finish(); notify(s.Close()) regardless of clean/truncated EOF.
// On a Feed error, fail and notify the Session via Close immediately.
```

Feed supports arbitrary fragmentation/coalescing, delivers frames immediately,
and retains at most one partial frame (`MAX_PACKET + 4` bytes). Callback errors
are terminal too. `Finish()` detects truncation and ends input. A decoder never
recovers after error. Feed budget and control-frame rate limits belong to the
host. A WebSocket host feeds binary-message payloads into this decoder, preserving
order; reject text messages. `ReadPacket` and `WritePacket` support synchronous
streams and short writes, and validate lengths before allocating packet storage.

## Conformance

```sh
go run ./cmd/conformance emit
go run ./cmd/conformance emit | go run ./cmd/conformance verify
go run ./cmd/conformance endpoint initiator
go run ./cmd/conformance endpoint responder
go test -race ./...
go vet ./...
```

Emit produces the ten framed-packet hex lines from `docs/sdk-contract.md`.
Verify decodes and compares message field values rather than serialized byte
order. It rejects missing, extra or semantically incorrect lines. Endpoint mode
is a streaming stdin/stdout hex-line protocol; connect the two processes through
crossed pipes. Each line flushes immediately. It performs handshake, OPEN/ACK,
`你好 TMP` / `ok` exchange, both window refunds, and bidirectional GOAWAY.
Stdout contains only packet hex; diagnostics use stderr.

Protobuf models in `pb/tmp/v1` are generated centrally with Buf. Do not edit them
or run an independent SDK-specific generator.

## Runnable application demos

See the [demo guide](../../examples/README.md) for interoperable Rust/Go/Node clients and servers, five concurrent application streams, flow control, file roundtrip, and transport bindings. All application data remains outside the core schema.
