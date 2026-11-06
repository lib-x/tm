# tunnel-mux Rust SDK

Install with `cargo add tunnel-mux@0.1.0`. Source and releases: <https://github.com/lib-x/tm>.

```rust
use tunnel_mux::{Session, Role, default_settings, pb};
let mut session = Session::new(Role::Initiator, default_settings(), vec!["bytes.v1".into()])?;
let hello = session.start()?;
// Send every returned packet in order through framing::encode or transport::write_packet.
# Ok::<(), tunnel_mux::Error>(())
```

`session.receive(packet)` returns `Outcome {outbound, events}`. Send outbound first, then process events. On `Event::Open`, authorize the authenticated peer and route before calling `accept(id)`. `send` requires ACK and explicit chunking to the peer's `max_data`; insufficient credit returns `Error::WouldBlock` without mutation. `consume(id,n)` returns WINDOW frames, and `reset`/`goaway` return ordered packet lists. `close()` reports all active streams as failed on carrier EOF/error. Session has a single serial owner; returned packets are already committed to its state.

`framing::Decoder::feed` accepts arbitrary binary chunks and calls back one packet at a time, retaining at most one partial frame. Call `finish()` on EOF. `transport::read_packet`/`write_packet` accept Tokio AsyncRead/AsyncWrite, so authenticated SSH channels, QUIC streams and TLS streams share the same adapter. Map WebSocket binary payloads into `Decoder::feed`, and send `framing::encode` as binary; reject text, enforce WS message limits and serialize writes. Transport authentication and all deadlines belong to the host. These async read/write helpers are not cancellation safe: dropping an in-flight future (including timeouts/select) requires closing the carrier and calling `Session::close()`. Never continue or retry on that carrier.

Generated models in `src/generated` come exclusively from root `buf generate`; end users do not need Buf/protoc. The core contains no application messages. Define terminal/file/clipboard/event formats in your own application; fixed64 ping tokens preserve all 64 bits.

Run `cargo test -p tunnel-mux` and `cargo run --example conformance -- endpoint initiator` (hex line protocol for integration testing). See root `docs/protocol-v1.md` for required carrier bindings and profiles. This SDK does not implement a desktop codec, relay server or automatic session replay.
