# TMP over QUIC

This standalone module runs the same five-route byte echo driver used by the
Go TCP and stdio demos: terminal, events, file, clipboard, and screenshot. The
fixtures are application bytes; the demo does not execute commands, access your
clipboard, or capture the screen.

Requires **Go 1.26 or newer**, matching quic-go v0.65.0's minimum Go version.
The SDK dependency is v0.1.0 with a local replacement to `../../sdk/go`.
quic-go and its dependencies belong only to this example module.

From the repository root:

```sh
cd examples/quic-go
# Fresh files only; certificate generation refuses to overwrite an existing key.
go run . cert --cert cert.pem --key key.pem
# Terminal 1:
go run . server 127.0.0.1:7001 --cert cert.pem --key key.pem
# Terminal 2:
go run . client 127.0.0.1:7001 --ca cert.pem
```

The address defaults to `127.0.0.1:7001` when omitted. Put the optional address
before flags. The server prints `LISTEN <actual address>` to stdout before
accepting a connection; use `127.0.0.1:0` for an ephemeral UDP port. The client
prints the shared driver's per-route PASS reports and `PASS all 5 streams` to
stdout. A zero exit status also confirms orderly QUIC shutdown. Errors go to
stderr. The server handles one connection and exits. Each run has a 15-second
context and stream deadline, including accept, handshake, and shutdown.

The certificate is a self-signed ECDSA P-256 localhost demo certificate, valid
for 24 hours, with DNS `localhost` and IP SANs `127.0.0.1` and `::1`. The private
key is created with mode 0600. The client requires an explicit `--ca` file and
verifies both trust and the address hostname/IP using normal TLS verification;
there is no verification bypass. The certificate works with `localhost:7001`,
`127.0.0.1:7001`, or `[::1]:7001` when the server listens on the same address.
Delete generated certificates and keys when finished; do not commit them.

This is **local demo TLS with server authentication only**. It does not
authenticate clients. A production host must add client authentication and
application authorization, appropriate certificates, and its own lifecycle
policy.

TMP runs over **one QUIC bidirectional stream** with ALPN `tmp-demo-v1`.
All five logical routes stay inside TMP; this does not map them to native QUIC
streams. The example imports `muxdemo.Run(reader, writer, initiator, log)` and
adds transport setup and cleanup without changing the SDK, its dependencies,
or the core protocol. Only the reusable driver owns a TMP Session.

The driver finishes the reciprocal GOAWAY exchange before transport cleanup.
The responder then closes its stream's send direction. The initiator reads
that EOF before closing its own send direction; the responder reads the
initiator's EOF before issuing QUIC `CloseWithError(0, ...)`. The initiator
waits for that successful remote connection close. This ordering proves the
responder's final GOAWAY reached the client before connection teardown can
discard buffered stream writes. Every wait is bounded by the same deadline.

For an automated smoke test, run from the repository root:

```sh
python3 examples/quic-go/smoke.py
# Exercise the shared driver and QUIC transport with the Go race detector:
python3 examples/quic-go/smoke.py --race
```

The runner builds in a temporary directory, generates two independent
certificates, starts a server on an ephemeral localhost port, checks all five
routes and both process exits, verifies rejection of the unrelated CA and a
missing `--ca`, and cleans up its child processes and files. It returns nonzero
on failure. A parent smoke harness can invoke this command as an optional QUIC
check; no global installation or fixed port is required. After dependencies
are downloaded, the test needs only localhost UDP access.
