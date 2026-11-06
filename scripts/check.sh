#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
buf format --diff --exit-code
buf lint
buf breaking --against conformance/v1-baseline.binpb
python3 scripts/check_generated.py
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo doc --locked --no-deps
(cd sdk/go && go test -mod=readonly -race ./... && go vet -mod=readonly ./...)
(cd sdk/node && npm ci && npm run build && npm test)
python3 scripts/interop.py
(cd examples/quic-go && go test -mod=readonly -race ./... && go vet -mod=readonly ./...)
python3 scripts/demo_smoke.py --built
