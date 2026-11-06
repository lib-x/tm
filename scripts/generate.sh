#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
buf format --diff --exit-code
buf lint
buf generate
