#!/usr/bin/env python3
"""Build and smoke-test the isolated QUIC demo on ephemeral localhost UDP ports."""

import argparse
from contextlib import contextmanager
from pathlib import Path
import select
import subprocess
import tempfile

MODULE = Path(__file__).resolve().parent


def run(*args, **kwargs):
    result = subprocess.run(
        list(map(str, args)), capture_output=True, text=True, timeout=25, **kwargs
    )
    if "DATA RACE" in result.stderr:
        raise RuntimeError(result.stderr)
    return result


def checked(*args):
    result = run(*args)
    if result.returncode:
        raise RuntimeError(f"{args}: {result.stdout}{result.stderr}")
    return result


@contextmanager
def server(binary, cert, key):
    process = subprocess.Popen(
        [str(binary), "server", "127.0.0.1:0", "--cert", str(cert), "--key", str(key)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        if not select.select([process.stdout], [], [], 10)[0]:
            raise RuntimeError("server did not announce its address")
        line = process.stdout.readline().strip()
        if not line.startswith("LISTEN "):
            raise RuntimeError(f"expected LISTEN, got {line!r}")
        yield process, line.removeprefix("LISTEN ")
    finally:
        stopped_by_runner = process.poll() is None
        if stopped_by_runner:
            process.terminate()
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate(timeout=5)
        allowed = (0, -15, -9) if stopped_by_runner else (0,)
        if process.returncode not in allowed:
            raise RuntimeError(f"server exited {process.returncode}: {stdout}{stderr}")
        if "DATA RACE" in stderr:
            raise RuntimeError(stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--race", action="store_true", help="build with Go's race detector")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="tmp-quic-demo-") as directory:
        work = Path(directory)
        binary = work / "quic-demo"
        build = ["go", "build"]
        if args.race:
            build.append("-race")
        subprocess.run(build + ["-o", str(binary), "."], cwd=MODULE, check=True, timeout=180)
        cert, key = work / "cert.pem", work / "key.pem"
        other_cert, other_key = work / "other.pem", work / "other-key.pem"
        checked(binary, "cert", "--cert", cert, "--key", key)
        checked(binary, "cert", "--cert", other_cert, "--key", other_key)
        with server(binary, cert, key) as (process, address):
            result = checked(binary, "client", address, "--ca", cert)
            if "PASS all 5 streams" not in result.stdout:
                raise RuntimeError(f"missing client PASS: {result.stdout}{result.stderr}")
            print(result.stdout, end="")
            process.wait(timeout=5)
        with server(binary, cert, key) as (_, address):
            rejected = run(binary, "client", address, "--ca", other_cert)
            if rejected.returncode <= 0 or "certificate" not in rejected.stderr.lower() or "PASS" in rejected.stdout:
                raise RuntimeError(f"unrelated CA was not rejected: {rejected.stdout}{rejected.stderr}")
        missing = run(binary, "client", "127.0.0.1:7001")
        if missing.returncode == 0 or "requires --ca" not in missing.stderr:
            raise RuntimeError("client accepted a missing --ca")
        print("PASS QUIC shutdown, unrelated CA rejection, required --ca")


if __name__ == "__main__":
    main()
