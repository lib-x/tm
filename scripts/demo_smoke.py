#!/usr/bin/env python3
"""Run the actual demo programs: 9 TCP + 9 stdio pairs, WS, files, failures, QUIC.

Run from any directory. Use --built after scripts/check.sh has built Node.
All peers bind loopback ephemeral ports and are killed on a bounded timeout.
"""
import argparse
import hashlib
import itertools
import os
from pathlib import Path
import queue
import socket
import subprocess
import tempfile
import threading

ROOT = Path(__file__).resolve().parents[1]
LANGUAGES = ('rust', 'go', 'node')
TIMEOUT = 25


def checked(command, **kwargs):
    return subprocess.run(command, check=True, timeout=120, **kwargs)


def finish(process):
    if process.poll() is None:
        process.kill()
    process.wait(timeout=5)
    for stream in (process.stdin, process.stdout, process.stderr):
        if stream is not None:
            stream.close()


def assert_pass(result):
    if result.returncode != 0 or b'PASS all 5 streams' not in result.stdout:
        raise RuntimeError(f'client failed ({result.returncode}):\n{result.stdout.decode()}\n{result.stderr.decode()}')
    for route in ('terminal', 'events', 'file', 'clipboard', 'screenshot'):
        if f'PASS {route} '.encode() not in result.stdout:
            raise RuntimeError(f'missing successful route: {route}')


def start_server(command, mode, log):
    server = subprocess.Popen(command + [mode, '127.0.0.1:0'], stdout=subprocess.PIPE, stderr=log, cwd=ROOT)
    lines = queue.Queue()
    threading.Thread(target=lambda: lines.put(server.stdout.readline(1024)), daemon=True).start()
    try:
        line = lines.get(timeout=TIMEOUT).decode().strip()
        if not line.startswith('LISTEN '):
            raise RuntimeError(f'server failed readiness: {line!r}')
        return server, line.removeprefix('LISTEN ')
    except BaseException:
        finish(server)
        log.seek(0)
        print(log.read().decode(), end='')
        raise


def network_pair(client, server, *, websocket=False, extra=(), expect_success=True):
    with tempfile.TemporaryFile() as log:
        process, address = start_server(server, 'ws-server' if websocket else 'server', log)
        try:
            result = subprocess.run(client + ['ws-client' if websocket else 'client', address, *extra],
                                    cwd=ROOT, capture_output=True, timeout=TIMEOUT)
            if expect_success:
                assert_pass(result)
            elif result.returncode == 0:
                raise RuntimeError('client unexpectedly succeeded')
            code = process.wait(timeout=TIMEOUT)
            if code != 0:
                log.seek(0)
                raise RuntimeError(f'server failed ({code}): {log.read().decode()}')
            return result
        finally:
            finish(process)


def stdio_pair(client, server):
    # Two pipes cross-connect raw bytes. No shell or text line conversion.
    client_read, server_write = os.pipe()
    server_read, client_write = os.pipe()
    processes = []
    with tempfile.TemporaryFile() as client_log, tempfile.TemporaryFile() as server_log:
        try:
            try:
                processes.append(subprocess.Popen(server + ['stdio-server'], cwd=ROOT,
                    stdin=server_read, stdout=server_write, stderr=server_log))
                processes.append(subprocess.Popen(client + ['stdio-client'], cwd=ROOT,
                    stdin=client_read, stdout=client_write, stderr=client_log))
            finally:
                for descriptor in (client_read, server_write, server_read, client_write):
                    os.close(descriptor)
            codes = [process.wait(timeout=TIMEOUT) for process in processes]
            client_log.seek(0)
            server_log.seek(0)
            output = client_log.read()
            errors = server_log.read()
            if codes != [0, 0] or b'PASS all 5 streams' not in output:
                raise RuntimeError(f'stdio failed {codes}: {output.decode()} {errors.decode()}')
        finally:
            for process in processes:
                finish(process)


def bad_carrier(server, malformed):
    with tempfile.TemporaryFile() as log:
        process, address = start_server(server, 'server', log)
        try:
            host, port = address.rsplit(':', 1)
            with socket.create_connection((host, int(port)), timeout=3) as peer:
                if malformed:
                    peer.sendall(b'\x00\x00\x00\x00')  # Forbidden empty Packet.
            if process.wait(timeout=TIMEOUT) == 0:
                raise RuntimeError('server accepted invalid framing or premature EOF')
        finally:
            finish(process)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--built', action='store_true', help='Node dependencies/build already prepared')
    parser.add_argument('--skip-quic', action='store_true', help='skip isolated QUIC module when localhost UDP is unavailable')
    args = parser.parse_args()
    checked(['cargo', 'build', '--locked', '--example', 'mux_demo'], cwd=ROOT)
    if not args.built:
        checked(['npm', 'ci'], cwd=ROOT / 'sdk/node')
        checked(['npm', 'run', 'build'], cwd=ROOT / 'sdk/node')
    with tempfile.TemporaryDirectory(prefix='tm-demos-') as temporary:
        tmp = Path(temporary)
        go_binary = tmp / 'mux-demo-go'
        checked(['go', 'build', '-mod=readonly', '-o', str(go_binary), './cmd/mux-demo'], cwd=ROOT / 'sdk/go')
        target = Path(os.environ.get('CARGO_TARGET_DIR', ROOT / 'target'))
        if not target.is_absolute():
            target = ROOT / target
        commands = {
            'rust': [str(target / 'debug/examples/mux_demo')],
            'go': [str(go_binary)],
            'node': ['node', str(ROOT / 'sdk/node/examples/mux.mjs')],
        }
        for client, server in itertools.product(LANGUAGES, repeat=2):
            network_pair(commands[client], commands[server])
            print(f'demo TCP {client} -> {server}: PASS', flush=True)
        for client, server in itertools.product(LANGUAGES, repeat=2):
            stdio_pair(commands[client], commands[server])
            print(f'demo stdio {client} -> {server}: PASS', flush=True)
        network_pair(commands['node'], commands['node'], websocket=True)
        print('demo WebSocket node -> node: PASS', flush=True)
        for language in LANGUAGES:
            bad_carrier(commands[language], malformed=False)
            bad_carrier(commands[language], malformed=True)
            print(f'demo {language} premature EOF + invalid framing: PASS', flush=True)
        # Real files, including zero-length FIN, across all server languages.
        for language, data in zip(LANGUAGES, (b'', bytes(range(256)) * 1601, b'TMP\x00binary\xff' * 30001)):
            source = tmp / f'{language}.input'
            output = tmp / f'{language}.output'
            source.write_bytes(data)
            result = network_pair(commands['node'], commands[language],
                extra=['--file', str(source), '--output', str(output)])
            assert output.read_bytes() == data
            assert hashlib.sha256(data).hexdigest().encode() in result.stdout
            # wx must refuse replacement and preserve the previous contents.
            network_pair(commands['node'], commands[language],
                extra=['--file', str(source), '--output', str(output)], expect_success=False)
            assert output.read_bytes() == data
            print(f'demo file node -> {language}: PASS ({len(data)} bytes, SHA-256, overwrite refused)', flush=True)
        too_large = tmp / 'oversize'
        with too_large.open('wb') as file:
            file.truncate(8 * 1024 * 1024 + 1)
        rejected = subprocess.run(commands['node'] + ['client', '--file', str(too_large)],
                                  capture_output=True, timeout=TIMEOUT, cwd=ROOT)
        assert rejected.returncode != 0 and b'8 MiB' in rejected.stderr
        print('demo oversized file refused: PASS', flush=True)
        checked(['python3', str(ROOT / 'scripts/demo_adversarial.py'),
                 '--rust-demo', commands['rust'][0], '--go-demo', str(go_binary)], cwd=ROOT)
    if not args.skip_quic:
        subprocess.run(['python3', str(ROOT / 'examples/quic-go/smoke.py'), '--race'],
                       cwd=ROOT, check=True, timeout=240)


if __name__ == '__main__':
    main()
