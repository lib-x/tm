#!/usr/bin/env python3
"""Adversarial demo regressions; run after npm ci && npm run build in sdk/node.

Run from any directory: python3 scripts/demo_adversarial.py
No builds or live SSH service are needed. The two stdio cases each allow the
real 15-second CLI deadline. Optionally check cached Rust/Go responders with
--rust-demo /path/to/mux_demo --go-demo /path/to/mux-demo-go. Rust additionally
checks crossed 4 KiB OS pipes and forged file echoes before full input/FIN.
"""
import argparse
import json
import os
from pathlib import Path
import select
import struct
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
NODE_ROOT = ROOT / 'sdk/node'
NODE = ['node', str(NODE_ROOT / 'examples/mux.mjs')]

# All protobuf serialization and session operations use the actual Node SDK.
# Python only relays the existing four-byte-length-prefixed frames.
HELPER = r"""
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { create } from '@bufbuild/protobuf';
import { Session, OpenSchema, ErrorCode, readPackets, SerialPacketWriter,
  nodeWritableSink } from './dist/index.js';

function check(ok, message) { if (!ok) throw new Error(message); }
async function ackOnly() {
  const session = new Session('responder');
  const writer = new SerialPacketWriter(nodeWritableSink(process.stdout));
  for await (const packet of readPackets(process.stdin)) {
    const result = session.receive(packet);
    await writer.writeAll(result.outbound);
    for (const event of result.events) {
      if (event.type === 'open') await writer.write(session.accept(event.id));
    }
  }
}
async function asymmetric(command) {
  const child = spawn(command[0], [...command.slice(1), 'stdio-server'],
    { stdio: ['pipe', 'pipe', 'pipe'] });
  let diagnostics = '';
  child.stderr.on('data', (bytes) => { diagnostics += bytes.toString(); });
  const exit = once(child, 'exit');
  exit.catch(() => {});
  const session = new Session('initiator', {
    maxData: 16384, streamWindow: 4096, connectionWindow: 65536, maxStreams: 5,
  }, ['bytes.v1']);
  const writer = new SerialPacketWriter(nodeWritableSink(child.stdin));
  const routes = ['terminal', 'events', 'file', 'clipboard', 'screenshot'];
  const payloads = [Buffer.from('terminal 你好\r\n'), Buffer.from('{"seq":1}\n'),
    Buffer.from(Array.from({ length: 8192 }, (_, i) => i % 251)),
    Buffer.from('clipboard 🌍'), Buffer.from([0, 255, 128, 1])];
  const streams = new Map();
  let completed = 0;
  let gotGoaway = false;
  let timer;
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error(`asymmetric echo stalled: ${completed}/5 FIN; ${diagnostics}`));
    }, 5000);
  });
  async function run() {
    await writer.write(session.start());
    for await (const packet of readPackets(child.stdout)) {
      const result = session.receive(packet);
      await writer.writeAll(result.outbound);
      for (const event of result.events) {
        if (event.type === 'ready') {
          for (const [index, route] of routes.entries()) {
            const opened = session.open(create(OpenSchema, { profile: 'bytes.v1', route }));
            streams.set(opened.id, { payload: payloads[index], received: 0, route });
            await writer.write(opened.packet);
          }
        } else if (event.type === 'accepted') {
          // In particular, file is ONE 8192-byte frame, larger than our window.
          await writer.write(session.send(event.id, streams.get(event.id).payload, true));
        } else if (event.type === 'data') {
          const state = streams.get(event.id);
          check(state, 'DATA for unknown/finished stream');
          const expected = state.payload.subarray(state.received, state.received + event.bytes.length);
          check(Buffer.from(event.bytes).equals(expected), `echo mismatch: ${state.route}`);
          state.received += event.bytes.length;
          if (event.bytes.length) await writer.writeAll(session.consume(event.id, event.bytes.length));
          if (event.fin) {
            check(state.received === state.payload.length, `early FIN: ${state.route}`);
            streams.delete(event.id);
            if (++completed === routes.length) await writer.writeAll(session.goaway());
          }
        } else if (event.type === 'reset') {
          throw new Error(`unexpected RESET: ${event.reason}`);
        } else if (event.type === 'goaway') {
          check(event.code === ErrorCode.CANCELLED && completed === 5 && streams.size === 0,
            'early/error GOAWAY');
          gotGoaway = true;
        }
      }
      if (gotGoaway) break;
    }
    check(gotGoaway, 'EOF before reciprocal GOAWAY');
    await writer.finish();
    child.stdin.end();
    const [code, signal] = await exit;
    check(code === 0, `responder exited ${code ?? signal}: ${diagnostics}`);
  }
  try { await Promise.race([run(), deadline]); }
  finally { clearTimeout(timer); session.close(); child.kill('SIGKILL'); }
}
try {
  if (process.argv[1] === 'ack-only') await ackOnly();
  else await asymmetric(JSON.parse(process.argv[2]));
} catch (error) { console.error(error.message); process.exitCode = 1; }
"""


# This peer uses valid TMP packets but lies about the file application: it sends
# the known fixture without consuming any request bytes or granting WINDOW.
RUST_FORGED_HELPER = r"""
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { Session, WouldBlock, readPackets, SerialPacketWriter,
  nodeWritableSink } from './dist/index.js';

function check(ok, message) { if (!ok) throw new Error(message); }
const child = spawn(process.argv[1], ['stdio-client'], { stdio: ['pipe', 'pipe', 'pipe'] });
child.stdin.on('error', () => {});
const exited = once(child, 'exit');
exited.catch(() => {});
let diagnostics = '';
child.stderr.on('data', (bytes) => { diagnostics += bytes.toString(); });
const session = new Session('responder');
const writer = new SerialPacketWriter(nodeWritableSink(child.stdin));
const payload = Buffer.from(Array.from({ length: 262144 }, (_, i) => i % 251));
const routes = new Map();
let fileID;
let sent = 0;
let received = 0;
let requestFin = false;
let timer;
async function pump() {
  if (fileID === undefined) return;
  while (sent < payload.length) {
    const end = Math.min(sent + 1024, payload.length);
    let packet;
    try { packet = session.send(fileID, payload.subarray(sent, end), end === payload.length); }
    catch (error) { if (error instanceof WouldBlock) return; throw error; }
    sent = end;
    await writer.write(packet);
  }
}
async function run() {
  try {
    for await (const packet of readPackets(child.stdout)) {
      const result = session.receive(packet);
      await writer.writeAll(result.outbound);
      for (const event of result.events) {
        if (event.type === 'open') {
          routes.set(event.id, event.open.route);
          await writer.write(session.accept(event.id));
          if (event.open.route === 'file') fileID = event.id;
        } else if (event.type === 'data') {
          if (routes.get(event.id) === 'file') {
            received += event.bytes.length;
            requestFin ||= event.fin;
            // Deliberately never consume: the request must stall at 64 KiB.
          } else {
            await writer.write(session.send(event.id, event.bytes, event.fin));
            if (event.bytes.length) await writer.writeAll(session.consume(event.id, event.bytes.length));
          }
        } else if (event.type === 'goaway') {
          // Let a vulnerable client finish its false success instead of waiting
          // for a timeout: the regression must reject its actual PASS/exit 0.
          await writer.writeAll(session.goaway());
          await writer.finish();
          child.stdin.end();
        }
      }
      await pump(); // WINDOW has no event and can release forged-response credit.
    }
  } catch (error) {
    // A rejecting client closes its carrier, which may interrupt a pending write.
    // Its exit status and diagnostic below must establish application rejection.
    if (error.code !== 'EPIPE' && error.code !== 'ECONNRESET'
        && !/closed|destroyed|write after end/i.test(error.message)) throw error;
  }
  const [code, signal] = await exited;
  check(code !== null && code !== 0, `client accepted forged echo (${code ?? signal}): ${diagnostics}`);
  check(received < payload.length && !requestFin, 'test accidentally sent the complete request + FIN');
  check(sent > received, `forged response never overtook request: sent=${sent}, received=${received}`);
  check(!/^PASS (?:file |all 5 streams)/m.test(diagnostics), `premature PASS: ${diagnostics}`);
  check(!/deadline/i.test(diagnostics), `deadline masked forged-echo rejection: ${diagnostics}`);
  check(/mismatch|unsent|premature|before.*(?:request|input)|(?:request|input).*incomplete/i.test(diagnostics),
    `missing forged-echo rejection diagnostic: ${diagnostics}`);
}
const deadline = new Promise((_, reject) => {
  timer = setTimeout(() => {
    child.kill('SIGKILL');
    reject(new Error(`forged-echo peer stalled: response=${sent}, request=${received}, FIN=${requestFin}; ${diagnostics}`));
  }, 5000);
});
try { await Promise.race([run(), deadline]); }
catch (error) { console.error(error.message); process.exitCode = 1; }
finally { clearTimeout(timer); session.close(); child.kill('SIGKILL'); }
"""


def helper(mode, *args):
    return ['node', '--input-type=module', '-e', HELPER, mode, *args]


def stop(process):
    if process.poll() is None:
        process.kill()
    process.wait(timeout=3)
    for stream in (process.stdin, process.stdout, process.stderr):
        if stream is not None:
            stream.close()


def read_exact(stream, count, deadline):
    chunks = []
    while count:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([stream], [], [], remaining)[0]:
            raise RuntimeError('timed out during stdio handshake')
        chunk = os.read(stream.fileno(), count)
        if not chunk:
            raise RuntimeError('EOF during stdio handshake')
        chunks.append(chunk)
        count -= len(chunk)
    return b''.join(chunks)


def read_frame(stream, deadline):
    header = read_exact(stream, 4, deadline)
    size, = struct.unpack('>I', header)
    if not 0 < size <= 131072:
        raise RuntimeError(f'invalid frame size (stdout contamination?): {size}')
    return header + read_exact(stream, size, deadline)


def asymmetric(command, label):
    result = subprocess.run(helper('asymmetric', json.dumps(command)), cwd=NODE_ROOT,
                            capture_output=True, timeout=8)
    if result.returncode != 0:
        raise RuntimeError(f'{label} asymmetric roundtrip failed: {result.stderr.decode()}')
    print(f'{label} asymmetric 4 KiB window / 16 KiB maxData / 8 KiB DATA + FIN: PASS', flush=True)


def blocked_stdio(*, eof=False):
    processes = []
    with tempfile.TemporaryFile() as client_log, tempfile.TemporaryFile() as peer_log:
        try:
            client = subprocess.Popen(NODE + ['stdio-client'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=client_log, cwd=ROOT, bufsize=0)
            processes.append(client)
            # Popen PIPE is an actual OS pipe, unlike Node's socket-backed spawn
            # pipes. On Linux, force a small capacity to guarantee backpressure.
            if sys.platform.startswith('linux'):
                import fcntl
                fcntl.fcntl(client.stdout.fileno(), fcntl.F_SETPIPE_SZ, 4096)
            peer = subprocess.Popen(helper('ack-only'), stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=peer_log, cwd=NODE_ROOT, bufsize=0)
            processes.append(peer)
            deadline = time.monotonic() + 5
            # One HELLO/WELCOME and five OPEN/ACK frames; then never read the
            # client's DATA. Its 256 KiB file must fill stdout before completion.
            for _ in range(6):
                peer.stdin.write(read_frame(client.stdout, deadline))
                client.stdin.write(read_frame(peer.stdout, deadline))
            if eof:
                client.stdin.close()
            try:
                code = client.wait(timeout=18)
            except subprocess.TimeoutExpired as error:
                raise RuntimeError('stdio client survived its 15-second deadline with stdout blocked') from error
            client_log.seek(0)
            diagnostics = client_log.read()
            if code == 0 or b'PASS all 5 streams' in diagnostics:
                raise RuntimeError(f'stdio client falsely succeeded: {diagnostics.decode()}')
            if not eof and b'deadline' not in diagnostics.lower():
                raise RuntimeError(f'stdio failed before exercising deadline: {diagnostics.decode()}')
            reason = ' + stdin EOF' if eof else ''
            print(f'Node full stdout OS pipe{reason}: nonzero exit within deadline: PASS', flush=True)
        finally:
            for process in reversed(processes):
                stop(process)


def rust_small_pipes(binary):
    if not sys.platform.startswith('linux'):
        print('Rust crossed 4 KiB OS pipes: SKIP (Linux F_SETPIPE_SZ required)', flush=True)
        return
    import fcntl
    descriptors = []
    processes = []
    with tempfile.TemporaryFile() as client_log, tempfile.TemporaryFile() as server_log:
        try:
            client_read, server_write = os.pipe()
            descriptors.extend((client_read, server_write))
            server_read, client_write = os.pipe()
            descriptors.extend((server_read, client_write))
            for descriptor in (server_write, client_write):
                fcntl.fcntl(descriptor, fcntl.F_SETPIPE_SZ, 4096)
                if fcntl.fcntl(descriptor, fcntl.F_GETPIPE_SZ) != 4096:
                    raise RuntimeError('could not establish exactly 4 KiB pipe capacity')
            processes.append(subprocess.Popen([str(binary), 'stdio-server'], cwd=ROOT,
                stdin=server_read, stdout=server_write, stderr=server_log))
            processes.append(subprocess.Popen([str(binary), 'stdio-client'], cwd=ROOT,
                stdin=client_read, stdout=client_write, stderr=client_log))
            for descriptor in descriptors:
                os.close(descriptor)
            descriptors.clear()
            deadline = time.monotonic() + 5
            try:
                codes = [process.wait(timeout=max(0.01, deadline - time.monotonic()))
                         for process in processes]
            except subprocess.TimeoutExpired as error:
                raise RuntimeError('Rust crossed 4 KiB pipes stalled before completing five streams') from error
            client_log.seek(0)
            server_log.seek(0)
            output, errors = client_log.read(), server_log.read()
            expected = [b'PASS terminal 23 bytes', b'PASS events 69 bytes',
                        b'PASS file 262144 bytes', b'PASS clipboard 27 bytes',
                        b'PASS screenshot 68 bytes', b'PASS all 5 streams']
            if codes != [0, 0] or sorted(output.splitlines()) != sorted(expected):
                raise RuntimeError(f'Rust 4 KiB pipe exchange failed {codes}: {output.decode()} {errors.decode()}')
            print('Rust crossed 4 KiB OS pipes: five streams + reciprocal GOAWAY: PASS', flush=True)
        finally:
            for descriptor in descriptors:
                os.close(descriptor)
            for process in reversed(processes):
                stop(process)


def rust_forged_echo(binary):
    result = subprocess.run(['node', '--input-type=module', '-e', RUST_FORGED_HELPER,
                             str(binary)], cwd=NODE_ROOT, capture_output=True, timeout=8)
    if result.returncode != 0:
        raise RuntimeError(f'Rust forged-echo regression failed: {result.stderr.decode()}')
    print('Rust forged file echo before full request + FIN: explicit rejection without PASS: PASS', flush=True)


def ssh_arguments():
    # Inspect argv via a fake ssh. This cannot connect to a host or run a remote
    # shell, and exits before any handshake or output-file creation can occur.
    with tempfile.TemporaryDirectory(prefix='tm-ssh-args-') as directory:
        tmp = Path(directory)
        log = tmp / 'argv.json'
        fake = tmp / 'ssh'
        fake.write_text(f'#!{sys.executable}\nimport json, sys\nfrom pathlib import Path\n'
                        f'Path({str(log)!r}).write_text(json.dumps(sys.argv[1:]))\nsys.exit(73)\n')
        fake.chmod(0o755)
        env = dict(os.environ, PATH=str(tmp) + os.pathsep + os.environ.get('PATH', ''))
        result = subprocess.run(NODE + ['ssh-client'], cwd=ROOT, env=env,
                                capture_output=True, timeout=5)
        if result.returncode == 0 or b'requires a host' not in result.stderr or log.exists():
            raise RuntimeError(f'bare ssh-client was not rejected before spawn: {result.stderr.decode()}')
        source = tmp / 'input'
        source.write_bytes(b'\x00TMP\xff')
        output = tmp / 'output'
        for command in (None, '/operator/chosen-demo stdio-server'):
            args = ['ssh-client', 'example.invalid']
            if command is not None:
                args.append(command)
            args += ['--file', str(source), '--output', str(output)]
            result = subprocess.run(NODE + args, cwd=ROOT, env=env,
                                    capture_output=True, timeout=5)
            if result.returncode == 0 or not log.exists():
                raise RuntimeError(f'SSH file flags failed parsing: {result.stderr.decode()}')
            argv = json.loads(log.read_text())
            expected = ['-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                        'example.invalid', command or 'mux-demo stdio-server']
            if argv != expected or output.exists():
                raise RuntimeError(f'incorrect SSH argv or premature output creation: {argv!r}')
            log.unlink()
        print('Node SSH required host + optional command/file flags (fake ssh, no network): PASS', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rust-demo', type=Path, help='optional already-built Rust client/responder (also runs Rust regressions)')
    parser.add_argument('--go-demo', type=Path, help='optional already-built Go responder')
    args = parser.parse_args()
    asymmetric(NODE, 'Node')
    for label, binary in (('Rust', args.rust_demo), ('Go', args.go_demo)):
        if binary is not None:
            asymmetric([str(binary.resolve())], label)
    if args.rust_demo is not None:
        rust_small_pipes(args.rust_demo.resolve())
        rust_forged_echo(args.rust_demo.resolve())
    ssh_arguments()
    blocked_stdio()
    blocked_stdio(eof=True)


if __name__ == '__main__':
    main()
