#!/usr/bin/env python3
"""Wire model matrix and six real bidirectional state-machine exchanges."""
import itertools
from pathlib import Path
import queue
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
COMMANDS = {
    "rust": (["cargo", "run", "--quiet", "--locked", "--example", "conformance", "--"], ROOT),
    "go": (["go", "run", "-mod=readonly", "./cmd/conformance"], ROOT / "sdk/go"),
    "node": (["node", "dist/conformance.js"], ROOT / "sdk/node"),
}


def run(lang, args, data=None):
    cmd, cwd = COMMANDS[lang]
    return subprocess.run(cmd + args, cwd=cwd, input=data, capture_output=True, timeout=60, check=True).stdout


def endpoint_exchange(left, right):
    processes = []
    messages = queue.Queue()
    logs = [[], []]
    def reader(index, process):
        for line in process.stdout:
            messages.put((index, line))
        messages.put((index, None))
    def errors(index, process):
        logs[index].append(process.stderr.read())
    try:
        for index, (lang, role) in enumerate(((left, "initiator"), (right, "responder"))):
            cmd, cwd = COMMANDS[lang]
            proc = subprocess.Popen(cmd + ["endpoint", role], cwd=cwd, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(proc)
            threading.Thread(target=reader, args=(index, proc), daemon=True).start()
            threading.Thread(target=errors, args=(index, proc), daemon=True).start()
        ended = set()
        frame_count = 0
        deadline = time.monotonic() + 60
        while len(ended) < 2:
            index, line = messages.get(timeout=max(0.01, deadline-time.monotonic()))
            if line is None:
                ended.add(index)
                continue
            # Validate output really is a hex frame before forwarding it.
            raw = bytes.fromhex(line.decode().strip())
            assert len(raw) >= 5 and int.from_bytes(raw[:4], "big") == len(raw)-4
            other = processes[1-index]
            if other.poll() is not None:
                raise RuntimeError("peer exited before the exchange ended")
            other.stdin.write(line)
            other.stdin.flush()
            frame_count += 1
        for proc in processes:
            assert proc.wait(timeout=5) == 0, logs
        assert frame_count >= 12, frame_count
        print(f"state interop {left} -> {right}: PASS ({frame_count} frames)")
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream:
                    stream.close()


def main():
    golden = (ROOT / "conformance/golden.hex").read_bytes()
    for source in COMMANDS:
        emitted = run(source, ["emit"])
        for target in COMMANDS:
            run(target, ["verify"], emitted)
            print(f"wire interop {source} -> {target}: PASS")
        run(source, ["verify"], golden)
    for left, right in itertools.permutations(COMMANDS, 2):
        endpoint_exchange(left, right)

if __name__ == "__main__":
    main()
