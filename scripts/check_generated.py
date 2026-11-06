#!/usr/bin/env python3
"""Check Buf reproducibility, including untracked initial generated files."""
from hashlib import sha256
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
DIRECTORIES = ["sdk/rust/src/generated", "sdk/go/pb", "sdk/node/src/gen"]

def snapshot():
    result = {}
    for directory in DIRECTORIES:
        for path in sorted((ROOT / directory).rglob("*")):
            if path.is_file():
                result[str(path.relative_to(ROOT))] = sha256(path.read_bytes()).hexdigest()
    return result

before = snapshot()
subprocess.run(["buf", "generate"], cwd=ROOT, check=True)
after = snapshot()
changed = sorted(name for name in before.keys() | after.keys() if before.get(name) != after.get(name))
if changed:
    raise SystemExit("Buf generated files changed; review and commit them:\n" + "\n".join(changed))
print("Buf generated files: reproducible")
