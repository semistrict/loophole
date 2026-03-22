#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# ///
"""Run Go benchmarks and save results with metadata to bench-results/."""

import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def run(cmd: list[str], **kwargs) -> str:
    return subprocess.check_output(cmd, text=True, **kwargs).strip()


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    os.chdir(root)

    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    buildtags = os.environ.get("BUILDTAGS", "")

    # Collect git metadata.
    sha = run(["git", "rev-parse", "--short", "HEAD"])
    dirty_rc = subprocess.call(
        ["git", "diff", "--quiet"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    ) | subprocess.call(
        ["git", "diff", "--cached", "--quiet"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    dirty = dirty_rc != 0
    diff = run(["git", "diff", "HEAD"]) if dirty else ""

    # Determine packages (same filter as `make test`).
    all_pkgs = run(
        ["go", "list", "-tags", buildtags, "./..."]
    ).splitlines()
    pkgs = [
        p for p in all_pkgs if not p.endswith(("/e2e", "/linuxutil", "/containerstorage"))
    ]

    # Run benchmarks, streaming output to terminal and capturing for the JSON file.
    print(f"Running benchmarks (count={count})...")
    proc = subprocess.Popen(
        [
            "go", "test",
            "-tags", buildtags,
            "-bench=.",
            "-run=^$",
            "-benchmem",
            f"-count={count}",
            "-timeout", "600s",
        ]
        + pkgs,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, "LOG_LEVEL": "error"},
    )
    lines: list[str] = []
    for line in proc.stdout:
        sys.stdout.write(line)
        lines.append(line)
    proc.wait()
    raw = "".join(lines)

    # Build result JSON.
    now = datetime.now(timezone.utc)
    out = {
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "head_sha": sha,
        "dirty": dirty,
        "diff": diff,
        "go_version": run(["go", "version"]),
        "goos": run(["go", "env", "GOOS"]),
        "goarch": run(["go", "env", "GOARCH"]),
        "hostname": platform.node(),
        "count": count,
        "exit_code": proc.returncode,
        "raw": raw,
    }

    outdir = root / "bench-results"
    outdir.mkdir(exist_ok=True)
    filename = f"{now.strftime('%Y%m%d-%H%M%S')}_{sha}_go.json"
    outpath = outdir / filename
    outpath.write_text(json.dumps(out, indent=2) + "\n")
    print(f"Saved to {outpath.relative_to(root)}")
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
