#!/usr/bin/env python3
"""Run the rypipe-core throughput benchmark and record results.

This is a thin wrapper around the Rust `bench_throughput` example. It exists so
Python contributors can run the engine benchmark without remembering the cargo
invocation, and so CI can write machine-readable results to `.benchmarks/`.

Usage:
    python benchmarks/bench_throughput.py
    python benchmarks/bench_throughput.py --output .benchmarks/rypipe.json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def run_benchmark() -> list[dict[str, float | str]]:
    cmd = [
        "cargo",
        "run",
        "--release",
        "-p",
        "rypipe-core",
        "--example",
        "bench_throughput",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    lines = result.stdout.splitlines()

    # Parse lines like:
    # single-thread               1.591s     5000000 rows   3142971 rows/s  188.5 MB/s  RSS  320 MB
    pattern = re.compile(
        r"^(?P<name>.+?)\s+(?P<time>\d+\.\d+)s\s+(?P<rows>\d+) rows\s+"
        r"(?P<rows_per_s>\d+) rows/s\s+(?P<mb_per_s>\d+\.\d+) MB/s\s+"
        r"RSS\s+(?P<memory>\S+)"
    )
    records: list[dict[str, float | str]] = []
    for line in lines:
        m = pattern.match(line)
        if m:
            records.append(
                {
                    "name": m.group("name").strip(),
                    "time_seconds": float(m.group("time")),
                    "rows": int(m.group("rows")),
                    "rows_per_second": int(m.group("rows_per_s")),
                    "mb_per_second": float(m.group("mb_per_s")),
                    "memory": m.group("memory"),
                }
            )
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description="Run rypipe throughput benchmark")
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON file to write results to",
    )
    args = parser.parse_args()

    print("Running rypipe throughput benchmark...")
    records = run_benchmark()

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "benchmark": "bench_throughput",
        "unit": "rows_per_second",
        "results": records,
    }

    for r in records:
        print(
            f"{r['name']:24} {r['time_seconds']:8.3f}s  {r['rows']:10} rows  "
            f"{r['rows_per_second']:8} rows/s  {r['mb_per_second']:6.1f} MB/s  RSS {r['memory']}"
        )

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"\nWrote results to {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
