"""Measure a prebuilt engine_probe with 100 MiB inputs and fresh child processes."""

import argparse
import ctypes
import json
import os
from pathlib import Path
import statistics
import subprocess
import time


def rss(process):
    if os.name == "nt":
        class Counters(ctypes.Structure):
            _fields_ = [("cb", ctypes.c_ulong), ("faults", ctypes.c_ulong)] + [
                (name, ctypes.c_size_t) for name in (
                    "peak", "working", "paged_peak", "paged", "nonpaged_peak",
                    "nonpaged", "pagefile", "pagefile_peak",
                )
            ]
        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        if not ctypes.windll.psapi.GetProcessMemoryInfo(
            ctypes.c_void_p(process._handle), ctypes.byref(counters), counters.cb
        ):
            raise ctypes.WinError()
        return counters.working, counters.peak
    status = Path(f"/proc/{process.pid}/status").read_text()
    values = {line.split(":")[0]: int(line.split()[1]) * 1024
              for line in status.splitlines() if line.startswith(("VmRSS:", "VmHWM:"))}
    return values["VmRSS"], values["VmHWM"]


def measure(executable, mode, path, kind, width, budget, columns):
    with subprocess.Popen([str(executable), mode, str(path), kind, str(width), str(budget), str(columns)], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
        if process.stdout.readline().strip() != "ready":
            raise RuntimeError(process.stderr.read())
        baseline, peak = rss(process)
        process.stdin.write("\n")
        process.stdin.flush()
        while process.poll() is None:
            try:
                _, observed = rss(process)
                peak = max(peak, observed)
            except (OSError, ProcessLookupError, KeyError):
                if process.poll() is None:
                    raise
            time.sleep(0.002)
        output, error = process.communicate()
        if process.returncode:
            raise RuntimeError(error)
    result = json.loads(output)
    result.update(baseline_rss=baseline, peak_rss=peak, rss_increase=peak - baseline)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reps", type=int, default=10)
    parser.add_argument("--mib", type=int, default=100)
    parser.add_argument("--width", type=int, default=128)
    parser.add_argument("--columns", type=int, default=1)
    parser.add_argument("--budget-mib", type=int, default=10)
    parser.add_argument("--kinds", nargs="+", choices=["plain", "continued", "comments", "blank"],
                        default=["plain", "continued", "comments", "blank"])
    parser.add_argument("--modes", nargs="+", choices=["boundary", "bytes", "parallel", "mmap", "stream", "stream-bytes", "iterator", "stream-parallel"],
                        default=["boundary", "bytes", "parallel", "mmap", "stream"])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if min(args.reps, args.mib, args.width, args.columns, args.budget_mib) < 1:
        parser.error("repetitions, sizes, width and columns must be positive")
    root = Path(__file__).resolve().parents[1]
    executable = root / "target/release/examples" / ("engine_probe.exe" if os.name == "nt" else "engine_probe")
    directory = root / "target/engine-measurements"
    directory.mkdir(exist_ok=True)
    budget = args.budget_mib * 1024 * 1024
    report = {"repetitions": args.reps, "stream_budget_bytes": budget,
              "width": args.width, "columns": args.columns, "cases": []}
    value = b"x" * args.width
    records_by_kind = {"plain": value + b"\n", "continued": value[:args.width // 2] + b"\\\n" + value[args.width // 2:] + b"\n",
                       "comments": b"# ignored\n" + value + b"\n", "blank": value + b"\n\n"}
    for kind in args.kinds:
        record = records_by_kind[kind]
        path = directory / f"{kind}.txt"
        records = (args.mib * 1024 * 1024 + len(record) - 1) // len(record)
        with path.open("wb") as file:
            block_rows = max(1, (1024 * 1024) // len(record))
            block = record * block_rows
            for _ in range(records // block_rows):
                file.write(block)
            file.write(record * (records % block_rows))
        for mode in args.modes:
            runs = [measure(executable, mode, path, kind, args.width, budget, args.columns) for _ in range(args.reps)]
            median = {key: statistics.median(run[key] for run in runs) for key in runs[0]}
            if "seconds" in median:
                median["mib_per_second"] = path.stat().st_size / (1024 ** 2) / median["seconds"]
            case = dict(kind=kind, mode=mode, input_bytes=path.stat().st_size, median=median, runs=runs)
            if mode == "boundary":
                ratios = sorted(run["ratio"] for run in runs)
                case["distribution"] = {"min": ratios[0], "p90": ratios[max(0, (9 * len(ratios) + 9) // 10 - 1)],
                                        "max": ratios[-1], "over_1_05": sum(ratio > 1.05 for ratio in ratios)}
            report["cases"].append(case)
            print(json.dumps({key: value for key, value in case.items() if key != "runs"}), flush=True)
    (args.output or directory / "results.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
