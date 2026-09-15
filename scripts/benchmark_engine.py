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


def measure(executable, mode, path, kind):
    with subprocess.Popen([str(executable), mode, str(path), kind], stdin=subprocess.PIPE,
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
    parser.add_argument("--reps", type=int, default=3)
    args = parser.parse_args()
    if args.reps < 1:
        parser.error("--reps must be positive")
    root = Path(__file__).resolve().parents[1]
    executable = root / "target/release/examples" / ("engine_probe.exe" if os.name == "nt" else "engine_probe")
    directory = root / "target/engine-measurements"
    directory.mkdir(exist_ok=True)
    report = {"repetitions": args.reps, "stream_budget_bytes": 10 * 1024 * 1024, "cases": []}
    for kind, record in [("plain", b"x" * 128 + b"\n"),
                         ("continued", b"x" * 64 + b"\\\n" + b"x" * 64 + b"\n")]:
        path = directory / f"{kind}.txt"
        records = (100 * 1024 * 1024 + len(record) - 1) // len(record)
        with path.open("wb") as file:
            block = record * 8192
            for _ in range(records // 8192):
                file.write(block)
            file.write(record * (records % 8192))
        for mode in (["boundary"] if kind == "plain" else []) + ["bytes", "parallel", "mmap", "stream"]:
            runs = [measure(executable, mode, path, kind) for _ in range(args.reps)]
            median = {key: statistics.median(run[key] for run in runs) for key in runs[0]}
            if "seconds" in median:
                median["mib_per_second"] = path.stat().st_size / (1024 ** 2) / median["seconds"]
            case = dict(kind=kind, mode=mode, input_bytes=path.stat().st_size, median=median, runs=runs)
            report["cases"].append(case)
            print(json.dumps({key: value for key, value in case.items() if key != "runs"}), flush=True)
    (directory / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
