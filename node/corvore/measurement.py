"""Bounded interval measurement using local Linux counters."""

import argparse
import json
from pathlib import Path
import re
import time

from corvore.diagnostics import read_text, snapshot


def counters(device):
    block = Path("/sys/class/block") / device
    if (block / "partition").exists():
        raise ValueError("select a whole disk, not a partition")
    identity = {
        "path": str(block.resolve(strict=True)),
        "dev": read_text(block / "dev"),
    }
    sequence = block / "diskseq"
    identity["diskseq"] = read_text(sequence) if sequence.exists() else None

    cpu_line = read_text("/proc/stat").splitlines()[0].split()
    if cpu_line[0] != "cpu" or len(cpu_line) < 9:
        raise ValueError("invalid CPU counters")
    cpu = [int(value) for value in cpu_line[1:9]]
    disk = [int(value) for value in read_text(block / "stat").split()]
    if len(disk) < 11 or any(value < 0 for value in cpu + disk):
        raise ValueError("invalid kernel counters")
    return {
        "identity": identity,
        "cpu": cpu,
        "written_sectors": disk[6],
        "monotonic_ns": time.monotonic_ns(),
    }


def calculate(first, last):
    if first["identity"] != last["identity"]:
        raise ValueError("block device identity changed")
    elapsed = (last["monotonic_ns"] - first["monotonic_ns"]) / 1e9
    cpu_delta = [b - a for a, b in zip(first["cpu"], last["cpu"])]
    sectors = last["written_sectors"] - first["written_sectors"]
    if elapsed <= 0 or sectors < 0 or any(value < 0 for value in cpu_delta):
        raise ValueError("counter regression or invalid interval")
    total = sum(cpu_delta)
    if total <= 0:
        raise ValueError("CPU counters did not advance")

    # First eight fields only: guest times are already included in user/nice.
    # Report execution separately from idle, iowait and steal accounting.
    execution = sum(cpu_delta[i] for i in (0, 1, 2, 5, 6))
    written_bytes = sectors * 512
    return {
        "elapsed_seconds": elapsed,
        "cpu_execution_percent": 100 * execution / total,
        "cpu_idle_percent": 100 * cpu_delta[3] / total,
        "cpu_iowait_percent": 100 * cpu_delta[4] / total,
        "cpu_steal_percent": 100 * cpu_delta[7] / total,
        "written_bytes": written_bytes,
        "written_bytes_per_second": written_bytes / elapsed,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True,
                        choices=("development-host", "field-node"))
    parser.add_argument("--device", required=True)
    parser.add_argument("--seconds", type=int, default=10)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args.device):
        parser.error("device must be a block-device name, not a path")
    if not 1 <= args.seconds <= 60:
        parser.error("seconds must be between 1 and 60")

    try:
        first = counters(args.device)
        time.sleep(args.seconds)
        last = counters(args.device)
        interval = calculate(first, last)
        report = snapshot(args.context)
        report["report_type"] = "local_interval_measurement"
        report["block_device"] = args.device
        report["block_identity"] = last["identity"]
        report["requested_seconds"] = args.seconds
        report["interval"] = interval
        report["scope"] = "whole-system CPU and selected whole-disk writes"
        print(json.dumps(report, indent=2, allow_nan=False))
    except (OSError, ValueError, IndexError) as exc:
        parser.exit(1, f"Measurement failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
