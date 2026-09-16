"""Read-only local system snapshot; no radio or network operations."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform

READ_LIMIT = 65536


def read_text(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(READ_LIMIT + 1)
    if len(raw) > READ_LIMIT:
        raise ValueError("input exceeds read limit")
    return raw.decode("utf-8").rstrip("\x00\n")


def memory():
    fields = {}
    wanted = {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}
    for line in read_text("/proc/meminfo").splitlines():
        name, value = line.split(":", 1)
        if name in wanted:
            number, unit = value.split()
            if unit != "kB" or int(number) < 0:
                raise ValueError("invalid memory field")
            fields[name] = int(number) * 1024
    if set(fields) != wanted:
        raise ValueError("required memory fields missing")
    return fields


def uptime():
    value = float(read_text("/proc/uptime").split()[0])
    if not 0 <= value < float("inf"):
        raise ValueError("invalid uptime")
    return value


def storage():
    import os

    stat = os.statvfs("/")
    return {
        "path": "/",
        "total_bytes": stat.f_blocks * stat.f_frsize,
        "free_bytes": stat.f_bfree * stat.f_frsize,
        "available_to_unprivileged_bytes": stat.f_bavail * stat.f_frsize,
    }


def temperatures():
    root = Path("/sys/class/thermal")
    zones = []
    truncated = False
    for zone in root.iterdir():
        if not zone.name.startswith("thermal_zone"):
            continue
        if not zone.name.removeprefix("thermal_zone").isdigit():
            continue
        if len(zones) == 16:
            truncated = True
            break
        zones.append(zone)
    if not zones:
        raise FileNotFoundError("no thermal zones exposed")

    readings = []
    for zone in sorted(zones, key=lambda item: item.name):
        try:
            sensor_type = read_text(zone / "type")
            temperature = int(read_text(zone / "temp"))
            if not -273150 <= temperature <= 1000000:
                raise ValueError("temperature outside sanity limits")
            readings.append({
                "zone": zone.name,
                "type": sensor_type,
                "status": "available",
                "celsius": temperature / 1000,
            })
        except (OSError, UnicodeError, ValueError) as exc:
            readings.append({
                "zone": zone.name,
                "status": "unavailable",
                "celsius": None,
                "reason": type(exc).__name__,
            })
    return {"zones": readings, "truncated": truncated}


def snapshot(context):
    readings = {}
    sources = {
        "memory_bytes": memory,
        "uptime_seconds": uptime,
        "root_filesystem_bytes": storage,
        "thermal_sensors": temperatures,
        "board_model": lambda: read_text("/sys/firmware/devicetree/base/model"),
    }
    for name, reader in sources.items():
        try:
            readings[name] = {"status": "available", "value": reader()}
        except (OSError, UnicodeError, ValueError, IndexError) as exc:
            readings[name] = {
                "status": "unavailable",
                "value": None,
                "reason": type(exc).__name__,
            }
    return {
        "schema_version": 1,
        "report_type": "local_system_snapshot",
        "context_declared_by_operator": context,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "clock_accuracy_verified": False,
        "system": platform.system(),
        "kernel_release": platform.release(),
        "architecture": platform.machine(),
        "readings": readings,
        "resource_budgets_validated": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--context", required=True,
        choices=("development-host", "field-node"),
        help="Operator-declared context; not hardware identity verification.",
    )
    args = parser.parse_args()
    print(json.dumps(snapshot(args.context), indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
