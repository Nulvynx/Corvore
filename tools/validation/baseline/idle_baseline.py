#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
import platform
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


OUTPUT_DIR = Path("/var/lib/corvore/validation/idle-baseline")
REPORT = OUTPUT_DIR / "idle-baseline.json"
COMPLETED = OUTPUT_DIR / "completed.json"
FAILURE = OUTPUT_DIR / "failure.json"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def read_text(path):
    return Path(path).read_text().strip()


def read_boot_id():
    return read_text("/proc/sys/kernel/random/boot_id")


def read_uptime():
    return float(read_text("/proc/uptime").split()[0])


def read_meminfo():
    result = {}

    for line in Path("/proc/meminfo").read_text().splitlines():
        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        parts = value.strip().split()

        if not parts:
            continue

        try:
            number = int(parts[0])
        except ValueError:
            continue

        if len(parts) > 1 and parts[1] == "kB":
            number *= 1024

        result[key] = number

    wanted = (
        "MemTotal",
        "MemFree",
        "MemAvailable",
        "Buffers",
        "Cached",
        "SwapTotal",
        "SwapFree",
    )

    return {
        key: result.get(key)
        for key in wanted
    }


def read_cpu():
    line = Path("/proc/stat").read_text().splitlines()[0]
    fields = line.split()

    if not fields or fields[0] != "cpu":
        raise RuntimeError("invalid /proc/stat cpu line")

    values = [int(x) for x in fields[1:]]

    while len(values) < 10:
        values.append(0)

    return {
        "user": values[0],
        "nice": values[1],
        "system": values[2],
        "idle": values[3],
        "iowait": values[4],
        "irq": values[5],
        "softirq": values[6],
        "steal": values[7],
        "guest": values[8],
        "guest_nice": values[9],
    }


def cpu_percentages(start, end):
    delta = {
        key: end[key] - start[key]
        for key in start
    }

    if any(value < 0 for value in delta.values()):
        raise RuntimeError("CPU counter regression")

    total = sum(
        delta[key]
        for key in (
            "user",
            "nice",
            "system",
            "idle",
            "iowait",
            "irq",
            "softirq",
            "steal",
        )
    )

    if total <= 0:
        raise RuntimeError("invalid CPU delta")

    def pct(value):
        return (value / total) * 100.0

    return {
        "user_percent": pct(delta["user"] + delta["nice"]),
        "system_percent": pct(
            delta["system"] +
            delta["irq"] +
            delta["softirq"]
        ),
        "idle_percent": pct(delta["idle"]),
        "iowait_percent": pct(delta["iowait"]),
        "steal_percent": pct(delta["steal"]),
        "busy_percent": pct(
            total - delta["idle"] - delta["iowait"]
        ),
        "ticks_observed": total,
    }


def root_device_major_minor():
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, sep, right = line.partition(" - ")

        if not sep:
            continue

        fields = left.split()

        if len(fields) < 5:
            continue

        mountpoint = fields[4]

        if mountpoint == "/":
            return fields[2]

    raise RuntimeError("root filesystem not found in mountinfo")


def whole_disk_for_major_minor(major_minor):
    path = Path("/sys/dev/block") / major_minor

    if not path.exists():
        raise RuntimeError(
            f"sysfs block device missing: {major_minor}"
        )

    resolved = path.resolve()

    if (resolved / "partition").exists():
        disk = resolved.parent
    else:
        disk = resolved

    name = disk.name

    if not (Path("/sys/class/block") / name / "stat").exists():
        raise RuntimeError(
            f"whole disk stat unavailable: {name}"
        )

    return name, str(disk)


def disk_write_state(disk):
    fields = (
        Path("/sys/class/block") /
        disk /
        "stat"
    ).read_text().split()

    if len(fields) < 7:
        raise RuntimeError("invalid block stat")

    sectors_written = int(fields[6])

    dev = read_text(
        Path("/sys/class/block") / disk / "dev"
    )

    diskseq_path = (
        Path("/sys/class/block") / disk / "diskseq"
    )

    diskseq = (
        read_text(diskseq_path)
        if diskseq_path.exists()
        else None
    )

    return {
        "sectors_written": sectors_written,
        "bytes_written": sectors_written * 512,
        "dev": dev,
        "diskseq": diskseq,
    }


def thermal_readings():
    values = []

    for zone in sorted(
        Path("/sys/class/thermal").glob("thermal_zone*")
    ):
        temp_path = zone / "temp"

        if not temp_path.exists():
            continue

        try:
            raw = float(read_text(temp_path))
        except (ValueError, OSError):
            continue

        zone_type = None
        type_path = zone / "type"

        if type_path.exists():
            try:
                zone_type = read_text(type_path)
            except OSError:
                pass

        celsius = raw / 1000.0 if raw > 500 else raw

        values.append({
            "zone": zone.name,
            "type": zone_type,
            "celsius": celsius,
        })

    return values


def cpu_temperature(readings):
    for item in readings:
        if item.get("type") == "cpu-thermal":
            return item.get("celsius")

    if readings:
        return readings[0].get("celsius")

    return None


def root_fs():
    st = os.statvfs("/")

    return {
        "total_bytes": st.f_blocks * st.f_frsize,
        "free_bytes": st.f_bfree * st.f_frsize,
        "available_to_unprivileged_bytes":
            st.f_bavail * st.f_frsize,
    }


def loadavg():
    values = read_text("/proc/loadavg").split()

    return {
        "load_1m": float(values[0]),
        "load_5m": float(values[1]),
        "load_15m": float(values[2]),
    }


def systemd_analyze():
    try:
        proc = subprocess.run(
            ["/usr/bin/systemd-analyze", "time"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
            check=False,
        )

        return {
            "exit_code": proc.returncode,
            "output": proc.stdout.strip(),
        }

    except Exception as exc:
        return {
            "exit_code": None,
            "error": type(exc).__name__,
        }


def failed_units():
    try:
        proc = subprocess.run(
            [
                "/usr/bin/systemctl",
                "--failed",
                "--no-legend",
                "--plain",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
            check=False,
        )

        lines = [
            line
            for line in proc.stdout.splitlines()
            if line.strip()
        ]

        return {
            "exit_code": proc.returncode,
            "count": len(lines),
            "units": lines[:25],
            "truncated": len(lines) > 25,
        }

    except Exception as exc:
        return {
            "exit_code": None,
            "error": type(exc).__name__,
        }


def atomic_json(path, data):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
        mode=0o700,
    )

    tmp = path.with_name(
        f".{path.name}.{os.getpid()}.tmp"
    )

    encoded = (
        json.dumps(
            data,
            indent=2,
            sort_keys=False,
        ) + "\n"
    ).encode()

    fd = os.open(
        tmp,
        os.O_WRONLY |
        os.O_CREAT |
        os.O_EXCL,
        0o600,
    )

    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(tmp, path)

        dirfd = os.open(
            path.parent,
            os.O_RDONLY | os.O_DIRECTORY,
        )

        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)

    finally:
        if tmp.exists():
            tmp.unlink()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--duration",
        type=int,
        default=120,
    )

    parser.add_argument(
        "--sample-interval",
        type=int,
        default=5,
    )

    args = parser.parse_args()

    if not 60 <= args.duration <= 600:
        raise SystemExit(
            "duration must be between 60 and 600 seconds"
        )

    if not 1 <= args.sample_interval <= 30:
        raise SystemExit(
            "sample interval must be between 1 and 30 seconds"
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
        mode=0o700,
    )

    if COMPLETED.exists():
        print(
            "CORVORE_IDLE_BASELINE=ALREADY_COMPLETED",
            flush=True,
        )
        return 0

    FAILURE.unlink(missing_ok=True)

    try:
        started_utc = utc_now()
        boot_id = read_boot_id()
        uptime_start = read_uptime()

        major_minor = root_device_major_minor()

        disk, disk_identity_path = \
            whole_disk_for_major_minor(major_minor)

        disk_start = disk_write_state(disk)
        cpu_start = read_cpu()
        memory_start = read_meminfo()
        load_start = loadavg()

        temp_samples = []
        mem_available_samples = []

        monotonic_start = time.monotonic()
        deadline = monotonic_start + args.duration

        while True:
            now = time.monotonic()

            thermal = thermal_readings()
            cpu_temp = cpu_temperature(thermal)

            if cpu_temp is not None:
                temp_samples.append(cpu_temp)

            mem = read_meminfo()
            available = mem.get("MemAvailable")

            if available is not None:
                mem_available_samples.append(available)

            remaining = deadline - now

            if remaining <= 0:
                break

            time.sleep(
                min(
                    args.sample_interval,
                    remaining,
                )
            )

        monotonic_end = time.monotonic()
        elapsed = monotonic_end - monotonic_start

        cpu_end = read_cpu()
        disk_end = disk_write_state(disk)
        memory_end = read_meminfo()
        load_end = loadavg()
        uptime_end = read_uptime()

        if disk_start["dev"] != disk_end["dev"]:
            raise RuntimeError("disk device identity changed")

        if disk_start["diskseq"] != disk_end["diskseq"]:
            raise RuntimeError("disk sequence changed")

        written = (
            disk_end["bytes_written"] -
            disk_start["bytes_written"]
        )

        if written < 0:
            raise RuntimeError(
                "disk write counter regression"
            )

        bytes_per_second = written / elapsed

        gb_per_day_decimal = (
            bytes_per_second * 86400 / 1_000_000_000
        )

        gib_per_day = (
            bytes_per_second * 86400 / (1024 ** 3)
        )

        temperatures = {}

        if temp_samples:
            temperatures = {
                "sample_count": len(temp_samples),
                "minimum_celsius": min(temp_samples),
                "maximum_celsius": max(temp_samples),
                "average_celsius":
                    sum(temp_samples) / len(temp_samples),
                "last_celsius": temp_samples[-1],
            }
        else:
            temperatures = {
                "sample_count": 0,
                "status": "unavailable",
            }

        mem_available_summary = {}

        if mem_available_samples:
            mem_available_summary = {
                "sample_count":
                    len(mem_available_samples),
                "minimum_bytes":
                    min(mem_available_samples),
                "maximum_bytes":
                    max(mem_available_samples),
                "average_bytes":
                    sum(mem_available_samples) /
                    len(mem_available_samples),
            }
        else:
            mem_available_summary = {
                "sample_count": 0,
                "status": "unavailable",
            }

        board_model = None

        model_path = Path(
            "/sys/firmware/devicetree/base/model"
        )

        if model_path.exists():
            board_model = (
                model_path.read_bytes()
                .rstrip(b"\x00")
                .decode(
                    errors="replace"
                )
            )

        report = {
            "schema_version": 1,
            "report_type":
                "clean_native_idle_baseline",
            "context":
                "Raspberry Pi OS Lite ARM64 + CORVORE development baseline",
            "production_runtime": False,
            "radio_profile":
                "wifi_and_bluetooth_temporarily_disabled",
            "clock_accuracy_verified": False,
            "resource_budgets_validated": False,
            "started_utc": started_utc,
            "finished_utc": utc_now(),
            "duration_requested_seconds":
                args.duration,
            "duration_actual_seconds":
                elapsed,
            "sample_interval_seconds":
                args.sample_interval,
            "boot_id": boot_id,
            "board_model": board_model,
            "system": platform.system(),
            "kernel_release": platform.release(),
            "architecture": platform.machine(),
            "boot": {
                "uptime_at_measurement_start_seconds":
                    uptime_start,
                "uptime_at_measurement_end_seconds":
                    uptime_end,
                "systemd_analyze":
                    systemd_analyze(),
            },
            "cpu": cpu_percentages(
                cpu_start,
                cpu_end,
            ),
            "memory": {
                "start_bytes": memory_start,
                "end_bytes": memory_end,
                "memavailable_window":
                    mem_available_summary,
            },
            "temperature": temperatures,
            "load": {
                "start": load_start,
                "end": load_end,
            },
            "root_filesystem": root_fs(),
            "disk_write": {
                "root_major_minor":
                    major_minor,
                "whole_disk":
                    disk,
                "identity_path":
                    disk_identity_path,
                "dev":
                    disk_end["dev"],
                "diskseq":
                    disk_end["diskseq"],
                "bytes_written":
                    written,
                "bytes_per_second":
                    bytes_per_second,
                "estimated_gb_per_day_decimal":
                    gb_per_day_decimal,
                "estimated_gib_per_day":
                    gib_per_day,
                "estimate_note":
                    "Linear extrapolation from this short idle observation window; not a measured 24-hour total.",
            },
            "systemd_failed_units":
                failed_units(),
        }

        atomic_json(REPORT, report)

        digest = hashlib.sha256(
            REPORT.read_bytes()
        ).hexdigest()

        marker = {
            "status": "completed",
            "report_sha256": digest,
            "boot_id": boot_id,
            "duration_actual_seconds":
                elapsed,
            "clock_accuracy_verified": False,
            "resource_budgets_validated": False,
        }

        atomic_json(COMPLETED, marker)

        print("CORVORE_IDLE_BASELINE=PASS", flush=True)
        print(
            f"REPORT_SHA256={digest}",
            flush=True,
        )

        return 0

    except Exception as exc:
        failure = {
            "status": "failed",
            "timestamp_utc": utc_now(),
            "exception_type":
                type(exc).__name__,
            "message": str(exc)[:500],
            "clock_accuracy_verified": False,
        }

        try:
            atomic_json(FAILURE, failure)
        finally:
            print(
                "CORVORE_IDLE_BASELINE=FAIL",
                flush=True,
            )

        raise


if __name__ == "__main__":
    raise SystemExit(main())
