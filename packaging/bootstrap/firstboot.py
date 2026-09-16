#!/usr/bin/env python3

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

BASE = Path("/var/lib/corvore/bootstrap")

ARTIFACT = (
    "/opt/corvore/bootstrap/"
    "corvore-diagnostics.pyz"
)

REPORT = BASE / "firstboot.json"
MARKER = BASE / "completed.json"
FAILURE = BASE / "failure.json"

MAX_OUTPUT = 2 * 1024 * 1024


def atomic_json(path, payload):
    temporary = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=BASE,
            prefix=".corvore-",
            delete=False,
        ) as handle:

            temporary = Path(handle.name)

            json.dump(
                payload,
                handle,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )

            handle.write("\n")
            handle.flush()

            os.fsync(handle.fileno())

        os.replace(temporary, path)

        fd = os.open(
            BASE,
            os.O_RDONLY | os.O_DIRECTORY,
        )

        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    os.umask(0o077)

    BASE.mkdir(
        parents=True,
        exist_ok=True,
    )

    if MARKER.exists():
        print("CORVORE_FIRSTBOOT=ALREADY_COMPLETED")
        return 0

    try:
        result = subprocess.run(
            [
                sys.executable,
                ARTIFACT,
                "diagnostics",
                "--context",
                "field-node",
            ],
            capture_output=True,
            timeout=60,
            check=False,
        )

        if result.returncode != 0:
            raise RuntimeError(
                "diagnostics failed: "
                + result.stderr.decode(
                    "utf-8",
                    errors="replace",
                )[-500:]
            )

        if len(result.stdout) > MAX_OUTPUT:
            raise RuntimeError(
                "diagnostic output too large"
            )

        report = json.loads(result.stdout)

        if not isinstance(report, dict):
            raise RuntimeError(
                "invalid report structure"
            )

        report["clock_accuracy_verified"] = False

        report["corvore_bootstrap"] = {
            "version": "0.0.1",
            "type": "native-development",
            "production_runtime": False,
        }

        report["boot_id"] = Path(
            "/proc/sys/kernel/random/boot_id"
        ).read_text().strip()

        stat = os.statvfs("/")

        report["root_filesystem_actual"] = {
            "total_bytes": (
                stat.f_blocks * stat.f_frsize
            ),
            "available_bytes": (
                stat.f_bavail * stat.f_frsize
            ),
        }

        atomic_json(REPORT, report)

        digest = hashlib.sha256(
            REPORT.read_bytes()
        ).hexdigest()

        atomic_json(
            MARKER,
            {
                "status": "completed",
                "report_sha256": digest,
                "clock_accuracy_verified": False,
            },
        )

        FAILURE.unlink(missing_ok=True)

        print("CORVORE_FIRSTBOOT=PASS")
        return 0

    except Exception as exc:

        atomic_json(
            FAILURE,
            {
                "status": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc)[:500],
                "clock_accuracy_verified": False,
            },
        )

        print("CORVORE_FIRSTBOOT=FAILED")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
