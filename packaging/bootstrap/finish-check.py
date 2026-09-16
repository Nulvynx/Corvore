#!/usr/bin/env python3

import hashlib
import json
import os
import subprocess
from pathlib import Path

BASE = Path("/var/lib/corvore/bootstrap")

REPORT = BASE / "firstboot.json"
MARKER = BASE / "completed.json"

CLOUD_FINISHED = Path(
    "/var/lib/cloud/instance/boot-finished"
)


def main():
    if not REPORT.is_file() or not MARKER.is_file():
        print(
            "CORVORE_FINISH=WAITING_FOR_REPORT",
            flush=True,
        )
        return 0

    try:
        marker = json.loads(MARKER.read_text())

        if marker.get("status") != "completed":
            print("CORVORE_FINISH=INVALID_STATUS")
            return 0

        actual = hashlib.sha256(
            REPORT.read_bytes()
        ).hexdigest()

        if actual != marker.get("report_sha256"):
            print("CORVORE_FINISH=HASH_MISMATCH")
            return 0

    except (OSError, ValueError, TypeError):
        print("CORVORE_FINISH=INVALID_REPORT")
        return 0

    if not CLOUD_FINISHED.is_file():
        print(
            "CORVORE_FINISH=WAITING_FOR_CLOUD_INIT",
            flush=True,
        )
        return 0

    print(
        "CORVORE_FINISH=SHUTDOWN_REQUESTED",
        flush=True,
    )

    os.sync()

    subprocess.run(
        ["/usr/bin/systemctl", "--no-block", "poweroff"],
        check=True,
        timeout=15,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
