#!/usr/bin/env python3

"""Engineering-only persistence smoke test."""

import argparse
import json
from pathlib import Path
import time

from corvore.storage import (
    append_observation,
    initialize_databases,
    read_observations_after,
    storage_status,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
    )

    parser.add_argument(
        "--state-dir",
        required=True,
    )

    parser.add_argument(
        "--count",
        type=int,
        default=3,
    )

    args = parser.parse_args()

    if not 1 <= args.count <= 10:
        parser.error(
            "count must be between 1 and 10"
        )

    state = Path(args.state_dir)

    initialize_databases(state)

    writes = []

    for index in range(args.count):
        writes.append(
            append_observation(
                state,
                observation_id=(
                    f"storage-smoke-observation-"
                    f"{index:04d}"
                ),
                boot_id="storage-smoke-boot",
                monotonic_ns=time.monotonic_ns(),
                source_kind="test-harness",
                source_instance="storage-smoke",
                observation_kind="storage.smoke",
                payload={
                    "synthetic": True,
                    "index": index,
                },
            )
        )

    rows = read_observations_after(
        state,
        after_ingest_seq=0,
        limit=100,
    )

    expected = list(
        range(1, args.count + 1)
    )

    actual = [
        row["ingest_seq"]
        for row in rows
    ]

    if actual != expected:
        raise SystemExit(
            "unexpected ingest sequence"
        )

    print(
        json.dumps(
            {
                "status": "pass",
                "writes": writes,
                "read_count": len(rows),
                "storage":
                    storage_status(state),
            },
            indent=2,
            allow_nan=False,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
