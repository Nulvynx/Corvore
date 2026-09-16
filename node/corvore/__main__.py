"""CORVORE command-line entry point."""

import argparse
import json

from corvore import __version__


DEFAULT_STATE_DIR = "/var/lib/corvore"


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="corvorectl",
        description=(
            "CORVORE local administration "
            "and diagnostics."
        ),
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    commands = parser.add_subparsers(
        dest="command",
    )

    diagnostic = commands.add_parser(
        "diagnostics",
        help="Read a local system snapshot.",
    )

    diagnostic.add_argument(
        "--context",
        required=True,
        choices=(
            "development-host",
            "field-node",
        ),
    )

    storage = commands.add_parser(
        "storage",
        help="Manage local persistence.",
    )

    storage_commands = \
        storage.add_subparsers(
            dest="storage_command",
            required=True,
        )

    storage_init = \
        storage_commands.add_parser(
            "init",
            help="Initialize or migrate databases.",
        )

    storage_init.add_argument(
        "--state-dir",
        default=DEFAULT_STATE_DIR,
    )

    storage_status = \
        storage_commands.add_parser(
            "status",
            help="Report database state.",
        )

    storage_status.add_argument(
        "--state-dir",
        default=DEFAULT_STATE_DIR,
    )

    args = parser.parse_args()

    if args.command == "diagnostics":
        from corvore.diagnostics import snapshot

        print(
            json.dumps(
                snapshot(args.context),
                indent=2,
                allow_nan=False,
            )
        )

        return 0

    if args.command == "storage":
        from corvore.storage import (
            StorageError,
            initialize_databases,
            storage_status as get_status,
        )

        try:
            if args.storage_command == "init":
                result = initialize_databases(
                    args.state_dir
                )
            else:
                result = get_status(
                    args.state_dir
                )

        except (
            OSError,
            ValueError,
            StorageError,
        ) as exc:
            parser.exit(
                1,
                f"Storage operation failed: "
                f"{exc}\n",
            )

        print(
            json.dumps(
                result,
                indent=2,
                allow_nan=False,
            )
        )

        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
