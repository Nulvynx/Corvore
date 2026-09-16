"""Minimal command-line entry point."""

import argparse

from corvore import __version__


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="corvorectl",
        description="CORVORE development foundation; device runtime is not implemented.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    commands = parser.add_subparsers(dest="command")
    diagnostic = commands.add_parser(
        "diagnostics", help="Read a local system snapshot."
    )
    diagnostic.add_argument(
        "--context", required=True,
        choices=("development-host", "field-node"),
        help="Operator-declared context; not hardware identity verification.",
    )
    args = parser.parse_args()
    if args.command == "diagnostics":
        import json
        from corvore.diagnostics import snapshot

        print(json.dumps(snapshot(args.context), indent=2, allow_nan=False))
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
