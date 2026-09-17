#!/usr/bin/env python3

"""Build the CORVORE native-image runtime filesystem overlay."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import shutil
import stat
import zipfile


def copy_file(
    source: Path,
    destination: Path,
    *,
    mode: int,
    epoch: int,
) -> None:
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
        mode=0o755,
    )

    shutil.copyfile(
        source,
        destination,
    )

    destination.chmod(mode)

    destination.touch()

    import os

    os.utime(
        destination,
        (epoch, epoch),
    )


def extract_package(
    wheel: Path,
    destination: Path,
    *,
    epoch: int,
) -> None:
    import os

    with zipfile.ZipFile(wheel) as archive:
        members = [
            item
            for item in archive.infolist()
            if (
                not item.is_dir()
                and item.filename.startswith(
                    "corvore/"
                )
            )
        ]

        if not members:
            raise RuntimeError(
                "wheel contains no corvore package"
            )

        for member in members:
            relative = Path(
                member.filename
            )

            if (
                relative.is_absolute()
                or ".." in relative.parts
            ):
                raise RuntimeError(
                    "unsafe wheel member"
                )

            target = (
                destination
                / relative
            )

            target.parent.mkdir(
                parents=True,
                exist_ok=True,
                mode=0o755,
            )

            with archive.open(member) as source:
                with target.open("wb") as output:
                    shutil.copyfileobj(
                        source,
                        output,
                    )

            target.chmod(0o644)

            os.utime(
                target,
                (epoch, epoch),
            )


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(
                1024 * 1024
            ),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def write_manifest(
    root: Path,
    *,
    epoch: int,
) -> None:
    import os

    manifest = (
        root
        / "opt/corvore/MANIFEST.sha256"
    )

    manifest.parent.mkdir(
        parents=True,
        exist_ok=True,
        mode=0o755,
    )

    records = []

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue

        if path == manifest:
            continue

        relative = path.relative_to(root)

        records.append(
            f"{hash_file(path)}  /{relative}"
        )

    manifest.write_text(
        "\n".join(records) + "\n",
        encoding="utf-8",
    )

    manifest.chmod(0o644)

    os.utime(
        manifest,
        (epoch, epoch),
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
    )

    parser.add_argument(
        "--wheel",
        required=True,
        type=Path,
    )

    parser.add_argument(
        "--output",
        required=True,
        type=Path,
    )

    parser.add_argument(
        "--source-root",
        required=True,
        type=Path,
    )

    parser.add_argument(
        "--epoch",
        required=True,
        type=int,
    )

    args = parser.parse_args()

    wheel = args.wheel.resolve(
        strict=True
    )

    source_root = \
        args.source_root.resolve(
            strict=True
        )

    output = args.output.resolve()

    if output.exists():
        raise RuntimeError(
            "output path already exists"
        )

    output.mkdir(
        parents=True,
        mode=0o755,
    )

    app_root = (
        output
        / "opt/corvore/app"
    )

    app_root.mkdir(
        parents=True,
        mode=0o755,
    )

    extract_package(
        wheel,
        app_root,
        epoch=args.epoch,
    )

    copy_file(
        source_root
        / "packaging/bin/corvored",
        output
        / "usr/local/bin/corvored",
        mode=0o755,
        epoch=args.epoch,
    )

    copy_file(
        source_root
        / "packaging/bin/corvorectl",
        output
        / "usr/local/bin/corvorectl",
        mode=0o755,
        epoch=args.epoch,
    )

    copy_file(
        source_root
        / "packaging/systemd/corvored.service",
        output
        / (
            "usr/lib/systemd/system/"
            "corvored.service"
        ),
        mode=0o644,
        epoch=args.epoch,
    )

    copy_file(
        source_root
        / "packaging/sysusers/corvore.conf",
        output
        / (
            "usr/lib/sysusers.d/"
            "corvore.conf"
        ),
        mode=0o644,
        epoch=args.epoch,
    )

    copy_file(
        source_root
        / "docs/RUNTIME.md",
        output
        / (
            "usr/share/doc/corvore/"
            "RUNTIME.md"
        ),
        mode=0o644,
        epoch=args.epoch,
    )

    copy_file(
        source_root
        / "packaging/cloud/cloud-init.disabled",
        output
        / "etc/cloud/cloud-init.disabled",
        mode=0o644,
        epoch=args.epoch,
    )

    write_manifest(
        output,
        epoch=args.epoch,
    )

    print(
        f"overlay={output}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
