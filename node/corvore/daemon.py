"""Long-running unprivileged CORVORE core service."""

from __future__ import annotations

import argparse
import grp
import json
import os
from pathlib import Path
import pwd
import signal
import sys
import threading
from typing import Any

from corvore.ingress import (
    IngressServer,
)
from corvore.storage import (
    StorageError,
    initialize_databases,
    processing_checkpoint,
)


DEFAULT_STATE_DIR = "/var/lib/corvore"
DEFAULT_RUNTIME_DIR = "/run/corvore"
DEFAULT_INGRESS_SOCKET = (
    "/run/corvore-ingress/events.sock"
)
DEFAULT_INGRESS_PEER_USER = (
    "corvore-collector"
)
DEFAULT_INGRESS_GROUP = (
    "corvore-ingress"
)
STATUS_FILENAME = "corvored.json"
STATUS_SCHEMA_VERSION = 1


class DaemonError(RuntimeError):
    """Runtime lifecycle invariant failed."""


def _read_boot_id() -> str:
    path = Path(
        "/proc/sys/kernel/random/boot_id"
    )

    try:
        value = path.read_text(
            encoding="ascii"
        ).strip()
    except OSError as exc:
        raise DaemonError(
            "kernel boot identifier "
            "is unavailable"
        ) from exc

    if (
        not value
        or len(value) > 128
        or any(
            character not in
            "0123456789abcdefABCDEF-"
            for character in value
        )
    ):
        raise DaemonError(
            "kernel boot identifier "
            "is invalid"
        )

    return value


def _prepare_runtime_directory(
    runtime_dir: Path,
) -> None:
    if runtime_dir.is_symlink():
        raise DaemonError(
            "runtime directory must not "
            "be a symlink"
        )

    runtime_dir.mkdir(
        parents=True,
        exist_ok=True,
        mode=0o750,
    )

    if not runtime_dir.is_dir():
        raise DaemonError(
            "runtime path is not "
            "a directory"
        )


def _status_payload(
    *,
    state: str,
    boot_id: str,
    checkpoint: int | None,
) -> dict[str, Any]:
    if state not in {
        "starting",
        "running",
        "stopping",
        "stopped",
        "failed",
    }:
        raise ValueError(
            "invalid runtime state"
        )

    return {
        "schema_version":
            STATUS_SCHEMA_VERSION,
        "service": "corvored",
        "state": state,
        "pid": os.getpid(),
        "boot_id": boot_id,
        "processing_checkpoint":
            checkpoint,
    }


def _write_status(
    runtime_dir: Path,
    payload: dict[str, Any],
) -> None:
    encoded = (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")

    destination = (
        runtime_dir
        / STATUS_FILENAME
    )

    temporary = (
        runtime_dir
        / (
            f".{STATUS_FILENAME}."
            f"{os.getpid()}.tmp"
        )
    )

    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
    )

    descriptor = None

    try:
        descriptor = os.open(
            temporary,
            flags,
            0o600,
        )

        with os.fdopen(
            descriptor,
            "wb",
            closefd=True,
        ) as stream:
            descriptor = None
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(
            temporary,
            destination,
        )

    finally:
        if descriptor is not None:
            os.close(descriptor)

        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def read_runtime_status(
    runtime_dir: (
        str | os.PathLike[str]
    ) = DEFAULT_RUNTIME_DIR,
) -> dict[str, Any]:
    root = Path(runtime_dir)

    if root.is_symlink():
        raise DaemonError(
            "runtime directory must not "
            "be a symlink"
        )

    path = root / STATUS_FILENAME

    if path.is_symlink():
        raise DaemonError(
            "runtime status must not "
            "be a symlink"
        )

    try:
        raw = path.read_bytes()
    except FileNotFoundError as exc:
        raise DaemonError(
            "runtime status unavailable"
        ) from exc

    if len(raw) > 16 * 1024:
        raise DaemonError(
            "runtime status exceeds "
            "size limit"
        )

    try:
        payload = json.loads(
            raw.decode("utf-8")
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise DaemonError(
            "runtime status is invalid"
        ) from exc

    if not isinstance(
        payload,
        dict,
    ):
        raise DaemonError(
            "runtime status must be "
            "a JSON object"
        )

    return payload


def _resolve_ingress_identity(
    user_name: str,
    group_name: str,
) -> tuple[int, int]:
    try:
        user = pwd.getpwnam(
            user_name
        )
    except KeyError as exc:
        raise DaemonError(
            "ingress peer user "
            "does not exist"
        ) from exc

    try:
        group = grp.getgrnam(
            group_name
        )
    except KeyError as exc:
        raise DaemonError(
            "ingress group does not exist"
        ) from exc

    return (
        int(user.pw_uid),
        int(group.gr_gid),
    )


def run_service(
    *,
    state_dir: (
        str | os.PathLike[str]
    ) = DEFAULT_STATE_DIR,
    runtime_dir: (
        str | os.PathLike[str]
    ) = DEFAULT_RUNTIME_DIR,
    stop_event: (
        threading.Event | None
    ) = None,
    ingress_socket: (
        str | os.PathLike[str] | None
    ) = None,
    ingress_allowed_uid: int | None = None,
    ingress_socket_gid: int | None = None,
) -> int:
    runtime_root = Path(runtime_dir)

    _prepare_runtime_directory(
        runtime_root
    )

    boot_id = _read_boot_id()

    _write_status(
        runtime_root,
        _status_payload(
            state="starting",
            boot_id=boot_id,
            checkpoint=None,
        ),
    )

    if stop_event is None:
        stop_event = threading.Event()

    ingress_server = None
    ingress_thread = None

    try:
        initialize_databases(
            state_dir
        )

        checkpoint = (
            processing_checkpoint(
                state_dir
            )
        )

        if ingress_socket is not None:
            if ingress_allowed_uid is None:
                raise DaemonError(
                    "ingress peer UID required"
                )

            ingress_server = IngressServer(
                state_dir=state_dir,
                socket_path=ingress_socket,
                boot_id=boot_id,
                allowed_uid=(
                    ingress_allowed_uid
                ),
                socket_gid=(
                    ingress_socket_gid
                ),
                stop_event=stop_event,
            )

            ingress_thread = threading.Thread(
                target=ingress_server.run,
                name="corvore-ingress",
                daemon=True,
            )

            ingress_thread.start()

            if not ingress_server.ready_event.wait(
                timeout=5
            ):
                raise DaemonError(
                    "ingress startup timed out"
                )

            if ingress_server.failure is not None:
                raise DaemonError(
                    "ingress startup failed: "
                    f"{ingress_server.failure}"
                )

        _write_status(
            runtime_root,
            _status_payload(
                state="running",
                boot_id=boot_id,
                checkpoint=checkpoint,
            ),
        )

        while not stop_event.wait(0.2):
            if (
                ingress_server is not None
                and ingress_server.failure
                is not None
            ):
                raise DaemonError(
                    "ingress failed: "
                    f"{ingress_server.failure}"
                )

        if (
            ingress_server is not None
            and ingress_server.failure
            is not None
        ):
            raise DaemonError(
                "ingress failed: "
                f"{ingress_server.failure}"
            )

        if ingress_thread is not None:
            ingress_thread.join(
                timeout=5
            )

            if ingress_thread.is_alive():
                raise DaemonError(
                    "ingress shutdown timed out"
                )

        _write_status(
            runtime_root,
            _status_payload(
                state="stopping",
                boot_id=boot_id,
                checkpoint=(
                    processing_checkpoint(
                        state_dir
                    )
                ),
            ),
        )

        _write_status(
            runtime_root,
            _status_payload(
                state="stopped",
                boot_id=boot_id,
                checkpoint=(
                    processing_checkpoint(
                        state_dir
                    )
                ),
            ),
        )

        return 0

    except Exception:
        stop_event.set()

        if (
            ingress_thread is not None
            and ingress_thread.is_alive()
        ):
            ingress_thread.join(
                timeout=5
            )

        try:
            checkpoint = \
                processing_checkpoint(
                    state_dir
                )
        except Exception:
            checkpoint = None

        try:
            _write_status(
                runtime_root,
                _status_payload(
                    state="failed",
                    boot_id=boot_id,
                    checkpoint=checkpoint,
                ),
            )
        except Exception:
            pass

        raise


def main(
    argv: list[str] | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        prog="corvored",
        description=(
            "CORVORE unprivileged "
            "core runtime."
        ),
    )

    parser.add_argument(
        "--state-dir",
        default=DEFAULT_STATE_DIR,
    )

    parser.add_argument(
        "--runtime-dir",
        default=DEFAULT_RUNTIME_DIR,
    )

    parser.add_argument(
        "--ingress-socket",
        default=DEFAULT_INGRESS_SOCKET,
    )

    parser.add_argument(
        "--ingress-peer-user",
        default=DEFAULT_INGRESS_PEER_USER,
    )

    parser.add_argument(
        "--ingress-group",
        default=DEFAULT_INGRESS_GROUP,
    )

    parser.add_argument(
        "--no-ingress",
        action="store_true",
    )

    args = parser.parse_args(argv)

    stop_event = threading.Event()

    def request_shutdown(
        signum: int,
        _frame: Any,
    ) -> None:
        del signum
        stop_event.set()

    signal.signal(
        signal.SIGTERM,
        request_shutdown,
    )

    signal.signal(
        signal.SIGINT,
        request_shutdown,
    )

    try:
        ingress_socket = None
        ingress_uid = None
        ingress_gid = None

        if not args.no_ingress:
            (
                ingress_uid,
                ingress_gid,
            ) = _resolve_ingress_identity(
                args.ingress_peer_user,
                args.ingress_group,
            )

            ingress_socket = (
                args.ingress_socket
            )

        return run_service(
            state_dir=args.state_dir,
            runtime_dir=args.runtime_dir,
            stop_event=stop_event,
            ingress_socket=ingress_socket,
            ingress_allowed_uid=(
                ingress_uid
            ),
            ingress_socket_gid=(
                ingress_gid
            ),
        )

    except (
        OSError,
        ValueError,
        StorageError,
        DaemonError,
    ) as exc:
        print(
            f"corvored: {exc}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
