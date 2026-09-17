import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from corvore.daemon import (
    DaemonError,
    read_runtime_status,
    run_service,
)
from corvore.ingress import (
    send_event,
)
from corvore.storage import (
    processing_checkpoint,
    read_observations_after,
    storage_status,
)


class DaemonLifecycleTests(
    unittest.TestCase
):
    def setUp(self):
        self.temp = \
            tempfile.TemporaryDirectory()

        root = Path(self.temp.name)

        self.state = root / "state"
        self.runtime = root / "runtime"

    def tearDown(self):
        self.temp.cleanup()

    def _wait_for_state(
        self,
        expected: str,
        timeout: float = 5.0,
    ):
        deadline = (
            time.monotonic()
            + timeout
        )

        while time.monotonic() < deadline:
            try:
                status = read_runtime_status(
                    self.runtime
                )
            except DaemonError:
                time.sleep(0.01)
                continue

            if status.get("state") \
                    == expected:
                return status

            time.sleep(0.01)

        self.fail(
            f"runtime did not reach "
            f"{expected!r}"
        )

    def test_service_initializes_and_stops_cleanly(
        self,
    ):
        stop = threading.Event()

        result = {}

        def target():
            result["code"] = run_service(
                state_dir=self.state,
                runtime_dir=self.runtime,
                stop_event=stop,
            )

        thread = threading.Thread(
            target=target,
            daemon=True,
        )

        thread.start()

        running = self._wait_for_state(
            "running"
        )

        self.assertEqual(
            running["service"],
            "corvored",
        )

        self.assertEqual(
            running[
                "processing_checkpoint"
            ],
            0,
        )

        persistence = storage_status(
            self.state
        )

        self.assertEqual(
            persistence["databases"][
                "core"
            ]["integrity_check"],
            "ok",
        )

        self.assertEqual(
            persistence["databases"][
                "observations"
            ]["integrity_check"],
            "ok",
        )

        stop.set()

        thread.join(timeout=5)

        self.assertFalse(
            thread.is_alive()
        )

        self.assertEqual(
            result["code"],
            0,
        )

        stopped = \
            read_runtime_status(
                self.runtime
            )

        self.assertEqual(
            stopped["state"],
            "stopped",
        )

        self.assertEqual(
            processing_checkpoint(
                self.state
            ),
            0,
        )

    def test_service_ingress_commits_observation(
        self,
    ):
        stop = threading.Event()

        socket_path = (
            Path(self.temp.name)
            / "ingress"
            / "events.sock"
        )

        result = {}

        def target():
            result["code"] = run_service(
                state_dir=self.state,
                runtime_dir=self.runtime,
                stop_event=stop,
                ingress_socket=socket_path,
                ingress_allowed_uid=(
                    os.getuid()
                ),
            )

        thread = threading.Thread(
            target=target,
            daemon=True,
        )

        thread.start()

        running = self._wait_for_state(
            "running"
        )

        self.assertEqual(
            running["service"],
            "corvored",
        )

        self.assertTrue(
            socket_path.is_socket()
        )

        response = send_event(
            socket_path,
            json.dumps({
                "tag": "wifi.ap.new",
                "time":
                    "2026-09-17T11:00:00Z",
                "data": {
                    "mac":
                        "AA:BB:CC:DD:EE:FF",
                    "frequency":
                        2412,
                    "rssi":
                        -40,
                },
            }),
            source_instance="wlan0",
        )

        self.assertEqual(
            response["status"],
            "committed",
        )

        rows = read_observations_after(
            self.state,
            after_ingest_seq=0,
        )

        self.assertEqual(
            len(rows),
            1,
        )

        self.assertEqual(
            rows[0][
                "observation_kind"
            ],
            "wifi.ap.new",
        )

        self.assertEqual(
            rows[0][
                "ingest_seq"
            ],
            response[
                "ingest_seq"
            ],
        )

        stop.set()

        thread.join(
            timeout=5
        )

        self.assertFalse(
            thread.is_alive()
        )

        self.assertEqual(
            result["code"],
            0,
        )

        self.assertFalse(
            socket_path.exists()
        )

    def test_runtime_directory_symlink_is_rejected(
        self,
    ):
        target = (
            Path(self.temp.name)
            / "actual-runtime"
        )

        target.mkdir()

        os.symlink(
            target,
            self.runtime,
        )

        with self.assertRaises(
            DaemonError
        ):
            run_service(
                state_dir=self.state,
                runtime_dir=self.runtime,
                stop_event=(
                    threading.Event()
                ),
            )

    def test_process_handles_sigterm(
        self,
    ):
        environment = dict(
            os.environ
        )

        environment["PYTHONPATH"] = \
            "node"

        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "corvore.daemon",
                "--state-dir",
                str(self.state),
                "--runtime-dir",
                str(self.runtime),
                "--no-ingress",
            ],
            cwd=Path.cwd(),
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            running = \
                self._wait_for_state(
                    "running"
                )

            self.assertEqual(
                running["pid"],
                process.pid,
            )

            process.send_signal(
                signal.SIGTERM
            )

            stdout, stderr = \
                process.communicate(
                    timeout=5
                )

            self.assertEqual(
                process.returncode,
                0,
                msg=(
                    f"stdout={stdout!r} "
                    f"stderr={stderr!r}"
                ),
            )

            status = \
                read_runtime_status(
                    self.runtime
                )

            self.assertEqual(
                status["state"],
                "stopped",
            )

        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


class RuntimeStatusTests(
    unittest.TestCase
):
    def test_status_file_is_bounded_json(self):
        with tempfile.TemporaryDirectory() \
                as directory:
            root = Path(directory)

            runtime = root / "runtime"
            state = root / "state"

            stop = threading.Event()

            thread = threading.Thread(
                target=run_service,
                kwargs={
                    "state_dir": state,
                    "runtime_dir":
                        runtime,
                    "stop_event": stop,
                },
                daemon=True,
            )

            thread.start()

            deadline = (
                time.monotonic()
                + 5
            )

            path = (
                runtime
                / "corvored.json"
            )

            while (
                time.monotonic()
                < deadline
            ):
                if path.is_file():
                    try:
                        payload = \
                            json.loads(
                                path.read_text()
                            )
                    except (
                        OSError,
                        json.JSONDecodeError,
                    ):
                        time.sleep(0.01)
                        continue

                    if payload.get(
                        "state"
                    ) == "running":
                        break

                time.sleep(0.01)
            else:
                self.fail(
                    "runtime status "
                    "not available"
                )

            self.assertLess(
                path.stat().st_size,
                16 * 1024,
            )

            self.assertEqual(
                payload[
                    "schema_version"
                ],
                1,
            )

            stop.set()
            thread.join(timeout=5)

            self.assertFalse(
                thread.is_alive()
            )


if __name__ == "__main__":
    unittest.main()
