"""Live bridge boundary tests without a radio or external network."""

import json
import os
from pathlib import Path
import tempfile
import threading
import unittest

from corvore.bridge_source import (
    BridgeSourceError,
    live_bettercap_events,
    read_api_credentials,
    relay_events,
)
from corvore.bridge_spool import DurableEventSpool
from corvore.ingress import IngressServer
from corvore.storage import (
    initialize_databases,
    read_observations_after,
)

from test_bridge_spool import BOOT_A, event_bytes


class FakeConnection:
    def __init__(self, messages):
        self.messages = messages

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def __iter__(self):
        return iter(self.messages)


class BridgeSourceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.credentials = self.root / "credentials.json"

        self.credentials.write_text(
            json.dumps({
                "username": "collector",
                "password": "local-test-secret",
            }),
            encoding="utf-8",
        )

        self.credentials.chmod(0o600)

    def tearDown(self):
        self.temp.cleanup()

    def test_credentials_require_private_permissions(self):
        self.assertEqual(
            read_api_credentials(self.credentials),
            ("collector", "local-test-secret"),
        )

        self.credentials.chmod(0o644)

        with self.assertRaises(BridgeSourceError):
            read_api_credentials(self.credentials)

    def test_credentials_symlink_is_rejected(self):
        link = self.root / "credentials-link"
        link.symlink_to(self.credentials)

        with self.assertRaises(OSError):
            read_api_credentials(link)

    def test_websocket_is_loopback_and_filters_other_events(self):
        captured = {}

        def connector(uri, **kwargs):
            captured["uri"] = uri
            captured["options"] = kwargs

            return FakeConnection([
                json.dumps({
                    "tag": "sys.log",
                    "time": "2026-09-17T12:00:00Z",
                    "data": {"Message": "not an observation"},
                }),
                event_bytes().decode("utf-8"),
            ])

        events = list(live_bettercap_events(
            port=8081,
            credentials_file=self.credentials,
            connector=connector,
        ))

        self.assertEqual(events, [event_bytes()])
        self.assertEqual(
            captured["uri"],
            "ws://127.0.0.1:8081/api/events",
        )
        self.assertIsNone(captured["options"]["proxy"])
        self.assertEqual(captured["options"]["max_queue"], 1)
        self.assertEqual(
            captured["options"]["max_size"],
            64 * 1024,
        )

    def test_invalid_passive_event_is_not_accepted(self):
        def connector(_uri, **_kwargs):
            return FakeConnection([
                json.dumps({
                    "tag": "wifi.ap.new",
                    "time": "now",
                    "data": [],
                }),
            ])

        with self.assertRaises(ValueError):
            list(live_bettercap_events(
                port=8081,
                credentials_file=self.credentials,
                connector=connector,
            ))

    def test_oversized_websocket_event_is_rejected(self):
        def connector(_uri, **_kwargs):
            return FakeConnection(["X" * (64 * 1024 + 1)])

        with self.assertRaises(BridgeSourceError):
            list(live_bettercap_events(
                port=8081,
                credentials_file=self.credentials,
                connector=connector,
            ))

    def test_relay_commits_distinct_identical_emissions(self):
        state = self.root / "state"
        socket_path = self.root / "ingress" / "events.sock"
        spool_dir = self.root / "spool"

        initialize_databases(state)

        stop = threading.Event()

        server = IngressServer(
            state_dir=state,
            socket_path=socket_path,
            boot_id=BOOT_A,
            allowed_uid=os.getuid(),
            stop_event=stop,
        )

        thread = threading.Thread(
            target=server.run,
            daemon=True,
        )

        thread.start()

        try:
            self.assertTrue(server.ready_event.wait(timeout=5))

            if server.failure is not None:
                raise server.failure

            with DurableEventSpool(
                spool_dir,
                boot_id_provider=lambda: BOOT_A,
            ) as spool:
                accepted = relay_events(
                    spool,
                    socket_path,
                    "wlan0",
                    iter([event_bytes(), event_bytes()]),
                )

                self.assertEqual(accepted, 2)
                self.assertEqual(spool.pending_count(), 0)

            rows = read_observations_after(
                state,
                after_ingest_seq=0,
            )

            self.assertEqual(len(rows), 2)
            self.assertNotEqual(
                rows[0]["observation_id"],
                rows[1]["observation_id"],
            )

        finally:
            stop.set()
            thread.join(timeout=5)

    def test_ingress_failure_retains_queued_event(self):
        with DurableEventSpool(
            self.root / "spool",
            boot_id_provider=lambda: BOOT_A,
        ) as spool:
            with self.assertRaises(Exception):
                relay_events(
                    spool,
                    self.root / "missing.sock",
                    "wlan0",
                    iter([event_bytes()]),
                )

            self.assertEqual(spool.pending_count(), 1)
            self.assertEqual(spool.peek().raw_event, event_bytes())


if __name__ == "__main__":
    unittest.main()
