"""Origin provenance through a device restart; no radio required."""

import json
import os
from pathlib import Path
import tempfile
import threading
import unittest

from corvore.bridge_spool import DurableEventSpool
from corvore.ingress import IngressServer, send_event
from corvore.storage import initialize_databases, read_observations_after
from test_bridge_spool import BOOT_A, BOOT_B, event_bytes


class CrossBootIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.state = root / "state"
        self.socket = root / "ingress" / "events.sock"
        self.spool_dir = root / "spool"
        self.boot = BOOT_A
        initialize_databases(self.state)
        self.active = []

    def tearDown(self):
        for stop, thread in self.active:
            stop.set()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.temp.cleanup()

    def start_server(self, boot_id):
        stop = threading.Event()
        server = IngressServer(
            state_dir=self.state,
            socket_path=self.socket,
            boot_id=boot_id,
            allowed_uid=os.getuid(),
            stop_event=stop,
            require_origin=True,
        )
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        self.assertTrue(server.ready_event.wait(timeout=5))
        if server.failure is not None:
            raise server.failure
        self.active.append((stop, thread))
        return stop, thread

    def stop_server(self, stop, thread):
        stop.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.active.remove((stop, thread))

    def spool(self):
        return DurableEventSpool(
            self.spool_dir,
            boot_id_provider=lambda: self.boot,
        )

    def test_undelivered_event_retains_old_boot_after_restart(self):
        with self.spool() as spool:
            delivery_id = spool.enqueue(
                event_bytes(), source_instance="wlan0"
            )
            captured = spool.peek().origin_monotonic_ns

        self.boot = BOOT_B
        self.start_server(BOOT_B)

        with self.spool() as spool:
            self.assertEqual(spool.peek().delivery_id, delivery_id)
            self.assertTrue(spool.dispatch_one(self.socket))
            self.assertEqual(spool.pending_count(), 0)

        rows = read_observations_after(
            self.state, after_ingest_seq=0
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["boot_id"], BOOT_A)
        self.assertEqual(rows[0]["monotonic_ns"], captured)

    def test_lost_ack_before_restart_does_not_duplicate(self):
        stop, thread = self.start_server(BOOT_A)

        with self.spool() as spool:
            spool.enqueue(event_bytes(), source_instance="wlan0")
            entry = spool.peek()

            first = send_event(
                self.socket,
                entry.raw_event,
                source_instance=entry.source_instance,
                delivery_id=entry.delivery_id,
                origin_boot_id=entry.boot_id,
                origin_monotonic_ns=entry.origin_monotonic_ns,
            )
            self.assertEqual(first["status"], "committed")
            self.assertEqual(spool.pending_count(), 1)

        self.stop_server(stop, thread)
        self.boot = BOOT_B
        self.start_server(BOOT_B)

        with self.spool() as spool:
            self.assertTrue(spool.dispatch_one(self.socket))
            self.assertEqual(spool.pending_count(), 0)

        rows = read_observations_after(
            self.state, after_ingest_seq=0
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ingest_seq"], first["ingest_seq"])
        self.assertEqual(
            rows[0]["observation_id"], first["observation_id"]
        )
        self.assertEqual(rows[0]["boot_id"], BOOT_A)
        self.assertEqual(
            rows[0]["monotonic_ns"], entry.origin_monotonic_ns
        )

    def test_production_ingress_rejects_missing_origin(self):
        self.start_server(BOOT_B)

        response = send_event(
            self.socket,
            event_bytes(),
            source_instance="wlan0",
            delivery_id="e31575a3-8721-496a-b8d2-2f037a7c2c6e",
        )
        self.assertEqual(response["status"], "rejected")
        self.assertEqual(
            read_observations_after(self.state, after_ingest_seq=0),
            [],
        )


if __name__ == "__main__":
    unittest.main()
