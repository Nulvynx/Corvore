"""Spool -> authenticated ingress -> persistent observation integration."""

import os
from pathlib import Path
import tempfile
import threading
import unittest

from corvore.bridge_spool import DurableEventSpool
from corvore.ingress import IngressServer, send_event
from corvore.storage import initialize_databases, read_observations_after
from test_bridge_spool import BOOT_A, event_bytes


class SpoolIngressIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.state = root / 'state'
        self.socket = root / 'ingress' / 'events.sock'
        self.spool_dir = root / 'bridge-spool'
        initialize_databases(self.state)
        self.stop = threading.Event()
        self.server = IngressServer(
            state_dir=self.state, socket_path=self.socket,
            boot_id=BOOT_A, allowed_uid=os.getuid(), stop_event=self.stop,
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        self.assertTrue(self.server.ready_event.wait(timeout=5))
        if self.server.failure is not None:
            raise self.server.failure

    def tearDown(self):
        self.stop.set()
        self.thread.join(timeout=5)
        self.temp.cleanup()

    def test_spool_drains_identical_source_emissions_independently(self):
        with DurableEventSpool(
            self.spool_dir, boot_id_provider=lambda: BOOT_A,
        ) as spool:
            one = spool.enqueue(event_bytes(), source_instance='wlan0')
            two = spool.enqueue(event_bytes(), source_instance='wlan0')
            self.assertNotEqual(one, two)
            self.assertEqual(spool.dispatch_pending(self.socket), 2)
            self.assertEqual(spool.pending_count(), 0)
        rows = read_observations_after(self.state, after_ingest_seq=0)
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0]['observation_id'], rows[1]['observation_id'])

    def test_lost_ack_replays_into_one_observation(self):
        with DurableEventSpool(
            self.spool_dir, boot_id_provider=lambda: BOOT_A,
        ) as spool:
            original = spool.enqueue(event_bytes(), source_instance='wlan0')
            def lost_ack(path, raw, **kwargs):
                response = send_event(path, raw, **kwargs)
                self.assertEqual(response['status'], 'committed')
                raise TimeoutError('simulate lost ACK after core commit')
            with self.assertRaises(TimeoutError):
                spool.dispatch_one(self.socket, sender=lost_ack)
            self.assertEqual(spool.peek().delivery_id, original)
            self.assertEqual(len(read_observations_after(
                self.state, after_ingest_seq=0,
            )), 1)
            self.assertTrue(spool.dispatch_one(self.socket))
            self.assertEqual(spool.pending_count(), 0)
        self.assertEqual(len(read_observations_after(
            self.state, after_ingest_seq=0,
        )), 1)


if __name__ == '__main__':
    unittest.main()
