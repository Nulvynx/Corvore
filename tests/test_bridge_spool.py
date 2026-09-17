"""Tests for the durable passive-event bridge spool; no radio required."""

from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid

from corvore.bridge_spool import (
    DurableEventSpool,
    SpoolDeliveryRejected,
    SpoolError,
    SpoolFullError,
)
from corvore.collector import deterministic_observation_id


BOOT_A = 'e31575a3-8721-496a-b8d2-2f037a7c2c6e'
BOOT_B = '3b860d5f-ed59-481d-a182-a6b7943dd4b8'


def event_bytes():
    return json.dumps({
        'tag': 'wifi.ap.new',
        'time': '2026-09-17T12:20:00Z',
        'data': {'mac': '02:00:00:00:00:01', 'frequency': 2412},
    }, separators=(',', ':')).encode('utf-8')


class SpoolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name) / 'private-spool'
        self.boot = BOOT_A

    def tearDown(self):
        self.temp.cleanup()

    def open_spool(self, **kwargs):
        return DurableEventSpool(
            self.directory, boot_id_provider=lambda: self.boot, **kwargs
        )

    @staticmethod
    def committed(_socket, _raw, *, source_instance, delivery_id,
                  **_provenance):
        return {
            'status': 'committed', 'ingest_seq': 17,
            'observation_id': deterministic_observation_id(
                boot_id=BOOT_A, source_instance=source_instance,
                delivery_id=delivery_id,
            ),
        }

    def test_reopen_preserves_bytes_delivery_id_and_fifo(self):
        raw = event_bytes()
        with self.open_spool() as spool:
            first = spool.enqueue(raw, source_instance='wlan0')
            second = spool.enqueue(raw, source_instance='wlan0')
            self.assertNotEqual(first, second)
            self.assertEqual(spool.pending_count(), 2)
        with self.open_spool() as spool:
            entry = spool.peek()
            self.assertEqual(entry.delivery_id, first)
            self.assertEqual(entry.raw_event, raw)
            self.assertTrue(spool.dispatch_one('/unused', sender=self.committed))
            self.assertEqual(spool.peek().delivery_id, second)
            self.assertTrue(spool.dispatch_one('/unused', sender=self.committed))
            self.assertEqual(spool.pending_count(), 0)
            self.assertFalse(spool.dispatch_one('/unused', sender=self.committed))

    def test_transport_failure_retains_exact_event(self):
        raw = event_bytes()
        with self.open_spool() as spool:
            delivery_id = spool.enqueue(raw, source_instance='wlan0')
            def broken(*_args, **_kwargs):
                raise TimeoutError('transport failure')
            with self.assertRaises(TimeoutError):
                spool.dispatch_one('/unused', sender=broken)
            self.assertEqual(spool.peek().delivery_id, delivery_id)
            self.assertEqual(spool.peek().raw_event, raw)
            self.assertTrue(spool.dispatch_one('/unused', sender=self.committed))

    def test_lost_ack_after_commit_reuses_original_delivery_id(self):
        seen = []
        with self.open_spool() as spool:
            delivery_id = spool.enqueue(event_bytes(), source_instance='wlan0')
            def lost_ack(path, raw, *, source_instance, delivery_id,
                         **_provenance):
                seen.append(delivery_id)
                self.committed(path, raw, source_instance=source_instance,
                               delivery_id=delivery_id)
                raise TimeoutError('ACK not received')
            with self.assertRaises(TimeoutError):
                spool.dispatch_one('/unused', sender=lost_ack)
            self.assertEqual(spool.pending_count(), 1)
            self.assertTrue(spool.dispatch_one(
                '/unused', sender=lambda path, raw, **kw: (
                    seen.append(kw['delivery_id']) or self.committed(path, raw, **kw)
                )
            ))
            self.assertEqual(seen, [delivery_id, delivery_id])
            self.assertEqual(spool.pending_count(), 0)

    def test_rejected_and_forged_acks_retain_item(self):
        with self.open_spool() as spool:
            spool.enqueue(event_bytes(), source_instance='wlan0')
            cases = [
                lambda *_args, **_kwargs: {'status': 'rejected'},
                lambda *_args, **_kwargs: {'status': 'committed', 'ingest_seq': 1,
                                           'observation_id': str(uuid.uuid4())},
                lambda *_args, **_kwargs: {'status': 'committed', 'ingest_seq': True,
                                           'observation_id': str(uuid.uuid4())},
            ]
            for sender in cases:
                with self.assertRaises(SpoolDeliveryRejected):
                    spool.dispatch_one('/unused', sender=sender)
                self.assertEqual(spool.pending_count(), 1)

    def test_cross_boot_replay_preserves_original_provenance(self):
        with self.open_spool() as spool:
            delivery_id = spool.enqueue(event_bytes(), source_instance='wlan0')
            original = spool.peek()
        self.boot = BOOT_B
        with self.open_spool() as spool:
            current = spool.peek()
            self.assertEqual(current.delivery_id, delivery_id)
            self.assertEqual(current.boot_id, BOOT_A)
            self.assertEqual(
                current.origin_monotonic_ns,
                original.origin_monotonic_ns,
            )
            self.assertTrue(
                spool.dispatch_one('/unused', sender=self.committed)
            )
            self.assertEqual(spool.pending_count(), 0)

    def test_full_queue_is_fail_closed_without_drop(self):
        with self.open_spool(max_events=1) as spool:
            first = spool.enqueue(event_bytes(), source_instance='wlan0')
            with self.assertRaises(SpoolFullError):
                spool.enqueue(event_bytes(), source_instance='wlan0')
            self.assertEqual(spool.peek().delivery_id, first)
            self.assertEqual(spool.pending_count(), 1)

    def test_disallowed_source_event_rejected_before_storage(self):
        bad = json.dumps({'tag': 'wifi.deauth', 'time': 'now', 'data': {}}).encode()
        with self.open_spool() as spool:
            with self.assertRaises(ValueError):
                spool.enqueue(bad, source_instance='wlan0')
            self.assertEqual(spool.pending_count(), 0)

    def test_spool_path_symlink_rejected(self):
        target = Path(self.temp.name) / 'target'
        target.mkdir(mode=0o700)
        self.directory.symlink_to(target, target_is_directory=True)
        with self.assertRaises(SpoolError):
            with self.open_spool():
                pass

    def test_permissive_directory_rejected(self):
        self.directory.mkdir(mode=0o700)
        self.directory.chmod(0o755)
        with self.assertRaises(SpoolError):
            with self.open_spool():
                pass

    def test_second_owner_rejected(self):
        with self.open_spool():
            with self.assertRaises(SpoolError):
                with self.open_spool():
                    pass

    def test_integrity_mismatch_detected_before_delivery(self):
        with self.open_spool() as spool:
            spool.enqueue(event_bytes(), source_instance='wlan0')
            spool._connection().execute("UPDATE pending SET sha256='0' WHERE seq=1")
            with self.assertRaises(SpoolError):
                spool.dispatch_one('/unused', sender=self.committed)
            self.assertEqual(spool.pending_count(), 1)

    def test_database_identity_mismatch_is_rejected(self):
        with self.open_spool():
            pass
        with closing(sqlite3.connect(self.directory / 'bridge-queue.db')) as db:
            db.execute('PRAGMA application_id=0')
        with self.assertRaises(SpoolError):
            with self.open_spool():
                pass


if __name__ == '__main__':
    unittest.main()
