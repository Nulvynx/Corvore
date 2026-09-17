import json
import os
from pathlib import Path
import socket
import stat
import tempfile
import threading
import time
import unittest

from corvore.ingress import (
    IngressProtocolError,
    IngressServer,
    MAX_EVENT_BYTES,
    decode_event_frame,
    encode_event_frame,
    send_event,
)
from corvore.storage import (
    initialize_databases,
    read_observations_after,
)


BOOT_ID = (
    "11111111-2222-3333-4444-"
    "555555555555"
)
DELIVERY_ID = "e31575a3-8721-496a-b8d2-2f037a7c2c6e"
OTHER_DELIVERY_ID = "3b860d5f-ed59-481d-a182-a6b7943dd4b8"


def valid_event():
    return {
        "tag": "wifi.ap.new",
        "time":
            "2026-09-17T10:30:00Z",
        "data": {
            "mac":
                "AA:BB:CC:DD:EE:FF",
            "rssi": -45,
            "frequency": 2412,
        },
    }


class IngressProtocolTests(unittest.TestCase):
    def test_frame_round_trip(self):
        raw = json.dumps(
            valid_event()
        ).encode("utf-8")

        frame = encode_event_frame(
            raw,
            source_instance="wlan0",
            delivery_id=DELIVERY_ID,
        )

        (
            source,
            delivery_id,
            decoded,
        ) = decode_event_frame(
            frame
        )

        self.assertEqual(
            source,
            "wlan0",
        )

        self.assertEqual(delivery_id, DELIVERY_ID)
        self.assertEqual(
            decoded,
            raw,
        )

    def test_unknown_header_field_is_rejected(
        self,
    ):
        header = json.dumps({
            "message_type":
                "bettercap_event",
            "schema_version": 2,
            "source_instance":
                "wlan0",
            "delivery_id": DELIVERY_ID,
            "unexpected": True,
        }).encode("utf-8")

        frame = (
            header
            + b"\n"
            + json.dumps(
                valid_event()
            ).encode("utf-8")
        )

        with self.assertRaises(
            IngressProtocolError
        ):
            decode_event_frame(frame)

    def test_oversized_source_event_is_rejected(
        self,
    ):
        with self.assertRaises(
            IngressProtocolError
        ):
            encode_event_frame(
                b"A"
                * (
                    MAX_EVENT_BYTES
                    + 1
                ),
                source_instance="wlan0",
                delivery_id=DELIVERY_ID,
            )

    def test_invalid_delivery_id_is_rejected(self):
        with self.assertRaises(IngressProtocolError):
            encode_event_frame(
                json.dumps(valid_event()),
                source_instance="wlan0",
                delivery_id="invalid",
            )


class IngressServerTests(unittest.TestCase):
    def setUp(self):
        self.temp = (
            tempfile.TemporaryDirectory()
        )

        self.root = Path(
            self.temp.name
        )

        self.state = (
            self.root
            / "state"
        )

        self.socket_path = (
            self.root
            / "ingress"
            / "events.sock"
        )

        initialize_databases(
            self.state
        )

        self.servers = []

    def tearDown(self):
        for (
            stop,
            thread,
            _server,
        ) in self.servers:
            stop.set()
            thread.join(timeout=5)

        self.temp.cleanup()

    def _start_server(
        self,
        *,
        allowed_uid=None,
    ):
        if allowed_uid is None:
            allowed_uid = os.getuid()

        stop = threading.Event()

        server = IngressServer(
            state_dir=self.state,
            socket_path=self.socket_path,
            boot_id=BOOT_ID,
            allowed_uid=allowed_uid,
            stop_event=stop,
        )

        thread = threading.Thread(
            target=server.run,
            daemon=True,
        )

        thread.start()

        self.assertTrue(
            server.ready_event.wait(
                timeout=5
            )
        )

        if server.failure is not None:
            raise server.failure

        self.servers.append(
            (
                stop,
                thread,
                server,
            )
        )

        return (
            stop,
            thread,
            server,
        )

    def test_ack_follows_committed_persistence(
        self,
    ):
        self._start_server()

        response = send_event(
            self.socket_path,
            json.dumps(valid_event()),
            source_instance="wlan0",
            delivery_id=DELIVERY_ID,
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
            rows[0]["ingest_seq"],
            response["ingest_seq"],
        )

    def test_retry_after_unread_ack_is_idempotent(
        self,
    ):
        self._start_server()

        raw = json.dumps(
            valid_event()
        )

        frame = encode_event_frame(
            raw,
            source_instance="wlan0",
            delivery_id=DELIVERY_ID,
        )

        client = socket.socket(
            socket.AF_UNIX,
            socket.SOCK_SEQPACKET,
        )

        try:
            client.connect(
                str(self.socket_path)
            )

            sent = client.send(frame)

            self.assertEqual(
                sent,
                len(frame),
            )

            # Do not read the ACK.
            # From the sender's application
            # perspective it has been lost.
        finally:
            client.close()

        deadline = (
            time.monotonic()
            + 2.0
        )

        while True:
            rows = read_observations_after(
                self.state,
                after_ingest_seq=0,
            )

            if rows:
                break

            if time.monotonic() >= deadline:
                self.fail(
                    "first event was not persisted"
                )

            time.sleep(0.01)

        self.assertEqual(
            len(rows),
            1,
        )

        first_seq = rows[0][
            "ingest_seq"
        ]

        retry = send_event(
            self.socket_path,
            raw,
            source_instance="wlan0",
            delivery_id=DELIVERY_ID,
        )

        self.assertEqual(
            retry["status"],
            "committed",
        )

        self.assertEqual(
            retry["ingest_seq"],
            first_seq,
        )

        rows = read_observations_after(
            self.state,
            after_ingest_seq=0,
        )

        self.assertEqual(
            len(rows),
            1,
        )

    def test_identical_source_emissions_are_distinct(self):
        self._start_server()
        raw = json.dumps(valid_event())
        first = send_event(
            self.socket_path, raw,
            source_instance="wlan0", delivery_id=DELIVERY_ID,
        )
        second = send_event(
            self.socket_path, raw,
            source_instance="wlan0", delivery_id=OTHER_DELIVERY_ID,
        )
        self.assertEqual(first["status"], "committed")
        self.assertEqual(second["status"], "committed")
        self.assertNotEqual(first["ingest_seq"], second["ingest_seq"])
        self.assertNotEqual(first["observation_id"], second["observation_id"])
        self.assertEqual(
            len(read_observations_after(self.state, after_ingest_seq=0)),
            2,
        )

    def test_reused_delivery_id_with_changed_payload_rejected(self):
        _, _, server = self._start_server()
        first = send_event(
            self.socket_path, json.dumps(valid_event()),
            source_instance="wlan0", delivery_id=DELIVERY_ID,
        )
        changed = valid_event()
        changed["data"]["rssi"] = -90
        conflict = send_event(
            self.socket_path, json.dumps(changed),
            source_instance="wlan0", delivery_id=DELIVERY_ID,
        )
        self.assertEqual(first["status"], "committed")
        self.assertEqual(conflict["status"], "rejected")
        self.assertIsNone(server.failure)
        self.assertEqual(
            len(read_observations_after(self.state, after_ingest_seq=0)),
            1,
        )

    def test_invalid_event_is_rejected_without_persistence(
        self,
    ):
        self._start_server()

        value = valid_event()
        value["tag"] = "wifi.deauth"

        response = send_event(
            self.socket_path,
            json.dumps(value),
            source_instance="wlan0",
            delivery_id=DELIVERY_ID,
        )

        self.assertEqual(
            response["status"],
            "rejected",
        )

        rows = read_observations_after(
            self.state,
            after_ingest_seq=0,
        )

        self.assertEqual(
            rows,
            [],
        )

    def test_peer_uid_is_enforced(self):
        self._start_server(
            allowed_uid=(
                os.getuid()
                + 1
            )
        )

        with self.assertRaises(
            IngressProtocolError
        ):
            send_event(
                self.socket_path,
                json.dumps(
                    valid_event()
                ),
                source_instance="wlan0",
                delivery_id=DELIVERY_ID,
            )

        rows = read_observations_after(
            self.state,
            after_ingest_seq=0,
        )

        self.assertEqual(
            rows,
            [],
        )

    def test_socket_permissions_and_cleanup(
        self,
    ):
        (
            stop,
            thread,
            server,
        ) = self._start_server()

        self.assertTrue(
            self.socket_path.is_socket()
        )

        mode = stat.S_IMODE(
            self.socket_path.stat().st_mode
        )

        self.assertEqual(
            mode,
            0o660,
        )

        stop.set()
        thread.join(timeout=5)

        self.assertFalse(
            thread.is_alive()
        )

        self.assertIsNone(
            server.failure
        )

        self.assertFalse(
            self.socket_path.exists()
        )


if __name__ == "__main__":
    unittest.main()
