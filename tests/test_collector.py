import json
import unittest

from corvore.collector import (
    CollectorEventError,
    build_observation,
    deterministic_observation_id,
    parse_bettercap_event,
)


BOOT_ID = "11111111-2222-3333-4444-555555555555"


def valid_event():
    return {
        "tag": "wifi.ap.new",
        "time": "2026-09-17T10:30:00.123456789+02:00",
        "data": {
            "mac": "AA:BB:CC:DD:EE:FF",
            "hostname": "lab-ap",
            "frequency": 2412,
            "rssi": -48,
            "encryption": "WPA2",
            "clients": [],
        },
    }


class BettercapCollectorContractTests(
    unittest.TestCase
):
    def test_valid_passive_event_is_normalized(
        self,
    ):
        event = parse_bettercap_event(
            json.dumps(valid_event())
        )

        observation = build_observation(
            event,
            boot_id=BOOT_ID,
            source_instance="wlan0",
            monotonic_ns=123456,
        )

        self.assertEqual(
            observation["source_kind"],
            "bettercap",
        )

        self.assertEqual(
            observation["observation_kind"],
            "wifi.ap.new",
        )

        self.assertEqual(
            observation["payload"][
                "schema_version"
            ],
            1,
        )

        self.assertEqual(
            observation["payload"][
                "source"
            ]["time"],
            valid_event()["time"],
        )

        self.assertIsNone(
            observation["observed_at_utc"]
        )

        self.assertFalse(
            observation[
                "clock_accuracy_verified"
            ]
        )

    def test_observation_id_is_stable_within_boot(
        self,
    ):
        event = parse_bettercap_event(
            json.dumps(valid_event())
        )

        first = deterministic_observation_id(
            event,
            boot_id=BOOT_ID,
            source_instance="wlan0",
        )

        second = deterministic_observation_id(
            event,
            boot_id=BOOT_ID,
            source_instance="wlan0",
        )

        self.assertEqual(first, second)

    def test_observation_id_changes_between_boots(
        self,
    ):
        event = parse_bettercap_event(
            json.dumps(valid_event())
        )

        first = deterministic_observation_id(
            event,
            boot_id=BOOT_ID,
            source_instance="wlan0",
        )

        second = deterministic_observation_id(
            event,
            boot_id=(
                "aaaaaaaa-bbbb-cccc-dddd-"
                "eeeeeeeeeeee"
            ),
            source_instance="wlan0",
        )

        self.assertNotEqual(first, second)

    def test_unsupported_event_tag_is_rejected(
        self,
    ):
        value = valid_event()
        value["tag"] = "wifi.deauth"

        with self.assertRaises(
            CollectorEventError
        ):
            parse_bettercap_event(
                json.dumps(value)
            )

    def test_unknown_envelope_field_is_rejected(
        self,
    ):
        value = valid_event()
        value["unexpected"] = True

        with self.assertRaises(
            CollectorEventError
        ):
            parse_bettercap_event(
                json.dumps(value)
            )

    def test_non_object_data_is_rejected(
        self,
    ):
        value = valid_event()
        value["data"] = []

        with self.assertRaises(
            CollectorEventError
        ):
            parse_bettercap_event(
                json.dumps(value)
            )

    def test_oversized_event_is_rejected(
        self,
    ):
        value = valid_event()

        value["data"]["hostname"] = (
            "A" * (64 * 1024)
        )

        with self.assertRaises(
            CollectorEventError
        ):
            parse_bettercap_event(
                json.dumps(value)
            )

    def test_excessive_nesting_is_rejected(
        self,
    ):
        nested = {}

        current = nested

        for _ in range(18):
            current["child"] = {}
            current = current["child"]

        value = valid_event()
        value["data"] = nested

        with self.assertRaises(
            CollectorEventError
        ):
            parse_bettercap_event(
                json.dumps(value)
            )

    def test_non_finite_json_number_is_rejected(
        self,
    ):
        raw = (
            '{"tag":"wifi.ap.new",'
            '"time":"2026-09-17T10:30:00Z",'
            '"data":{"rssi":NaN}}'
        )

        with self.assertRaises(
            CollectorEventError
        ):
            parse_bettercap_event(raw)

    def test_invalid_source_instance_is_rejected(
        self,
    ):
        event = parse_bettercap_event(
            json.dumps(valid_event())
        )

        with self.assertRaises(
            CollectorEventError
        ):
            build_observation(
                event,
                boot_id=BOOT_ID,
                source_instance="../wlan0",
                monotonic_ns=1,
            )


if __name__ == "__main__":
    unittest.main()
