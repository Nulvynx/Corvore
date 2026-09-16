import hashlib
import sqlite3
import tempfile
from pathlib import Path
import unittest

from corvore.storage import (
    CORE_APPLICATION_ID,
    OBSERVATIONS_APPLICATION_ID,
    StorageError,
    append_observation,
    initialize_databases,
    read_observations_after,
    storage_status,
)


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(
            self.temp.name
        ) / "state"

    def tearDown(self):
        self.temp.cleanup()

    def test_initialization_policy(self):
        result = initialize_databases(
            self.state
        )

        core = result[
            "databases"
        ]["core"]

        obs = result[
            "databases"
        ]["observations"]

        self.assertEqual(
            core["application_id"],
            CORE_APPLICATION_ID,
        )

        self.assertEqual(
            obs["application_id"],
            OBSERVATIONS_APPLICATION_ID,
        )

        self.assertEqual(
            core["journal_mode"],
            "wal",
        )

        self.assertEqual(
            obs["journal_mode"],
            "wal",
        )

        self.assertEqual(
            core["synchronous_policy"],
            "FULL",
        )

        self.assertEqual(
            obs["synchronous_policy"],
            "NORMAL",
        )

        self.assertTrue(
            core["foreign_keys_enabled"]
        )

        self.assertTrue(
            obs["foreign_keys_enabled"]
        )

        self.assertEqual(
            core["integrity_check"],
            "ok",
        )

        self.assertEqual(
            obs["integrity_check"],
            "ok",
        )

        self.assertEqual(
            core["user_version"],
            1,
        )

        self.assertEqual(
            obs["user_version"],
            1,
        )

    def test_initialization_is_idempotent(self):
        first = initialize_databases(
            self.state
        )

        second = initialize_databases(
            self.state
        )

        self.assertEqual(
            first["databases"]["core"][
                "migrations"
            ],
            second["databases"]["core"][
                "migrations"
            ],
        )

        self.assertEqual(
            first["databases"][
                "observations"
            ]["migrations"],
            second["databases"][
                "observations"
            ]["migrations"],
        )

    def test_append_survives_reopen(self):
        initialize_databases(
            self.state
        )

        one = append_observation(
            self.state,
            observation_id=(
                "test-observation-0001"
            ),
            boot_id="test-boot-0001",
            monotonic_ns=100,
            source_kind="test-harness",
            source_instance="unit",
            observation_kind="storage.contract",
            payload={
                "index": 1,
                "text": "alpha",
            },
        )

        two = append_observation(
            self.state,
            observation_id=(
                "test-observation-0002"
            ),
            boot_id="test-boot-0001",
            monotonic_ns=200,
            source_kind="test-harness",
            source_instance="unit",
            observation_kind="storage.contract",
            payload={
                "index": 2,
                "text": "beta",
            },
        )

        self.assertEqual(
            one["ingest_seq"],
            1,
        )

        self.assertEqual(
            two["ingest_seq"],
            2,
        )

        status = storage_status(
            self.state
        )

        self.assertEqual(
            status["databases"][
                "observations"
            ]["integrity_check"],
            "ok",
        )

        rows = read_observations_after(
            self.state,
            after_ingest_seq=0,
        )

        self.assertEqual(
            [
                row["ingest_seq"]
                for row in rows
            ],
            [1, 2],
        )

        self.assertEqual(
            rows[0]["payload"]["text"],
            "alpha",
        )

        self.assertEqual(
            rows[1]["payload"]["text"],
            "beta",
        )

    def test_observation_log_is_append_only(self):
        initialize_databases(
            self.state
        )

        append_observation(
            self.state,
            observation_id=(
                "test-observation-0001"
            ),
            boot_id="test-boot-0001",
            monotonic_ns=100,
            source_kind="test-harness",
            source_instance="unit",
            observation_kind="storage.contract",
            payload={"index": 1},
        )

        connection = sqlite3.connect(
            self.state / "observations.db"
        )

        try:
            with self.assertRaises(
                sqlite3.IntegrityError
            ):
                connection.execute(
                    """
                    UPDATE observation_log
                    SET observation_kind = 'changed'
                    WHERE ingest_seq = 1
                    """
                )

            with self.assertRaises(
                sqlite3.IntegrityError
            ):
                connection.execute(
                    """
                    DELETE FROM observation_log
                    WHERE ingest_seq = 1
                    """
                )

        finally:
            connection.close()

    def test_wrong_database_identity_rejected(self):
        self.state.mkdir(
            parents=True
        )

        db = self.state / "core.db"

        connection = sqlite3.connect(db)
        connection.execute(
            "PRAGMA application_id = 12345"
        )
        connection.close()

        with self.assertRaises(
            StorageError
        ):
            initialize_databases(
                self.state
            )


class StorageContractHardeningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = (
            Path(self.temp.name)
            / "state"
        )

        initialize_databases(
            self.state
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_non_string_json_key_is_rejected(self):
        with self.assertRaises(
            ValueError
        ):
            append_observation(
                self.state,
                observation_id=(
                    "test-invalid-key-0001"
                ),
                boot_id="test-boot-0001",
                monotonic_ns=1,
                source_kind="test-harness",
                source_instance="unit",
                observation_kind=(
                    "storage.contract"
                ),
                payload={
                    1: "integer",
                    "1": "string",
                },
            )

    def test_non_finite_float_is_rejected(self):
        with self.assertRaises(
            ValueError
        ):
            append_observation(
                self.state,
                observation_id=(
                    "test-invalid-float-01"
                ),
                boot_id="test-boot-0001",
                monotonic_ns=1,
                source_kind="test-harness",
                source_instance="unit",
                observation_kind=(
                    "storage.contract"
                ),
                payload={
                    "value": float("nan")
                },
            )

    def test_verified_clock_requires_utc(self):
        with self.assertRaises(
            ValueError
        ):
            append_observation(
                self.state,
                observation_id=(
                    "test-time-invalid-0001"
                ),
                boot_id="test-boot-0001",
                monotonic_ns=1,
                source_kind="test-harness",
                source_instance="unit",
                observation_kind=(
                    "storage.contract"
                ),
                payload={"value": 1},
                clock_accuracy_verified=True,
            )

        with self.assertRaises(
            ValueError
        ):
            append_observation(
                self.state,
                observation_id=(
                    "test-time-invalid-0002"
                ),
                boot_id="test-boot-0001",
                monotonic_ns=2,
                source_kind="test-harness",
                source_instance="unit",
                observation_kind=(
                    "storage.contract"
                ),
                payload={"value": 2},
                observed_at_utc=(
                    "2026-09-17T00:00:00+02:00"
                ),
                clock_accuracy_verified=True,
            )

        append_observation(
            self.state,
            observation_id=(
                "test-time-valid-0001"
            ),
            boot_id="test-boot-0001",
            monotonic_ns=3,
            source_kind="test-harness",
            source_instance="unit",
            observation_kind=(
                "storage.contract"
            ),
            payload={"value": 3},
            observed_at_utc=(
                "2026-09-16T22:00:00Z"
            ),
            clock_accuracy_verified=True,
        )

        rows = read_observations_after(
            self.state,
            after_ingest_seq=0,
        )

        self.assertEqual(
            rows[-1]["observed_at_utc"],
            "2026-09-16T22:00:00Z",
        )

        self.assertTrue(
            rows[-1][
                "clock_accuracy_verified"
            ]
        )

    def test_status_does_not_modify_database_files(self):
        paths = (
            self.state / "core.db",
            self.state / "observations.db",
        )

        def fingerprint(path):
            stat = path.stat()

            return (
                stat.st_size,
                stat.st_mtime_ns,
                hashlib.sha256(
                    path.read_bytes()
                ).hexdigest(),
            )

        before = {
            path.name: fingerprint(path)
            for path in paths
        }

        storage_status(
            self.state
        )

        after = {
            path.name: fingerprint(path)
            for path in paths
        }

        self.assertEqual(
            before,
            after,
        )


class StorageSchemaPolicyTests(
    unittest.TestCase
):
    def test_observation_sequence_avoids_autoincrement(self):
        with tempfile.TemporaryDirectory() as directory:
            state = (
                Path(directory)
                / "state"
            )

            initialize_databases(
                state
            )

            connection = sqlite3.connect(
                state / "observations.db"
            )

            try:
                row = connection.execute(
                    """
                    SELECT sql
                    FROM sqlite_master
                    WHERE type = 'table'
                      AND name = 'observation_log'
                    """
                ).fetchone()
            finally:
                connection.close()

            self.assertIsNotNone(row)

            schema = str(row[0]).upper()

            self.assertNotIn(
                "AUTOINCREMENT",
                schema,
            )

            sequences = []

            for index in range(1, 4):
                result = append_observation(
                    state,
                    observation_id=(
                        f"sequence-test-"
                        f"observation-{index:04d}"
                    ),
                    boot_id=(
                        "sequence-test-boot"
                    ),
                    monotonic_ns=index,
                    source_kind="test-harness",
                    source_instance="schema",
                    observation_kind=(
                        "storage.sequence"
                    ),
                    payload={
                        "index": index,
                    },
                )

                sequences.append(
                    result["ingest_seq"]
                )

            self.assertEqual(
                sequences,
                [1, 2, 3],
            )

if __name__ == "__main__":
    unittest.main()
