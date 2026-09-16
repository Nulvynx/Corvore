import sqlite3
import tempfile
from pathlib import Path
import unittest

from corvore.storage import (
    MigrationError,
    ObservationNotFoundError,
    OutboxEvent,
    ProcessingSequenceError,
    _validate_migration_sql,
    acknowledge_outbox_event,
    append_observation,
    commit_processing_step,
    initialize_databases,
    processing_checkpoint,
    read_observations_after,
    read_pending_outbox,
    storage_status,
)


class ProcessingRecoveryTests(
    unittest.TestCase
):
    def setUp(self):
        self.temp = \
            tempfile.TemporaryDirectory()

        self.state = (
            Path(self.temp.name)
            / "state"
        )

        initialize_databases(
            self.state
        )

    def tearDown(self):
        self.temp.cleanup()

    def append(
        self,
        index: int,
    ):
        return append_observation(
            self.state,
            observation_id=(
                f"test-observation-"
                f"{index:04d}"
            ),
            boot_id="test-boot-0001",
            monotonic_ns=index * 100,
            source_kind="test-harness",
            source_instance="recovery",
            observation_kind=(
                "storage.recovery"
            ),
            payload={
                "index": index,
            },
        )

    def event(
        self,
        index: int,
    ):
        return OutboxEvent(
            event_id=(
                f"test-outbox-event-"
                f"{index:04d}"
            ),
            topic="model.changed",
            payload={
                "index": index,
            },
        )

    def test_cross_database_crash_boundary_replays(self):
        self.append(1)

        self.assertEqual(
            processing_checkpoint(
                self.state
            ),
            0,
        )

        pending = \
            read_observations_after(
                self.state,
                after_ingest_seq=0,
            )

        self.assertEqual(
            [
                row["ingest_seq"]
                for row in pending
            ],
            [1],
        )

        result = \
            commit_processing_step(
                self.state,
                ingest_seq=1,
                events=[
                    self.event(1),
                ],
            )

        self.assertEqual(
            result["status"],
            "applied",
        )

        self.assertEqual(
            processing_checkpoint(
                self.state
            ),
            1,
        )

    def test_retry_is_idempotent(self):
        self.append(1)

        first = commit_processing_step(
            self.state,
            ingest_seq=1,
            events=[
                self.event(1),
            ],
        )

        second = commit_processing_step(
            self.state,
            ingest_seq=1,
            events=[
                self.event(1),
            ],
        )

        self.assertEqual(
            first["status"],
            "applied",
        )

        self.assertEqual(
            second["status"],
            "already_applied",
        )

        pending = read_pending_outbox(
            self.state
        )

        self.assertEqual(
            len(pending),
            1,
        )

        self.assertEqual(
            pending[0]["event_id"],
            self.event(1).event_id,
        )

    def test_processing_gap_is_rejected(self):
        self.append(1)
        self.append(2)

        with self.assertRaises(
            ProcessingSequenceError
        ):
            commit_processing_step(
                self.state,
                ingest_seq=2,
            )

        self.assertEqual(
            processing_checkpoint(
                self.state
            ),
            0,
        )

    def test_missing_observation_is_rejected(self):
        with self.assertRaises(
            ObservationNotFoundError
        ):
            commit_processing_step(
                self.state,
                ingest_seq=1,
            )

        self.assertEqual(
            processing_checkpoint(
                self.state
            ),
            0,
        )

    def test_outbox_and_checkpoint_rollback_together(self):
        self.append(1)

        duplicate = OutboxEvent(
            event_id=(
                "test-duplicate-event-0001"
            ),
            topic="model.changed",
            payload={"copy": 1},
        )

        with self.assertRaises(
            sqlite3.IntegrityError
        ):
            commit_processing_step(
                self.state,
                ingest_seq=1,
                events=[
                    duplicate,
                    duplicate,
                ],
            )

        self.assertEqual(
            processing_checkpoint(
                self.state
            ),
            0,
        )

        self.assertEqual(
            read_pending_outbox(
                self.state
            ),
            [],
        )

        result = commit_processing_step(
            self.state,
            ingest_seq=1,
            events=[
                self.event(1),
            ],
        )

        self.assertEqual(
            result["status"],
            "applied",
        )

        self.assertEqual(
            processing_checkpoint(
                self.state
            ),
            1,
        )

    def test_outbox_delivery_is_at_least_once_until_ack(self):
        self.append(1)

        commit_processing_step(
            self.state,
            ingest_seq=1,
            events=[
                self.event(1),
            ],
        )

        first = read_pending_outbox(
            self.state
        )

        second = read_pending_outbox(
            self.state
        )

        self.assertEqual(
            first,
            second,
        )

        self.assertEqual(
            len(first),
            1,
        )

        acknowledged = \
            acknowledge_outbox_event(
                self.state,
                event_id=(
                    self.event(1).event_id
                ),
            )

        self.assertEqual(
            acknowledged["status"],
            "acknowledged",
        )

        self.assertEqual(
            read_pending_outbox(
                self.state
            ),
            [],
        )

        again = \
            acknowledge_outbox_event(
                self.state,
                event_id=(
                    self.event(1).event_id
                ),
            )

        self.assertEqual(
            again["status"],
            "already_acknowledged",
        )

    def test_checkpoint_advances_contiguously(self):
        for index in range(1, 5):
            self.append(index)

        for index in range(1, 5):
            result = \
                commit_processing_step(
                    self.state,
                    ingest_seq=index,
                )

            self.assertEqual(
                result[
                    "processing_checkpoint"
                ],
                index,
            )

        self.assertEqual(
            processing_checkpoint(
                self.state
            ),
            4,
        )


class MigrationSafetyTests(
    unittest.TestCase
):
    def test_trigger_body_is_allowed(self):
        sql = """
        CREATE TABLE sample(
            id INTEGER PRIMARY KEY
        );

        CREATE TRIGGER sample_guard
        BEFORE DELETE ON sample
        BEGIN
            SELECT RAISE(
                ABORT,
                'immutable'
            );
        END;
        """

        _validate_migration_sql(sql)

    def test_explicit_transaction_control_is_rejected(self):
        samples = (
            "BEGIN;",
            "BEGIN IMMEDIATE;",
            "COMMIT;",
            "ROLLBACK;",
            "SAVEPOINT x;",
            "RELEASE x;",
            "END TRANSACTION;",
            "-- comment\\nBEGIN EXCLUSIVE;",
        )

        for sql in samples:
            with self.subTest(sql=sql):
                with self.assertRaises(
                    MigrationError
                ):
                    _validate_migration_sql(
                        sql
                    )

    def test_transaction_control_after_valid_statement_is_rejected(
        self,
    ):
        samples = (
            (
                "CREATE TABLE sample"
                "(id INTEGER); COMMIT;"
            ),
            (
                "CREATE TABLE sample"
                "(id INTEGER); BEGIN IMMEDIATE;"
            ),
            (
                "CREATE TABLE sample"
                "(id INTEGER); "
                "SAVEPOINT hidden;"
            ),
        )

        for sql in samples:
            with self.subTest(sql=sql):
                with self.assertRaises(
                    MigrationError
                ):
                    _validate_migration_sql(
                        sql
                    )

    def test_migration_metadata_tamper_is_detected(self):
        with tempfile.TemporaryDirectory() \
                as directory:
            state = Path(
                directory
            ) / "state"

            initialize_databases(
                state
            )

            connection = sqlite3.connect(
                state / "core.db"
            )

            connection.execute(
                """
                UPDATE schema_migrations
                SET sha256 = ?
                WHERE version = 1
                """,
                ("0" * 64,),
            )

            connection.commit()
            connection.close()

            with self.assertRaises(
                MigrationError
            ):
                storage_status(
                    state
                )


if __name__ == "__main__":
    unittest.main()
