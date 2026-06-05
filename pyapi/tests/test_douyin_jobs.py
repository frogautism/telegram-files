import sqlite3
import unittest

from app.db import init_schema
from app.douyin_jobs import (
    active_job_for_file,
    cancel_job,
    create_job,
    get_job,
    increment_job,
    list_jobs,
    new_job_id,
    serialize_job,
    update_job,
)


class DouyinJobsTest(unittest.TestCase):
    def _connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        return conn

    def test_new_job_id_unique(self) -> None:
        self.assertNotEqual(new_job_id(), new_job_id())

    def test_create_serialize_shape(self) -> None:
        conn = self._connection()
        job = create_job(
            conn,
            kind="file_download",
            source_id="src",
            file_unique_id="douyin:1:primary:0",
            url="https://example.com",
            total=1,
        )
        self.assertEqual(
            set(job.keys()),
            {
                "id",
                "sourceId",
                "fileUniqueId",
                "url",
                "kind",
                "state",
                "total",
                "success",
                "failed",
                "skipped",
                "step",
                "error",
                "createdAt",
                "updatedAt",
                "startedAt",
                "completedAt",
            },
        )
        self.assertEqual(job["kind"], "file_download")
        self.assertEqual(job["sourceId"], "src")
        self.assertEqual(job["fileUniqueId"], "douyin:1:primary:0")
        self.assertEqual(job["state"], "queued")
        self.assertEqual(job["total"], 1)
        self.assertEqual(job["startedAt"], 0)
        self.assertEqual(job["completedAt"], 0)

    def test_update_sets_started_and_completed(self) -> None:
        conn = self._connection()
        job = create_job(conn, kind="source_refresh", source_id="src")
        running = update_job(conn, job["id"], state="running", step="working")
        self.assertEqual(running["state"], "running")
        self.assertGreater(running["startedAt"], 0)
        self.assertEqual(running["completedAt"], 0)
        self.assertEqual(running["step"], "working")

        done = update_job(conn, job["id"], state="completed", success=2)
        self.assertEqual(done["state"], "completed")
        self.assertGreater(done["completedAt"], 0)
        self.assertEqual(done["success"], 2)
        # started_at preserved.
        self.assertEqual(done["startedAt"], running["startedAt"])

        self.assertIsNone(update_job(conn, "missing", state="running"))

    def test_increment_job(self) -> None:
        conn = self._connection()
        job = create_job(conn, kind="batch_download")
        increment_job(conn, job["id"], success=1)
        increment_job(conn, job["id"], success=2, failed=1, skipped=3)
        updated = get_job(conn, job["id"])
        self.assertEqual(updated["success"], 3)
        self.assertEqual(updated["failed"], 1)
        self.assertEqual(updated["skipped"], 3)
        self.assertIsNone(increment_job(conn, "missing", success=1))

    def test_list_jobs_active_first(self) -> None:
        conn = self._connection()
        done = create_job(conn, kind="file_download", state="completed")
        running = create_job(conn, kind="file_download", state="running")
        queued = create_job(conn, kind="source_refresh", state="queued")

        ordered = list_jobs(conn)
        ids = [job["id"] for job in ordered]
        # Active (queued/running) come before terminal ones.
        self.assertLess(ids.index(running["id"]), ids.index(done["id"]))
        self.assertLess(ids.index(queued["id"]), ids.index(done["id"]))

        only_completed = list_jobs(conn, status="completed")
        self.assertEqual([job["id"] for job in only_completed], [done["id"]])

    def test_cancel_job(self) -> None:
        conn = self._connection()
        job = create_job(conn, kind="file_download", state="running")
        cancelled = cancel_job(conn, job["id"])
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertGreater(cancelled["completedAt"], 0)

        # Cancelling a terminal job is a no-op (state preserved).
        again = cancel_job(conn, job["id"])
        self.assertEqual(again["state"], "cancelled")
        self.assertIsNone(cancel_job(conn, "missing"))

    def test_active_job_for_file(self) -> None:
        conn = self._connection()
        create_job(
            conn,
            kind="file_download",
            file_unique_id="douyin:1:primary:0",
            state="completed",
        )
        self.assertIsNone(active_job_for_file(conn, "douyin:1:primary:0"))
        active = create_job(
            conn,
            kind="file_download",
            file_unique_id="douyin:1:primary:0",
            state="running",
        )
        found = active_job_for_file(conn, "douyin:1:primary:0")
        self.assertEqual(found["id"], active["id"])

    def test_serialize_accepts_row_and_dict(self) -> None:
        conn = self._connection()
        job = create_job(conn, kind="batch_download")
        row = conn.execute(
            "SELECT * FROM douyin_job WHERE id = ?", (job["id"],)
        ).fetchone()
        self.assertEqual(serialize_job(row)["kind"], "batch_download")


if __name__ == "__main__":
    unittest.main()
