import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from app.db import init_schema
from app.download_jobs import (
    JOB_STATE_COMPLETED,
    JOB_STATE_FAILED,
    JOB_STATE_QUEUED,
    JOB_STATE_STARTING,
    VERIFICATION_COMPLETED_UNVERIFIED,
    VERIFICATION_COMPLETED_VERIFIED,
    VERIFICATION_FILE_MISSING,
    complete_download_job_for_file,
    due_download_jobs,
    find_download_job,
    mark_download_job_failed,
    mark_download_job_monitoring,
    recover_interrupted_download_jobs,
    upsert_download_job,
    verify_download_integrity,
)
from app.file_record_ops import upsert_tdlib_file_record


class DownloadJobsTest(unittest.TestCase):
    def _connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        return conn

    def test_job_failure_uses_backoff_and_is_not_due_until_retry_at(self) -> None:
        conn = self._connection()
        upsert_download_job(
            conn,
            telegram_id=1,
            chat_id=100,
            message_id=200,
            file_id=300,
            source="auto",
        )

        mark_download_job_failed(
            conn,
            telegram_id=1,
            chat_id=100,
            message_id=200,
            file_id=300,
            error="TDLib not ready",
        )

        job = find_download_job(
            conn,
            telegram_id=1,
            chat_id=100,
            message_id=200,
            file_id=300,
        )
        self.assertEqual(job["state"], JOB_STATE_FAILED)
        self.assertEqual(job["attempts"], 1)
        self.assertIn("TDLib not ready", job["error"])
        self.assertEqual(due_download_jobs(conn, now_ms=int(time.time() * 1000)), [])
        self.assertEqual(
            due_download_jobs(conn, now_ms=int(job["retryAt"]) + 1)[0]["fileId"],
            300,
        )

    def test_recovery_requeues_interrupted_jobs(self) -> None:
        conn = self._connection()
        upsert_download_job(
            conn,
            telegram_id=1,
            chat_id=100,
            message_id=200,
            file_id=300,
            state=JOB_STATE_STARTING,
        )
        recovered = recover_interrupted_download_jobs(conn)

        self.assertEqual(recovered, 1)
        job = due_download_jobs(conn, now_ms=int(time.time() * 1000) + 1)[0]
        self.assertEqual(job["state"], JOB_STATE_QUEUED)
        self.assertEqual(job["fileId"], 300)

    def test_completion_verifies_size_and_updates_file_record(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "file.bin"
            path.write_bytes(b"abcd")
            upsert_tdlib_file_record(
                conn,
                file_payload={
                    "id": 300,
                    "telegramId": 1,
                    "uniqueId": "unique-300",
                    "messageId": 200,
                    "chatId": 100,
                    "mediaAlbumId": 0,
                    "fileName": "file.bin",
                    "type": "file",
                    "mimeType": "application/octet-stream",
                    "size": 4,
                    "downloadedSize": 4,
                    "thumbnail": "",
                    "downloadStatus": "completed",
                    "date": 1710000000,
                    "caption": "",
                    "localPath": str(path),
                    "hasSensitiveContent": False,
                    "startDate": 0,
                    "completionDate": 1710000100000,
                    "transferStatus": "idle",
                    "extra": None,
                    "threadChatId": 0,
                    "messageThreadId": 0,
                    "reactionCount": 0,
                },
            )
            upsert_download_job(
                conn,
                telegram_id=1,
                chat_id=100,
                message_id=200,
                file_id=300,
                unique_id="unique-300",
            )
            mark_download_job_monitoring(
                conn,
                telegram_id=1,
                chat_id=100,
                message_id=200,
                file_id=300,
                unique_id="unique-300",
                expected_size=4,
                downloaded_size=4,
            )

            status = complete_download_job_for_file(
                conn,
                telegram_id=1,
                file_id=300,
                unique_id="unique-300",
                local_path=str(path),
                expected_size=4,
                downloaded_size=4,
            )

        self.assertEqual(status, VERIFICATION_COMPLETED_VERIFIED)
        job = find_download_job(
            conn,
            telegram_id=1,
            chat_id=100,
            message_id=200,
            file_id=300,
        )
        self.assertEqual(job["state"], JOB_STATE_COMPLETED)
        self.assertEqual(job["verificationStatus"], VERIFICATION_COMPLETED_VERIFIED)
        row = conn.execute(
            "SELECT verification_status, download_error FROM file_record WHERE unique_id = ?",
            ("unique-300",),
        ).fetchone()
        self.assertEqual(row["verification_status"], VERIFICATION_COMPLETED_VERIFIED)
        self.assertEqual(row["download_error"], "")

    def test_integrity_verification_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "file.bin"
            path.write_bytes(b"abcd")
            self.assertEqual(
                verify_download_integrity(
                    local_path=str(path),
                    expected_size=4,
                    downloaded_size=4,
                ),
                VERIFICATION_COMPLETED_VERIFIED,
            )
            self.assertEqual(
                verify_download_integrity(
                    local_path=str(path),
                    expected_size=5,
                    downloaded_size=4,
                ),
                VERIFICATION_COMPLETED_UNVERIFIED,
            )
        self.assertEqual(
            verify_download_integrity(
                local_path="",
                expected_size=4,
                downloaded_size=0,
            ),
            VERIFICATION_FILE_MISSING,
        )


if __name__ == "__main__":
    unittest.main()
