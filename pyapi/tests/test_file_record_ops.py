import sqlite3
import unittest

from app.db import init_schema, list_files
from app.file_record_ops import update_transfer_status, upsert_tdlib_file_record


class FileRecordOpsTest(unittest.TestCase):
    def _base_payload(self, **overrides):
        payload = {
            "telegramId": 1,
            "uniqueId": "file-1",
            "id": 111,
            "messageId": 50,
            "chatId": 100,
            "mediaAlbumId": 0,
            "fileName": "file-1.jpg",
            "type": "photo",
            "mimeType": "image/jpeg",
            "size": 1234,
            "downloadedSize": 0,
            "thumbnail": "",
            "downloadStatus": "idle",
            "date": 1710000000,
            "caption": "",
            "localPath": "",
            "hasSensitiveContent": False,
            "startDate": 0,
            "completionDate": 0,
            "transferStatus": "idle",
            "extra": {"width": 640, "height": 480, "type": "x"},
            "threadChatId": 0,
            "messageThreadId": 0,
            "reactionCount": 0,
        }
        payload.update(overrides)
        return payload

    def test_upsert_keeps_distinct_file_ids_for_same_unique_id(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)

        base_payload = {
            "telegramId": 1,
            "uniqueId": "shared-unique",
            "chatId": 100,
            "mediaAlbumId": 0,
            "type": "photo",
            "mimeType": "image/jpeg",
            "size": 1234,
            "downloadedSize": 1234,
            "thumbnail": "",
            "downloadStatus": "completed",
            "date": 1710000000,
            "caption": "existing",
            "localPath": "D:/downloads/existing.jpg",
            "hasSensitiveContent": False,
            "startDate": 0,
            "completionDate": 1710000100000,
            "extra": {"width": 640, "height": 480, "type": "x"},
            "threadChatId": 0,
            "messageThreadId": 0,
            "reactionCount": 0,
        }

        upsert_tdlib_file_record(
            conn,
            file_payload={
                **base_payload,
                "id": 111,
                "messageId": 50,
                "fileName": "existing-a.jpg",
                "transferStatus": "completed",
            },
        )
        upsert_tdlib_file_record(
            conn,
            file_payload={
                **base_payload,
                "id": 222,
                "messageId": 60,
                "fileName": "existing-b.jpg",
                "transferStatus": "idle",
            },
        )

        rows = conn.execute(
            """
            SELECT id, transfer_status
            FROM file_record
            WHERE telegram_id = ? AND unique_id = ?
            ORDER BY id ASC
            """,
            (1, "shared-unique"),
        ).fetchall()

        self.assertEqual(
            [(row["id"], row["transfer_status"]) for row in rows],
            [(111, "completed"), (222, "idle")],
        )

    def test_list_files_does_not_apply_already_downloaded_filter_by_default(
        self,
    ) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)

        upsert_tdlib_file_record(
            conn,
            file_payload=self._base_payload(
                uniqueId="completed-file",
                id=101,
                messageId=101,
                fileName="completed.jpg",
                downloadStatus="completed",
                downloadedSize=1234,
                completionDate=1710000100000,
                localPath="D:/downloads/completed.jpg",
            ),
        )
        upsert_tdlib_file_record(
            conn,
            file_payload=self._base_payload(
                uniqueId="idle-file",
                id=102,
                messageId=102,
                fileName="idle.jpg",
            ),
        )

        default_result = list_files(conn, telegram_id=1, chat_id=100, filters={})
        self.assertEqual(default_result["size"], 2)
        self.assertEqual(
            [item["uniqueId"] for item in default_result["files"]],
            ["idle-file", "completed-file"],
        )

        downloaded_result = list_files(
            conn,
            telegram_id=1,
            chat_id=100,
            filters={"alreadyDownloaded": "true"},
        )
        self.assertEqual(
            [item["uniqueId"] for item in downloaded_result["files"]],
            ["completed-file"],
        )

        not_downloaded_result = list_files(
            conn,
            telegram_id=1,
            chat_id=100,
            filters={"alreadyDownloaded": "false"},
        )
        self.assertEqual(
            [item["uniqueId"] for item in not_downloaded_result["files"]],
            ["idle-file"],
        )

    def test_update_transfer_status_uses_row_unique_id_after_file_id_fallback(
        self,
    ) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        upsert_tdlib_file_record(
            conn,
            file_payload=self._base_payload(
                uniqueId="actual-unique",
                id=777,
                messageId=777,
                downloadStatus="completed",
                localPath="D:/downloads/source.jpg",
                completionDate=1710000100000,
            ),
        )

        result = update_transfer_status(
            conn,
            telegram_id=1,
            file_id=777,
            unique_id="stale-unique",
            transfer_status="completed",
            local_path="D:/archive/source.jpg",
        )

        self.assertEqual(result["uniqueId"], "actual-unique")
        row = conn.execute(
            "SELECT transfer_status, local_path FROM file_record WHERE unique_id = ?",
            ("actual-unique",),
        ).fetchone()
        self.assertEqual(row["transfer_status"], "completed")
        self.assertEqual(row["local_path"], "D:/archive/source.jpg")


if __name__ == "__main__":
    unittest.main()
