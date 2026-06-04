import sqlite3
import tempfile
import unittest
from pathlib import Path

from app.db import init_schema
from app.douyin_store import (
    delete_douyin_source,
    douyin_aweme_exists,
    douyin_file_for_transfer,
    douyin_transfer_candidates,
    get_douyin_source,
    list_douyin_files,
    list_douyin_sources,
    remove_douyin_download,
    update_douyin_file_status,
    update_douyin_source_auto_settings,
    update_douyin_source_display,
    upsert_douyin_aweme,
    upsert_douyin_source,
)


class DouyinStoreTest(unittest.TestCase):
    def _connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        return conn

    def test_source_and_aweme_are_serialized_as_douyin_files(self) -> None:
        conn = self._connection()
        source = upsert_douyin_source(
            conn,
            url="https://www.douyin.com/video/123",
            url_type="video",
            title="Video",
        )
        record = upsert_douyin_aweme(
            conn,
            source_id=source["id"],
            aweme={
                "aweme_id": "123",
                "desc": "hello #tag",
                "create_time": 1710000000,
                "author": {"nickname": "author"},
                "video": {"width": 1280, "height": 720, "duration": 5000},
            },
        )

        self.assertIsNotNone(record)
        files = list_douyin_files(conn, source_id=source["id"])["files"]
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["source"], "douyin")
        self.assertEqual(files[0]["uniqueId"], "douyin:123:primary:0")
        self.assertEqual(files[0]["type"], "video")
        self.assertEqual(files[0]["extra"]["duration"], 5)

    def test_status_remove_and_transfer_candidate_flow(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "123.mp4"
            path.write_bytes(b"abc")
            source = upsert_douyin_source(conn, url="https://www.douyin.com/video/123")
            record = upsert_douyin_aweme(
                conn,
                source_id=source["id"],
                aweme={"aweme_id": "123", "desc": "hello"},
            )
            unique_id = record["uniqueId"]

            completed = update_douyin_file_status(
                conn,
                unique_id=unique_id,
                download_status="completed",
                local_path=str(path),
                downloaded_size=3,
                size=3,
            )

            self.assertEqual(completed["downloadStatus"], "completed")
            self.assertEqual(
                douyin_transfer_candidates(conn, source_id=source["id"])[0]["uniqueId"],
                unique_id,
            )
            transfer_row = douyin_file_for_transfer(conn, unique_id=unique_id)
            self.assertEqual(transfer_row["local_path"], str(path))

            removed = remove_douyin_download(conn, unique_id)
            self.assertEqual(removed["downloadStatus"], "idle")
            self.assertFalse(path.exists())

    def test_auto_settings_are_source_scoped(self) -> None:
        conn = self._connection()
        source = upsert_douyin_source(conn, url="https://www.douyin.com/user/sec")
        updated = update_douyin_source_auto_settings(
            conn,
            source_id=source["id"],
            auto_payload={"download": {"enabled": True}},
        )
        self.assertTrue(updated["auto"]["download"]["enabled"])
        self.assertIsNone(
            update_douyin_source_auto_settings(
                conn,
                source_id="missing",
                auto_payload={"download": {"enabled": True}},
            )
        )

    def test_file_listing_uses_total_count_and_date_cursor(self) -> None:
        conn = self._connection()
        source = upsert_douyin_source(conn, url="https://www.douyin.com/user/sec")
        for aweme_id, created_at in (("old", 10), ("new", 30), ("middle", 20)):
            upsert_douyin_aweme(
                conn,
                source_id=source["id"],
                aweme={
                    "aweme_id": aweme_id,
                    "desc": aweme_id,
                    "create_time": created_at,
                    "author": {"nickname": "author"},
                },
            )

        first_page = list_douyin_files(
            conn,
            source_id=source["id"],
            filters={"type": "media", "sort": "date", "order": "desc", "limit": "2"},
        )
        self.assertEqual(first_page["count"], 3)
        self.assertEqual(
            [item["awemeId"] for item in first_page["files"]],
            ["new", "middle"],
        )
        self.assertNotEqual(first_page["nextFromMessageId"], 0)

        last_file = first_page["files"][-1]
        second_page = list_douyin_files(
            conn,
            source_id=source["id"],
            filters={
                "type": "media",
                "sort": "date",
                "order": "desc",
                "limit": "2",
                "fromMessageId": str(last_file["id"]),
                "fromSortField": str(last_file["date"]),
            },
        )
        self.assertEqual(second_page["count"], 3)
        self.assertEqual([item["awemeId"] for item in second_page["files"]], ["old"])
        self.assertEqual(second_page["nextFromMessageId"], 0)


    def test_source_display_update_and_dict_fields(self) -> None:
        conn = self._connection()
        source = upsert_douyin_source(conn, url="https://www.douyin.com/user/sec")

        # New fields with defaults present on the serialized dict.
        self.assertEqual(source["displayName"], "")
        self.assertFalse(source["autoRefresh"]["enabled"])
        self.assertEqual(source["autoRefresh"]["intervalSeconds"], 1800)
        self.assertEqual(source["refreshStatus"], "idle")
        self.assertEqual(source["totalFiles"], 0)
        self.assertEqual(source["completedDownloads"], 0)
        self.assertEqual(source["failedDownloads"], 0)

        updated = update_douyin_source_display(
            conn,
            source["id"],
            display_name="My Source",
            auto_refresh_enabled=True,
            auto_refresh_interval=60,  # floored to 1800
        )
        self.assertEqual(updated["displayName"], "My Source")
        self.assertTrue(updated["autoRefresh"]["enabled"])
        self.assertEqual(updated["autoRefresh"]["intervalSeconds"], 1800)
        self.assertIsNone(
            update_douyin_source_display(conn, "missing", display_name="x")
        )

    def test_source_counts_reflect_files(self) -> None:
        conn = self._connection()
        source = upsert_douyin_source(conn, url="https://www.douyin.com/user/sec")
        for aweme_id in ("a", "b", "c"):
            upsert_douyin_aweme(
                conn, source_id=source["id"], aweme={"aweme_id": aweme_id, "desc": aweme_id}
            )
        update_douyin_file_status(
            conn,
            unique_id="douyin:a:primary:0",
            download_status="error",
            error="boom",
        )

        listed = list_douyin_sources(conn)[0]
        self.assertEqual(listed["totalFiles"], 3)
        self.assertEqual(listed["failedDownloads"], 1)
        fetched = get_douyin_source(conn, source["id"])
        self.assertEqual(fetched["totalFiles"], 3)
        self.assertEqual(fetched["failedDownloads"], 1)

    def test_delete_preserves_files_by_default(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "keep.mp4"
            path.write_bytes(b"abc")
            source = upsert_douyin_source(conn, url="https://www.douyin.com/video/1")
            upsert_douyin_aweme(conn, source_id=source["id"], aweme={"aweme_id": "1"})
            frame_path = Path(temp_dir) / "frame.jpg"
            frame_path.write_bytes(b"frame")
            update_douyin_file_status(
                conn,
                unique_id="douyin:1:primary:0",
                download_status="completed",
                local_path=str(path),
            )
            conn.execute(
                """
                INSERT INTO douyin_frame(
                    frame_uid, file_unique_id, aweme_id, source_id, frame_index,
                    timestamp_ms, local_path, created_at
                )
                VALUES('frame-1', 'douyin:1:primary:0', '1', ?, 0, 0, ?, 1)
                """,
                (source["id"], str(frame_path)),
            )
            conn.commit()

            result = delete_douyin_source(conn, source["id"])
            self.assertEqual(result, {"deleted": True, "removedFiles": 1})
            self.assertTrue(path.exists())  # file preserved on disk
            self.assertTrue(frame_path.exists())  # frame file preserved on disk
            self.assertIsNone(get_douyin_source(conn, source["id"]))
            self.assertEqual(list_douyin_files(conn, source_id=source["id"])["count"], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM douyin_frame").fetchone()[0], 0)

    def test_delete_with_delete_files_unlinks(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "gone.mp4"
            path.write_bytes(b"abc")
            source = upsert_douyin_source(conn, url="https://www.douyin.com/video/1")
            upsert_douyin_aweme(conn, source_id=source["id"], aweme={"aweme_id": "1"})
            frame_path = Path(temp_dir) / "frame.jpg"
            frame_path.write_bytes(b"frame")
            update_douyin_file_status(
                conn,
                unique_id="douyin:1:primary:0",
                download_status="completed",
                local_path=str(path),
            )
            conn.execute(
                """
                INSERT INTO douyin_frame(
                    frame_uid, file_unique_id, aweme_id, source_id, frame_index,
                    timestamp_ms, local_path, created_at
                )
                VALUES('frame-1', 'douyin:1:primary:0', '1', ?, 0, 0, ?, 1)
                """,
                (source["id"], str(frame_path)),
            )
            conn.commit()

            result = delete_douyin_source(conn, source["id"], delete_files=True)
            self.assertEqual(result, {"deleted": True, "removedFiles": 1})
            self.assertFalse(path.exists())
            self.assertFalse(frame_path.exists())
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM douyin_frame").fetchone()[0], 0)

        self.assertEqual(
            delete_douyin_source(conn, "missing"),
            {"deleted": False, "removedFiles": 0},
        )

    def test_incremental_upsert_does_not_reset_completed(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "1.mp4"
            path.write_bytes(b"abc")
            source = upsert_douyin_source(conn, url="https://www.douyin.com/video/1")
            self.assertFalse(douyin_aweme_exists(conn, "1"))
            upsert_douyin_aweme(conn, source_id=source["id"], aweme={"aweme_id": "1"})
            self.assertTrue(douyin_aweme_exists(conn, "1"))
            update_douyin_file_status(
                conn,
                unique_id="douyin:1:primary:0",
                download_status="completed",
                local_path=str(path),
                size=3,
                downloaded_size=3,
            )

            # Re-discovering the same aweme must not reset a completed download.
            upsert_douyin_aweme(conn, source_id=source["id"], aweme={"aweme_id": "1"})
            files = list_douyin_files(conn, source_id=source["id"])["files"]
            self.assertEqual(files[0]["downloadStatus"], "completed")
            self.assertEqual(files[0]["localPath"], str(path))


if __name__ == "__main__":
    unittest.main()
