import sqlite3
import tempfile
import unittest
from pathlib import Path

from app.db import init_schema
from app.douyin_store import (
    douyin_file_for_transfer,
    douyin_transfer_candidates,
    list_douyin_files,
    remove_douyin_download,
    update_douyin_file_status,
    update_douyin_source_auto_settings,
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


if __name__ == "__main__":
    unittest.main()
