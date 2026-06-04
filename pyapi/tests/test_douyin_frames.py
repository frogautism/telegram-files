import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import douyin_frames
from app.config import AppConfig
from app.db import init_schema
from app.douyin_store import douyin_file_row, upsert_douyin_aweme, upsert_douyin_source
from app.routers.douyin_frames import router as frames_router


def _make_config(app_root: Path) -> AppConfig:
    return AppConfig(
        app_root=app_root,
        db_type="sqlite",
        data_path="data.db",
        version="test",
        telegram_api_id=0,
        telegram_api_hash="",
        telegram_log_level=1,
        tdlib_shared_lib="",
        douyin_downloader_path="",
        douyin_path=str(app_root / "douyin"),
    )


def _make_test_video(target: Path) -> bool:
    exe = douyin_frames.ffmpeg_path()
    if not exe:
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            exe,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=2:size=128x128:rate=10",
            "-pix_fmt",
            "yuv420p",
            str(target),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result.returncode == 0 and target.exists()


class DouyinFramesCoreTest(unittest.TestCase):
    def _connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        return conn

    def _seed_video(self, conn: sqlite3.Connection, local_path: str) -> str:
        source = upsert_douyin_source(conn, url="https://www.douyin.com/video/777")
        record = upsert_douyin_aweme(
            conn,
            source_id=source["id"],
            aweme={"aweme_id": "777", "desc": "vid"},
        )
        unique_id = record["uniqueId"]
        conn.execute(
            "UPDATE douyin_file SET local_path = ?, download_status = 'completed' WHERE unique_id = ?",
            (local_path, unique_id),
        )
        conn.commit()
        return unique_id

    def test_reject_non_video(self) -> None:
        conn = self._connection()
        source = upsert_douyin_source(conn, url="https://www.douyin.com/note/1")
        record = upsert_douyin_aweme(
            conn,
            source_id=source["id"],
            aweme={"aweme_id": "1", "desc": "pic", "images": [{"x": 1}]},
        )
        with self.assertRaises(ValueError):
            douyin_frames.extract_frames(conn, unique_id=record["uniqueId"], mode="interval")

    def test_reject_missing_row(self) -> None:
        conn = self._connection()
        with self.assertRaises(ValueError):
            douyin_frames.extract_frames(conn, unique_id="douyin:nope:primary:0", mode="interval")

    def test_reject_missing_local_file(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            missing = str(Path(tmp) / "video" / "gone.mp4")
            unique_id = self._seed_video(conn, missing)
            with self.assertRaises(ValueError):
                douyin_frames.extract_frames(conn, unique_id=unique_id, mode="interval")

    def test_missing_ffmpeg_raises_runtime_error(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "video" / "777.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"not a real video")
            unique_id = self._seed_video(conn, str(video))
            original = douyin_frames.ffmpeg_path
            douyin_frames.ffmpeg_path = lambda: ""
            try:
                with self.assertRaises(RuntimeError):
                    douyin_frames.extract_frames(conn, unique_id=unique_id, mode="interval")
            finally:
                douyin_frames.ffmpeg_path = original

    def test_list_and_delete_empty(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "video" / "777.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed_video(conn, str(video))
            self.assertEqual(douyin_frames.list_frames(conn, unique_id), [])
            self.assertEqual(douyin_frames.delete_frames(conn, unique_id), 0)

    def test_frames_dir_layout(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "author" / "video" / "777.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed_video(conn, str(video))
            row = douyin_file_row(conn, unique_id=unique_id)
            frames_dir = douyin_frames.frames_dir_for(row)
            self.assertEqual(frames_dir, Path(tmp) / "author" / "frames" / "777")
            self.assertTrue(frames_dir.exists())

    @unittest.skipUnless(douyin_frames.ffmpeg_available(), "ffmpeg not available")
    def test_interval_extraction_and_replace(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "author" / "video" / "777.mp4"
            self.assertTrue(_make_test_video(video))
            unique_id = self._seed_video(conn, str(video))

            result = douyin_frames.extract_frames(
                conn, unique_id=unique_id, mode="interval", interval=1, max_frames=5
            )
            self.assertGreater(result["extracted"], 0)
            self.assertEqual(len(result["frames"]), result["extracted"])
            first = result["frames"][0]
            self.assertEqual(first["fileUniqueId"], unique_id)
            self.assertEqual(
                first["url"], f"/douyin/file/{unique_id}/frames/{first['id']}"
            )
            for frame in result["frames"]:
                self.assertTrue(Path(
                    conn.execute(
                        "SELECT local_path FROM douyin_frame WHERE id = ?", (frame["id"],)
                    ).fetchone()[0]
                ).exists())

            # re-extract with replace=True replaces old frames/files
            old_paths = [
                row[0]
                for row in conn.execute(
                    "SELECT local_path FROM douyin_frame WHERE file_unique_id = ?",
                    (unique_id,),
                ).fetchall()
            ]
            result2 = douyin_frames.extract_frames(
                conn, unique_id=unique_id, mode="interval", interval=1, max_frames=3, replace=True
            )
            self.assertGreater(result2["extracted"], 0)
            count = conn.execute(
                "SELECT COUNT(*) FROM douyin_frame WHERE file_unique_id = ?",
                (unique_id,),
            ).fetchone()[0]
            self.assertEqual(count, result2["extracted"])
            self.assertTrue(len(old_paths) > 0)


class DouyinFramesRouterTest(unittest.TestCase):
    def _client(self, tmp: str) -> tuple[TestClient, sqlite3.Connection, str]:
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        app_root = Path(tmp) / "root"
        app_root.mkdir(parents=True, exist_ok=True)
        app = FastAPI()
        app.state.db = conn
        app.state.config = _make_config(app_root)
        app.include_router(frames_router)
        return TestClient(app), conn, str(app_root)

    def _seed(self, conn: sqlite3.Connection, local_path: str) -> str:
        source = upsert_douyin_source(conn, url="https://www.douyin.com/video/888")
        record = upsert_douyin_aweme(
            conn,
            source_id=source["id"],
            aweme={"aweme_id": "888", "desc": "vid"},
        )
        unique_id = record["uniqueId"]
        conn.execute(
            "UPDATE douyin_file SET local_path = ?, download_status = 'completed' WHERE unique_id = ?",
            (local_path, unique_id),
        )
        conn.commit()
        return unique_id

    def test_list_empty_and_delete_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client, conn, _ = self._client(tmp)
            video = Path(tmp) / "video" / "888.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed(conn, str(video))
            resp = client.get(f"/douyin/file/{unique_id}/frames")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), [])
            resp = client.delete(f"/douyin/file/{unique_id}/frames")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"deleted": 0})

    def test_extract_non_video_returns_400(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client, conn, _ = self._client(tmp)
            source = upsert_douyin_source(conn, url="https://www.douyin.com/note/9")
            record = upsert_douyin_aweme(
                conn,
                source_id=source["id"],
                aweme={"aweme_id": "9", "desc": "pic", "images": [{"x": 1}]},
            )
            resp = client.post(
                f"/douyin/file/{record['uniqueId']}/frames/extract",
                json={"mode": "interval"},
            )
            self.assertEqual(resp.status_code, 400)
            # job row recorded as failed
            job = conn.execute(
                "SELECT state, kind FROM douyin_job WHERE file_unique_id = ?",
                (record["uniqueId"],),
            ).fetchone()
            self.assertEqual(job["state"], "failed")
            self.assertEqual(job["kind"], "frame_extract")

    def test_get_missing_frame_returns_404(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client, conn, _ = self._client(tmp)
            video = Path(tmp) / "video" / "888.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed(conn, str(video))
            resp = client.get(f"/douyin/file/{unique_id}/frames/9999")
            self.assertEqual(resp.status_code, 404)

    @unittest.skipUnless(douyin_frames.ffmpeg_available(), "ffmpeg not available")
    def test_extract_serve_and_delete_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client, conn, _ = self._client(tmp)
            video = Path(tmp) / "author" / "video" / "888.mp4"
            self.assertTrue(_make_test_video(video))
            unique_id = self._seed(conn, str(video))

            resp = client.post(
                f"/douyin/file/{unique_id}/frames/extract",
                json={"mode": "interval", "interval": 1, "maxFrames": 4},
            )
            self.assertEqual(resp.status_code, 200, resp.text)
            body = resp.json()
            self.assertIn("jobId", body)
            self.assertGreater(body["extracted"], 0)
            frame_id = body["frames"][0]["id"]

            job = conn.execute(
                "SELECT state FROM douyin_job WHERE id = ?", (body["jobId"],)
            ).fetchone()
            self.assertEqual(job["state"], "completed")

            img = client.get(f"/douyin/file/{unique_id}/frames/{frame_id}")
            self.assertEqual(img.status_code, 200)
            self.assertEqual(img.headers["content-type"], "image/jpeg")

            listed = client.get(f"/douyin/file/{unique_id}/frames")
            self.assertEqual(len(listed.json()), body["extracted"])

            deleted = client.delete(f"/douyin/file/{unique_id}/frames")
            self.assertEqual(deleted.json()["deleted"], body["extracted"])
            self.assertEqual(client.get(f"/douyin/file/{unique_id}/frames").json(), [])


if __name__ == "__main__":
    unittest.main()
