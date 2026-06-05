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
from app.douyin_store import (
    douyin_file_row,
    remove_douyin_download,
    upsert_douyin_aweme,
    upsert_douyin_source,
)
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

    def test_fractional_interval_is_preserved(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "author" / "video" / "777.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed_video(conn, str(video))
            captured_args: list[list[str]] = []
            original_ffmpeg_path = douyin_frames.ffmpeg_path
            original_run_ffmpeg = douyin_frames._run_ffmpeg
            douyin_frames.ffmpeg_path = lambda: "ffmpeg"

            def fake_run_ffmpeg(args: list[str], **_: object) -> str:
                captured_args.append(args)
                Path(args[-1].replace("%04d", "0001")).write_bytes(b"frame")
                Path(args[-1].replace("%04d", "0002")).write_bytes(b"frame")
                return "pts_time:0\npts_time:0.3\n"

            douyin_frames._run_ffmpeg = fake_run_ffmpeg
            try:
                result = douyin_frames.extract_frames(
                    conn,
                    unique_id=unique_id,
                    mode="interval",
                    interval=0.3,
                    max_frames=2,
                )
            finally:
                douyin_frames.ffmpeg_path = original_ffmpeg_path
                douyin_frames._run_ffmpeg = original_run_ffmpeg

            self.assertEqual(result["extracted"], 2)
            self.assertIn("fps=1/0.3,showinfo", captured_args[0])
            self.assertEqual(result["frames"][0]["timestampMs"], 0)
            self.assertEqual(result["frames"][1]["timestampMs"], 300)

    def test_keyframe_timestamps_come_from_ffmpeg_output(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "author" / "video" / "777.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed_video(conn, str(video))
            original_ffmpeg_path = douyin_frames.ffmpeg_path
            original_run_ffmpeg = douyin_frames._run_ffmpeg
            douyin_frames.ffmpeg_path = lambda: "ffmpeg"

            def fake_run_ffmpeg(args: list[str], **_: object) -> str:
                Path(args[-1].replace("%04d", "0001")).write_bytes(b"frame")
                Path(args[-1].replace("%04d", "0002")).write_bytes(b"frame")
                return "pts_time:0\npts_time:1.5\n"

            douyin_frames._run_ffmpeg = fake_run_ffmpeg
            try:
                result = douyin_frames.extract_frames(
                    conn,
                    unique_id=unique_id,
                    mode="keyframe",
                    max_frames=2,
                )
            finally:
                douyin_frames.ffmpeg_path = original_ffmpeg_path
                douyin_frames._run_ffmpeg = original_run_ffmpeg

            self.assertEqual(result["extracted"], 2)
            self.assertEqual(result["frames"][0]["timestampMs"], 0)
            self.assertEqual(result["frames"][1]["timestampMs"], 1500)

    def test_reextract_replaces_old_rows_and_files(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "author" / "video" / "777.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed_video(conn, str(video))
            captured_args: list[list[str]] = []
            original_ffmpeg_path = douyin_frames.ffmpeg_path
            original_run_ffmpeg = douyin_frames._run_ffmpeg
            douyin_frames.ffmpeg_path = lambda: "ffmpeg"
            produced_counts = iter([3, 1])

            def fake_run_ffmpeg(args: list[str], **_: object) -> str:
                captured_args.append(args)
                output_pattern = args[-1]
                for index in range(1, next(produced_counts) + 1):
                    Path(output_pattern.replace("%04d", f"{index:04d}")).write_bytes(
                        b"frame"
                    )
                return "pts_time:0\npts_time:1\npts_time:2\n"

            douyin_frames._run_ffmpeg = fake_run_ffmpeg
            try:
                first = douyin_frames.extract_frames(
                    conn,
                    unique_id=unique_id,
                    mode="interval",
                    interval=1,
                    max_frames=3,
                )
                first_paths = [
                    Path(row["local_path"])
                    for row in conn.execute(
                        "SELECT local_path FROM douyin_frame WHERE file_unique_id = ?",
                        (unique_id,),
                    ).fetchall()
                ]
                second = douyin_frames.extract_frames(
                    conn,
                    unique_id=unique_id,
                    mode="interval",
                    interval=1,
                    max_frames=1,
                )
            finally:
                douyin_frames.ffmpeg_path = original_ffmpeg_path
                douyin_frames._run_ffmpeg = original_run_ffmpeg

            self.assertEqual(first["extracted"], 3)
            self.assertEqual(second["extracted"], 1)
            rows = conn.execute(
                "SELECT local_path FROM douyin_frame WHERE file_unique_id = ?",
                (unique_id,),
            ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertTrue(Path(rows[0]["local_path"]).exists())
            self.assertTrue(first_paths[0].exists())
            self.assertFalse(first_paths[1].exists())
            self.assertFalse(first_paths[2].exists())

    def test_max_frames_above_limit_is_rejected(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "author" / "video" / "777.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed_video(conn, str(video))
            with self.assertRaises(ValueError):
                douyin_frames.extract_frames(
                    conn,
                    unique_id=unique_id,
                    mode="interval",
                    max_frames=douyin_frames.MAX_FRAMES_LIMIT + 999,
                )

    def test_invalid_frame_format_is_rejected(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "author" / "video" / "777.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed_video(conn, str(video))
            with self.assertRaises(ValueError):
                douyin_frames.extract_frames(
                    conn, unique_id=unique_id, mode="interval", fmt="gif"
                )

    def test_failed_reextract_preserves_old_rows_and_files(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "author" / "video" / "777.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed_video(conn, str(video))
            original_ffmpeg_path = douyin_frames.ffmpeg_path
            original_run_ffmpeg = douyin_frames._run_ffmpeg
            douyin_frames.ffmpeg_path = lambda: "ffmpeg"

            def first_run(args: list[str], **_: object) -> str:
                Path(args[-1].replace("%04d", "0001")).write_bytes(b"frame")
                return "pts_time:0\n"

            def failing_run(args: list[str], **_: object) -> str:
                raise RuntimeError("boom")

            try:
                douyin_frames._run_ffmpeg = first_run
                first = douyin_frames.extract_frames(
                    conn,
                    unique_id=unique_id,
                    mode="interval",
                    interval=1,
                    max_frames=1,
                )
                old_frame_path = Path(
                    conn.execute(
                        "SELECT local_path FROM douyin_frame WHERE file_unique_id = ?",
                        (unique_id,),
                    ).fetchone()["local_path"]
                )

                douyin_frames._run_ffmpeg = failing_run
                with self.assertRaises(RuntimeError):
                    douyin_frames.extract_frames(
                        conn,
                        unique_id=unique_id,
                        mode="interval",
                        interval=1,
                        max_frames=1,
                    )
            finally:
                douyin_frames.ffmpeg_path = original_ffmpeg_path
                douyin_frames._run_ffmpeg = original_run_ffmpeg

            rows = conn.execute(
                "SELECT local_path FROM douyin_frame WHERE file_unique_id = ?",
                (unique_id,),
            ).fetchall()
            self.assertEqual(first["extracted"], 1)
            self.assertEqual(len(rows), 1)
            self.assertEqual(Path(rows[0]["local_path"]), old_frame_path)
            self.assertTrue(old_frame_path.exists())

    def test_remove_download_deletes_frames(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "author" / "video" / "777.mp4"
            frame = Path(tmp) / "author" / "frames" / "777" / "out_0001.jpg"
            video.parent.mkdir(parents=True, exist_ok=True)
            frame.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            frame.write_bytes(b"frame")
            unique_id = self._seed_video(conn, str(video))
            conn.execute(
                """
                INSERT INTO douyin_frame(
                    frame_uid, file_unique_id, aweme_id, source_id, frame_index,
                    timestamp_ms, local_path, width, height, size, mode, format,
                    tags, created_at
                )
                VALUES('douyin:777:frame:0', ?, '777', '', 0, 0, ?, 0, 0, 5,
                       'interval', 'jpg', '', 1)
                """,
                (unique_id, str(frame)),
            )
            conn.commit()

            self.assertIsNotNone(remove_douyin_download(conn, unique_id))

            remaining = conn.execute(
                "SELECT COUNT(*) FROM douyin_frame WHERE file_unique_id = ?",
                (unique_id,),
            ).fetchone()[0]
            self.assertEqual(remaining, 0)
            self.assertFalse(frame.exists())

    @unittest.skipUnless(douyin_frames.ffmpeg_available(), "ffmpeg not available")
    def test_interval_extraction_replaces_existing_frames(self) -> None:
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

            # Re-extraction always replaces old frame rows and files.
            old_paths = [
                row[0]
                for row in conn.execute(
                    "SELECT local_path FROM douyin_frame WHERE file_unique_id = ?",
                    (unique_id,),
                ).fetchall()
            ]
            result2 = douyin_frames.extract_frames(
                conn, unique_id=unique_id, mode="interval", interval=1, max_frames=3
            )
            self.assertGreater(result2["extracted"], 0)
            count = conn.execute(
                "SELECT COUNT(*) FROM douyin_frame WHERE file_unique_id = ?",
                (unique_id,),
            ).fetchone()[0]
            self.assertEqual(count, result2["extracted"])
            self.assertTrue(len(old_paths) > 0)

    @unittest.skipUnless(douyin_frames.ffmpeg_available(), "ffmpeg not available")
    def test_timestamp_extraction_records_requested_timestamp(self) -> None:
        conn = self._connection()
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "author" / "video" / "777.mp4"
            self.assertTrue(_make_test_video(video))
            unique_id = self._seed_video(conn, str(video))

            result = douyin_frames.extract_frames(
                conn,
                unique_id=unique_id,
                mode="timestamp",
                timestamp_ms=1000,
            )

            self.assertEqual(result["extracted"], 1)
            self.assertEqual(result["frames"][0]["timestampMs"], 1000)


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
            job_count = conn.execute(
                "SELECT COUNT(*) FROM douyin_job WHERE file_unique_id = ?",
                (record["uniqueId"],),
            ).fetchone()[0]
            self.assertEqual(job_count, 0)

    def test_extract_rejects_replace_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client, conn, _ = self._client(tmp)
            video = Path(tmp) / "video" / "888.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed(conn, str(video))
            resp = client.post(
                f"/douyin/file/{unique_id}/frames/extract",
                json={"mode": "interval", "replace": False},
            )
            self.assertEqual(resp.status_code, 422)

    def test_extract_rejects_too_many_frames(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client, conn, _ = self._client(tmp)
            video = Path(tmp) / "video" / "888.mp4"
            video.parent.mkdir(parents=True, exist_ok=True)
            video.write_bytes(b"x")
            unique_id = self._seed(conn, str(video))
            resp = client.post(
                f"/douyin/file/{unique_id}/frames/extract",
                json={
                    "mode": "interval",
                    "maxFrames": douyin_frames.MAX_FRAMES_LIMIT + 1,
                },
            )
            self.assertEqual(resp.status_code, 400)

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
            self.assertNotIn("jobId", body)
            self.assertGreater(body["extracted"], 0)
            frame_id = body["frames"][0]["id"]
            job_count = conn.execute("SELECT COUNT(*) FROM douyin_job").fetchone()[0]
            self.assertEqual(job_count, 0)

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
