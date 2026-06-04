import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import douyin_frames
from app.config import AppConfig
from app.db import init_schema
from app.routers.douyin_health import router as health_router


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


class DouyinHealthTest(unittest.TestCase):
    def _client(self, tmp: str) -> TestClient:
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        app_root = Path(tmp) / "root"
        app_root.mkdir(parents=True, exist_ok=True)
        app = FastAPI()
        app.state.db = conn
        app.state.config = _make_config(app_root)
        app.include_router(health_router)
        return TestClient(app)

    def test_health_reports_shape_and_ffmpeg(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = self._client(tmp)
            resp = client.get("/douyin/health")
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            for key in (
                "downloaderAvailable",
                "downloaderError",
                "version",
                "cookieValid",
                "proxy",
                "outputPath",
                "outputWritable",
                "browserFallbackAvailable",
                "ffmpegAvailable",
                "ffmpegPath",
            ):
                self.assertIn(key, body)

            self.assertEqual(body["ffmpegAvailable"], douyin_frames.ffmpeg_available())
            if body["ffmpegAvailable"]:
                self.assertTrue(body["ffmpegPath"])
            else:
                self.assertEqual(body["ffmpegPath"], "")

            # No cookie configured by default -> cookieValid False
            self.assertFalse(body["cookieValid"])
            # output path is created and writable
            self.assertTrue(body["outputPath"])
            self.assertTrue(body["outputWritable"])
            self.assertIsInstance(body["downloaderAvailable"], bool)


if __name__ == "__main__":
    unittest.main()
