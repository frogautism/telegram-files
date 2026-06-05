import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.config import AppConfig
from app.db import init_schema
from app.douyin_jobs import create_job, get_job
from app.douyin_runtime import (
    cancel_download,
    discover_source,
    retry_job,
    runtime_from_app,
    start_download_task,
)
from app.douyin_store import (
    douyin_file_row,
    get_douyin_source,
    upsert_douyin_aweme,
    upsert_douyin_source,
)


def _config(app_root: Path) -> AppConfig:
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


class DouyinRuntimeTest(unittest.IsolatedAsyncioTestCase):
    def _app(self) -> SimpleNamespace:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        self.addCleanup(conn.close)
        return SimpleNamespace(
            state=SimpleNamespace(db=conn, config=_config(Path(temp_dir.name)))
        )

    def _file(self, app: SimpleNamespace) -> str:
        source = upsert_douyin_source(
            app.state.db,
            url="https://www.douyin.com/video/1",
        )
        record = upsert_douyin_aweme(
            app.state.db,
            source_id=source["id"],
            aweme={"aweme_id": "1", "desc": "one"},
        )
        return record["uniqueId"]

    async def test_retry_file_download_returns_new_active_job(self) -> None:
        app = self._app()
        unique_id = self._file(app)
        old_job = create_job(
            app.state.db,
            kind="file_download",
            file_unique_id=unique_id,
            state="failed",
        )

        async def fake_download_task(*_args, **_kwargs):
            await asyncio.sleep(0.05)

        with (
            patch("app.douyin_runtime._download_file_task", fake_download_task),
            patch("app.douyin_runtime.emit_douyin_file_status", AsyncMock()),
            patch("app.douyin_runtime.emit_douyin_job", AsyncMock()),
        ):
            retried = await retry_job(app, old_job["id"])

        self.assertNotEqual(retried["id"], old_job["id"])
        self.assertEqual(retried["state"], "running")
        self.assertEqual(retried["fileUniqueId"], unique_id)

        runtime = runtime_from_app(app)
        task = runtime.tasks.get(unique_id)
        if task is not None:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_remove_active_download_is_not_rewritten_to_paused(self) -> None:
        app = self._app()
        unique_id = self._file(app)
        started = asyncio.Event()

        async def fake_download(*_args, **_kwargs):
            started.set()
            await asyncio.sleep(10)

        with (
            patch("app.douyin_runtime.download_aweme", fake_download),
            patch("app.douyin_runtime.emit_douyin_file_status", AsyncMock()),
            patch("app.douyin_runtime.emit_douyin_job", AsyncMock()),
        ):
            start_download_task(app, unique_id)
            await started.wait()
            removed = await cancel_download(app, unique_id, remove=True)
            task = runtime_from_app(app).tasks.get(unique_id)
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)

        self.assertEqual(removed["downloadStatus"], "idle")
        row = douyin_file_row(app.state.db, unique_id=unique_id)
        self.assertEqual(row["download_status"], "idle")
        job = get_job(app.state.db, runtime_from_app(app).file_jobs.get(unique_id, ""))
        self.assertIsNone(job)

    async def test_concurrent_refresh_skip_does_not_overwrite_source_status(self) -> None:
        app = self._app()
        source = upsert_douyin_source(
            app.state.db,
            url="https://www.douyin.com/user/sec",
            status="idle",
        )
        runtime = runtime_from_app(app)
        runtime.refreshing_sources.add(source["id"])

        result = await discover_source(app, url=source["url"], source_id=source["id"])

        self.assertEqual(result["jobId"], "")
        self.assertEqual(get_douyin_source(app.state.db, source["id"])["status"], "idle")


if __name__ == "__main__":
    unittest.main()
