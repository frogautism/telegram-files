import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from app.config import AppConfig
from app.db import init_schema
from app.douyin_file_lifecycle import (
    DouyinFileLifecycle,
    DouyinLifecycleRuntime,
)
from app.douyin_jobs import create_job
from app.douyin_runtime import (
    discover_source,
    retry_job,
    runtime_from_app,
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

    def _lifecycle(self, app: SimpleNamespace, downloader) -> DouyinFileLifecycle:
        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                emit_file=AsyncMock(),
                emit_job=AsyncMock(),
            ),
        )
        app.state.douyin_file_lifecycle = lifecycle
        return lifecycle

    async def test_retry_file_download_returns_new_active_job(self) -> None:
        app = self._app()
        unique_id = self._file(app)
        old_job = create_job(
            app.state.db,
            kind="file_download",
            file_unique_id=unique_id,
            state="failed",
        )

        async def fake_download(*_args, **_kwargs):
            await asyncio.sleep(0.05)
            return {"localPath": "", "size": 0, "assets": []}

        lifecycle = self._lifecycle(app, fake_download)

        retried = await retry_job(app, old_job["id"])

        self.assertNotEqual(retried["id"], old_job["id"])
        self.assertEqual(retried["state"], "running")
        self.assertEqual(retried["fileUniqueId"], unique_id)

        self.assertTrue(lifecycle.is_active(unique_id))
        await lifecycle.close()

    async def test_remove_active_download_is_not_rewritten_to_paused(self) -> None:
        app = self._app()
        unique_id = self._file(app)
        started = asyncio.Event()

        async def fake_download(*_args, **_kwargs):
            started.set()
            await asyncio.sleep(10)

        lifecycle = self._lifecycle(app, fake_download)

        lifecycle.start(unique_id)
        await started.wait()
        removed = await lifecycle.remove(unique_id)
        await lifecycle.close()

        self.assertEqual(removed["downloadStatus"], "idle")
        row = douyin_file_row(app.state.db, unique_id=unique_id)
        self.assertEqual(row["download_status"], "idle")
        job = app.state.db.execute(
            """
            SELECT state
            FROM douyin_job
            WHERE file_unique_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (unique_id,),
        ).fetchone()
        self.assertEqual(job["state"], "cancelled")

    async def test_concurrent_refresh_skip_does_not_overwrite_source_status(
        self,
    ) -> None:
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
        self.assertEqual(
            get_douyin_source(app.state.db, source["id"])["status"], "idle"
        )


if __name__ == "__main__":
    unittest.main()
