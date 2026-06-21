import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from app.config import AppConfig
from app.db import init_schema
from app.douyin_file_lifecycle import (
    DouyinFileLifecycle,
    DouyinLifecycleRuntime,
)
from app.douyin_jobs import list_jobs
from app.douyin_store import (
    douyin_file_row,
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


class DouyinFileLifecycleTest(unittest.IsolatedAsyncioTestCase):
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

    async def _wait(self, lifecycle: DouyinFileLifecycle, unique_id: str) -> None:
        for _ in range(100):
            if not lifecycle.is_active(unique_id):
                return
            await asyncio.sleep(0.001)
        self.fail("Douyin lifecycle task did not finish")

    async def test_completion_owns_assets_status_job_and_events(self) -> None:
        app = self._app()
        unique_id = self._file(app)
        emit_file = AsyncMock()
        emit_job = AsyncMock()
        sync_assets = Mock()

        async def downloader(*_args, **kwargs):
            kwargs["on_event"]({"kind": "total", "total": 4})
            return {
                "localPath": "D:/downloads/one.mp4",
                "size": 123,
                "assets": [{"path": "D:/downloads/one.mp4"}],
            }

        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                sync_assets=sync_assets,
                emit_file=emit_file,
                emit_job=emit_job,
            ),
        )

        started = lifecycle.start(unique_id, session_id="session-a")
        self.assertEqual(started["downloadStatus"], "downloading")
        await self._wait(lifecycle, unique_id)

        row = douyin_file_row(app.state.db, unique_id=unique_id)
        job = list_jobs(app.state.db, limit=1)[0]
        self.assertEqual(row["download_status"], "completed")
        self.assertEqual(row["local_path"], "D:/downloads/one.mp4")
        self.assertEqual(job["state"], "completed")
        self.assertEqual(job["total"], 4)
        sync_assets.assert_called_once()
        self.assertEqual(emit_file.await_count, 2)
        self.assertTrue(
            all(
                call.kwargs["session_id"] == "session-a"
                for call in emit_file.await_args_list
            )
        )
        self.assertTrue(
            all(
                call.kwargs["session_id"] == "session-a"
                for call in emit_job.await_args_list
            )
        )

    async def test_failure_is_local_to_lifecycle_interface(self) -> None:
        app = self._app()
        unique_id = self._file(app)
        emit_file = AsyncMock()
        emit_job = AsyncMock()

        async def downloader(*_args, **_kwargs):
            raise RuntimeError("download exploded")

        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                emit_file=emit_file,
                emit_job=emit_job,
            ),
        )

        lifecycle.start(unique_id)
        await self._wait(lifecycle, unique_id)

        row = douyin_file_row(app.state.db, unique_id=unique_id)
        job = list_jobs(app.state.db, limit=1)[0]
        self.assertEqual(row["download_status"], "error")
        self.assertEqual(job["state"], "failed")
        self.assertIn("download exploded", job["error"])

    async def test_toggle_pause_and_duplicate_start_share_one_task(self) -> None:
        app = self._app()
        unique_id = self._file(app)
        entered = asyncio.Event()
        emit_file = AsyncMock()

        async def downloader(*_args, **_kwargs):
            entered.set()
            await asyncio.sleep(10)

        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                emit_file=emit_file,
                emit_job=AsyncMock(),
            ),
        )

        first = lifecycle.start(unique_id)
        second = lifecycle.start(unique_id)
        await entered.wait()
        self.assertEqual(first["uniqueId"], second["uniqueId"])
        self.assertTrue(lifecycle.is_active(unique_id))
        self.assertEqual(len(list_jobs(app.state.db)), 1)

        paused = await lifecycle.toggle_pause(unique_id, is_paused=True)
        await lifecycle.close()
        self.assertEqual(paused["downloadStatus"], "paused")
        self.assertFalse(lifecycle.is_active(unique_id))
        self.assertEqual(emit_file.await_count, 2)

    async def test_remove_and_close_do_not_rewrite_removed_file_to_paused(self) -> None:
        app = self._app()
        unique_id = self._file(app)
        entered = asyncio.Event()

        async def downloader(*_args, **_kwargs):
            entered.set()
            await asyncio.sleep(10)

        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                emit_file=AsyncMock(),
                emit_job=AsyncMock(),
            ),
        )

        lifecycle.start(unique_id)
        await entered.wait()
        removed = await lifecycle.remove(unique_id)
        await lifecycle.close()

        row = douyin_file_row(app.state.db, unique_id=unique_id)
        self.assertEqual(removed["downloadStatus"], "idle")
        self.assertEqual(row["download_status"], "idle")
        self.assertEqual(list_jobs(app.state.db, limit=1)[0]["state"], "cancelled")

    async def test_completed_start_short_circuits_without_new_job(self) -> None:
        app = self._app()
        unique_id = self._file(app)
        app.state.db.execute(
            """
            UPDATE douyin_file
            SET download_status = 'completed',
                local_path = 'D:/downloads/one.mp4',
                downloaded_size = size
            WHERE unique_id = ?
            """,
            (unique_id,),
        )
        app.state.db.commit()
        downloader = AsyncMock()
        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                emit_file=AsyncMock(),
                emit_job=AsyncMock(),
            ),
        )

        result = lifecycle.start(unique_id)

        self.assertEqual(result["downloadStatus"], "completed")
        self.assertFalse(lifecycle.is_active(unique_id))
        self.assertEqual(list_jobs(app.state.db), [])
        downloader.assert_not_awaited()

    async def test_pause_stays_cancelled_when_downloader_swallows_cancellation(
        self,
    ) -> None:
        app = self._app()
        unique_id = self._file(app)
        entered = asyncio.Event()

        async def downloader(*_args, **_kwargs):
            entered.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                return {
                    "localPath": "D:/downloads/late.mp4",
                    "size": 123,
                    "assets": [],
                }

        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                emit_file=AsyncMock(),
                emit_job=AsyncMock(),
            ),
        )

        lifecycle.start(unique_id)
        await entered.wait()
        await lifecycle.pause(unique_id)
        await self._wait(lifecycle, unique_id)

        row = douyin_file_row(app.state.db, unique_id=unique_id)
        job = list_jobs(app.state.db, limit=1)[0]
        self.assertEqual(row["download_status"], "paused")
        self.assertEqual(job["state"], "cancelled")

    async def test_pause_stays_cancelled_when_downloader_raises_after_cancel(
        self,
    ) -> None:
        app = self._app()
        unique_id = self._file(app)
        entered = asyncio.Event()

        async def downloader(*_args, **_kwargs):
            entered.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError as exc:
                raise RuntimeError("late downloader failure") from exc

        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                emit_file=AsyncMock(),
                emit_job=AsyncMock(),
            ),
        )

        lifecycle.start(unique_id)
        await entered.wait()
        await lifecycle.pause(unique_id)
        await self._wait(lifecycle, unique_id)

        row = douyin_file_row(app.state.db, unique_id=unique_id)
        job = list_jobs(app.state.db, limit=1)[0]
        self.assertEqual(row["download_status"], "paused")
        self.assertEqual(job["state"], "cancelled")
        self.assertNotIn("late downloader failure", job["error"])

    async def test_inactive_remove_leaves_no_marker_for_fresh_start(self) -> None:
        app = self._app()
        unique_id = self._file(app)

        async def downloader(*_args, **_kwargs):
            return {
                "localPath": "D:/downloads/fresh.mp4",
                "size": 12,
                "assets": [],
            }

        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                emit_file=AsyncMock(),
                emit_job=AsyncMock(),
            ),
        )

        await lifecycle.remove(unique_id)
        lifecycle.start(unique_id)
        await self._wait(lifecycle, unique_id)

        row = douyin_file_row(app.state.db, unique_id=unique_id)
        self.assertEqual(row["download_status"], "completed")

    async def test_shutdown_cancels_unmarked_task_to_paused(self) -> None:
        app = self._app()
        unique_id = self._file(app)
        entered = asyncio.Event()

        async def downloader(*_args, **_kwargs):
            entered.set()
            await asyncio.sleep(10)

        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                emit_file=AsyncMock(),
                emit_job=AsyncMock(),
            ),
        )

        lifecycle.start(unique_id)
        await entered.wait()
        await lifecycle.close()

        row = douyin_file_row(app.state.db, unique_id=unique_id)
        job = list_jobs(app.state.db, limit=1)[0]
        self.assertEqual(row["download_status"], "paused")
        self.assertEqual(job["state"], "cancelled")

    async def test_immediate_resume_waits_for_cancelled_task_then_restarts(
        self,
    ) -> None:
        app = self._app()
        unique_id = self._file(app)
        entered = asyncio.Event()
        calls = 0

        async def downloader(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                await asyncio.sleep(10)
            return {
                "localPath": "D:/downloads/resumed.mp4",
                "size": 20,
                "assets": [],
            }

        lifecycle = DouyinFileLifecycle(
            app,
            DouyinLifecycleRuntime(
                downloader=downloader,
                emit_file=AsyncMock(),
                emit_job=AsyncMock(),
            ),
        )

        lifecycle.start(unique_id)
        await entered.wait()
        await lifecycle.pause(unique_id)
        resumed = await lifecycle.toggle_pause(unique_id, is_paused=False)
        await self._wait(lifecycle, unique_id)

        row = douyin_file_row(app.state.db, unique_id=unique_id)
        self.assertEqual(resumed["downloadStatus"], "downloading")
        self.assertEqual(row["download_status"], "completed")
        self.assertEqual(calls, 2)


if __name__ == "__main__":
    unittest.main()
