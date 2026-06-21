from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI

from .config import AppConfig
from .douyin_asset_store import sync_douyin_downloaded_assets
from .douyin_bridge import download_aweme, metadata_from_row
from .douyin_events import (
    emit_douyin_file_status,
    emit_douyin_job,
    file_status_event,
)
from .douyin_jobs import active_job_for_file, create_job, get_job, update_job
from .douyin_store import (
    douyin_file_row,
    remove_douyin_download,
    serialize_douyin_file,
    update_douyin_file_status,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DouyinLifecycleRuntime:
    downloader: Any = download_aweme
    sync_assets: Any = sync_douyin_downloaded_assets
    emit_file: Any = emit_douyin_file_status
    emit_job: Any = emit_douyin_job


class DouyinFileLifecycle:
    """Owns the complete lifecycle of managed Douyin files.

    One module behind which task ownership, state transitions, job persistence,
    event production, asset completion, pause, removal, and retry semantics
    live. Routes, discovery, jobs, and workers cross the same interface.
    """

    def __init__(
        self,
        app: FastAPI,
        runtime: DouyinLifecycleRuntime | None = None,
    ) -> None:
        self._app = app
        self._runtime = runtime or DouyinLifecycleRuntime()
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._cancel_actions: dict[str, str] = {}
        self._file_jobs: dict[str, str] = {}

    def start(self, unique_id: str, *, session_id: str = "") -> dict[str, Any] | None:
        db = self._db
        row = douyin_file_row(db, unique_id=unique_id)
        if row is None:
            return None
        current = serialize_douyin_file(row)
        if current["downloadStatus"] == "completed":
            return current
        if self.is_active(unique_id):
            return current

        payload = update_douyin_file_status(
            db,
            unique_id=unique_id,
            download_status="downloading",
            downloaded_size=0,
        )
        self._cancel_actions.pop(unique_id, None)
        job = create_job(
            db,
            kind="file_download",
            source_id=str(row["source_id"] or ""),
            file_unique_id=unique_id,
            total=1,
            state="running",
            step="downloading",
        )
        self._file_jobs[unique_id] = str(job["id"])
        task = asyncio.create_task(self._run(unique_id, session_id=session_id))
        self._tasks[unique_id] = task
        return payload

    async def toggle_pause(
        self,
        unique_id: str,
        *,
        is_paused: bool | None,
        session_id: str = "",
    ) -> dict[str, Any] | None:
        row = douyin_file_row(self._db, unique_id=unique_id)
        if row is None:
            return None
        should_pause = (
            str(row["download_status"] or "") == "downloading"
            if is_paused is None
            else is_paused
        )
        if should_pause:
            return await self.pause(unique_id, session_id=session_id)
        task = self._tasks.get(unique_id)
        if task is not None and not task.done():
            await asyncio.gather(task, return_exceptions=True)
        return self.start(unique_id, session_id=session_id)

    async def pause(
        self,
        unique_id: str,
        *,
        session_id: str = "",
    ) -> dict[str, Any] | None:
        db = self._db
        self._signal_cancel(unique_id, action="pause")
        payload = update_douyin_file_status(
            db,
            unique_id=unique_id,
            download_status="paused",
        )
        if payload is not None:
            await self._runtime.emit_file(
                file_status_event(payload),
                session_id=session_id,
            )
        return payload

    async def remove(
        self,
        unique_id: str,
        *,
        session_id: str = "",
    ) -> dict[str, Any] | None:
        self._signal_cancel(unique_id, action="remove")
        payload = remove_douyin_download(self._db, unique_id)
        if payload is not None:
            await self._runtime.emit_file(
                file_status_event(payload, removed=True),
                session_id=session_id,
            )
        return payload

    def retry(self, unique_id: str) -> dict[str, Any] | None:
        self.start(unique_id)
        return active_job_for_file(self._db, unique_id)

    def is_active(self, unique_id: str) -> bool:
        task = self._tasks.get(unique_id)
        return task is not None and not task.done()

    async def close(self) -> None:
        tasks = [task for task in self._tasks.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._cancel_actions.clear()
        self._file_jobs.clear()

    @property
    def _db(self) -> sqlite3.Connection:
        return self._app.state.db

    def _cancel_action(self, unique_id: str) -> str:
        return self._cancel_actions.get(unique_id, "")

    def _forget(self, unique_id: str) -> None:
        self._tasks.pop(unique_id, None)
        self._cancel_actions.pop(unique_id, None)
        self._file_jobs.pop(unique_id, None)

    def _signal_cancel(self, unique_id: str, *, action: str) -> None:
        task = self._tasks.get(unique_id)
        if task is None or task.done():
            self._cancel_actions.pop(unique_id, None)
            return
        self._cancel_actions[unique_id] = action
        task.cancel()

    async def _run(self, unique_id: str, *, session_id: str = "") -> None:
        app = self._app
        db: sqlite3.Connection = app.state.db
        config: AppConfig = app.state.config
        job_id = self._file_jobs.get(unique_id, "")
        progress_tasks: set[asyncio.Task[None]] = set()

        async def _emit_job(**fields: Any) -> None:
            if not job_id:
                return
            updated = update_job(db, job_id, **fields)
            if updated is not None:
                await self._runtime.emit_job(updated, session_id=session_id)

        async def _emit_current(status: str, **patch: Any) -> None:
            payload = update_douyin_file_status(
                db,
                unique_id=unique_id,
                download_status=status,
                **patch,
            )
            if payload is not None:
                await self._runtime.emit_file(
                    file_status_event(payload), session_id=session_id
                )

        loop = asyncio.get_running_loop()

        try:
            row = douyin_file_row(db, unique_id=unique_id)
            if row is None:
                return
            aweme = metadata_from_row(row)
            await _emit_current("downloading")
            await _emit_job(step="downloading")

            async def _apply_bridge_event(event: dict[str, Any]) -> None:
                kind = str(event.get("kind") or "")
                if kind == "total":
                    update_job(db, job_id, total=int(event.get("total") or 0))
                elif kind == "step":
                    detail = str(event.get("detail") or "")
                    step_text = str(event.get("step") or "")
                    update_job(
                        db,
                        job_id,
                        step=f"{step_text}: {detail}" if detail else step_text,
                    )
                elif kind == "item":
                    status = str(event.get("status") or "")
                    update_job(db, job_id, step=f"item:{status}" if status else "item")
                elif kind == "author":
                    nickname = str(event.get("nickname") or "")
                    if nickname:
                        update_job(db, job_id, step=f"author:{nickname}")
                else:
                    return
                updated = get_job(db, job_id)
                if updated is not None:
                    await self._runtime.emit_job(updated, session_id=session_id)

            def _schedule_bridge_event(event: dict[str, Any]) -> None:
                task = asyncio.create_task(_apply_bridge_event(event))
                progress_tasks.add(task)

                def _report_failure(done: asyncio.Task[None]) -> None:
                    progress_tasks.discard(done)
                    try:
                        done.result()
                    except Exception:
                        logger.debug(
                            "Douyin job progress update failed",
                            exc_info=True,
                        )

                task.add_done_callback(_report_failure)

            def _on_bridge_event(event: dict[str, Any]) -> None:
                if not job_id:
                    return
                loop.call_soon_threadsafe(_schedule_bridge_event, dict(event))

            result = await self._runtime.downloader(
                config,
                db,
                aweme,
                on_event=_on_bridge_event,
            )
            action = self._cancel_action(unique_id)
            if action:
                await _emit_job(state="cancelled", step="cancelled")
                return
            local_path = str(result.get("localPath") or "")
            size = int(result.get("size") or 0)
            if not local_path:
                raise RuntimeError(
                    "Douyin media download finished without a local file"
                )
            self._runtime.sync_assets(
                db,
                primary_unique_id=unique_id,
                assets=result.get("assets")
                if isinstance(result.get("assets"), list)
                else [],
            )
            await _emit_current(
                "completed",
                local_path=local_path,
                downloaded_size=size,
                size=size,
            )
            await _emit_job(state="completed", success=1, step="completed")
        except asyncio.CancelledError:
            if not self._cancel_action(unique_id):
                await _emit_current("paused")
            await _emit_job(state="cancelled", step="cancelled")
        except Exception as exc:
            if self._cancel_action(unique_id):
                await _emit_job(state="cancelled", step="cancelled")
            else:
                logger.warning("Douyin download failed unique=%s: %s", unique_id, exc)
                await _emit_current("error", error=str(exc))
                await _emit_job(
                    state="failed",
                    failed=1,
                    error=str(exc),
                    step="error",
                )
        finally:
            if progress_tasks:
                await asyncio.gather(*progress_tasks, return_exceptions=True)
            self._forget(unique_id)


def create_douyin_file_lifecycle(app: FastAPI) -> DouyinFileLifecycle:
    return DouyinFileLifecycle(app)


def douyin_file_lifecycle(app: FastAPI) -> DouyinFileLifecycle:
    lifecycle = getattr(app.state, "douyin_file_lifecycle", None)
    if not isinstance(lifecycle, DouyinFileLifecycle):
        lifecycle = create_douyin_file_lifecycle(app)
        app.state.douyin_file_lifecycle = lifecycle
    return lifecycle
