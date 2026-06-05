from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Any

from fastapi import FastAPI

from .app_state import (
    EVENT_TYPE_DOUYIN_JOB,
    EVENT_TYPE_FILE_STATUS,
    _build_ws_payload,
    _emit_ws_payload,
)
from .config import AppConfig
from .douyin_asset_store import sync_douyin_downloaded_assets
from .douyin_bridge import discover_awemes, download_aweme, metadata_from_row
from .douyin_jobs import (
    active_job_for_file,
    create_job,
    get_job,
    update_job,
)
from .douyin_jobs import (
    cancel_job as store_cancel_job,
)
from .douyin_store import (
    douyin_aweme_exists,
    douyin_file_for_transfer,
    douyin_file_row,
    douyin_transfer_candidates,
    get_douyin_source,
    list_douyin_sources,
    mark_douyin_source_status,
    now_ms,
    record_source_refresh_result,
    record_source_refresh_start,
    remove_douyin_download,
    serialize_douyin_file,
    source_id_for_url,
    update_douyin_file_status,
    update_douyin_transfer_status,
    upsert_douyin_aweme,
    upsert_douyin_source,
)
from .transfer_ops import execute_transfer

logger = logging.getLogger(__name__)

# Cooldown before an errored auto-download file is retried. Without this an
# errored file would be re-attempted every worker tick (10s), hammering the
# source. Measured against the file's ``updated_at`` (epoch-ms).
AUTO_ERROR_RETRY_MS = 5 * 60 * 1000


class DouyinRuntime:
    def __init__(self) -> None:
        self.tasks: dict[str, asyncio.Task] = {}
        self.cancel_actions: dict[str, str] = {}
        # Map unique_id -> active job id for single-file downloads.
        self.file_jobs: dict[str, str] = {}
        self.refreshing_sources: set[str] = set()

    def cancel(self, unique_id: str, *, remove: bool = False) -> None:
        self.cancel_actions[unique_id] = "remove" if remove else "pause"
        task = self.tasks.get(unique_id)
        if task is not None:
            task.cancel()

    def cancel_action(self, unique_id: str) -> str:
        return self.cancel_actions.get(unique_id, "")

    def forget(self, unique_id: str) -> None:
        self.tasks.pop(unique_id, None)
        self.cancel_actions.pop(unique_id, None)
        self.file_jobs.pop(unique_id, None)


def runtime_from_app(app: FastAPI) -> DouyinRuntime:
    runtime = getattr(app.state, "douyin_runtime", None)
    if isinstance(runtime, DouyinRuntime):
        return runtime
    runtime = DouyinRuntime()
    app.state.douyin_runtime = runtime
    return runtime


def _douyin_source_labels(parsed: dict[str, Any], awemes: list[dict[str, Any]]) -> tuple[str, str]:
    url_type = str(parsed.get("type") or "douyin")
    for aweme in awemes:
        author = aweme.get("author") if isinstance(aweme, dict) else None
        if not isinstance(author, dict):
            continue
        nickname = str(author.get("nickname") or "").strip()
        if nickname:
            return nickname, nickname
    if url_type == "user":
        return "Douyin user", ""
    return url_type or "douyin", ""


async def emit_douyin_file_status(payload: dict[str, Any], *, session_id: str = "") -> None:
    await _emit_ws_payload(
        _build_ws_payload(EVENT_TYPE_FILE_STATUS, payload),
        session_id=session_id or None,
    )


async def emit_douyin_job(payload: dict[str, Any], *, session_id: str = "") -> None:
    await _emit_ws_payload(
        _build_ws_payload(EVENT_TYPE_DOUYIN_JOB, payload),
        session_id=session_id or None,
    )


def _aweme_id_of(aweme: dict[str, Any]) -> str:
    return str(aweme.get("aweme_id") or aweme.get("group_id") or "").strip()


def _aweme_is_pinned(aweme: dict[str, Any]) -> bool:
    """Return True when the aweme is pinned ("top") on the user feed.

    Douyin pins posts to the top of a feed, so they appear first even when
    older than newer, unpinned posts. The flag is commonly ``is_top == 1`` but
    may arrive as a string or other truthy value, so convert tolerantly.
    """
    try:
        return bool(int(aweme.get("is_top") or 0))
    except (TypeError, ValueError):
        return bool(aweme.get("is_top"))


async def discover_source(
    app: FastAPI,
    *,
    url: str,
    mode: str | None = None,
    preload_only: bool = True,
    source_id: str | None = None,
    backfill: bool = False,
) -> dict[str, Any]:
    db: sqlite3.Connection = app.state.db
    config: AppConfig = app.state.config
    source_id = str(source_id or source_id_for_url(url))
    runtime = runtime_from_app(app)
    if source_id in runtime.refreshing_sources:
        refreshed = get_douyin_source(db, source_id)
        result = dict(refreshed or {"id": source_id, "url": url})
        result.update(
            {"discovered": 0, "new": 0, "existing": 0, "failed": 0, "jobId": ""}
        )
        return result
    runtime.refreshing_sources.add(source_id)
    source = upsert_douyin_source(
        db, url=url, status="discovering", source_id=source_id
    )
    source_id = str(source["id"])
    record_source_refresh_start(db, source_id)
    job = create_job(db, kind="source_refresh", source_id=source_id, url=url, state="running")
    job_id = str(job["id"])
    await emit_douyin_job(job)

    discovered = 0
    new_count = 0
    existing_count = 0
    failed = 0
    newest_aweme_id = ""
    newest_create_time = 0
    try:
        resolved_url, parsed, awemes = await discover_awemes(config, db, url, mode=mode)
        discovered = len(awemes)
        title, author_name = _douyin_source_labels(parsed, awemes)
        source = upsert_douyin_source(
            db,
            url=url,
            resolved_url=resolved_url,
            url_type=str(parsed.get("type") or ""),
            title=title,
            author_name=author_name,
            status="idle" if preload_only else "downloading",
            source_id=source_id,
        )
        update_job(db, job_id, total=discovered, step="discovered")

        to_download: list[str] = []
        for aweme in awemes:
            aweme_id = _aweme_id_of(aweme)
            if not aweme_id:
                failed += 1
                continue
            create_time = int(aweme.get("create_time") or 0)
            if create_time >= newest_create_time:
                newest_create_time = create_time
                newest_aweme_id = aweme_id

            already = douyin_aweme_exists(db, aweme_id)
            if already:
                existing_count += 1
                # Incremental: awemes are newest-first; once we hit a known
                # aweme the remainder is already known, so stop upserting.
                # Exception: Douyin pins ("top") posts to the head of a user
                # feed, so the first item(s) are often old and already known.
                # Skip those instead of breaking so the scan reaches genuinely
                # new, unpinned posts further down.
                if not backfill and not _aweme_is_pinned(aweme):
                    break
                continue

            new_count += 1
            record = upsert_douyin_aweme(db, source_id=source_id, aweme=aweme)
            if record is not None:
                to_download.append(str(record["uniqueId"]))
                await emit_douyin_file_status(
                    {
                        "source": "douyin",
                        "fileId": record["id"],
                        "uniqueId": record["uniqueId"],
                        "downloadStatus": record["downloadStatus"],
                        "localPath": record["localPath"],
                        "completionDate": record["completionDate"],
                        "downloadedSize": record["downloadedSize"],
                        "transferStatus": record["transferStatus"],
                    }
                )

        mark_douyin_source_status(db, source_id, "idle")
        record_source_refresh_result(
            db,
            source_id,
            discovered=discovered,
            new=new_count,
            existing=existing_count,
            failed=failed,
            newest_aweme_id=newest_aweme_id,
            newest_create_time=newest_create_time,
        )
        if not preload_only:
            for unique_id in to_download:
                start_download_task(app, unique_id)

        finished = update_job(
            db,
            job_id,
            state="completed",
            total=discovered,
            success=new_count,
            skipped=existing_count,
            failed=failed,
            step="done",
        )
        if finished is not None:
            await emit_douyin_job(finished)

        refreshed = get_douyin_source(db, source_id) or source
        refreshed = dict(refreshed)
        refreshed.update(
            {
                "discovered": discovered,
                "new": new_count,
                "existing": existing_count,
                "failed": failed,
                "jobId": job_id,
            }
        )
        return refreshed
    except Exception as exc:
        mark_douyin_source_status(db, source_id, "error", error=str(exc))
        record_source_refresh_result(
            db,
            source_id,
            discovered=discovered,
            new=new_count,
            existing=existing_count,
            failed=failed,
            newest_aweme_id=newest_aweme_id,
            newest_create_time=newest_create_time,
            error=str(exc),
        )
        failed_job = update_job(
            db, job_id, state="failed", error=str(exc), step="error"
        )
        if failed_job is not None:
            await emit_douyin_job(failed_job)
        raise
    finally:
        runtime.refreshing_sources.discard(source_id)


def start_download_task(
    app: FastAPI,
    unique_id: str,
    *,
    session_id: str = "",
) -> dict[str, Any] | None:
    db: sqlite3.Connection = app.state.db
    runtime = runtime_from_app(app)
    row = douyin_file_row(db, unique_id=unique_id)
    if row is None:
        return None
    current = serialize_douyin_file(row)
    if current["downloadStatus"] == "completed":
        return current
    if unique_id in runtime.tasks and not runtime.tasks[unique_id].done():
        return current

    payload = update_douyin_file_status(
        db,
        unique_id=unique_id,
        download_status="downloading",
        downloaded_size=0,
    )
    runtime.cancel_actions.pop(unique_id, None)
    job = create_job(
        db,
        kind="file_download",
        source_id=str(row["source_id"] or ""),
        file_unique_id=unique_id,
        total=1,
        state="running",
        step="downloading",
    )
    runtime.file_jobs[unique_id] = str(job["id"])
    task = asyncio.create_task(_download_file_task(app, unique_id, session_id=session_id))
    runtime.tasks[unique_id] = task
    return payload


async def _download_file_task(app: FastAPI, unique_id: str, *, session_id: str = "") -> None:
    db: sqlite3.Connection = app.state.db
    config: AppConfig = app.state.config
    runtime = runtime_from_app(app)
    job_id = runtime.file_jobs.get(unique_id, "")

    async def _emit_job(**fields: Any) -> None:
        if not job_id:
            return
        updated = update_job(db, job_id, **fields)
        if updated is not None:
            await emit_douyin_job(updated, session_id=session_id)

    async def _emit_current(status: str, **patch: Any) -> None:
        payload = update_douyin_file_status(
            db,
            unique_id=unique_id,
            download_status=status,
            **patch,
        )
        if payload is not None:
            await emit_douyin_file_status(
                {
                    "source": "douyin",
                    "fileId": payload["id"],
                    "uniqueId": payload["uniqueId"],
                    "downloadStatus": payload["downloadStatus"],
                    "localPath": payload["localPath"],
                    "completionDate": payload["completionDate"],
                    "downloadedSize": payload["downloadedSize"],
                    "transferStatus": payload["transferStatus"],
                },
                session_id=session_id,
            )

    loop = asyncio.get_running_loop()

    try:
        row = douyin_file_row(db, unique_id=unique_id)
        if row is None:
            return
        aweme = metadata_from_row(row)
        await _emit_current("downloading")
        await _emit_job(step="downloading")

        def _apply_bridge_event(event: dict[str, Any]) -> None:
            kind = str(event.get("kind") or "")
            if kind == "total":
                update_job(db, job_id, total=int(event.get("total") or 0))
            elif kind == "step":
                detail = str(event.get("detail") or "")
                step_text = str(event.get("step") or "")
                update_job(db, job_id, step=f"{step_text}: {detail}" if detail else step_text)
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
                asyncio.run_coroutine_threadsafe(
                    emit_douyin_job(updated, session_id=session_id), loop
                )

        def _on_bridge_event(event: dict[str, Any]) -> None:
            if not job_id:
                return
            try:
                _apply_bridge_event(event)
            except Exception:  # pragma: no cover - progress reporting best-effort
                logger.debug("Douyin job progress update failed", exc_info=True)

        result = await download_aweme(config, db, aweme, on_event=_on_bridge_event)
        action = runtime.cancel_action(unique_id)
        if action:
            if action != "remove":
                await _emit_current("paused")
            await _emit_job(state="cancelled", step="cancelled")
            return
        local_path = str(result.get("localPath") or "")
        size = int(result.get("size") or 0)
        if not local_path:
            raise RuntimeError("Douyin media download finished without a local file")
        sync_douyin_downloaded_assets(
            db,
            primary_unique_id=unique_id,
            assets=result.get("assets") if isinstance(result.get("assets"), list) else [],
        )
        await _emit_current(
            "completed",
            local_path=local_path,
            downloaded_size=size,
            size=size,
        )
        await _emit_job(state="completed", success=1, step="completed")
    except asyncio.CancelledError:
        if runtime.cancel_action(unique_id) != "remove":
            await _emit_current("paused")
        await _emit_job(state="cancelled", step="cancelled")
    except Exception as exc:
        logger.warning("Douyin download failed unique=%s: %s", unique_id, exc)
        await _emit_current("error", error=str(exc))
        await _emit_job(state="failed", failed=1, error=str(exc), step="error")
    finally:
        runtime.forget(unique_id)


async def cancel_download(app: FastAPI, unique_id: str, *, remove: bool = False) -> dict[str, Any] | None:
    db: sqlite3.Connection = app.state.db
    runtime = runtime_from_app(app)
    runtime.cancel(unique_id, remove=remove)
    if remove:
        payload = remove_douyin_download(db, unique_id)
    else:
        payload = update_douyin_file_status(
            db,
            unique_id=unique_id,
            download_status="paused",
        )
    if payload is not None:
        await emit_douyin_file_status(
            {
                "source": "douyin",
                "fileId": payload["id"],
                "uniqueId": payload["uniqueId"],
                "downloadStatus": payload["downloadStatus"],
                "localPath": payload["localPath"],
                "completionDate": payload["completionDate"],
                "downloadedSize": payload["downloadedSize"],
                "transferStatus": payload["transferStatus"],
                "removed": remove,
            }
        )
    return payload


async def cancel_job(app: FastAPI, job_id: str) -> dict[str, Any] | None:
    db: sqlite3.Connection = app.state.db
    job = get_job(db, job_id)
    if job is None:
        return None
    if job["state"] not in {"queued", "running"}:
        return job
    file_unique_id = str(job.get("fileUniqueId") or "")
    if job["kind"] != "file_download" or not file_unique_id:
        return job

    # Cancel the underlying download task; its handler marks the job cancelled
    # and emits the ws event.
    await cancel_download(app, file_unique_id)
    cancelled = get_job(db, job_id)
    if cancelled is not None and cancelled["state"] in {"queued", "running"}:
        cancelled = store_cancel_job(db, job_id)
    if cancelled is not None:
        await emit_douyin_job(cancelled)
    return cancelled


async def retry_job(app: FastAPI, job_id: str) -> dict[str, Any] | None:
    db: sqlite3.Connection = app.state.db
    job = get_job(db, job_id)
    if job is None:
        return None
    kind = str(job.get("kind") or "")
    if kind == "file_download":
        file_unique_id = str(job.get("fileUniqueId") or "")
        if not file_unique_id:
            return job
        start_download_task(app, file_unique_id)
        return active_job_for_file(db, file_unique_id) or get_job(db, job_id) or job
    if kind == "source_refresh":
        source_id = str(job.get("sourceId") or "")
        source = get_douyin_source(db, source_id) if source_id else None
        url = str(job.get("url") or (source or {}).get("url") or "")
        if not url:
            return job
        return await discover_source(
            app,
            url=url,
            preload_only=True,
            source_id=source_id or None,
            backfill=False,
        )
    return job


async def douyin_worker_loop(app: FastAPI) -> None:
    while True:
        try:
            await _run_auto_download(app)
            await _run_auto_transfer(app)
            await _run_auto_refresh(app)
        except Exception as exc:
            logger.exception("Douyin worker loop error: %s", exc)
        await asyncio.sleep(10)


async def _run_auto_refresh(app: FastAPI) -> None:
    db: sqlite3.Connection = app.state.db
    now = now_ms()
    for source in list_douyin_sources(db):
        auto_refresh = source.get("autoRefresh")
        if not isinstance(auto_refresh, dict) or not bool(auto_refresh.get("enabled")):
            continue
        interval_ms = max(1800, int(auto_refresh.get("intervalSeconds") or 1800)) * 1000
        last_completed = int(source.get("lastRefreshCompletedAt") or 0)
        if last_completed and (now - last_completed) < interval_ms:
            continue
        if str(source.get("refreshStatus") or "") == "refreshing":
            continue
        url = str(source.get("url") or "")
        source_id = str(source.get("id") or "")
        if not url:
            continue
        try:
            await discover_source(
                app,
                url=url,
                preload_only=True,
                source_id=source_id,
                backfill=False,
            )
        except Exception as exc:
            logger.warning("Douyin auto-refresh failed source=%s: %s", source_id, exc)


async def _run_auto_download(app: FastAPI) -> None:
    db: sqlite3.Connection = app.state.db
    for source in list_douyin_sources(db):
        auto = source.get("auto")
        if not isinstance(auto, dict):
            continue
        download_cfg = auto.get("download")
        if not isinstance(download_cfg, dict) or not bool(download_cfg.get("enabled")):
            continue
        source_id = str(source.get("id") or "")
        result = db.execute(
            """
            SELECT unique_id
            FROM douyin_file
            WHERE source_id = ?
              AND (
                download_status = 'idle'
                OR (download_status = 'error' AND updated_at < ?)
              )
            ORDER BY date DESC, id DESC
            LIMIT 3
            """,
            (source_id, now_ms() - AUTO_ERROR_RETRY_MS),
        ).fetchall()
        for row in result:
            start_download_task(app, str(row["unique_id"] or ""), session_id=f"douyin:{source_id}")


async def _run_auto_transfer(app: FastAPI) -> None:
    db: sqlite3.Connection = app.state.db
    for source in list_douyin_sources(db):
        auto = source.get("auto")
        if not isinstance(auto, dict):
            continue
        transfer_cfg = auto.get("transfer")
        if not isinstance(transfer_cfg, dict) or not bool(transfer_cfg.get("enabled")):
            continue
        rule = transfer_cfg.get("rule")
        if not isinstance(rule, dict):
            continue
        for candidate in douyin_transfer_candidates(
            db,
            source_id=str(source.get("id") or ""),
            limit=50,
        ):
            unique_id = str(candidate.get("uniqueId") or "")
            row = douyin_file_for_transfer(db, unique_id=unique_id)
            if row is None:
                continue
            in_progress = update_douyin_transfer_status(
                db,
                unique_id=unique_id,
                transfer_status="transferring",
            )
            if in_progress is not None:
                await emit_douyin_file_status(in_progress)
            try:
                transfer_status, resolved_path = await asyncio.to_thread(
                    execute_transfer,
                    row,
                    rule,
                )
            except Exception as exc:
                logger.warning("Douyin transfer failed unique=%s: %s", unique_id, exc)
                transfer_status = "error"
                resolved_path = None
            final = update_douyin_transfer_status(
                db,
                unique_id=unique_id,
                transfer_status=transfer_status,
                local_path=resolved_path,
            )
            if final is not None:
                await emit_douyin_file_status(final)
