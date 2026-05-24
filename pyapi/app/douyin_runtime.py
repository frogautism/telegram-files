from __future__ import annotations

import asyncio
import logging
import sqlite3
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI

from .app_state import EVENT_TYPE_FILE_STATUS, _build_ws_payload, _emit_ws_payload
from .config import AppConfig
from .douyin_bridge import discover_awemes, download_aweme, metadata_from_row
from .douyin_store import (
    douyin_file_for_transfer,
    douyin_file_row,
    douyin_transfer_candidates,
    list_douyin_sources,
    mark_douyin_source_status,
    now_ms,
    remove_douyin_download,
    serialize_douyin_file,
    update_douyin_file_status,
    update_douyin_transfer_status,
    upsert_douyin_aweme,
    upsert_douyin_source,
)
from .transfer_ops import execute_transfer

logger = logging.getLogger(__name__)


class DouyinRuntime:
    def __init__(self) -> None:
        self.tasks: dict[str, asyncio.Task] = {}
        self.cancelled: set[str] = set()

    def cancel(self, unique_id: str) -> None:
        self.cancelled.add(unique_id)
        task = self.tasks.get(unique_id)
        if task is not None:
            task.cancel()

    def forget(self, unique_id: str) -> None:
        self.tasks.pop(unique_id, None)
        self.cancelled.discard(unique_id)


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


async def discover_source(
    app: FastAPI,
    *,
    url: str,
    mode: str | None = None,
    preload_only: bool = True,
) -> dict[str, Any]:
    db: sqlite3.Connection = app.state.db
    config: AppConfig = app.state.config
    source = upsert_douyin_source(db, url=url, status="discovering")
    source_id = str(source["id"])
    try:
        resolved_url, parsed, awemes = await discover_awemes(config, db, url, mode=mode)
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
        for aweme in awemes:
            record = upsert_douyin_aweme(db, source_id=source_id, aweme=aweme)
            if record is not None:
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
        if not preload_only:
            for item in awemes:
                record = upsert_douyin_aweme(db, source_id=source_id, aweme=item)
                if record is not None:
                    start_download_task(app, str(record["uniqueId"]))
        refreshed = source
        refreshed["discovered"] = len(awemes)
        return refreshed
    except Exception as exc:
        mark_douyin_source_status(db, source_id, "error", error=str(exc))
        raise


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
    task = asyncio.create_task(_download_file_task(app, unique_id, session_id=session_id))
    runtime.tasks[unique_id] = task
    return payload


async def _download_file_task(app: FastAPI, unique_id: str, *, session_id: str = "") -> None:
    db: sqlite3.Connection = app.state.db
    config: AppConfig = app.state.config
    runtime = runtime_from_app(app)

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

    try:
        row = douyin_file_row(db, unique_id=unique_id)
        if row is None:
            return
        aweme = metadata_from_row(row)
        await _emit_current("downloading")

        def _on_bridge_event(_event: dict[str, Any]) -> None:
            return None

        result = await download_aweme(config, db, aweme, on_event=_on_bridge_event)
        if unique_id in runtime.cancelled:
            await _emit_current("paused")
            return
        local_path = str(result.get("localPath") or "")
        size = int(result.get("size") or 0)
        await _emit_current(
            "completed",
            local_path=local_path,
            downloaded_size=size,
            size=size,
        )
    except asyncio.CancelledError:
        await _emit_current("paused")
    except Exception as exc:
        logger.warning("Douyin download failed unique=%s: %s", unique_id, exc)
        await _emit_current("error", error=str(exc))
    finally:
        runtime.forget(unique_id)


async def cancel_download(app: FastAPI, unique_id: str, *, remove: bool = False) -> dict[str, Any] | None:
    db: sqlite3.Connection = app.state.db
    runtime = runtime_from_app(app)
    runtime.cancel(unique_id)
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


async def douyin_worker_loop(app: FastAPI) -> None:
    while True:
        try:
            await _run_auto_download(app)
            await _run_auto_transfer(app)
        except Exception as exc:
            logger.exception("Douyin worker loop error: %s", exc)
        await asyncio.sleep(10)


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
              AND download_status IN ('idle', 'error')
            ORDER BY date DESC, id DESC
            LIMIT 3
            """,
            (source_id,),
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
