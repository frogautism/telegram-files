from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import FileResponse, RedirectResponse

from ..deps import get_db
from ..douyin_bridge import DouyinBridgeUnavailable
from ..douyin_runtime import cancel_download, discover_source, start_download_task
from ..douyin_store import (
    douyin_file_row,
    get_douyin_source,
    list_douyin_files,
    list_douyin_sources,
    update_douyin_file_tags,
    update_douyin_files_tags,
    update_douyin_source_auto_settings,
)
from ..route_utils import _get_filters, _int_or_default, _parse_batch_files

router = APIRouter(prefix="/douyin")


def _source_error(exc: Exception) -> HTTPException:
    if isinstance(exc, DouyinBridgeUnavailable):
        return HTTPException(status_code=503, detail=str(exc))
    return HTTPException(status_code=400, detail=str(exc))


def _payload_unique(payload: dict[str, Any]) -> str:
    unique_id = str(payload.get("uniqueId") or "").strip()
    if unique_id:
        return unique_id
    file_id = _int_or_default(payload.get("fileId"), 0)
    return str((payload.get("file") or {}).get("uniqueId") or "").strip() if file_id == 0 else ""


@router.get("/sources")
def douyin_sources(db: sqlite3.Connection = Depends(get_db)) -> list[dict[str, Any]]:
    return list_douyin_sources(db)


@router.post("/sources")
async def douyin_source_create(
    payload: dict[str, Any],
    request: Request,
) -> dict[str, Any]:
    url = str(payload.get("url") or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="url is required.")
    mode = str(payload.get("mode") or "").strip() or None
    preload_only = bool(payload.get("preloadOnly", True))
    try:
        return await discover_source(
            request.app,
            url=url,
            mode=mode,
            preload_only=preload_only,
        )
    except Exception as exc:
        raise _source_error(exc) from exc


@router.get("/sources/{sourceId}/files")
def douyin_source_files(
    sourceId: str,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    if get_douyin_source(db, sourceId) is None:
        raise HTTPException(status_code=404, detail="Douyin source not found.")
    return list_douyin_files(db, source_id=sourceId, filters=_get_filters(request))


@router.get("/files")
def douyin_files(
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    return list_douyin_files(db, filters=_get_filters(request))


@router.get("/file/{uniqueId}")
async def douyin_file_preview(
    uniqueId: str,
    db: sqlite3.Connection = Depends(get_db),
):
    row = douyin_file_row(db, unique_id=uniqueId)
    if row is None:
        raise HTTPException(status_code=404, detail="File not found")
    local_path = str(row["local_path"] or "").strip()
    if local_path:
        path = Path(local_path)
        if path.exists() and path.is_file():
            return FileResponse(
                path=str(path),
                media_type=str(row["mime_type"] or "application/octet-stream"),
            )
    thumbnail_url = str(row["thumbnail_url"] or "").strip()
    if thumbnail_url:
        return RedirectResponse(thumbnail_url)
    raise HTTPException(status_code=404, detail="File not found")


@router.post("/file/start-download")
async def douyin_file_start_download(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    unique_id = _payload_unique(payload)
    file_id = _int_or_default(payload.get("fileId"), 0)
    if not unique_id and file_id > 0:
        row = douyin_file_row(db, file_id=file_id)
        unique_id = str(row["unique_id"] or "") if row is not None else ""
    if not unique_id:
        raise HTTPException(status_code=400, detail="uniqueId or fileId is required.")
    result = start_download_task(
        request.app,
        unique_id,
        session_id=getattr(request.state, "session_id", ""),
    )
    if result is None:
        raise HTTPException(status_code=404, detail="File not found.")
    return result


@router.post("/file/cancel-download")
async def douyin_file_cancel_download(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    unique_id = _payload_unique(payload)
    file_id = _int_or_default(payload.get("fileId"), 0)
    if not unique_id and file_id > 0:
        row = douyin_file_row(db, file_id=file_id)
        unique_id = str(row["unique_id"] or "") if row is not None else ""
    if not unique_id:
        raise HTTPException(status_code=400, detail="uniqueId or fileId is required.")
    result = await cancel_download(request.app, unique_id)
    if result is None:
        raise HTTPException(status_code=404, detail="File not found.")
    return Response(status_code=200)


@router.post("/file/toggle-pause-download")
async def douyin_file_toggle_pause_download(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    unique_id = _payload_unique(payload)
    file_id = _int_or_default(payload.get("fileId"), 0)
    if not unique_id and file_id > 0:
        row = douyin_file_row(db, file_id=file_id)
        unique_id = str(row["unique_id"] or "") if row is not None else ""
    if not unique_id:
        raise HTTPException(status_code=400, detail="uniqueId or fileId is required.")
    row = douyin_file_row(db, unique_id=unique_id)
    if row is None:
        raise HTTPException(status_code=404, detail="File not found.")
    status = str(row["download_status"] or "")
    if status == "downloading":
        await cancel_download(request.app, unique_id)
    else:
        start_download_task(request.app, unique_id, session_id=getattr(request.state, "session_id", ""))
    return Response(status_code=200)


@router.post("/file/remove")
async def douyin_file_remove(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    unique_id = _payload_unique(payload)
    file_id = _int_or_default(payload.get("fileId"), 0)
    if not unique_id and file_id > 0:
        row = douyin_file_row(db, file_id=file_id)
        unique_id = str(row["unique_id"] or "") if row is not None else ""
    if not unique_id:
        raise HTTPException(status_code=400, detail="uniqueId or fileId is required.")
    await cancel_download(request.app, unique_id, remove=True)
    return Response(status_code=200)


@router.post("/files/start-download-multiple")
async def douyin_files_start_download_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, int]:
    processed = 0
    failed = 0
    for item in _parse_batch_files(payload):
        unique_id = item["uniqueId"]
        if not unique_id and item["fileId"] > 0:
            row = douyin_file_row(db, file_id=item["fileId"])
            unique_id = str(row["unique_id"] or "") if row is not None else ""
        if not unique_id or start_download_task(request.app, unique_id) is None:
            failed += 1
            continue
        processed += 1
    return {"processed": processed, "failed": failed}


@router.post("/files/cancel-download-multiple")
async def douyin_files_cancel_download_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, int]:
    processed = 0
    failed = 0
    for item in _parse_batch_files(payload):
        unique_id = item["uniqueId"]
        if not unique_id and item["fileId"] > 0:
            row = douyin_file_row(db, file_id=item["fileId"])
            unique_id = str(row["unique_id"] or "") if row is not None else ""
        if not unique_id:
            failed += 1
            continue
        if await cancel_download(request.app, unique_id) is None:
            failed += 1
            continue
        processed += 1
    return {"processed": processed, "failed": failed}


@router.post("/files/toggle-pause-download-multiple")
async def douyin_files_toggle_pause_download_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, int]:
    processed = 0
    failed = 0
    is_paused = bool(payload.get("isPaused", True))
    for item in _parse_batch_files(payload):
        unique_id = item["uniqueId"]
        if not unique_id and item["fileId"] > 0:
            row = douyin_file_row(db, file_id=item["fileId"])
            unique_id = str(row["unique_id"] or "") if row is not None else ""
        if not unique_id:
            failed += 1
            continue
        if is_paused:
            if await cancel_download(request.app, unique_id) is None:
                failed += 1
                continue
        elif start_download_task(request.app, unique_id) is None:
            failed += 1
            continue
        processed += 1
    return {"processed": processed, "failed": failed}


@router.post("/files/remove-multiple")
async def douyin_files_remove_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, int]:
    processed = 0
    failed = 0
    for item in _parse_batch_files(payload):
        unique_id = item["uniqueId"]
        if not unique_id and item["fileId"] > 0:
            row = douyin_file_row(db, file_id=item["fileId"])
            unique_id = str(row["unique_id"] or "") if row is not None else ""
        if not unique_id:
            failed += 1
            continue
        await cancel_download(request.app, unique_id, remove=True)
        processed += 1
    return {"processed": processed, "failed": failed}


@router.post("/files/update-tags")
def douyin_files_update_tags(
    payload: dict[str, Any],
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    files = payload.get("files")
    if not isinstance(files, list):
        raise HTTPException(status_code=400, detail="'files' must be an array.")
    unique_ids = [
        str(item.get("uniqueId") or "").strip()
        for item in files
        if isinstance(item, dict)
    ]
    update_douyin_files_tags(db, unique_ids, "" if payload.get("tags") is None else str(payload.get("tags")))
    return Response(status_code=200)


@router.post("/file/{uniqueId}/update-tags")
def douyin_file_update_tags(
    uniqueId: str,
    payload: dict[str, Any],
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    update_douyin_file_tags(db, uniqueId, "" if payload.get("tags") is None else str(payload.get("tags")))
    return Response(status_code=200)


@router.post("/sources/{sourceId}/update-auto-settings")
def douyin_source_update_auto_settings(
    sourceId: str,
    payload: dict[str, Any] | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    updated = update_douyin_source_auto_settings(
        db,
        source_id=sourceId,
        auto_payload=payload if isinstance(payload, dict) else {},
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Douyin source not found.")
    return Response(status_code=200)
