from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from .. import douyin_frames
from ..deps import get_db
from ..douyin_store import float_or_default, int_or_default

router = APIRouter(prefix="/douyin")

_MEDIA_TYPES = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "webp": "image/webp",
}


@router.post("/file/{uniqueId}/frames/extract")
async def douyin_extract_frames(
    uniqueId: str,
    payload: dict[str, Any] | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    payload = payload or {}
    mode = str(payload.get("mode") or "interval").strip()
    interval = float_or_default(payload.get("interval"), 5.0)
    timestamp_ms = (
        int_or_default(payload.get("timestampMs"), 0)
        if payload.get("timestampMs") is not None
        else None
    )
    max_frames = int_or_default(payload.get("maxFrames"), 60)
    fmt = str(payload.get("format") or "jpg").strip() or "jpg"
    replace = bool(payload.get("replace", True))

    total = 1 if mode == "timestamp" else max(1, max_frames)
    job_id = douyin_frames._create_frame_job(db, uniqueId, total)

    try:
        result = await asyncio.to_thread(
            douyin_frames.extract_frames,
            db,
            unique_id=uniqueId,
            mode=mode,
            interval=interval,
            timestamp_ms=timestamp_ms,
            max_frames=max_frames,
            fmt=fmt,
            replace=replace,
        )
    except ValueError as exc:
        douyin_frames._finish_frame_job(
            db, job_id, success=0, failed=1, error=str(exc), state="failed"
        )
        message = str(exc)
        status = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status, detail=message) from exc
    except RuntimeError as exc:
        douyin_frames._finish_frame_job(
            db, job_id, success=0, failed=1, error=str(exc), state="failed"
        )
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - unexpected failure path
        douyin_frames._finish_frame_job(
            db, job_id, success=0, failed=1, error=str(exc), state="failed"
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    extracted = int(result.get("extracted", 0))
    douyin_frames._finish_frame_job(
        db, job_id, success=extracted, failed=0, state="completed"
    )
    return {"jobId": job_id, "extracted": extracted, "frames": result.get("frames", [])}


@router.get("/file/{uniqueId}/frames")
def douyin_list_frames(
    uniqueId: str,
    db: sqlite3.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return douyin_frames.list_frames(db, uniqueId)


@router.get("/file/{uniqueId}/frames/{frameId}")
def douyin_get_frame(
    uniqueId: str,
    frameId: int,
    db: sqlite3.Connection = Depends(get_db),
):
    row = douyin_frames.get_frame_row(db, uniqueId, frameId)
    if row is None:
        raise HTTPException(status_code=404, detail="Frame not found")
    local_path = str(row["local_path"] or "").strip()
    if not local_path:
        raise HTTPException(status_code=404, detail="Frame not found")
    path = Path(local_path)
    if not (path.exists() and path.is_file()):
        raise HTTPException(status_code=404, detail="Frame not found")
    fmt = str(row["format"] or "jpg").strip().lower()
    media_type = _MEDIA_TYPES.get(fmt, "image/jpeg")
    return FileResponse(path=str(path), media_type=media_type)


@router.delete("/file/{uniqueId}/frames")
def douyin_delete_frames(
    uniqueId: str,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, int]:
    deleted = douyin_frames.delete_frames(db, uniqueId)
    return {"deleted": deleted}
