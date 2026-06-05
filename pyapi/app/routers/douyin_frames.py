from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from .. import douyin_frames
from ..deps import get_db
from ..douyin_jobs import create_job, update_job
from ..douyin_store import douyin_file_row

router = APIRouter(prefix="/douyin")

_MEDIA_TYPES = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "webp": "image/webp",
}


class FrameExtractPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["interval", "timestamp", "keyframe"] = "interval"
    interval: float = Field(
        default=douyin_frames.DEFAULT_INTERVAL_SECONDS,
        ge=douyin_frames.MIN_INTERVAL_SECONDS,
    )
    timestamp_ms: int | None = Field(default=None, alias="timestampMs", ge=0)
    max_frames: int = Field(
        default=douyin_frames.DEFAULT_MAX_FRAMES,
        alias="maxFrames",
        ge=1,
        le=douyin_frames.MAX_FRAMES_LIMIT,
    )
    fmt: Literal["jpg", "jpeg", "png", "webp"] = Field(default="jpg", alias="format")


@router.post("/file/{uniqueId}/frames/extract")
async def douyin_extract_frames(
    uniqueId: str,
    payload: FrameExtractPayload | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    payload = payload or FrameExtractPayload()
    total = 1 if payload.mode == "timestamp" else payload.max_frames
    row = douyin_file_row(db, unique_id=uniqueId)
    source_id = str(row["source_id"] or "") if row is not None else ""
    job = create_job(
        db,
        kind="frame_extract",
        source_id=source_id,
        file_unique_id=uniqueId.strip(),
        total=total,
        state="running",
        step="extracting",
    )
    job_id = str(job["id"])

    try:
        result = await asyncio.to_thread(
            douyin_frames.extract_frames,
            db,
            unique_id=uniqueId,
            mode=payload.mode,
            interval=payload.interval,
            timestamp_ms=payload.timestamp_ms,
            max_frames=payload.max_frames,
            fmt=payload.fmt,
        )
    except ValueError as exc:
        update_job(
            db,
            job_id,
            state="failed",
            success=0,
            failed=1,
            error=str(exc),
            step="failed",
        )
        message = str(exc)
        status = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status, detail=message) from exc
    except RuntimeError as exc:
        update_job(
            db,
            job_id,
            state="failed",
            success=0,
            failed=1,
            error=str(exc),
            step="failed",
        )
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - unexpected failure path
        update_job(
            db,
            job_id,
            state="failed",
            success=0,
            failed=1,
            error=str(exc),
            step="failed",
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    extracted = int(result.get("extracted", 0))
    update_job(
        db,
        job_id,
        state="completed",
        total=extracted,
        success=extracted,
        failed=0,
        step="done",
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
