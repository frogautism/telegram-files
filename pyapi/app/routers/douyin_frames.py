from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from .. import douyin_frames
from ..db import create_connection
from ..deps import get_db

router = APIRouter(prefix="/douyin")

_MEDIA_TYPES = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "webp": "image/webp",
}


class FrameExtractPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    mode: str = "interval"
    interval: float = douyin_frames.DEFAULT_INTERVAL_SECONDS
    timestamp_ms: int | None = Field(default=None, alias="timestampMs")
    max_frames: int = Field(default=douyin_frames.DEFAULT_MAX_FRAMES, alias="maxFrames")
    fmt: str = Field(default="jpg", alias="format")

    def to_options(self) -> douyin_frames.FrameExtractOptions:
        return douyin_frames.FrameExtractOptions.from_values(
            mode=self.mode,
            interval=self.interval,
            timestamp_ms=self.timestamp_ms,
            max_frames=self.max_frames,
            fmt=self.fmt,
        )


@router.post("/file/{uniqueId}/frames/extract")
async def douyin_extract_frames(
    uniqueId: str,
    request: Request,
    payload: FrameExtractPayload | None = None,
) -> dict[str, Any]:
    payload = payload or FrameExtractPayload()
    config = request.app.state.config
    try:
        options = payload.to_options()

        def _run() -> dict[str, Any]:
            conn = create_connection(config)
            try:
                return douyin_frames.extract_frames(
                    conn,
                    unique_id=uniqueId,
                    mode=options.mode,
                    interval=options.interval,
                    timestamp_ms=options.timestamp_ms,
                    max_frames=options.max_frames,
                    fmt=options.fmt,
                )
            finally:
                conn.close()

        result = await asyncio.to_thread(_run)
    except ValueError as exc:
        message = str(exc)
        status = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status, detail=message) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - unexpected failure path
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    extracted = int(result.get("extracted", 0))
    return {"extracted": extracted, "frames": result.get("frames", [])}


@router.get("/file/{uniqueId}/frames")
async def douyin_list_frames(
    uniqueId: str,
    db: sqlite3.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return douyin_frames.list_frames(db, uniqueId)


@router.get("/file/{uniqueId}/frames/{frameId}")
async def douyin_get_frame(
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
async def douyin_delete_frames(
    uniqueId: str,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, int]:
    deleted = douyin_frames.delete_frames(db, uniqueId)
    return {"deleted": deleted}
