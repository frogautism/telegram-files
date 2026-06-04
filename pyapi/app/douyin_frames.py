from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import uuid
from pathlib import Path
from typing import Any

from .douyin_store import douyin_file_row, float_or_default, int_or_default, now_ms

VIDEO_TYPES = {"video"}


def ffmpeg_path() -> str:
    env = str(os.getenv("FFMPEG_BINARY") or "").strip()
    if env and Path(env).exists():
        return env
    found = shutil.which("ffmpeg")
    return found or ""


def ffmpeg_available() -> bool:
    return bool(ffmpeg_path())


def ffprobe_path() -> str:
    env = str(os.getenv("FFPROBE_BINARY") or "").strip()
    if env and Path(env).exists():
        return env
    found = shutil.which("ffprobe")
    return found or ""


def ffprobe_available() -> bool:
    return bool(ffprobe_path())


def frames_dir_for(file_row: sqlite3.Row) -> Path:
    local_path_text = str(file_row["local_path"] or "").strip()
    aweme_id = str(file_row["aweme_id"] or "").strip() or "unknown"
    local_path = Path(local_path_text)
    author_dir = (
        local_path.parent.parent if local_path.parent.name == "video" else local_path.parent
    )
    target = author_dir / "frames" / aweme_id
    target.mkdir(parents=True, exist_ok=True)
    return target


def _probe_dimensions(path: Path) -> tuple[int, int]:
    exe = ffprobe_path()
    if not exe:
        return (0, 0)
    try:
        result = subprocess.run(
            [
                exe,
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height",
                "-of",
                "csv=s=x:p=0",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        text = (result.stdout or "").strip()
        if "x" in text:
            w_str, _, h_str = text.partition("x")
            return (int_or_default(w_str.strip(), 0), int_or_default(h_str.strip(), 0))
    except (OSError, ValueError, subprocess.SubprocessError):
        return (0, 0)
    return (0, 0)


def _run_ffmpeg(args: list[str]) -> None:
    exe = ffmpeg_path()
    if not exe:
        raise RuntimeError("ffmpeg is not available")
    try:
        result = subprocess.run(
            [exe, "-hide_banner", "-loglevel", "error", "-y", *args],
            capture_output=True,
            text=True,
            timeout=600,
        )
    except OSError as exc:
        raise RuntimeError(f"ffmpeg execution failed: {exc}") from exc
    if result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg failed (code {result.returncode}): {(result.stderr or '').strip()}"
        )


def serialize_frame(row: sqlite3.Row) -> dict[str, Any]:
    frame_id = int_or_default(row["id"], 0)
    file_unique_id = str(row["file_unique_id"] or "")
    return {
        "id": frame_id,
        "frameUid": str(row["frame_uid"] or ""),
        "fileUniqueId": file_unique_id,
        "awemeId": str(row["aweme_id"] or ""),
        "frameIndex": int_or_default(row["frame_index"], 0),
        "timestampMs": int_or_default(row["timestamp_ms"], 0),
        "width": int_or_default(row["width"], 0),
        "height": int_or_default(row["height"], 0),
        "size": int_or_default(row["size"], 0),
        "mode": str(row["mode"] or ""),
        "format": str(row["format"] or "jpg"),
        "tags": row["tags"],
        "createdAt": int_or_default(row["created_at"], 0),
        "url": f"/douyin/file/{file_unique_id}/frames/{frame_id}",
    }


def _frame_rows(db: sqlite3.Connection, unique_id: str) -> list[sqlite3.Row]:
    return db.execute(
        "SELECT * FROM douyin_frame WHERE file_unique_id = ? ORDER BY frame_index ASC, id ASC",
        (unique_id.strip(),),
    ).fetchall()


def list_frames(db: sqlite3.Connection, unique_id: str) -> list[dict[str, Any]]:
    return [serialize_frame(row) for row in _frame_rows(db, unique_id)]


def get_frame_row(
    db: sqlite3.Connection, unique_id: str, frame_id: int
) -> sqlite3.Row | None:
    return db.execute(
        "SELECT * FROM douyin_frame WHERE file_unique_id = ? AND id = ? LIMIT 1",
        (unique_id.strip(), frame_id),
    ).fetchone()


def delete_frames(db: sqlite3.Connection, unique_id: str) -> int:
    rows = _frame_rows(db, unique_id)
    for row in rows:
        local_path = str(row["local_path"] or "").strip()
        if local_path:
            try:
                Path(local_path).unlink(missing_ok=True)
            except OSError:
                pass
    db.execute(
        "DELETE FROM douyin_frame WHERE file_unique_id = ?",
        (unique_id.strip(),),
    )
    db.commit()
    return len(rows)


def _create_frame_job(db: sqlite3.Connection, unique_id: str, total: int) -> str:
    row = douyin_file_row(db, unique_id=unique_id)
    source_id = str(row["source_id"] or "") if row is not None else ""
    job_id = uuid.uuid4().hex
    ts = now_ms()
    db.execute(
        """
        INSERT INTO douyin_job(
            id, source_id, file_unique_id, url, kind, state, step,
            total, success, failed, skipped, error,
            created_at, updated_at, started_at, completed_at
        )
        VALUES(?, ?, ?, '', 'frame_extract', 'running', 'extracting', ?, 0, 0, 0, '', ?, ?, ?, NULL)
        """,
        (job_id, source_id, unique_id.strip(), total, ts, ts, ts),
    )
    db.commit()
    return job_id


def _finish_frame_job(
    db: sqlite3.Connection,
    job_id: str,
    *,
    success: int,
    failed: int,
    error: str = "",
    state: str = "completed",
) -> None:
    ts = now_ms()
    db.execute(
        """
        UPDATE douyin_job
        SET state = ?, success = ?, failed = ?, total = ?, error = ?,
            step = ?, updated_at = ?, completed_at = ?
        WHERE id = ?
        """,
        (state, success, failed, success, error, state, ts, ts, job_id),
    )
    db.commit()


def _insert_frame(
    db: sqlite3.Connection,
    *,
    file_row: sqlite3.Row,
    aweme_id: str,
    frame_index: int,
    timestamp_ms: int,
    local_path: Path,
    mode: str,
    fmt: str,
) -> None:
    width, height = _probe_dimensions(local_path)
    try:
        size = local_path.stat().st_size
    except OSError:
        size = 0
    frame_uid = f"douyin:{aweme_id}:frame:{frame_index}"
    db.execute(
        """
        INSERT INTO douyin_frame(
            frame_uid, file_unique_id, aweme_id, source_id, frame_index,
            timestamp_ms, local_path, width, height, size, mode, format, tags, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?)
        ON CONFLICT(frame_uid) DO UPDATE SET
            file_unique_id = excluded.file_unique_id,
            aweme_id = excluded.aweme_id,
            source_id = excluded.source_id,
            frame_index = excluded.frame_index,
            timestamp_ms = excluded.timestamp_ms,
            local_path = excluded.local_path,
            width = excluded.width,
            height = excluded.height,
            size = excluded.size,
            mode = excluded.mode,
            format = excluded.format,
            created_at = excluded.created_at
        """,
        (
            frame_uid,
            str(file_row["unique_id"] or ""),
            aweme_id,
            str(file_row["source_id"] or ""),
            frame_index,
            timestamp_ms,
            str(local_path),
            width,
            height,
            size,
            mode,
            fmt,
            now_ms(),
        ),
    )


def extract_frames(
    db: sqlite3.Connection,
    *,
    unique_id: str,
    mode: str,
    interval: float = 5.0,
    timestamp_ms: int | None = None,
    max_frames: int = 60,
    fmt: str = "jpg",
    replace: bool = True,
) -> dict[str, Any]:
    row = douyin_file_row(db, unique_id=unique_id)
    if row is None:
        raise ValueError("File not found.")
    file_type = str(row["type"] or "").strip()
    if file_type not in VIDEO_TYPES:
        raise ValueError(f"Frame extraction is only supported for video files (got '{file_type}').")
    local_path_text = str(row["local_path"] or "").strip()
    if not local_path_text:
        raise ValueError("Video has no local file to extract frames from.")
    video_path = Path(local_path_text)
    if not (video_path.exists() and video_path.is_file()):
        raise ValueError("Local video file is missing on disk.")

    if not ffmpeg_available():
        raise RuntimeError("ffmpeg is not available; cannot extract frames.")

    if mode not in {"interval", "timestamp", "keyframe"}:
        raise ValueError(f"Unsupported mode '{mode}'.")

    interval = max(0.1, float_or_default(interval, 5.0))
    max_frames = max(1, int_or_default(max_frames, 60))
    fmt = (fmt or "jpg").strip().lower() or "jpg"
    aweme_id = str(row["aweme_id"] or "").strip() or "unknown"

    if replace:
        delete_frames(db, unique_id)

    out_dir = frames_dir_for(row)
    # Clean stale jpgs in target so enumeration is deterministic.
    for stale in out_dir.glob(f"out_*.{fmt}"):
        try:
            stale.unlink()
        except OSError:
            pass

    out_pattern = str(out_dir / f"out_%04d.{fmt}")

    if mode == "interval":
        _run_ffmpeg(
            [
                "-i",
                str(video_path),
                "-vf",
                f"fps=1/{interval:g}",
                "-frames:v",
                str(max_frames),
                "-q:v",
                "3",
                out_pattern,
            ]
        )
    elif mode == "timestamp":
        sec = max(0.0, (timestamp_ms or 0) / 1000.0)
        _run_ffmpeg(
            [
                "-ss",
                str(sec),
                "-i",
                str(video_path),
                "-frames:v",
                "1",
                "-q:v",
                "2",
                str(out_dir / f"out_0001.{fmt}"),
            ]
        )
    else:  # keyframe
        _run_ffmpeg(
            [
                "-i",
                str(video_path),
                "-vf",
                "select='eq(pict_type,I)'",
                "-vsync",
                "vfr",
                "-frames:v",
                str(max_frames),
                "-q:v",
                "3",
                out_pattern,
            ]
        )

    produced = sorted(out_dir.glob(f"out_*.{fmt}"))
    frames: list[dict[str, Any]] = []
    for index, produced_path in enumerate(produced):
        if mode == "interval":
            frame_ts = round(interval * index * 1000)
        elif mode == "timestamp":
            frame_ts = int(timestamp_ms or 0)
        else:
            frame_ts = 0
        _insert_frame(
            db,
            file_row=row,
            aweme_id=aweme_id,
            frame_index=index,
            timestamp_ms=frame_ts,
            local_path=produced_path,
            mode=mode,
            fmt=fmt,
        )
    db.commit()

    frames = list_frames(db, unique_id)
    return {"extracted": len(frames), "frames": frames}
