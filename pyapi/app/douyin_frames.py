from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

from .douyin_store import douyin_file_row, float_or_default, int_or_default, now_ms

VIDEO_TYPES = {"video"}
FRAME_MODES = {"interval", "timestamp", "keyframe"}
FRAME_FORMATS = {"jpg", "jpeg", "png", "webp"}
DEFAULT_INTERVAL_SECONDS = 5.0
MIN_INTERVAL_SECONDS = 0.1
DEFAULT_MAX_FRAMES = 60
MAX_FRAMES_LIMIT = 120

_STRIPED_FRAME_LOCKS = tuple(Lock() for _ in range(32))


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


def _lock_for_file(unique_id: str) -> Lock:
    normalized = unique_id.strip()
    bucket = sum(normalized.encode("utf-8")) % len(_STRIPED_FRAME_LOCKS)
    return _STRIPED_FRAME_LOCKS[bucket]


def normalize_mode(value: Any) -> str:
    mode = str(value or "interval").strip().lower()
    if mode not in FRAME_MODES:
        raise ValueError(f"Unsupported mode '{mode}'.")
    return mode


def normalize_format(value: Any) -> str:
    fmt = str(value or "jpg").strip().lower() or "jpg"
    if fmt not in FRAME_FORMATS:
        raise ValueError(f"Unsupported frame format '{fmt}'.")
    return fmt


def normalize_interval(value: Any) -> float:
    interval = float_or_default(value, DEFAULT_INTERVAL_SECONDS)
    if interval < MIN_INTERVAL_SECONDS:
        raise ValueError(f"Frame interval must be at least {MIN_INTERVAL_SECONDS:g}s.")
    return interval


def normalize_timestamp_ms(value: Any) -> int:
    return max(0, int_or_default(value, 0))


def normalize_max_frames(value: Any) -> int:
    max_frames = int_or_default(value, DEFAULT_MAX_FRAMES)
    if max_frames < 1:
        raise ValueError("Max frames must be at least 1.")
    if max_frames > MAX_FRAMES_LIMIT:
        raise ValueError(f"Max frames cannot exceed {MAX_FRAMES_LIMIT}.")
    return max_frames


@dataclass(frozen=True)
class FrameExtractOptions:
    mode: str = "interval"
    interval: float = DEFAULT_INTERVAL_SECONDS
    timestamp_ms: int = 0
    max_frames: int = DEFAULT_MAX_FRAMES
    fmt: str = "jpg"

    @classmethod
    def from_values(
        cls,
        *,
        mode: Any = "interval",
        interval: Any = DEFAULT_INTERVAL_SECONDS,
        timestamp_ms: Any = None,
        max_frames: Any = DEFAULT_MAX_FRAMES,
        fmt: Any = "jpg",
    ) -> "FrameExtractOptions":
        return cls(
            mode=normalize_mode(mode),
            interval=normalize_interval(interval),
            timestamp_ms=normalize_timestamp_ms(timestamp_ms),
            max_frames=normalize_max_frames(max_frames),
            fmt=normalize_format(fmt),
        )


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


def _run_ffmpeg(args: list[str], *, loglevel: str = "error") -> str:
    exe = ffmpeg_path()
    if not exe:
        raise RuntimeError("ffmpeg is not available")
    try:
        result = subprocess.run(
            [exe, "-hide_banner", "-loglevel", loglevel, "-y", *args],
            capture_output=True,
            text=True,
            timeout=600,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"ffmpeg execution failed: {exc}") from exc
    if result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg failed (code {result.returncode}): {(result.stderr or '').strip()}"
        )
    return result.stderr or ""


def _showinfo_timestamps(stderr: str) -> list[int]:
    timestamps: list[int] = []
    for line in stderr.splitlines():
        marker = "pts_time:"
        if marker not in line:
            continue
        text = line.split(marker, 1)[1].split()[0]
        try:
            timestamps.append(max(0, round(float(text) * 1000)))
        except ValueError:
            continue
    return timestamps


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


def _insert_frame(
    db: sqlite3.Connection,
    *,
    file_row: sqlite3.Row,
    aweme_id: str,
    frame_index: int,
    timestamp_ms: int,
    local_path: Path,
    dimensions: tuple[int, int],
    mode: str,
    fmt: str,
) -> None:
    width, height = dimensions
    try:
        size = local_path.stat().st_size
    except OSError:
        size = 0
    file_unique_id = str(file_row["unique_id"] or "")
    frame_uid = f"{file_unique_id}:frame:{frame_index}"
    db.execute(
        """
        INSERT INTO douyin_frame(
            frame_uid, file_unique_id, aweme_id, source_id, frame_index,
            timestamp_ms, local_path, width, height, size, mode, format, tags, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?)
        """,
        (
            frame_uid,
            file_unique_id,
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


def _stale_frame_paths(rows: list[sqlite3.Row]) -> list[Path]:
    paths: list[Path] = []
    for row in rows:
        local_path = str(row["local_path"] or "").strip()
        if local_path:
            paths.append(Path(local_path))
    return paths


def _cleanup_paths(paths: list[Path]) -> None:
    for path in paths:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def _stale_output_paths(out_dir: Path, final_paths: set[Path]) -> list[Path]:
    paths: list[Path] = []
    for stale_format in FRAME_FORMATS:
        for path in out_dir.glob(f"out_*.{stale_format}"):
            if path not in final_paths:
                paths.append(path)
    return paths


def _replace_frame_files(
    *,
    produced: list[Path],
    out_dir: Path,
    fmt: str,
) -> list[Path]:
    final_paths = [out_dir / f"out_{index + 1:04d}.{fmt}" for index in range(len(produced))]
    for source, target in zip(produced, final_paths, strict=True):
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(source, target)
    return final_paths


def extract_frames(
    db: sqlite3.Connection,
    *,
    unique_id: str,
    mode: str,
    interval: float = DEFAULT_INTERVAL_SECONDS,
    timestamp_ms: int | None = None,
    max_frames: int = DEFAULT_MAX_FRAMES,
    fmt: str = "jpg",
) -> dict[str, Any]:
    options = FrameExtractOptions.from_values(
        mode=mode,
        interval=interval,
        timestamp_ms=timestamp_ms,
        max_frames=max_frames,
        fmt=fmt,
    )

    if not ffmpeg_available():
        raise RuntimeError("ffmpeg is not available; cannot extract frames.")

    with _lock_for_file(unique_id):
        row = douyin_file_row(db, unique_id=unique_id)
        if row is None:
            raise ValueError("File not found.")
        file_type = str(row["type"] or "").strip()
        if file_type not in VIDEO_TYPES:
            raise ValueError(
                f"Frame extraction is only supported for video files (got '{file_type}')."
            )
        local_path_text = str(row["local_path"] or "").strip()
        if not local_path_text:
            raise ValueError("Video has no local file to extract frames from.")
        video_path = Path(local_path_text)
        if not (video_path.exists() and video_path.is_file()):
            raise ValueError("Local video file is missing on disk.")

        aweme_id = str(row["aweme_id"] or "").strip() or "unknown"
        dimensions = _probe_dimensions(video_path)
        out_dir = frames_dir_for(row)
        with tempfile.TemporaryDirectory(prefix=".extract-", dir=out_dir) as tmp:
            tmp_dir = Path(tmp)
            out_pattern = str(tmp_dir / f"out_%04d.{options.fmt}")

            if options.mode == "interval":
                stderr = _run_ffmpeg(
                    [
                        "-i",
                        str(video_path),
                        "-vf",
                        f"fps=1/{options.interval:g},showinfo",
                        "-frames:v",
                        str(options.max_frames),
                        "-q:v",
                        "3",
                        out_pattern,
                    ],
                    loglevel="info",
                )
            elif options.mode == "timestamp":
                sec = options.timestamp_ms / 1000.0
                stderr = _run_ffmpeg(
                    [
                        "-i",
                        str(video_path),
                        "-ss",
                        str(sec),
                        "-vf",
                        "showinfo",
                        "-frames:v",
                        "1",
                        "-q:v",
                        "2",
                        str(tmp_dir / f"out_0001.{options.fmt}"),
                    ],
                    loglevel="info",
                )
            else:
                stderr = _run_ffmpeg(
                    [
                        "-i",
                        str(video_path),
                        "-vf",
                        "select='eq(pict_type,I)',showinfo",
                        "-fps_mode",
                        "vfr",
                        "-frames:v",
                        str(options.max_frames),
                        "-q:v",
                        "3",
                        out_pattern,
                    ],
                    loglevel="info",
                )

            produced = sorted(tmp_dir.glob(f"out_*.{options.fmt}"))
            if not produced:
                raise RuntimeError("ffmpeg did not produce any frames")

            timestamps = (
                [options.timestamp_ms]
                if options.mode == "timestamp"
                else _showinfo_timestamps(stderr)
            )

            old_rows = _frame_rows(db, unique_id)
            final_paths = _replace_frame_files(
                produced=produced,
                out_dir=out_dir,
                fmt=options.fmt,
            )

            try:
                db.execute(
                    "DELETE FROM douyin_frame WHERE file_unique_id = ?",
                    (unique_id.strip(),),
                )
                for index, produced_path in enumerate(final_paths):
                    frame_ts = timestamps[index] if index < len(timestamps) else 0
                    _insert_frame(
                        db,
                        file_row=row,
                        aweme_id=aweme_id,
                        frame_index=index,
                        timestamp_ms=frame_ts,
                        local_path=produced_path,
                        dimensions=dimensions,
                        mode=options.mode,
                        fmt=options.fmt,
                    )
                db.commit()
            except sqlite3.Error:
                db.rollback()
                raise

        final_path_set = set(final_paths)
        _cleanup_paths(
            [
                path
                for path in [
                    *_stale_frame_paths(old_rows),
                    *_stale_output_paths(out_dir, final_path_set),
                ]
                if path not in final_path_set
            ]
        )

    frames = list_frames(db, unique_id)
    return {"extracted": len(frames), "frames": frames}
