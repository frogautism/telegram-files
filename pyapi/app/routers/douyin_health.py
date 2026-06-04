from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Request

from .. import douyin_frames
from ..deps import get_db

router = APIRouter(prefix="/douyin")


def _downloader_status(config: Any) -> tuple[bool, str, str]:
    version = ""
    try:
        from ..douyin_config import vendored_downloader_version

        version = vendored_downloader_version()
    except Exception as exc:  # pragma: no cover - import dependent
        return (False, str(exc), version)
    try:
        from ..douyin_bridge import _imports

        _imports(config)
    except Exception as exc:
        return (False, str(exc), version)
    return (True, "", version)


def _output_writable(output_path: str) -> bool:
    if not output_path:
        return False
    base = Path(output_path)
    try:
        base.mkdir(parents=True, exist_ok=True)
        probe = base / f".douyin_health_{uuid.uuid4().hex}.tmp"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def _browser_fallback_available(config_dict: dict[str, Any]) -> bool:
    fallback = config_dict.get("browser_fallback")
    enabled = bool(fallback.get("enabled")) if isinstance(fallback, dict) else False
    if not enabled:
        return False
    try:
        import playwright  # noqa: F401
    except Exception:
        return False
    return True


@router.get("/health")
def douyin_health(
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    config = request.app.state.config

    downloader_available, downloader_error, version = _downloader_status(config)

    cookie = ""
    proxy = ""
    output_path = ""
    config_dict: dict[str, Any] = {}
    try:
        from ..douyin_config import build_douyin_config

        config_dict = build_douyin_config(config, db)
        cookie = str(config_dict.get("cookie") or "").strip()
        proxy = str(config_dict.get("proxy") or "").strip()
        output_path = str(config_dict.get("path") or "").strip()
    except Exception as exc:  # pragma: no cover - config dependent
        if not downloader_error:
            downloader_error = str(exc)

    ffmpeg_exe = douyin_frames.ffmpeg_path()

    return {
        "downloaderAvailable": downloader_available,
        "downloaderError": downloader_error,
        "version": version,
        "cookieValid": bool(cookie),
        "proxy": proxy,
        "outputPath": output_path,
        "outputWritable": _output_writable(output_path),
        "browserFallbackAvailable": _browser_fallback_available(config_dict),
        "ffmpegAvailable": bool(ffmpeg_exe),
        "ffmpegPath": ffmpeg_exe,
    }
