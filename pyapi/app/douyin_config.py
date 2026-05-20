from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .config import AppConfig
from .db import get_settings_by_keys


DOUYIN_SETTING_KEYS = [
    "douyinCookies",
    "douyinProxy",
    "douyinPath",
    "douyinThread",
    "douyinRetryTimes",
    "douyinRateLimit",
    "douyinPreloadLimit",
    "douyinCover",
    "douyinMusic",
    "douyinAvatar",
    "douyinJson",
    "douyinComments",
    "douyinBrowserFallback",
    "douyinBrowserHeadless",
]


def _bool(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _int(value: Any, default: int) -> int:
    try:
        if value is None or value == "":
            return default
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def _cookie_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return text
    if isinstance(parsed, dict):
        return "; ".join(f"{key}={val}" for key, val in parsed.items() if key)
    return text


def douyin_runtime_path(config: AppConfig) -> str:
    configured = str(config.douyin_path or "").strip()
    if configured:
        return configured
    return str(config.app_root / "douyin")


def douyin_downloader_path(config: AppConfig) -> str:
    configured = str(config.douyin_downloader_path or "").strip()
    if configured:
        return configured

    candidates = [
        Path("D:/dev/douyin-downloader"),
        Path("/app/douyin-downloader"),
        Path.cwd().parent / "douyin-downloader",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return ""


def build_douyin_config(config: AppConfig, db) -> dict[str, Any]:
    settings = get_settings_by_keys(db, DOUYIN_SETTING_KEYS)
    output_path = str(settings.get("douyinPath") or "").strip() or douyin_runtime_path(
        config
    )
    Path(output_path).mkdir(parents=True, exist_ok=True)

    comments_enabled = _bool(settings.get("douyinComments"), False)
    return {
        "path": output_path,
        "cookie": _cookie_text(settings.get("douyinCookies")),
        "proxy": str(settings.get("douyinProxy") or os.getenv("DOUYIN_PROXY") or ""),
        "thread": max(1, _int(settings.get("douyinThread"), 3)),
        "retry_times": max(0, _int(settings.get("douyinRetryTimes"), 3)),
        "rate_limit": max(0.1, _float(settings.get("douyinRateLimit"), 2.0)),
        "preload_limit": max(1, _int(settings.get("douyinPreloadLimit"), 50)),
        "cover": _bool(settings.get("douyinCover"), True),
        "music": _bool(settings.get("douyinMusic"), True),
        "avatar": _bool(settings.get("douyinAvatar"), False),
        "json": _bool(settings.get("douyinJson"), True),
        "database": False,
        "folderstyle": True,
        "download_pinned": False,
        "comments": {
            "enabled": comments_enabled,
            "include_replies": False,
            "max_comments": 0,
            "page_size": 20,
        },
        "browser_fallback": {
            "enabled": _bool(settings.get("douyinBrowserFallback"), True),
            "headless": _bool(settings.get("douyinBrowserHeadless"), True),
            "max_scrolls": 120,
            "idle_rounds": 6,
            "wait_timeout_seconds": 300,
        },
        "number": {
            "post": max(1, _int(settings.get("douyinPreloadLimit"), 50)),
            "like": max(1, _int(settings.get("douyinPreloadLimit"), 50)),
            "mix": max(1, _int(settings.get("douyinPreloadLimit"), 50)),
            "music": max(1, _int(settings.get("douyinPreloadLimit"), 50)),
            "collect": max(1, _int(settings.get("douyinPreloadLimit"), 50)),
            "collectmix": max(1, _int(settings.get("douyinPreloadLimit"), 50)),
        },
        "increase": {
            "post": False,
            "like": False,
            "mix": False,
            "music": False,
        },
        "mode": ["post"],
    }
