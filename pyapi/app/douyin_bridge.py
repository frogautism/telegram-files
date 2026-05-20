from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from .config import AppConfig
from .douyin_config import build_douyin_config, douyin_downloader_path


class DouyinBridgeUnavailable(RuntimeError):
    pass


def _ensure_import_path(app_config: AppConfig) -> None:
    package_path = douyin_downloader_path(app_config)
    if not package_path:
        raise DouyinBridgeUnavailable(
            "Douyin downloader package was not found. Set DOUYIN_DOWNLOADER_PATH."
        )
    root = Path(package_path).resolve()
    if not root.exists():
        raise DouyinBridgeUnavailable(f"Douyin downloader path does not exist: {root}")
    root_text = str(root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)


def _imports(app_config: AppConfig) -> dict[str, Any]:
    _ensure_import_path(app_config)
    try:
        from auth import CookieManager
        from config import ConfigLoader
        from control import QueueManager, RateLimiter, RetryHandler
        from core import DouyinAPIClient, DownloaderFactory, URLParser
        from storage import FileManager
        from utils.validators import is_short_url, normalize_short_url
    except Exception as exc:  # pragma: no cover - import path/env dependent
        raise DouyinBridgeUnavailable(str(exc)) from exc
    return {
        "CookieManager": CookieManager,
        "ConfigLoader": ConfigLoader,
        "QueueManager": QueueManager,
        "RateLimiter": RateLimiter,
        "RetryHandler": RetryHandler,
        "DouyinAPIClient": DouyinAPIClient,
        "DownloaderFactory": DownloaderFactory,
        "URLParser": URLParser,
        "FileManager": FileManager,
        "is_short_url": is_short_url,
        "normalize_short_url": normalize_short_url,
    }


def _cookie_dict(config_loader: Any) -> dict[str, str]:
    try:
        cookies = config_loader.get_cookies()
    except Exception:
        cookies = {}
    return cookies if isinstance(cookies, dict) else {}


def _make_config(app_config: AppConfig, db) -> dict[str, Any]:
    return build_douyin_config(app_config, db)


def _make_config_loader(imports: dict[str, Any], config_dict: dict[str, Any]) -> Any:
    loader = imports["ConfigLoader"](None)
    loader.update(**config_dict)
    return loader


async def resolve_and_parse_url(
    app_config: AppConfig,
    db,
    url: str,
) -> tuple[str, dict[str, Any]]:
    imports = _imports(app_config)
    cfg = _make_config_loader(imports, _make_config(app_config, db))
    proxy = str(cfg.get("proxy") or "").strip()
    async with imports["DouyinAPIClient"](_cookie_dict(cfg), proxy=proxy) as api_client:
        resolved_url = url.strip()
        if imports["is_short_url"](resolved_url):
            final_url = await api_client.resolve_short_url(
                imports["normalize_short_url"](resolved_url)
            )
            if final_url:
                resolved_url = final_url
        parsed = imports["URLParser"].parse(resolved_url)
        if not parsed:
            raise RuntimeError(f"Unsupported Douyin URL: {url}")
        return resolved_url, parsed


async def discover_awemes(
    app_config: AppConfig,
    db,
    url: str,
    *,
    mode: str | None = None,
) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    imports = _imports(app_config)
    config_dict = _make_config(app_config, db)
    cfg = _make_config_loader(imports, config_dict)
    proxy = str(cfg.get("proxy") or "").strip()
    limit = int(config_dict.get("preload_limit") or 50)
    resolved_url = url.strip()
    async with imports["DouyinAPIClient"](_cookie_dict(cfg), proxy=proxy) as api_client:
        if imports["is_short_url"](resolved_url):
            final_url = await api_client.resolve_short_url(
                imports["normalize_short_url"](resolved_url)
            )
            if final_url:
                resolved_url = final_url

        parsed = imports["URLParser"].parse(resolved_url)
        if not parsed:
            raise RuntimeError(f"Unsupported Douyin URL: {url}")

        url_type = str(parsed.get("type") or "")
        awemes: list[dict[str, Any]] = []
        if url_type in {"video", "gallery"}:
            aweme_id = str(parsed.get("aweme_id") or parsed.get("note_id") or "")
            detail = await api_client.get_video_detail(aweme_id)
            if detail:
                awemes.append(detail)
        elif url_type == "user":
            sec_uid = str(parsed.get("sec_uid") or "")
            selected_mode = (mode or "post").strip().lower()
            if selected_mode == "like":
                page = await api_client.get_user_like(sec_uid, count=limit)
            elif selected_mode == "music":
                page = await api_client.get_user_music(sec_uid, count=limit)
            else:
                page = await api_client.get_user_post(sec_uid, count=limit)
            awemes.extend(
                item for item in page.get("items", [])[:limit] if isinstance(item, dict)
            )
        elif url_type == "collection":
            mix_id = str(parsed.get("mix_id") or "")
            page = await api_client.get_mix_aweme(mix_id, count=limit)
            awemes.extend(
                item for item in page.get("items", [])[:limit] if isinstance(item, dict)
            )
        elif url_type == "music":
            music_id = str(parsed.get("music_id") or "")
            page = await api_client.get_music_aweme(music_id, count=limit)
            awemes.extend(
                item for item in page.get("items", [])[:limit] if isinstance(item, dict)
            )
        elif url_type == "live":
            # Live media is record-on-download only. Store a placeholder record via parsed metadata.
            awemes.append(
                {
                    "aweme_id": str(parsed.get("room_id") or ""),
                    "desc": f"Douyin live {parsed.get('room_id') or ''}",
                    "create_time": 0,
                    "aweme_type": "live",
                    "author": {},
                    "_douyin_live": True,
                }
            )

        return resolved_url, parsed, awemes


def _progress_detail(value: Any) -> str:
    text = str(value or "").strip()
    return text[:200]


class BridgeProgressReporter:
    def __init__(self, on_event):
        self.on_event = on_event

    def update_step(self, step: str, detail: str = "") -> None:
        self.on_event({"kind": "step", "step": step, "detail": _progress_detail(detail)})

    def set_item_total(self, total: int, detail: str = "") -> None:
        self.on_event(
            {"kind": "total", "total": int(total or 0), "detail": _progress_detail(detail)}
        )

    def advance_item(self, status: str, detail: str = "") -> None:
        self.on_event({"kind": "item", "status": status, "detail": _progress_detail(detail)})

    def on_author(self, nickname: str | None = None, sec_uid: str | None = None) -> None:
        self.on_event({"kind": "author", "nickname": nickname or "", "secUid": sec_uid or ""})


async def download_aweme(
    app_config: AppConfig,
    db,
    aweme: dict[str, Any],
    *,
    on_event=None,
) -> dict[str, Any]:
    imports = _imports(app_config)
    config_dict = _make_config(app_config, db)
    cfg = _make_config_loader(imports, config_dict)
    proxy = str(cfg.get("proxy") or "").strip()
    cookie_manager = imports["CookieManager"]()
    cookies = _cookie_dict(cfg)
    if cookies:
        cookie_manager.set_cookies(cookies)
    file_manager = imports["FileManager"](cfg.get("path"))
    rate_limiter = imports["RateLimiter"](max_per_second=float(cfg.get("rate_limit", 2) or 2))
    retry_handler = imports["RetryHandler"](max_retries=int(cfg.get("retry_times", 3) or 3))
    queue_manager = imports["QueueManager"](max_workers=int(cfg.get("thread", 3) or 3))
    reporter = BridgeProgressReporter(on_event or (lambda _event: None))

    async with imports["DouyinAPIClient"](cookies, proxy=proxy) as api_client:
        url_type = "live" if aweme.get("_douyin_live") else "video"
        downloader = imports["DownloaderFactory"].create(
            url_type,
            cfg,
            api_client,
            file_manager,
            cookie_manager,
            None,
            rate_limiter,
            retry_handler,
            queue_manager,
            progress_reporter=reporter,
        )
        if downloader is None:
            raise RuntimeError("Douyin downloader could not be created")
        if aweme.get("_douyin_live"):
            result = await downloader.download({"room_id": str(aweme.get("aweme_id") or "")})
            if int(getattr(result, "success", 0) or 0) <= 0:
                raise RuntimeError("Douyin live recording failed")
        else:
            ok = await downloader._download_aweme_assets(
                aweme,
                str((aweme.get("author") or {}).get("nickname") or "douyin"),
                mode="web",
            )
            if not ok:
                raise RuntimeError("Douyin media download failed")

    base_path = Path(str(config_dict.get("path") or ""))
    aweme_id = str(aweme.get("aweme_id") or aweme.get("group_id") or "")
    newest: Path | None = None
    if base_path.exists() and aweme_id:
        matches = [
            path
            for path in base_path.rglob(f"*{aweme_id}*")
            if path.is_file() and path.suffix.lower() in {".mp4", ".jpg", ".jpeg", ".png", ".webp"}
        ]
        if matches:
            newest = max(matches, key=lambda path: path.stat().st_mtime)
    return {
        "aweme": aweme,
        "localPath": str(newest or ""),
        "size": newest.stat().st_size if newest and newest.exists() else 0,
    }


def metadata_from_row(row: Any) -> dict[str, Any]:
    raw = row["metadata_json"] if row is not None and "metadata_json" in row.keys() else ""
    try:
        parsed = json.loads(str(raw or "{}"))
    except json.JSONDecodeError:
        parsed = {}
    return parsed if isinstance(parsed, dict) else {}


async def run_blocking_json(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)
