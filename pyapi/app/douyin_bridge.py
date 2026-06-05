from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from .config import AppConfig
from .douyin_assets import (
    asset_payload,
    changed_assets,
    douyin_asset_bucket,
    is_douyin_asset,
    is_primary_asset,
    snapshot_assets,
)
from .douyin_config import build_douyin_config, douyin_downloader_path


class DouyinBridgeUnavailable(RuntimeError):
    pass


def _ensure_import_path(app_config: AppConfig) -> str:
    """Inject an external downloader override path onto ``sys.path``.

    Returns the resolved override path, or an empty string when no valid
    override is configured (in which case the vendored package is used).
    """
    package_path = douyin_downloader_path(app_config)
    if not package_path:
        return ""
    root = Path(package_path).resolve()
    if not root.exists():
        return ""
    root_text = str(root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)
    return root_text


def _imports(app_config: AppConfig) -> dict[str, Any]:
    override_root = _ensure_import_path(app_config)
    try:
        if override_root:
            from auth import CookieManager
            from config import ConfigLoader
            from control import QueueManager, RateLimiter, RetryHandler
            from core import DouyinAPIClient, DownloaderFactory, URLParser
            from storage import FileManager
            from utils.validators import is_short_url, normalize_short_url
        else:
            from app.vendor.douyin_downloader.auth import CookieManager
            from app.vendor.douyin_downloader.config import ConfigLoader
            from app.vendor.douyin_downloader.control import (
                QueueManager,
                RateLimiter,
                RetryHandler,
            )
            from app.vendor.douyin_downloader.core import (
                DouyinAPIClient,
                DownloaderFactory,
                URLParser,
            )
            from app.vendor.douyin_downloader.storage import FileManager
            from app.vendor.douyin_downloader.utils.validators import (
                is_short_url,
                normalize_short_url,
            )
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


async def _collect_paged_user_items(
    api_client: Any,
    fetcher_name: str,
    sec_uid: str,
    *,
    limit: int,
    page_size: int = 20,
) -> list[dict[str, Any]]:
    fetcher = getattr(api_client, fetcher_name, None)
    if not callable(fetcher):
        raise RuntimeError(f"Douyin API client does not support {fetcher_name}")

    items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    max_cursor = 0
    while len(items) < limit:
        remaining = limit - len(items)
        page = await fetcher(sec_uid, max_cursor=max_cursor, count=min(page_size, remaining))
        page_items = [item for item in page.get("items", []) if isinstance(item, dict)]
        for item in page_items:
            aweme_id = str(item.get("aweme_id") or item.get("mix_id") or item.get("music_id") or "")
            dedupe_key = aweme_id or json.dumps(item, sort_keys=True, ensure_ascii=False)
            if dedupe_key in seen_ids:
                continue
            seen_ids.add(dedupe_key)
            items.append(item)
            if len(items) >= limit:
                break

        has_more = bool(page.get("has_more", False))
        next_cursor = int(str(page.get("max_cursor") or 0) or 0)
        if not has_more or next_cursor == max_cursor:
            break
        max_cursor = next_cursor
    return items


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
                fetcher_name = "get_user_like"
            elif selected_mode == "music":
                fetcher_name = "get_user_music"
            elif selected_mode == "mix":
                fetcher_name = "get_user_mix"
            else:
                fetcher_name = "get_user_post"
            awemes.extend(
                await _collect_paged_user_items(
                    api_client,
                    fetcher_name,
                    sec_uid,
                    limit=limit,
                )
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


def _replace_path(source: Path, target: Path) -> Path:
    if source == target:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    source.replace(target)
    return target


def _organize_downloaded_assets(
    base_path: Path,
    aweme_id: str,
    *,
    candidate_paths: list[Path] | None = None,
) -> dict[str, Any]:
    if not base_path.exists() or not aweme_id:
        return {"localPath": "", "size": 0, "assets": []}

    if candidate_paths is None:
        matches = [
            path
            for path in base_path.rglob(f"*{aweme_id}*")
            if is_douyin_asset(path)
        ]
    else:
        matches = [path for path in candidate_paths if is_douyin_asset(path)]
    if not matches:
        return {"localPath": "", "size": 0, "assets": []}

    organized: list[Path] = []
    for path in sorted(matches, key=lambda item: len(item.parts)):
        try:
            relative = path.relative_to(base_path)
        except ValueError:
            continue
        if not relative.parts:
            continue
        author_dir = relative.parts[0]
        bucket = douyin_asset_bucket(path)
        target = base_path / author_dir / bucket / path.name
        try:
            organized.append(_replace_path(path, target))
        except OSError:
            organized.append(path)

    primary = [
        path
        for path in organized
        if path.is_file() and is_primary_asset(path)
    ]
    if not primary:
        primary = [
            path
            for path in organized
            if path.is_file() and douyin_asset_bucket(path) in {"video", "live"}
        ]
    newest = max(primary, key=lambda path: path.stat().st_mtime) if primary else None
    return {
        "localPath": str(newest or ""),
        "size": newest.stat().st_size if newest and newest.exists() else 0,
        "assets": [asset_payload(path) for path in organized if path.exists()],
    }


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

    base_path = Path(str(config_dict.get("path") or ""))
    before_assets = snapshot_assets(base_path)

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
            download_assets = getattr(downloader, "download_aweme_assets", None)
            if callable(download_assets):
                ok = await download_assets(
                    aweme,
                    str((aweme.get("author") or {}).get("nickname") or "douyin"),
                    mode="video",
                )
            else:
                ok = await downloader._download_aweme_assets(
                    aweme,
                    str((aweme.get("author") or {}).get("nickname") or "douyin"),
                    mode="video",
                )
            if not ok:
                raise RuntimeError("Douyin media download failed")

    downloaded_files = getattr(downloader, "last_downloaded_files", [])
    candidate_paths = [
        path for path in (Path(str(item)) for item in downloaded_files) if path.exists()
    ]
    if not candidate_paths:
        after_assets = snapshot_assets(base_path)
        candidate_paths = changed_assets(before_assets, after_assets)
    aweme_id = str(aweme.get("aweme_id") or aweme.get("group_id") or "")
    organized = _organize_downloaded_assets(
        base_path,
        aweme_id,
        candidate_paths=candidate_paths,
    )
    if not organized["localPath"]:
        # Fall back to the historical aweme-id scan for compatibility with
        # external overrides whose file mtimes may not change as expected.
        organized = _organize_downloaded_assets(base_path, aweme_id)
    if not organized["localPath"]:
        raise RuntimeError("Douyin media download finished but no local asset was found")
    return {
        "aweme": aweme,
        "localPath": organized["localPath"],
        "size": organized["size"],
        "assets": organized["assets"],
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
