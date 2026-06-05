from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .douyin_store import (
    douyin_file_row,
    int_or_default,
    now_ms,
    unique_id_for_aweme,
)


def _file_type_for_path(path: Path) -> tuple[str, str]:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
        return "photo", {
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".webp": "image/webp",
            ".gif": "image/gif",
        }.get(suffix, "image/jpeg")
    if suffix in {".mp3", ".m4a", ".aac", ".wav"}:
        return "audio", {
            ".mp3": "audio/mpeg",
            ".m4a": "audio/mp4",
            ".aac": "audio/aac",
            ".wav": "audio/wav",
        }.get(suffix, "audio/mpeg")
    return "video", "video/mp4"


def sync_douyin_downloaded_assets(
    db: sqlite3.Connection,
    *,
    primary_unique_id: str,
    assets: list[dict[str, Any]],
) -> None:
    primary_row = douyin_file_row(db, unique_id=primary_unique_id)
    if primary_row is None:
        return
    aweme_id = str(primary_row["aweme_id"] or "")
    source_id = str(primary_row["source_id"] or "")
    caption = str(primary_row["caption"] or "")
    thumbnail_url = str(primary_row["thumbnail_url"] or "")
    metadata_json = str(primary_row["metadata_json"] or "")
    date = int_or_default(primary_row["date"], 0)
    ts = now_ms()
    media_assets: list[Path] = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        path = Path(str(asset.get("path") or ""))
        if not path.exists() or str(asset.get("bucket") or "") != "video":
            continue
        media_assets.append(path)

    for index, path in enumerate(media_assets):
        asset_kind = "primary" if index == 0 else "asset"
        unique_id = unique_id_for_aweme(aweme_id, asset_kind, index)
        file_type, mime_type = _file_type_for_path(path)
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        if unique_id == primary_unique_id:
            db.execute(
                """
                UPDATE douyin_file
                SET asset_index = ?, asset_kind = ?, file_name = ?, type = ?,
                    mime_type = ?, size = ?, downloaded_size = ?, local_path = ?,
                    download_status = 'completed', verification_status = ?,
                    completion_date = COALESCE(completion_date, ?), updated_at = ?
                WHERE unique_id = ?
                """,
                (
                    index,
                    asset_kind,
                    path.name,
                    file_type,
                    mime_type,
                    size,
                    size,
                    str(path),
                    "completed_verified" if size > 0 else "",
                    ts,
                    ts,
                    unique_id,
                ),
            )
            continue
        db.execute(
            """
            INSERT INTO douyin_file(
                unique_id, source_id, aweme_id, asset_index, asset_kind, file_name,
                type, mime_type, size, downloaded_size, thumbnail_url, caption, extra,
                local_path, download_status, transfer_status, download_error,
                verification_status, date, start_date, completion_date, tags,
                metadata_json, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', 'idle', '',
                   ?, ?, 0, ?, ?, ?, ?, ?)
            ON CONFLICT(unique_id) DO UPDATE SET
                source_id = excluded.source_id,
                asset_index = excluded.asset_index,
                asset_kind = excluded.asset_kind,
                file_name = excluded.file_name,
                type = excluded.type,
                mime_type = excluded.mime_type,
                size = excluded.size,
                downloaded_size = excluded.downloaded_size,
                local_path = excluded.local_path,
                download_status = 'completed',
                verification_status = excluded.verification_status,
                completion_date = COALESCE(douyin_file.completion_date, excluded.completion_date),
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                unique_id,
                source_id,
                aweme_id,
                index,
                asset_kind,
                path.name,
                file_type,
                mime_type,
                size,
                size,
                thumbnail_url,
                caption,
                json.dumps({"type": "douyin", "assetIndex": index}, separators=(",", ":")),
                str(path),
                "completed_verified" if size > 0 else "",
                date,
                ts,
                str(primary_row["tags"] or ""),
                metadata_json,
                ts,
                ts,
            ),
        )
    db.commit()
