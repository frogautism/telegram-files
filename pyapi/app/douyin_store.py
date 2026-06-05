from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .douyin_assets import sibling_asset_paths, unlink_asset_paths


def now_ms() -> int:
    return int(time.time() * 1000)


def int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def float_or_default(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def source_id_for_url(url: str) -> str:
    return hashlib.sha1(url.strip().encode("utf-8")).hexdigest()[:16]


def unique_id_for_aweme(aweme_id: str, asset_kind: str = "primary", index: int = 0) -> str:
    return f"douyin:{aweme_id}:{asset_kind}:{index}"


def _parse_json(text: Any) -> Any:
    if text is None:
        return None
    try:
        return json.loads(str(text))
    except json.JSONDecodeError:
        return None


def _format_date(seconds: int) -> str:
    if seconds <= 0:
        return ""
    return datetime.fromtimestamp(seconds).strftime("%Y-%m-%d %H:%M:%S")


def serialize_douyin_file(row: sqlite3.Row) -> dict[str, Any]:
    extra = _parse_json(row["extra"]) or {}
    file_id = int_or_default(row["id"], 0)
    unique_id = str(row["unique_id"] or "")
    date_seconds = int_or_default(row["date"], 0)
    status = str(row["download_status"] or "idle")
    local_path = str(row["local_path"] or "")
    thumbnail_url = str(row["thumbnail_url"] or "")
    return {
        "source": "douyin",
        "id": file_id,
        "telegramId": 0,
        "uniqueId": unique_id,
        "messageId": file_id,
        "mediaAlbumId": 0,
        "chatId": 0,
        "fileName": str(row["file_name"] or unique_id),
        "type": str(row["type"] or "file"),
        "mimeType": str(row["mime_type"] or "application/octet-stream"),
        "size": int_or_default(row["size"], 0),
        "downloadedSize": int_or_default(row["downloaded_size"], 0),
        "thumbnail": "",
        "thumbnailUrl": thumbnail_url,
        "thumbnailFile": None,
        "downloadStatus": status,
        "date": date_seconds,
        "formatDate": _format_date(date_seconds),
        "caption": str(row["caption"] or ""),
        "localPath": local_path,
        "hasSensitiveContent": False,
        "startDate": int_or_default(row["start_date"], 0),
        "completionDate": int_or_default(row["completion_date"], 0),
        "originalDeleted": False,
        "transferStatus": str(row["transfer_status"] or "idle"),
        "downloadError": str(row["download_error"] or ""),
        "verificationStatus": str(row["verification_status"] or ""),
        "extra": extra,
        "tags": row["tags"],
        "alreadyDownloaded": status == "completed" and bool(local_path),
        "loaded": True,
        "threadChatId": 0,
        "messageThreadId": 0,
        "hasReply": False,
        "reactionCount": 0,
        "sourceId": str(row["source_id"] or ""),
        "awemeId": str(row["aweme_id"] or ""),
    }


def _row_has(row: sqlite3.Row, key: str) -> bool:
    try:
        return key in row.keys()
    except (AttributeError, TypeError):
        return False


def _source_counts(row: sqlite3.Row) -> dict[str, int]:
    return {
        "totalFiles": int_or_default(row["total_files"], 0)
        if _row_has(row, "total_files")
        else 0,
        "completedDownloads": int_or_default(row["completed_downloads"], 0)
        if _row_has(row, "completed_downloads")
        else 0,
        "failedDownloads": int_or_default(row["failed_downloads"], 0)
        if _row_has(row, "failed_downloads")
        else 0,
    }


def _source_to_dict(row: sqlite3.Row, counts: dict[str, int] | None = None) -> dict[str, Any]:
    auto = _parse_json(row["auto_settings"]) or default_douyin_auto_settings()
    resolved_counts = counts if counts is not None else _source_counts(row)
    return {
        "id": str(row["id"] or ""),
        "url": str(row["url"] or ""),
        "resolvedUrl": str(row["resolved_url"] or ""),
        "urlType": str(row["url_type"] or ""),
        "title": str(row["title"] or ""),
        "authorName": str(row["author_name"] or ""),
        "status": str(row["status"] or "idle"),
        "auto": auto,
        "lastError": str(row["last_error"] or ""),
        "createdAt": int_or_default(row["created_at"], 0),
        "updatedAt": int_or_default(row["updated_at"], 0),
        "displayName": str(row["display_name"] or ""),
        "autoRefresh": {
            "enabled": bool(int_or_default(row["auto_refresh_enabled"], 0)),
            "intervalSeconds": max(
                1800, int_or_default(row["auto_refresh_interval"], 1800)
            ),
        },
        "lastRefreshStartedAt": int_or_default(row["last_refresh_started_at"], 0),
        "lastRefreshCompletedAt": int_or_default(row["last_refresh_completed_at"], 0),
        "lastDiscoveredCount": int_or_default(row["last_discovered_count"], 0),
        "newestAwemeId": str(row["newest_aweme_id"] or ""),
        "newestCreateTime": int_or_default(row["newest_create_time"], 0),
        "refreshStatus": str(row["refresh_status"] or "idle"),
        "refreshError": str(row["refresh_error"] or ""),
        "totalFiles": resolved_counts.get("totalFiles", 0),
        "completedDownloads": resolved_counts.get("completedDownloads", 0),
        "failedDownloads": resolved_counts.get("failedDownloads", 0),
    }


def _count_source_files(db: sqlite3.Connection, source_id: str) -> dict[str, int]:
    row = db.execute(
        """
        SELECT
            COUNT(*) AS total_files,
            SUM(CASE WHEN download_status = 'completed' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN download_status = 'error' THEN 1 ELSE 0 END) AS failed
        FROM douyin_file
        WHERE source_id = ?
        """,
        (source_id,),
    ).fetchone()
    if row is None:
        return {"totalFiles": 0, "completedDownloads": 0, "failedDownloads": 0}
    return {
        "totalFiles": int_or_default(row["total_files"], 0),
        "completedDownloads": int_or_default(row["completed"], 0),
        "failedDownloads": int_or_default(row["failed"], 0),
    }


def default_douyin_auto_settings() -> dict[str, Any]:
    return {
        "preload": {"enabled": False},
        "download": {
            "enabled": False,
            "rule": {
                "query": "",
                "fileTypes": ["photo", "video", "audio", "file"],
                "downloadHistory": True,
                "downloadCommentFiles": False,
                "filterExpr": "",
            },
        },
        "transfer": {
            "enabled": False,
            "rule": {
                "transferHistory": True,
                "destination": "",
                "transferPolicy": "GROUP_BY_TYPE",
                "duplicationPolicy": "OVERWRITE",
                "extra": {},
            },
        },
        "state": 0,
    }


def upsert_douyin_source(
    db: sqlite3.Connection,
    *,
    url: str,
    resolved_url: str = "",
    url_type: str = "",
    title: str = "",
    author_name: str = "",
    status: str = "idle",
    source_id: str | None = None,
) -> dict[str, Any]:
    normalized_url = url.strip()
    sid = source_id or source_id_for_url(normalized_url)
    ts = now_ms()
    db.execute(
        """
        INSERT INTO douyin_source(
            id, url, resolved_url, url_type, title, author_name, status,
            auto_settings, last_error, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            url = excluded.url,
            resolved_url = COALESCE(NULLIF(excluded.resolved_url, ''), douyin_source.resolved_url),
            url_type = COALESCE(NULLIF(excluded.url_type, ''), douyin_source.url_type),
            title = COALESCE(NULLIF(excluded.title, ''), douyin_source.title),
            author_name = COALESCE(NULLIF(excluded.author_name, ''), douyin_source.author_name),
            status = excluded.status,
            last_error = '',
            updated_at = excluded.updated_at
        """,
        (
            sid,
            normalized_url,
            resolved_url.strip(),
            url_type.strip(),
            title.strip(),
            author_name.strip(),
            status,
            json.dumps(default_douyin_auto_settings(), separators=(",", ":")),
            ts,
            ts,
        ),
    )
    db.commit()
    source = get_douyin_source(db, sid)
    if source is None:
        raise RuntimeError("failed to create Douyin source")
    return source


def mark_douyin_source_status(
    db: sqlite3.Connection,
    source_id: str,
    status: str,
    *,
    error: str = "",
) -> None:
    db.execute(
        """
        UPDATE douyin_source
        SET status = ?, last_error = ?, updated_at = ?
        WHERE id = ?
        """,
        (status, error, now_ms(), source_id),
    )
    db.commit()


def list_douyin_sources(db: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = db.execute(
        "SELECT * FROM douyin_source ORDER BY updated_at DESC, created_at DESC"
    ).fetchall()
    count_rows = db.execute(
        """
        SELECT
            source_id,
            COUNT(*) AS total_files,
            SUM(CASE WHEN download_status = 'completed' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN download_status = 'error' THEN 1 ELSE 0 END) AS failed
        FROM douyin_file
        GROUP BY source_id
        """
    ).fetchall()
    counts: dict[str, dict[str, int]] = {}
    for count_row in count_rows:
        counts[str(count_row["source_id"] or "")] = {
            "totalFiles": int_or_default(count_row["total_files"], 0),
            "completedDownloads": int_or_default(count_row["completed"], 0),
            "failedDownloads": int_or_default(count_row["failed"], 0),
        }
    return [
        _source_to_dict(
            row,
            counts.get(
                str(row["id"] or ""),
                {"totalFiles": 0, "completedDownloads": 0, "failedDownloads": 0},
            ),
        )
        for row in rows
    ]


def get_douyin_source(db: sqlite3.Connection, source_id: str) -> dict[str, Any] | None:
    sid = source_id.strip()
    row = db.execute(
        "SELECT * FROM douyin_source WHERE id = ? LIMIT 1",
        (sid,),
    ).fetchone()
    if row is None:
        return None
    return _source_to_dict(row, _count_source_files(db, sid))


def update_douyin_source_auto_settings(
    db: sqlite3.Connection,
    *,
    source_id: str,
    auto_payload: dict[str, Any],
) -> dict[str, Any] | None:
    if get_douyin_source(db, source_id) is None:
        return None
    payload = auto_payload if isinstance(auto_payload, dict) else {}
    db.execute(
        """
        UPDATE douyin_source
        SET auto_settings = ?, updated_at = ?
        WHERE id = ?
        """,
        (json.dumps(payload, separators=(",", ":"), ensure_ascii=False), now_ms(), source_id),
    )
    db.commit()
    return get_douyin_source(db, source_id)


def update_douyin_source_display(
    db: sqlite3.Connection,
    source_id: str,
    *,
    display_name: str | None = None,
    auto_refresh_enabled: bool | None = None,
    auto_refresh_interval: int | None = None,
) -> dict[str, Any] | None:
    if get_douyin_source(db, source_id) is None:
        return None
    sets: list[str] = []
    params: list[Any] = []
    if display_name is not None:
        sets.append("display_name = ?")
        params.append(display_name.strip())
    if auto_refresh_enabled is not None:
        sets.append("auto_refresh_enabled = ?")
        params.append(1 if auto_refresh_enabled else 0)
    if auto_refresh_interval is not None:
        sets.append("auto_refresh_interval = ?")
        params.append(max(1800, int(auto_refresh_interval)))
    sets.append("updated_at = ?")
    params.append(now_ms())
    params.append(source_id.strip())
    db.execute(
        f"UPDATE douyin_source SET {', '.join(sets)} WHERE id = ?",
        params,
    )
    db.commit()
    return get_douyin_source(db, source_id)


def delete_douyin_source(
    db: sqlite3.Connection,
    source_id: str,
    *,
    delete_files: bool = False,
) -> dict[str, Any]:
    sid = source_id.strip()
    existing = db.execute(
        "SELECT id FROM douyin_source WHERE id = ? LIMIT 1", (sid,)
    ).fetchone()
    if existing is None:
        return {"deleted": False, "removedFiles": 0}

    file_rows = db.execute(
        "SELECT id, unique_id, aweme_id, local_path FROM douyin_file WHERE source_id = ?",
        (sid,),
    ).fetchall()
    frame_rows = db.execute(
        """
        SELECT local_path
        FROM douyin_frame
        WHERE source_id = ?
           OR file_unique_id IN (
               SELECT unique_id FROM douyin_file WHERE source_id = ?
           )
        """,
        (sid, sid),
    ).fetchall()
    removed_files = len(file_rows)
    if delete_files:
        for file_row in file_rows:
            local_path = str(file_row["local_path"] or "").strip()
            unlink_asset_paths(
                sibling_asset_paths(
                    local_path,
                    str(file_row["aweme_id"] or ""),
                )
            )
        for frame_row in frame_rows:
            local_path = str(frame_row["local_path"] or "").strip()
            if not local_path:
                continue
            try:
                Path(local_path).unlink(missing_ok=True)
            except OSError:
                pass

    # Explicitly remove douyin_file rows (FK CASCADE may be disabled on some
    # connections, e.g. in-memory test databases without foreign_keys pragma).
    db.execute(
        """
        DELETE FROM douyin_frame
        WHERE source_id = ?
           OR file_unique_id IN (
               SELECT unique_id FROM douyin_file WHERE source_id = ?
           )
        """,
        (sid, sid),
    )
    db.execute("DELETE FROM douyin_file WHERE source_id = ?", (sid,))
    db.execute("DELETE FROM douyin_source WHERE id = ?", (sid,))
    db.commit()
    return {"deleted": True, "removedFiles": removed_files}


def record_source_refresh_start(db: sqlite3.Connection, source_id: str) -> None:
    db.execute(
        """
        UPDATE douyin_source
        SET refresh_status = 'refreshing',
            last_refresh_started_at = ?,
            refresh_error = '',
            updated_at = ?
        WHERE id = ?
        """,
        (now_ms(), now_ms(), source_id.strip()),
    )
    db.commit()


def record_source_refresh_result(
    db: sqlite3.Connection,
    source_id: str,
    *,
    discovered: int,
    new: int,
    existing: int,
    failed: int,
    newest_aweme_id: str = "",
    newest_create_time: int = 0,
    error: str = "",
) -> None:
    sid = source_id.strip()
    row = db.execute(
        "SELECT newest_create_time FROM douyin_source WHERE id = ? LIMIT 1",
        (sid,),
    ).fetchone()
    if row is None:
        return
    current_newest = int_or_default(row["newest_create_time"], 0)
    update_newest = (
        bool(newest_aweme_id) and int(newest_create_time or 0) >= current_newest
    )
    status = "error" if error else "idle"
    ts = now_ms()
    if update_newest:
        db.execute(
            """
            UPDATE douyin_source
            SET refresh_status = ?,
                last_refresh_completed_at = ?,
                last_discovered_count = ?,
                newest_aweme_id = ?,
                newest_create_time = ?,
                refresh_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                ts,
                max(0, int(discovered)),
                newest_aweme_id,
                int(newest_create_time or 0),
                error,
                ts,
                sid,
            ),
        )
    else:
        db.execute(
            """
            UPDATE douyin_source
            SET refresh_status = ?,
                last_refresh_completed_at = ?,
                last_discovered_count = ?,
                refresh_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (status, ts, max(0, int(discovered)), error, ts, sid),
        )
    db.commit()


def douyin_aweme_exists(db: sqlite3.Connection, aweme_id: str) -> bool:
    unique_id = unique_id_for_aweme(str(aweme_id).strip())
    row = db.execute(
        "SELECT 1 FROM douyin_file WHERE unique_id = ? LIMIT 1",
        (unique_id,),
    ).fetchone()
    return row is not None


def _aweme_media_type(aweme: dict[str, Any]) -> str:
    if aweme.get("image_post_info") or aweme.get("images") or aweme.get("image_list"):
        return "photo"
    return "video"


def _thumbnail_url(aweme: dict[str, Any]) -> str:
    video = aweme.get("video") if isinstance(aweme.get("video"), dict) else {}
    cover = video.get("cover") if isinstance(video.get("cover"), dict) else {}
    urls = cover.get("url_list") if isinstance(cover.get("url_list"), list) else []
    if urls:
        return str(urls[0] or "")
    images = aweme.get("images") or aweme.get("image_list") or []
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict):
            for key in ("display_image", "origin_image", "download_url"):
                value = first.get(key)
                if isinstance(value, dict) and isinstance(value.get("url_list"), list):
                    return str((value.get("url_list") or [""])[0] or "")
    return ""


def _aweme_extra(aweme: dict[str, Any], file_type: str) -> dict[str, Any]:
    video = aweme.get("video") if isinstance(aweme.get("video"), dict) else {}
    if file_type == "video":
        return {
            "width": int_or_default(video.get("width"), 0),
            "height": int_or_default(video.get("height"), 0),
            "duration": max(0, int_or_default(video.get("duration"), 0) // 1000),
            "mimeType": "video/mp4",
        }
    return {"width": 0, "height": 0, "type": "douyin"}


def _aweme_tags(aweme: dict[str, Any]) -> str:
    tags: list[str] = []
    seen: set[str] = set()
    for item in aweme.get("text_extra") or []:
        if not isinstance(item, dict):
            continue
        tag = str(item.get("hashtag_name") or item.get("tag_name") or "").strip()
        if tag and tag not in seen:
            seen.add(tag)
            tags.append(tag)
    return ",".join(tags)


def upsert_douyin_aweme(
    db: sqlite3.Connection,
    *,
    source_id: str,
    aweme: dict[str, Any],
    local_path: str = "",
    download_status: str = "idle",
) -> dict[str, Any] | None:
    aweme_id = str(aweme.get("aweme_id") or aweme.get("group_id") or "").strip()
    if not aweme_id:
        return None

    desc = str(aweme.get("desc") or aweme_id).strip()
    file_type = _aweme_media_type(aweme)
    unique_id = unique_id_for_aweme(aweme_id)
    create_time = int_or_default(aweme.get("create_time"), int(time.time()))
    mime_type = "video/mp4" if file_type == "video" else "image/jpeg"
    extension = ".mp4" if file_type == "video" else ".jpg"
    file_name = f"{aweme_id}{extension}"
    path_size = 0
    if local_path:
        try:
            path_size = Path(local_path).stat().st_size
        except OSError:
            path_size = 0

    existing = db.execute(
        "SELECT * FROM douyin_file WHERE unique_id = ? LIMIT 1",
        (unique_id,),
    ).fetchone()
    status = download_status
    if existing is not None and str(existing["download_status"] or "") == "completed":
        if not local_path:
            local_path = str(existing["local_path"] or "")
        status = "completed"
        path_size = int_or_default(existing["size"], path_size)

    completion_value = now_ms() if status == "completed" and local_path else None
    metadata_json = json.dumps(aweme, separators=(",", ":"), ensure_ascii=False)
    extra_json = json.dumps(_aweme_extra(aweme, file_type), separators=(",", ":"))
    row_values = (
        source_id,
        aweme_id,
        0,
        "primary",
        file_name,
        file_type,
        mime_type,
        path_size,
        path_size if status == "completed" else 0,
        _thumbnail_url(aweme),
        desc,
        extra_json,
        local_path,
        status,
        "idle",
        "",
        "completed_verified" if status == "completed" and local_path else "",
        create_time,
        now_ms() if status == "downloading" else 0,
        completion_value,
        _aweme_tags(aweme),
        metadata_json,
        now_ms(),
        now_ms(),
    )
    if existing is None:
        db.execute(
            """
            INSERT INTO douyin_file(
                unique_id, source_id, aweme_id, asset_index, asset_kind, file_name,
                type, mime_type, size, downloaded_size, thumbnail_url, caption, extra,
                local_path, download_status, transfer_status, download_error,
                verification_status, date, start_date, completion_date, tags,
                metadata_json, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (unique_id, *row_values),
        )
    else:
        db.execute(
            """
            UPDATE douyin_file
            SET source_id = ?, aweme_id = ?, asset_index = ?, asset_kind = ?,
                file_name = ?, type = ?, mime_type = ?,
                size = CASE WHEN ? > 0 THEN ? ELSE size END,
                downloaded_size = CASE WHEN ? > 0 THEN ? ELSE downloaded_size END,
                thumbnail_url = ?, caption = ?, extra = ?,
                local_path = COALESCE(NULLIF(?, ''), local_path),
                download_status = ?, transfer_status = COALESCE(transfer_status, ?),
                download_error = ?, verification_status = ?, date = ?,
                start_date = CASE WHEN ? > 0 THEN ? ELSE start_date END,
                completion_date = COALESCE(?, completion_date),
                tags = ?, metadata_json = ?, updated_at = ?
            WHERE unique_id = ?
            """,
            (
                source_id,
                aweme_id,
                0,
                "primary",
                file_name,
                file_type,
                mime_type,
                path_size,
                path_size,
                path_size if status == "completed" else 0,
                path_size if status == "completed" else 0,
                _thumbnail_url(aweme),
                desc,
                extra_json,
                local_path,
                status,
                "idle",
                "",
                "completed_verified" if status == "completed" and local_path else "",
                create_time,
                now_ms() if status == "downloading" else 0,
                now_ms() if status == "downloading" else 0,
                completion_value,
                _aweme_tags(aweme),
                metadata_json,
                now_ms(),
                unique_id,
            ),
        )
    db.commit()
    return find_douyin_file(db, unique_id=unique_id)


def find_douyin_file(
    db: sqlite3.Connection,
    *,
    unique_id: str = "",
    file_id: int = 0,
) -> dict[str, Any] | None:
    row = None
    if unique_id:
        row = db.execute(
            "SELECT * FROM douyin_file WHERE unique_id = ? LIMIT 1",
            (unique_id.strip(),),
        ).fetchone()
    elif file_id > 0:
        row = db.execute("SELECT * FROM douyin_file WHERE id = ? LIMIT 1", (file_id,)).fetchone()
    return serialize_douyin_file(row) if row is not None else None


def douyin_file_row(
    db: sqlite3.Connection,
    *,
    unique_id: str = "",
    file_id: int = 0,
) -> sqlite3.Row | None:
    if unique_id:
        return db.execute(
            "SELECT * FROM douyin_file WHERE unique_id = ? LIMIT 1",
            (unique_id.strip(),),
        ).fetchone()
    if file_id > 0:
        return db.execute("SELECT * FROM douyin_file WHERE id = ? LIMIT 1", (file_id,)).fetchone()
    return None


def list_douyin_files(
    db: sqlite3.Connection,
    *,
    source_id: str = "",
    filters: dict[str, str] | None = None,
) -> dict[str, Any]:
    filters = filters or {}
    base_clauses = ["1 = 1"]
    base_params: list[Any] = []
    if source_id:
        base_clauses.append("source_id = ?")
        base_params.append(source_id)

    search = str(filters.get("search") or "").strip()
    if search:
        base_clauses.append("(file_name LIKE ? OR caption LIKE ? OR aweme_id LIKE ?)")
        base_params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    file_type = str(filters.get("type") or "").strip()
    if file_type and file_type != "all":
        if file_type == "media":
            base_clauses.append("type IN ('photo', 'video')")
        else:
            base_clauses.append("type = ?")
            base_params.append(file_type)

    status = str(filters.get("downloadStatus") or "").strip()
    if status:
        base_clauses.append("download_status = ?")
        base_params.append(status)

    transfer_status = str(filters.get("transferStatus") or "").strip()
    if transfer_status:
        base_clauses.append("transfer_status = ?")
        base_params.append(transfer_status)

    total_count = int_or_default(
        db.execute(
            f"SELECT COUNT(*) FROM douyin_file WHERE {' AND '.join(base_clauses)}",
            base_params,
        ).fetchone()[0],
        0,
    )

    sort_key = str(filters.get("sort") or "date").strip().lower()
    sort_column = {
        "completion_date": "completion_date",
        "size": "size",
        "date": "date",
    }.get(sort_key, "date")
    order = str(filters.get("order") or "desc").strip().lower()
    if order not in {"asc", "desc"}:
        order = "desc"
    comparator = ">" if order == "asc" else "<"
    order_sql = "ASC" if order == "asc" else "DESC"

    page_clauses = list(base_clauses)
    page_params = list(base_params)
    from_id = int_or_default(filters.get("fromMessageId"), 0)
    if from_id > 0:
        cursor_value_raw = filters.get("fromSortField")
        if cursor_value_raw is None or cursor_value_raw == "":
            cursor_row = db.execute(
                f"SELECT {sort_column} FROM douyin_file WHERE id = ? LIMIT 1",
                (from_id,),
            ).fetchone()
            cursor_value_raw = cursor_row[0] if cursor_row is not None else None
        cursor_value = int_or_default(cursor_value_raw, 0)
        page_clauses.append(
            f"({sort_column} {comparator} ? OR ({sort_column} = ? AND id {comparator} ?))"
        )
        page_params.extend([cursor_value, cursor_value, from_id])

    limit = min(200, max(1, int_or_default(filters.get("limit"), 20)))
    where_sql = " AND ".join(page_clauses)
    rows = db.execute(
        f"""
        SELECT *
        FROM douyin_file
        WHERE {where_sql}
        ORDER BY {sort_column} {order_sql}, id {order_sql}
        LIMIT ?
        """,
        [*page_params, limit + 1],
    ).fetchall()
    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]
    files = [serialize_douyin_file(row) for row in rows]
    return {
        "files": files,
        "count": total_count,
        "nextFromMessageId": int_or_default(rows[-1]["id"], 0) if has_more and rows else 0,
    }


def update_douyin_file_status(
    db: sqlite3.Connection,
    *,
    unique_id: str,
    download_status: str,
    local_path: str | None = None,
    downloaded_size: int | None = None,
    size: int | None = None,
    error: str = "",
) -> dict[str, Any] | None:
    row = douyin_file_row(db, unique_id=unique_id)
    if row is None:
        return None

    next_path = str(row["local_path"] or "") if local_path is None else local_path
    resolved_size = int_or_default(row["size"], 0) if size is None else max(0, size)
    resolved_downloaded = (
        int_or_default(row["downloaded_size"], 0)
        if downloaded_size is None
        else max(0, downloaded_size)
    )
    if download_status == "completed" and next_path:
        try:
            resolved_size = Path(next_path).stat().st_size
            resolved_downloaded = resolved_size
        except OSError:
            pass
    db.execute(
        """
        UPDATE douyin_file
        SET download_status = ?, local_path = ?, downloaded_size = ?, size = ?,
            download_error = ?, verification_status = ?,
            start_date = CASE WHEN ? = 'downloading' AND start_date = 0 THEN ? ELSE start_date END,
            completion_date = CASE WHEN ? = 'completed' THEN ? ELSE completion_date END,
            updated_at = ?
        WHERE unique_id = ?
        """,
        (
            download_status,
            next_path,
            resolved_downloaded,
            resolved_size,
            error,
            "completed_verified" if download_status == "completed" and next_path else "",
            download_status,
            now_ms(),
            download_status,
            now_ms(),
            now_ms(),
            unique_id,
        ),
    )
    db.commit()
    return find_douyin_file(db, unique_id=unique_id)


def remove_douyin_download(db: sqlite3.Connection, unique_id: str) -> dict[str, Any] | None:
    row = douyin_file_row(db, unique_id=unique_id)
    if row is None:
        return None
    local_path = str(row["local_path"] or "")
    unlink_asset_paths(sibling_asset_paths(local_path, str(row["aweme_id"] or "")))
    frame_rows = db.execute(
        "SELECT local_path FROM douyin_frame WHERE file_unique_id = ?",
        (unique_id.strip(),),
    ).fetchall()
    for frame_row in frame_rows:
        frame_path = str(frame_row["local_path"] or "").strip()
        if not frame_path:
            continue
        try:
            Path(frame_path).unlink(missing_ok=True)
        except OSError:
            pass
    db.execute("DELETE FROM douyin_frame WHERE file_unique_id = ?", (unique_id.strip(),))
    db.execute(
        """
        UPDATE douyin_file
        SET download_status = 'idle',
            downloaded_size = 0,
            local_path = '',
            transfer_status = 'idle',
            completion_date = NULL,
            updated_at = ?
        WHERE unique_id = ?
        """,
        (now_ms(), unique_id),
    )
    db.commit()
    return find_douyin_file(db, unique_id=unique_id)


def update_douyin_file_tags(db: sqlite3.Connection, unique_id: str, tags: str) -> None:
    db.execute(
        "UPDATE douyin_file SET tags = ?, updated_at = ? WHERE unique_id = ?",
        (tags, now_ms(), unique_id.strip()),
    )
    db.commit()


def update_douyin_files_tags(db: sqlite3.Connection, unique_ids: list[str], tags: str) -> None:
    normalized = [item.strip() for item in unique_ids if item.strip()]
    if not normalized:
        return
    db.executemany(
        "UPDATE douyin_file SET tags = ?, updated_at = ? WHERE unique_id = ?",
        [(tags, now_ms(), unique_id) for unique_id in normalized],
    )
    db.commit()


def douyin_transfer_candidates(
    db: sqlite3.Connection,
    *,
    source_id: str = "",
    limit: int = 200,
) -> list[dict[str, Any]]:
    clauses = [
        "download_status = 'completed'",
        "transfer_status = 'idle'",
        "local_path IS NOT NULL",
        "TRIM(local_path) != ''",
    ]
    params: list[Any] = []
    if source_id:
        clauses.append("source_id = ?")
        params.append(source_id)
    rows = db.execute(
        f"""
        SELECT id, unique_id, source_id
        FROM douyin_file
        WHERE {' AND '.join(clauses)}
        ORDER BY completion_date DESC, id DESC
        LIMIT ?
        """,
        [*params, limit],
    ).fetchall()
    return [
        {
            "source": "douyin",
            "id": int_or_default(row["id"], 0),
            "fileId": int_or_default(row["id"], 0),
            "uniqueId": str(row["unique_id"] or ""),
            "sourceId": str(row["source_id"] or ""),
        }
        for row in rows
    ]


def douyin_file_for_transfer(
    db: sqlite3.Connection,
    *,
    unique_id: str,
) -> dict[str, Any] | None:
    row = douyin_file_row(db, unique_id=unique_id)
    if row is None:
        return None
    local_path = str(row["local_path"] or "")
    if not local_path or not Path(local_path).exists():
        return None
    return {
        "id": int_or_default(row["id"], 0),
        "unique_id": str(row["unique_id"] or ""),
        "telegram_id": 0,
        "chat_id": 0,
        "type": str(row["type"] or "file"),
        "file_name": str(row["file_name"] or ""),
        "caption": str(row["caption"] or ""),
        "local_path": local_path,
    }


def update_douyin_transfer_status(
    db: sqlite3.Connection,
    *,
    unique_id: str,
    transfer_status: str,
    local_path: str | None = None,
) -> dict[str, Any] | None:
    row = douyin_file_row(db, unique_id=unique_id)
    if row is None:
        return None
    next_path = str(row["local_path"] or "") if local_path is None else local_path
    db.execute(
        """
        UPDATE douyin_file
        SET transfer_status = ?, local_path = ?, updated_at = ?
        WHERE unique_id = ?
        """,
        (transfer_status, next_path, now_ms(), unique_id),
    )
    db.commit()
    return {
        "source": "douyin",
        "fileId": int_or_default(row["id"], 0),
        "uniqueId": unique_id,
        "transferStatus": transfer_status,
        "localPath": next_path,
    }
