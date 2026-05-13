from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any


JOB_STATE_QUEUED = "queued"
JOB_STATE_STARTING = "starting"
JOB_STATE_MONITORING = "monitoring"
JOB_STATE_COMPLETED = "completed"
JOB_STATE_FAILED = "failed"
JOB_STATE_CANCELLED = "cancelled"

VERIFICATION_COMPLETED_VERIFIED = "completed_verified"
VERIFICATION_COMPLETED_UNVERIFIED = "completed_unverified"
VERIFICATION_FILE_MISSING = "file_missing"

RETRY_BASE_SECONDS = 5
RETRY_MAX_SECONDS = 30 * 60


def _now_ms() -> int:
    return int(time.time() * 1000)


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _job_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": _int_or_default(row["id"], 0),
        "telegramId": _int_or_default(row["telegram_id"], 0),
        "chatId": _int_or_default(row["chat_id"], 0),
        "messageId": _int_or_default(row["message_id"], 0),
        "fileId": _int_or_default(row["file_id"], 0),
        "uniqueId": str(row["unique_id"] or ""),
        "sessionId": str(row["session_id"] or ""),
        "source": str(row["source"] or ""),
        "state": str(row["state"] or ""),
        "attempts": _int_or_default(row["attempts"], 0),
        "retryAt": _int_or_default(row["retry_at"], 0),
        "nextRetryDelaySeconds": _int_or_default(
            row["next_retry_delay_seconds"],
            RETRY_BASE_SECONDS,
        ),
        "error": str(row["error"] or ""),
        "expectedSize": _int_or_default(row["expected_size"], 0),
        "downloadedSize": _int_or_default(row["downloaded_size"], 0),
        "localPath": str(row["local_path"] or ""),
        "verificationStatus": str(row["verification_status"] or ""),
        "lastProgressAt": _int_or_default(row["last_progress_at"], 0),
    }


def upsert_download_job(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    message_id: int,
    file_id: int,
    unique_id: str = "",
    session_id: str = "",
    source: str = "auto",
    state: str = JOB_STATE_QUEUED,
) -> dict[str, Any] | None:
    if telegram_id <= 0 or chat_id == 0 or message_id == 0 or file_id <= 0:
        return None

    now_ms = _now_ms()
    db.execute(
        """
        INSERT INTO download_job(
            telegram_id, chat_id, message_id, file_id, unique_id, session_id,
            source, state, attempts, retry_at, next_retry_delay_seconds,
            error, expected_size, downloaded_size, local_path, verification_status,
            last_progress_at, last_restart_at, created_at, updated_at,
            started_at, completed_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, '', 0, 0, '', '', ?, NULL, ?, ?, NULL, NULL)
        ON CONFLICT(telegram_id, chat_id, message_id, file_id) DO UPDATE SET
            unique_id = COALESCE(NULLIF(excluded.unique_id, ''), download_job.unique_id),
            session_id = COALESCE(NULLIF(excluded.session_id, ''), download_job.session_id),
            source = excluded.source,
            state = CASE
                WHEN download_job.state = 'completed' THEN download_job.state
                WHEN download_job.state = 'cancelled' THEN excluded.state
                ELSE excluded.state
            END,
            retry_at = excluded.retry_at,
            error = '',
            updated_at = excluded.updated_at,
            completed_at = CASE
                WHEN download_job.state = 'completed' THEN download_job.completed_at
                ELSE NULL
            END
        """,
        (
            telegram_id,
            chat_id,
            message_id,
            file_id,
            unique_id.strip(),
            session_id.strip(),
            source,
            state,
            now_ms,
            RETRY_BASE_SECONDS,
            now_ms,
            now_ms,
            now_ms,
        ),
    )
    db.commit()
    return find_download_job(
        db,
        telegram_id=telegram_id,
        chat_id=chat_id,
        message_id=message_id,
        file_id=file_id,
    )


def find_download_job(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    message_id: int,
    file_id: int,
) -> dict[str, Any] | None:
    row = db.execute(
        """
        SELECT *
        FROM download_job
        WHERE telegram_id = ? AND chat_id = ? AND message_id = ? AND file_id = ?
        LIMIT 1
        """,
        (telegram_id, chat_id, message_id, file_id),
    ).fetchone()
    return _job_to_dict(row) if row is not None else None


def due_download_jobs(
    db: sqlite3.Connection,
    *,
    limit: int = 200,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    current_ms = _now_ms() if now_ms is None else now_ms
    rows = db.execute(
        """
        SELECT *
        FROM download_job
        WHERE state IN ('queued', 'failed')
          AND (retry_at IS NULL OR retry_at <= ?)
        ORDER BY updated_at ASC
        LIMIT ?
        """,
        (current_ms, limit),
    ).fetchall()
    return [_job_to_dict(row) for row in rows]


def recover_interrupted_download_jobs(db: sqlite3.Connection) -> int:
    now_ms = _now_ms()
    cursor = db.execute(
        """
        UPDATE download_job
        SET state = 'queued',
            retry_at = ?,
            error = CASE
                WHEN TRIM(COALESCE(error, '')) = '' THEN 'Recovered after restart'
                ELSE error
            END,
            updated_at = ?
        WHERE state IN ('starting', 'monitoring')
        """,
        (now_ms, now_ms),
    )
    db.commit()
    return int(cursor.rowcount or 0)


def mark_download_job_starting(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    message_id: int,
    file_id: int,
) -> None:
    now_ms = _now_ms()
    db.execute(
        """
        UPDATE download_job
        SET state = 'starting',
            started_at = COALESCE(started_at, ?),
            updated_at = ?,
            error = ''
        WHERE telegram_id = ? AND chat_id = ? AND message_id = ? AND file_id = ?
        """,
        (now_ms, now_ms, telegram_id, chat_id, message_id, file_id),
    )
    db.commit()


def mark_download_job_monitoring(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    message_id: int,
    file_id: int,
    unique_id: str,
    expected_size: int,
    downloaded_size: int,
) -> None:
    now_ms = _now_ms()
    db.execute(
        """
        UPDATE download_job
        SET state = 'monitoring',
            unique_id = COALESCE(NULLIF(?, ''), unique_id),
            expected_size = MAX(expected_size, ?),
            downloaded_size = MAX(downloaded_size, ?),
            last_progress_at = ?,
            retry_at = NULL,
            error = '',
            updated_at = ?
        WHERE telegram_id = ? AND chat_id = ? AND message_id = ? AND file_id = ?
        """,
        (
            unique_id.strip(),
            max(0, expected_size),
            max(0, downloaded_size),
            now_ms,
            now_ms,
            telegram_id,
            chat_id,
            message_id,
            file_id,
        ),
    )
    db.commit()


def mark_download_job_failed(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    message_id: int,
    file_id: int,
    error: str,
) -> None:
    row = find_download_job(
        db,
        telegram_id=telegram_id,
        chat_id=chat_id,
        message_id=message_id,
        file_id=file_id,
    )
    attempts = _int_or_default((row or {}).get("attempts"), 0) + 1
    previous_delay = _int_or_default(
        (row or {}).get("nextRetryDelaySeconds"),
        RETRY_BASE_SECONDS,
    )
    delay = min(
        RETRY_MAX_SECONDS,
        max(RETRY_BASE_SECONDS, previous_delay * 2 if attempts > 1 else previous_delay),
    )
    now_ms = _now_ms()
    db.execute(
        """
        UPDATE download_job
        SET state = 'failed',
            attempts = ?,
            retry_at = ?,
            next_retry_delay_seconds = ?,
            error = ?,
            updated_at = ?
        WHERE telegram_id = ? AND chat_id = ? AND message_id = ? AND file_id = ?
        """,
        (
            attempts,
            now_ms + (delay * 1000),
            delay,
            str(error or "Download failed"),
            now_ms,
            telegram_id,
            chat_id,
            message_id,
            file_id,
        ),
    )
    _set_file_error(
        db,
        telegram_id=telegram_id,
        file_id=file_id,
        error=str(error or "Download failed"),
    )
    db.commit()


def record_download_job_progress(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str,
    downloaded_size: int,
    expected_size: int,
    local_path: str,
) -> None:
    row = db.execute(
        """
        SELECT *
        FROM download_job
        WHERE telegram_id = ?
          AND file_id = ?
          AND (unique_id = ? OR TRIM(COALESCE(unique_id, '')) = '')
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (telegram_id, file_id, unique_id.strip()),
    ).fetchone()
    if row is None:
        return

    previous_downloaded = _int_or_default(row["downloaded_size"], 0)
    now_ms = _now_ms()
    last_progress_at = (
        now_ms
        if downloaded_size > previous_downloaded
        else _int_or_default(row["last_progress_at"], now_ms)
    )
    db.execute(
        """
        UPDATE download_job
        SET unique_id = COALESCE(NULLIF(?, ''), unique_id),
            expected_size = MAX(expected_size, ?),
            downloaded_size = MAX(downloaded_size, ?),
            local_path = COALESCE(NULLIF(?, ''), local_path),
            last_progress_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            unique_id.strip(),
            max(0, expected_size),
            max(0, downloaded_size),
            local_path.strip(),
            last_progress_at,
            now_ms,
            _int_or_default(row["id"], 0),
        ),
    )
    db.commit()


def complete_download_job_for_file(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str,
    local_path: str,
    expected_size: int,
    downloaded_size: int,
) -> str:
    verification_status = verify_download_integrity(
        local_path=local_path,
        expected_size=expected_size,
        downloaded_size=downloaded_size,
    )
    now_ms = _now_ms()
    db.execute(
        """
        UPDATE download_job
        SET state = 'completed',
            unique_id = COALESCE(NULLIF(?, ''), unique_id),
            expected_size = MAX(expected_size, ?),
            downloaded_size = MAX(downloaded_size, ?),
            local_path = ?,
            verification_status = ?,
            retry_at = NULL,
            error = '',
            updated_at = ?,
            completed_at = ?
        WHERE telegram_id = ?
          AND file_id = ?
          AND (unique_id = ? OR TRIM(COALESCE(unique_id, '')) = '')
        """,
        (
            unique_id.strip(),
            max(0, expected_size),
            max(0, downloaded_size),
            local_path.strip(),
            verification_status,
            now_ms,
            now_ms,
            telegram_id,
            file_id,
            unique_id.strip(),
        ),
    )
    _set_file_verification(
        db,
        telegram_id=telegram_id,
        file_id=file_id,
        unique_id=unique_id,
        verification_status=verification_status,
        error="" if verification_status == VERIFICATION_COMPLETED_VERIFIED else verification_status,
    )
    db.commit()
    return verification_status


def verify_download_integrity(
    *,
    local_path: str,
    expected_size: int,
    downloaded_size: int,
) -> str:
    path_text = str(local_path or "").strip()
    if not path_text:
        return VERIFICATION_FILE_MISSING

    path = Path(path_text)
    if not path.exists() or not path.is_file():
        return VERIFICATION_FILE_MISSING

    actual_size = path.stat().st_size
    normalized_expected = max(0, expected_size)
    if normalized_expected > 0:
        return (
            VERIFICATION_COMPLETED_VERIFIED
            if actual_size == normalized_expected
            else VERIFICATION_COMPLETED_UNVERIFIED
        )

    normalized_downloaded = max(0, downloaded_size)
    if normalized_downloaded > 0:
        return (
            VERIFICATION_COMPLETED_VERIFIED
            if actual_size == normalized_downloaded
            else VERIFICATION_COMPLETED_UNVERIFIED
        )

    return VERIFICATION_COMPLETED_UNVERIFIED


def stale_monitoring_jobs(
    db: sqlite3.Connection,
    *,
    stale_after_ms: int,
    limit: int = 50,
) -> list[dict[str, Any]]:
    cutoff = _now_ms() - max(1, stale_after_ms)
    rows = db.execute(
        """
        SELECT *
        FROM download_job
        WHERE state = 'monitoring'
          AND COALESCE(last_progress_at, updated_at) < ?
        ORDER BY COALESCE(last_progress_at, updated_at) ASC
        LIMIT ?
        """,
        (cutoff, limit),
    ).fetchall()
    return [_job_to_dict(row) for row in rows]


def mark_download_job_restarted(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    message_id: int,
    file_id: int,
    error: str,
) -> None:
    now_ms = _now_ms()
    db.execute(
        """
        UPDATE download_job
        SET attempts = attempts + 1,
            error = ?,
            last_restart_at = ?,
            last_progress_at = ?,
            updated_at = ?
        WHERE telegram_id = ? AND chat_id = ? AND message_id = ? AND file_id = ?
        """,
        (
            error,
            now_ms,
            now_ms,
            now_ms,
            telegram_id,
            chat_id,
            message_id,
            file_id,
        ),
    )
    db.commit()


def cancel_download_job(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str = "",
) -> None:
    now_ms = _now_ms()
    db.execute(
        """
        UPDATE download_job
        SET state = 'cancelled',
            retry_at = NULL,
            error = '',
            updated_at = ?
        WHERE telegram_id = ?
          AND file_id = ?
          AND (? = '' OR unique_id = ?)
          AND state != 'completed'
        """,
        (now_ms, telegram_id, file_id, unique_id.strip(), unique_id.strip()),
    )
    db.commit()


def _set_file_error(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    error: str,
) -> None:
    db.execute(
        """
        UPDATE file_record
        SET download_status = 'error',
            download_error = ?
        WHERE telegram_id = ? AND id = ? AND type != 'thumbnail'
        """,
        (error, telegram_id, file_id),
    )


def _set_file_verification(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str,
    verification_status: str,
    error: str,
) -> None:
    db.execute(
        """
        UPDATE file_record
        SET verification_status = ?,
            download_error = ?
        WHERE telegram_id = ?
          AND id = ?
          AND unique_id = ?
          AND type != 'thumbnail'
        """,
        (verification_status, error, telegram_id, file_id, unique_id.strip()),
    )
