from __future__ import annotations

import sqlite3
from typing import Any
from uuid import uuid4

from .douyin_store import now_ms

JOB_KINDS = {
    "source_refresh",
    "file_download",
    "batch_download",
    "frame_extract",
    "batch_frame_extract",
}
JOB_STATES = {"queued", "running", "completed", "failed", "cancelled"}
_TERMINAL_STATES = {"completed", "failed", "cancelled"}


def _int_or_zero(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def new_job_id() -> str:
    return uuid4().hex


def serialize_job(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    def _get(key: str) -> Any:
        if isinstance(row, dict):
            return row.get(key)
        return row[key]

    return {
        "id": str(_get("id") or ""),
        "sourceId": str(_get("source_id") or ""),
        "fileUniqueId": str(_get("file_unique_id") or ""),
        "url": str(_get("url") or ""),
        "kind": str(_get("kind") or ""),
        "state": str(_get("state") or ""),
        "total": _int_or_zero(_get("total")),
        "success": _int_or_zero(_get("success")),
        "failed": _int_or_zero(_get("failed")),
        "skipped": _int_or_zero(_get("skipped")),
        "step": str(_get("step") or ""),
        "error": str(_get("error") or ""),
        "createdAt": _int_or_zero(_get("created_at")),
        "updatedAt": _int_or_zero(_get("updated_at")),
        "startedAt": _int_or_zero(_get("started_at")),
        "completedAt": _int_or_zero(_get("completed_at")),
    }


def create_job(
    db: sqlite3.Connection,
    *,
    kind: str,
    source_id: str = "",
    file_unique_id: str = "",
    url: str = "",
    total: int = 0,
    state: str = "queued",
    step: str = "",
) -> dict[str, Any]:
    job_id = new_job_id()
    ts = now_ms()
    started_at = ts if state == "running" else None
    completed_at = ts if state in _TERMINAL_STATES else None
    db.execute(
        """
        INSERT INTO douyin_job(
            id, source_id, file_unique_id, url, kind, state, step,
            total, success, failed, skipped, error,
            created_at, updated_at, started_at, completed_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, '', ?, ?, ?, ?)
        """,
        (
            job_id,
            source_id,
            file_unique_id,
            url,
            kind,
            state,
            step,
            max(0, int(total or 0)),
            ts,
            ts,
            started_at,
            completed_at,
        ),
    )
    db.commit()
    job = get_job(db, job_id)
    if job is None:
        raise RuntimeError("failed to create Douyin job")
    return job


def update_job(
    db: sqlite3.Connection,
    job_id: str,
    *,
    state: str | None = None,
    total: int | None = None,
    success: int | None = None,
    failed: int | None = None,
    skipped: int | None = None,
    step: str | None = None,
    error: str | None = None,
    started_at: int | None = None,
    completed_at: int | None = None,
) -> dict[str, Any] | None:
    row = db.execute(
        "SELECT * FROM douyin_job WHERE id = ? LIMIT 1", (job_id,)
    ).fetchone()
    if row is None:
        return None

    sets: list[str] = []
    params: list[Any] = []
    if state is not None:
        sets.append("state = ?")
        params.append(state)
    if total is not None:
        sets.append("total = ?")
        params.append(max(0, int(total)))
    if success is not None:
        sets.append("success = ?")
        params.append(max(0, int(success)))
    if failed is not None:
        sets.append("failed = ?")
        params.append(max(0, int(failed)))
    if skipped is not None:
        sets.append("skipped = ?")
        params.append(max(0, int(skipped)))
    if step is not None:
        sets.append("step = ?")
        params.append(step)
    if error is not None:
        sets.append("error = ?")
        params.append(error)

    resolved_started = started_at
    if (
        resolved_started is None
        and state == "running"
        and _int_or_zero(row["started_at"]) == 0
    ):
        resolved_started = now_ms()
    if resolved_started is not None:
        sets.append("started_at = ?")
        params.append(resolved_started)

    resolved_completed = completed_at
    if resolved_completed is None and state in _TERMINAL_STATES:
        resolved_completed = now_ms()
    if resolved_completed is not None:
        sets.append("completed_at = ?")
        params.append(resolved_completed)

    sets.append("updated_at = ?")
    params.append(now_ms())
    params.append(job_id)

    db.execute(
        f"UPDATE douyin_job SET {', '.join(sets)} WHERE id = ?",
        params,
    )
    db.commit()
    return get_job(db, job_id)


def increment_job(
    db: sqlite3.Connection,
    job_id: str,
    *,
    success: int = 0,
    failed: int = 0,
    skipped: int = 0,
) -> dict[str, Any] | None:
    row = db.execute(
        "SELECT * FROM douyin_job WHERE id = ? LIMIT 1", (job_id,)
    ).fetchone()
    if row is None:
        return None
    db.execute(
        """
        UPDATE douyin_job
        SET success = success + ?,
            failed = failed + ?,
            skipped = skipped + ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            max(0, int(success)),
            max(0, int(failed)),
            max(0, int(skipped)),
            now_ms(),
            job_id,
        ),
    )
    db.commit()
    return get_job(db, job_id)


def get_job(db: sqlite3.Connection, job_id: str) -> dict[str, Any] | None:
    row = db.execute(
        "SELECT * FROM douyin_job WHERE id = ? LIMIT 1", (job_id.strip(),)
    ).fetchone()
    return serialize_job(row) if row is not None else None


def list_jobs(
    db: sqlite3.Connection,
    *,
    status: str = "",
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    status = (status or "").strip()
    if status:
        clauses.append("state = ?")
        params.append(status)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    capped = min(200, max(1, int(limit or 50)))
    rows = db.execute(
        f"""
        SELECT *
        FROM douyin_job
        {where_sql}
        ORDER BY
            CASE WHEN state IN ('queued', 'running') THEN 0 ELSE 1 END ASC,
            updated_at DESC,
            created_at DESC
        LIMIT ?
        """,
        [*params, capped],
    ).fetchall()
    return [serialize_job(row) for row in rows]


def cancel_job(db: sqlite3.Connection, job_id: str) -> dict[str, Any] | None:
    row = db.execute(
        "SELECT * FROM douyin_job WHERE id = ? LIMIT 1", (job_id.strip(),)
    ).fetchone()
    if row is None:
        return None
    if str(row["state"] or "") in _TERMINAL_STATES:
        return serialize_job(row)
    return update_job(db, job_id, state="cancelled")


def active_job_for_file(
    db: sqlite3.Connection, file_unique_id: str
) -> dict[str, Any] | None:
    row = db.execute(
        """
        SELECT *
        FROM douyin_job
        WHERE file_unique_id = ?
          AND state IN ('queued', 'running')
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (file_unique_id.strip(),),
    ).fetchone()
    return serialize_job(row) if row is not None else None
