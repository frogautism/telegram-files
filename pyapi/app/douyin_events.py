from __future__ import annotations

from typing import Any

from .app_state import (
    EVENT_TYPE_DOUYIN_JOB,
    EVENT_TYPE_FILE_STATUS,
    _build_ws_payload,
    _emit_ws_payload,
)


async def emit_douyin_file_status(
    payload: dict[str, Any], *, session_id: str = ""
) -> None:
    await _emit_ws_payload(
        _build_ws_payload(EVENT_TYPE_FILE_STATUS, payload),
        session_id=session_id or None,
    )


async def emit_douyin_job(payload: dict[str, Any], *, session_id: str = "") -> None:
    await _emit_ws_payload(
        _build_ws_payload(EVENT_TYPE_DOUYIN_JOB, payload),
        session_id=session_id or None,
    )


def file_status_event(
    file_payload: dict[str, Any], *, removed: bool | None = None
) -> dict[str, Any]:
    """Shape a serialized Douyin file row into the WS file-status event body.

    The download coordinator, source discovery, and cancellation all emit the
    same cherry-picked subset of fields; this keeps that shape in one place.
    """
    event = {
        "source": "douyin",
        "fileId": file_payload["id"],
        "uniqueId": file_payload["uniqueId"],
        "downloadStatus": file_payload["downloadStatus"],
        "localPath": file_payload["localPath"],
        "completionDate": file_payload["completionDate"],
        "downloadedSize": file_payload["downloadedSize"],
        "transferStatus": file_payload["transferStatus"],
    }
    if removed is not None:
        event["removed"] = removed
    return event
