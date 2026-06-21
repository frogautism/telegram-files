from __future__ import annotations

import sqlite3
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from ..app_state import (
    _is_pending_account,
    _session_id_from_request,
)
from ..db import (
    delete_auto_transfer_preset,
    list_auto_transfer_presets,
    save_auto_transfer_preset,
    update_auto_settings,
    update_chat_group_auto_settings,
)
from ..deps import get_db
from ..route_utils import (
    _bool_or_none,
    _int_or_default,
    _parse_batch_files,
)
from ..telegram_file_lifecycle import (
    FileLifecycleError,
    FileLifecycleNotFound,
    FileReference,
    StartDownload,
    telegram_file_lifecycle,
)

router = APIRouter()


@router.post("/{telegramId}/file/start-download")
async def file_start_download_route(
    telegramId: int,
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    chat_id = _int_or_default(payload.get("chatId"), 0)
    message_id = _int_or_default(payload.get("messageId"), 0)
    file_id = _int_or_default(payload.get("fileId"), 0)
    if chat_id == 0 or message_id == 0 or file_id == 0:
        raise HTTPException(
            status_code=400, detail="chatId, messageId and fileId are required."
        )

    session_id = _session_id_from_request(request)
    try:
        return await telegram_file_lifecycle(request.app).start(
            StartDownload(
                telegram_id=telegramId,
                chat_id=chat_id,
                message_id=message_id,
                file_id=file_id,
                source="manual",
                event_session_id=session_id,
                monitor_session_id=session_id,
            )
        )
    except FileLifecycleNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileLifecycleError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{telegramId}/file/cancel-download")
async def file_cancel_download_route(
    telegramId: int,
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    file_id = _int_or_default(payload.get("fileId"), 0)
    if file_id == 0:
        raise HTTPException(status_code=400, detail="fileId is required.")

    try:
        await telegram_file_lifecycle(request.app).cancel(
            FileReference(
                telegram_id=telegramId,
                file_id=file_id,
                unique_id=str(payload.get("uniqueId") or "").strip(),
                event_session_id=_session_id_from_request(request),
            )
        )
    except FileLifecycleNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileLifecycleError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return Response(status_code=200)


@router.post("/{telegramId}/file/toggle-pause-download")
async def file_toggle_pause_download_route(
    telegramId: int,
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    file_id = _int_or_default(payload.get("fileId"), 0)
    if file_id == 0:
        raise HTTPException(status_code=400, detail="fileId is required.")

    try:
        await telegram_file_lifecycle(request.app).toggle_pause(
            FileReference(
                telegram_id=telegramId,
                file_id=file_id,
                unique_id=str(payload.get("uniqueId") or "").strip(),
                event_session_id=_session_id_from_request(request),
            ),
            is_paused=_bool_or_none(payload.get("isPaused")),
        )
    except FileLifecycleNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileLifecycleError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return Response(status_code=200)


@router.post("/{telegramId}/file/remove")
async def file_remove_route(
    telegramId: int,
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    file_id = _int_or_default(payload.get("fileId"), 0)
    unique_id = str(payload.get("uniqueId") or "").strip()
    if file_id == 0 and not unique_id:
        raise HTTPException(status_code=400, detail="fileId or uniqueId is required.")

    try:
        await telegram_file_lifecycle(request.app).remove(
            FileReference(
                telegram_id=telegramId,
                file_id=file_id,
                unique_id=unique_id,
                event_session_id=_session_id_from_request(request),
            )
        )
    except FileLifecycleNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileLifecycleError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return Response(status_code=200)


@router.post("/{telegramId}/file/update-auto-settings")
def file_update_auto_settings_route(
    telegramId: int,
    chatId: int = Query(default=0),
    payload: dict[str, Any] | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    if chatId == 0:
        raise HTTPException(status_code=400, detail="chatId is required.")

    if _is_pending_account(str(telegramId)):
        raise HTTPException(
            status_code=400,
            detail="Pending account does not support automation settings.",
        )

    auto_payload = payload if isinstance(payload, dict) else {}
    update_auto_settings(
        db,
        telegram_id=telegramId,
        chat_id=chatId,
        auto_payload=auto_payload,
    )
    return Response(status_code=200)


@router.get("/{telegramId}/auto-transfer-presets")
def auto_transfer_presets_route(
    telegramId: int,
    db: sqlite3.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    if _is_pending_account(str(telegramId)):
        return []

    return list_auto_transfer_presets(db, telegram_id=telegramId)


@router.post("/{telegramId}/auto-transfer-presets")
def auto_transfer_preset_save_route(
    telegramId: int,
    payload: dict[str, Any] | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    if _is_pending_account(str(telegramId)):
        raise HTTPException(
            status_code=400,
            detail="Pending account does not support transfer presets.",
        )

    normalized_payload = payload if isinstance(payload, dict) else {}
    preset_id = str(normalized_payload.get("id") or uuid4().hex)
    try:
        return save_auto_transfer_preset(
            db,
            telegram_id=telegramId,
            preset_id=preset_id,
            name=str(normalized_payload.get("name") or ""),
            rule_payload=normalized_payload.get("rule"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{telegramId}/auto-transfer-presets/{presetId}/delete")
def auto_transfer_preset_delete_route(
    telegramId: int,
    presetId: str,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    if _is_pending_account(str(telegramId)):
        raise HTTPException(
            status_code=400,
            detail="Pending account does not support transfer presets.",
        )

    deleted = delete_auto_transfer_preset(
        db,
        telegram_id=telegramId,
        preset_id=presetId,
    )
    if not deleted:
        raise HTTPException(status_code=404, detail="Transfer preset not found.")
    return Response(status_code=200)


@router.post("/{telegramId}/chat-group/{groupId}/update-auto-settings")
def chat_group_update_auto_settings_route(
    telegramId: int,
    groupId: str,
    payload: dict[str, Any] | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    if _is_pending_account(str(telegramId)):
        raise HTTPException(
            status_code=400,
            detail="Pending account does not support automation settings.",
        )

    auto_payload = payload if isinstance(payload, dict) else {}
    updated = update_chat_group_auto_settings(
        db,
        telegram_id=telegramId,
        group_id=groupId,
        auto_payload=auto_payload,
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Group chat not found.")
    return Response(status_code=200)


@router.post("/files/start-download-multiple")
async def files_start_download_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    normalized_files = _parse_batch_files(payload)
    session_id = _session_id_from_request(request)
    root_path_cache: dict[int, str | None] = {}
    lifecycle = telegram_file_lifecycle(request.app)

    processed = 0
    failed = 0
    for item in normalized_files:
        if (
            item["telegramId"] <= 0
            or item["chatId"] == 0
            or item["messageId"] == 0
            or item["fileId"] == 0
        ):
            failed += 1
            continue

        try:
            await lifecycle.start(
                StartDownload(
                    telegram_id=item["telegramId"],
                    chat_id=item["chatId"],
                    message_id=item["messageId"],
                    file_id=item["fileId"],
                    source="manual",
                    event_session_id=session_id,
                    monitor_session_id=session_id,
                ),
                root_path_cache=root_path_cache,
            )
            processed += 1
        except FileLifecycleError:
            failed += 1

    if processed == 0 and failed > 0:
        raise HTTPException(
            status_code=400,
            detail="Failed to start download for the selected files.",
        )

    return {
        "processed": processed,
        "failed": failed,
    }


@router.post("/files/cancel-download-multiple")
async def files_cancel_download_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    normalized_files = _parse_batch_files(payload)
    session_id = _session_id_from_request(request)
    lifecycle = telegram_file_lifecycle(request.app)
    changed_accounts: set[int] = set()

    processed = 0
    failed = 0
    for item in normalized_files:
        if item["telegramId"] <= 0 or item["fileId"] == 0:
            failed += 1
            continue

        try:
            await lifecycle.cancel(
                FileReference(
                    telegram_id=item["telegramId"],
                    file_id=item["fileId"],
                    unique_id=item["uniqueId"],
                    event_session_id=session_id,
                ),
                emit_aggregate=False,
            )
            processed += 1
            changed_accounts.add(item["telegramId"])
        except FileLifecycleError:
            failed += 1

    for telegram_id in changed_accounts:
        await lifecycle.refresh_download_aggregate(session_id, telegram_id)

    return {
        "processed": processed,
        "failed": failed,
    }


@router.post("/files/toggle-pause-download-multiple")
async def files_toggle_pause_download_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    normalized_files = _parse_batch_files(payload)
    is_paused = _bool_or_none(payload.get("isPaused"))
    session_id = _session_id_from_request(request)
    lifecycle = telegram_file_lifecycle(request.app)
    changed_accounts: set[int] = set()

    processed = 0
    failed = 0
    for item in normalized_files:
        if item["telegramId"] <= 0 or item["fileId"] == 0:
            failed += 1
            continue

        try:
            await lifecycle.toggle_pause(
                FileReference(
                    telegram_id=item["telegramId"],
                    file_id=item["fileId"],
                    unique_id=item["uniqueId"],
                    event_session_id=session_id,
                ),
                is_paused=is_paused,
                emit_aggregate=False,
            )
            processed += 1
            changed_accounts.add(item["telegramId"])
        except FileLifecycleError:
            failed += 1

    for telegram_id in changed_accounts:
        await lifecycle.refresh_download_aggregate(session_id, telegram_id)

    return {
        "processed": processed,
        "failed": failed,
    }


@router.post("/files/remove-multiple")
async def files_remove_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    normalized_files = _parse_batch_files(payload)
    session_id = _session_id_from_request(request)
    lifecycle = telegram_file_lifecycle(request.app)
    changed_accounts: set[int] = set()

    processed = 0
    failed = 0
    for item in normalized_files:
        if item["telegramId"] <= 0 or (item["fileId"] == 0 and not item["uniqueId"]):
            failed += 1
            continue

        try:
            await lifecycle.remove(
                FileReference(
                    telegram_id=item["telegramId"],
                    file_id=item["fileId"],
                    unique_id=item["uniqueId"],
                    event_session_id=session_id,
                ),
                emit_aggregate=False,
            )
            processed += 1
            changed_accounts.add(item["telegramId"])
        except FileLifecycleError:
            failed += 1

    for telegram_id in changed_accounts:
        await lifecycle.refresh_download_aggregate(session_id, telegram_id)

    return {
        "processed": processed,
        "failed": failed,
    }
