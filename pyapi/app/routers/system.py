from __future__ import annotations

import sqlite3
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from ..db import get_settings_by_keys, upsert_settings
from ..deps import get_db
from ..download_runtime import reset_speed_state
from ..offline_reset import (
    clear_offline_reset_pin,
    has_offline_reset_pin,
    reset_offline_data,
    set_offline_reset_pin,
    verify_offline_reset_pin,
)
from ..settings_keys import default_value_for
from ..tdlib_downloads import reset_tdlib_file_preview_cache
from ..tdlib_monitor import reset_tdlib_monitor_state
from ..automation_workers import reset_worker_state

router = APIRouter()


@router.get("/settings")
def settings(
    keys: str = Query(default=""), db: sqlite3.Connection = Depends(get_db)
) -> dict[str, Any]:
    key_list = [key.strip() for key in keys.split(",") if key.strip()]
    if not key_list:
        raise HTTPException(
            status_code=400, detail="Query parameter 'keys' is required."
        )

    data = get_settings_by_keys(db, key_list)
    for key in key_list:
        if key in data:
            continue
        try:
            data[key] = default_value_for(key)
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    return data


@router.post("/settings/create")
def settings_create(
    payload: dict[str, Any], db: sqlite3.Connection = Depends(get_db)
) -> Response:
    if not payload:
        raise HTTPException(
            status_code=400, detail="Request body must be a non-empty JSON object."
        )

    normalized = {
        str(key): "" if value is None else str(value) for key, value in payload.items()
    }
    upsert_settings(db, normalized)
    return Response(status_code=200)


@router.post("/settings/offline-reset-pin")
def settings_offline_reset_pin(
    payload: dict[str, Any], db: sqlite3.Connection = Depends(get_db)
) -> dict[str, bool]:
    pin = payload.get("pin") if isinstance(payload, dict) else None
    current_pin = payload.get("currentPin") if isinstance(payload, dict) else None
    try:
        set_offline_reset_pin(db, pin=pin, current_pin=current_pin)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return {"enabled": True}


@router.post("/settings/offline-reset-pin/clear")
def settings_clear_offline_reset_pin(
    payload: dict[str, Any], db: sqlite3.Connection = Depends(get_db)
) -> dict[str, bool]:
    current_pin = payload.get("currentPin") if isinstance(payload, dict) else None
    try:
        clear_offline_reset_pin(db, current_pin=current_pin)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return {"enabled": False}


@router.post("/settings/offline-data/reset")
def settings_reset_offline_data(
    request: Request,
    payload: dict[str, Any],
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, int]:
    if not has_offline_reset_pin(db):
        raise HTTPException(
            status_code=400,
            detail="Set an offline reset PIN before using this action.",
        )

    pin = payload.get("pin") if isinstance(payload, dict) else None
    try:
        if not verify_offline_reset_pin(db, pin):
            raise HTTPException(status_code=403, detail="PIN is invalid.")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result = reset_offline_data(db)
    reset_worker_state()
    reset_tdlib_monitor_state()
    reset_speed_state()
    reset_tdlib_file_preview_cache()
    del request
    return result
