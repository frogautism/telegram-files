from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
from typing import Any

PIN_ENABLED_KEY = "offlineResetPinEnabled"
PIN_HASH_KEY = "_offlineResetPinHash"
PIN_SALT_KEY = "_offlineResetPinSalt"
PIN_ITERATIONS = 120_000


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_pin(pin: Any) -> str:
    normalized = _text(pin)
    if not normalized:
        raise ValueError("PIN is required.")
    if not normalized.isdigit():
        raise ValueError("PIN must contain only digits.")
    if len(normalized) < 4 or len(normalized) > 12:
        raise ValueError("PIN must be between 4 and 12 digits.")
    return normalized


def _hash_pin(pin: str, *, salt: bytes) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256",
        pin.encode("utf-8"),
        salt,
        PIN_ITERATIONS,
    ).hex()


def has_offline_reset_pin(db: sqlite3.Connection) -> bool:
    row = db.execute(
        "SELECT value FROM setting_record WHERE key = ? LIMIT 1",
        (PIN_HASH_KEY,),
    ).fetchone()
    return bool(_text(row["value"]) if row is not None else "")


def verify_offline_reset_pin(db: sqlite3.Connection, pin: Any) -> bool:
    normalized_pin = _normalize_pin(pin)
    row = db.execute(
        "SELECT key, value FROM setting_record WHERE key IN (?, ?)",
        (PIN_HASH_KEY, PIN_SALT_KEY),
    ).fetchall()
    values = {str(item["key"] or ""): _text(item["value"]) for item in row}
    stored_hash = values.get(PIN_HASH_KEY, "")
    stored_salt = values.get(PIN_SALT_KEY, "")
    if not stored_hash or not stored_salt:
        return False
    try:
        salt = bytes.fromhex(stored_salt)
    except ValueError:
        return False
    candidate = _hash_pin(normalized_pin, salt=salt)
    return hmac.compare_digest(candidate, stored_hash)


def set_offline_reset_pin(
    db: sqlite3.Connection,
    *,
    pin: Any,
    current_pin: Any | None = None,
) -> None:
    normalized_pin = _normalize_pin(pin)
    if has_offline_reset_pin(db):
        if current_pin is None or not verify_offline_reset_pin(db, current_pin):
            raise PermissionError("Current PIN is invalid.")

    salt = secrets.token_bytes(16)
    pin_hash = _hash_pin(normalized_pin, salt=salt)
    db.executemany(
        """
        INSERT INTO setting_record(key, value)
        VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        [
            (PIN_HASH_KEY, pin_hash),
            (PIN_SALT_KEY, salt.hex()),
            (PIN_ENABLED_KEY, "true"),
        ],
    )
    db.commit()


def clear_offline_reset_pin(db: sqlite3.Connection, *, current_pin: Any) -> None:
    if not has_offline_reset_pin(db):
        db.execute(
            """
            INSERT INTO setting_record(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (PIN_ENABLED_KEY, "false"),
        )
        db.commit()
        return

    if not verify_offline_reset_pin(db, current_pin):
        raise PermissionError("Current PIN is invalid.")

    db.execute(
        "DELETE FROM setting_record WHERE key IN (?, ?)",
        (PIN_HASH_KEY, PIN_SALT_KEY),
    )
    db.execute(
        """
        INSERT INTO setting_record(key, value)
        VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (PIN_ENABLED_KEY, "false"),
    )
    db.commit()


def _reset_automation_payload(raw: Any) -> str:
    payload = raw if isinstance(raw, str) else _text(raw)
    if not payload:
        return ""
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return payload

    if isinstance(parsed, dict) and isinstance(parsed.get("automations"), list):
        items = parsed["automations"]
        for item in items:
            if not isinstance(item, dict):
                continue
            item["state"] = 0
            preload = (
                item.get("preload") if isinstance(item.get("preload"), dict) else {}
            )
            preload["nextFromMessageId"] = 0
            item["preload"] = preload
            download = (
                item.get("download") if isinstance(item.get("download"), dict) else {}
            )
            download["nextFileType"] = ""
            download["nextFromMessageId"] = 0
            item["download"] = download
        return json.dumps(parsed, separators=(",", ":"), ensure_ascii=False)

    if isinstance(parsed, list):
        for item in parsed:
            if not isinstance(item, dict):
                continue
            item["state"] = 0
            preload = (
                item.get("preload") if isinstance(item.get("preload"), dict) else {}
            )
            preload["nextFromMessageId"] = 0
            item["preload"] = preload
            download = (
                item.get("download") if isinstance(item.get("download"), dict) else {}
            )
            download["nextFileType"] = ""
            download["nextFromMessageId"] = 0
            item["download"] = download
        return json.dumps(parsed, separators=(",", ":"), ensure_ascii=False)

    return payload


def _reset_group_auto_payload(raw: Any) -> str:
    payload = raw if isinstance(raw, str) else _text(raw)
    if not payload:
        return payload
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return payload
    if not isinstance(parsed, dict):
        return payload

    parsed["state"] = 0
    preload_root = (
        parsed.get("preload") if isinstance(parsed.get("preload"), dict) else {}
    )
    preload_root["nextFromMessageId"] = 0
    parsed["preload"] = preload_root
    download_root = (
        parsed.get("download") if isinstance(parsed.get("download"), dict) else {}
    )
    download_root["nextFileType"] = ""
    download_root["nextFromMessageId"] = 0
    parsed["download"] = download_root
    progress_map = parsed.get("progressByChat")
    if isinstance(progress_map, dict):
        for value in progress_map.values():
            if not isinstance(value, dict):
                continue
            value["state"] = 0
            preload = (
                value.get("preload") if isinstance(value.get("preload"), dict) else {}
            )
            preload["nextFromMessageId"] = 0
            value["preload"] = preload
            download = (
                value.get("download") if isinstance(value.get("download"), dict) else {}
            )
            download["nextFileType"] = ""
            download["nextFromMessageId"] = 0
            value["download"] = download
    return json.dumps(parsed, separators=(",", ":"), ensure_ascii=False)


def reset_offline_data(db: sqlite3.Connection) -> dict[str, int]:
    file_row = db.execute("SELECT COUNT(*) AS count FROM file_record").fetchone()
    statistic_row = db.execute(
        "SELECT COUNT(*) AS count FROM statistic_record"
    ).fetchone()
    automation_row = db.execute(
        "SELECT value FROM setting_record WHERE key = 'automation' LIMIT 1"
    ).fetchone()
    group_rows = db.execute(
        "SELECT id, auto_settings FROM chat_group_record"
    ).fetchall()

    file_count = int(file_row["count"] or 0) if file_row is not None else 0
    statistic_count = (
        int(statistic_row["count"] or 0) if statistic_row is not None else 0
    )

    db.execute("DELETE FROM file_record")
    db.execute("DELETE FROM statistic_record")

    if automation_row is not None:
        db.execute(
            "UPDATE setting_record SET value = ? WHERE key = 'automation'",
            (_reset_automation_payload(automation_row["value"]),),
        )

    group_reset_count = 0
    for row in group_rows:
        db.execute(
            "UPDATE chat_group_record SET auto_settings = ? WHERE id = ?",
            (_reset_group_auto_payload(row["auto_settings"]), _text(row["id"])),
        )
        group_reset_count += 1

    db.commit()
    return {
        "filesDeleted": file_count,
        "statisticsDeleted": statistic_count,
        "automationStateReset": 1 if automation_row is not None else 0,
        "groupAutomationReset": group_reset_count,
    }
