from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
from pathlib import Path
from typing import Any
from urllib import error, request


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _file_md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as source:
        while True:
            block = source.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    index = 1
    while True:
        candidate = parent / f"{stem}-{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def _row_prompt_fields(row: sqlite3.Row) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for key in row.keys():
        fields[key] = row[key]
        if "_" not in key:
            continue
        parts = [part for part in key.split("_") if part]
        if not parts:
            continue
        camel_name = parts[0] + "".join(
            part[:1].upper() + part[1:] for part in parts[1:]
        )
        fields.setdefault(camel_name, row[key])
    return fields


def _render_prompt_template(template: str, row: sqlite3.Row) -> str:
    fields = _row_prompt_fields(row)

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = fields.get(key)
        if value is None:
            return ""
        return str(value)

    return re.sub(r"\{([A-Za-z0-9_]+)\}", _replace, template)


def _extract_openai_message_content(payload: dict[str, Any]) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("openai response did not include choices")

    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    if not isinstance(message, dict):
        raise RuntimeError("openai response did not include a message")

    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if text is None:
                continue
            chunks.append(str(text))
        if chunks:
            return "\n".join(chunks).strip()

    raise RuntimeError("openai response content was empty")


def _parse_ai_classification(raw_content: str) -> tuple[str, str]:
    content = raw_content.strip()
    if content.startswith("```"):
        lines = [line for line in content.splitlines() if not line.startswith("```")]
        content = "\n".join(lines).strip()

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        stripped = content.strip().strip('"').strip("'")
        if not stripped:
            raise RuntimeError("ai classification response was empty")
        return stripped, ""

    if not isinstance(parsed, dict):
        raise RuntimeError("ai classification response must be a JSON object")

    path_value = str(parsed.get("path") or "").strip()
    reason_value = str(parsed.get("reason") or "").strip()
    if not path_value:
        raise RuntimeError("ai classification response is missing 'path'")
    return path_value, reason_value


def _normalize_ai_path(path_value: str) -> Path:
    normalized = str(path_value or "").strip().replace("\\", "/")
    parts = [part for part in normalized.split("/") if part not in {"", ".", ".."}]
    if not parts:
        raise RuntimeError("ai classification produced an empty path")
    return Path(*parts)


def _classify_ai_path(row: sqlite3.Row, rule: dict[str, Any]) -> Path:
    extra = rule.get("extra") if isinstance(rule.get("extra"), dict) else {}
    prompt_template = str(extra.get("promptTemplate") or "").strip()
    if not prompt_template:
        raise RuntimeError("prompt template is required for GROUP_BY_AI transfers")

    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for GROUP_BY_AI transfers")

    model = str(os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip() or "gpt-4o-mini"
    timeout_seconds = float(os.getenv("OPENAI_TIMEOUT_SECONDS") or "30")
    base_url = (
        str(os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1")
        .strip()
        .rstrip("/")
    )

    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "Classify the file into a destination path under the configured root. "
                    'Reply with JSON only using the shape {"path": "relative/path", "reason": "short reason"}. '
                    "The path must be relative, never absolute."
                ),
            },
            {
                "role": "user",
                "content": _render_prompt_template(prompt_template, row),
            },
        ],
    }

    req = request.Request(
        url=f"{base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"openai request failed: {exc.code} {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"openai request failed: {exc.reason}") from exc

    raw_content = _extract_openai_message_content(response_payload)
    classified_path, _ = _parse_ai_classification(raw_content)
    return _normalize_ai_path(classified_path)


def _transfer_target_path(row: sqlite3.Row, rule: dict[str, Any]) -> Path:
    destination = str(rule.get("destination") or "").strip()
    if not destination:
        raise RuntimeError("transfer destination is required")

    source_path = Path(str(row["local_path"] or "")).resolve()
    base_name = source_path.name
    transfer_policy = str(rule.get("transferPolicy") or "GROUP_BY_CHAT").upper()

    root = Path(destination)
    if transfer_policy == "DIRECT":
        return root / base_name
    if transfer_policy == "GROUP_BY_CHAT":
        return (
            root
            / str(_int_or_default(row["telegram_id"], 0))
            / str(_int_or_default(row["chat_id"], 0))
            / base_name
        )
    if transfer_policy == "GROUP_BY_TYPE":
        return root / str(row["type"] or "file") / base_name
    if transfer_policy == "GROUP_BY_AI":
        classified_path = _classify_ai_path(row, rule)
        if classified_path.suffix:
            return root / classified_path
        return root / classified_path / base_name

    raise RuntimeError(f"unsupported transfer policy: {transfer_policy}")


def execute_transfer(
    row: sqlite3.Row,
    rule: dict[str, Any],
) -> tuple[str, str | None]:
    source_path = Path(str(row["local_path"] or "")).resolve()
    if not source_path.exists() or not source_path.is_file():
        raise RuntimeError("source file not found")

    target = _transfer_target_path(row, rule).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    duplication_policy = str(rule.get("duplicationPolicy") or "OVERWRITE").upper()
    if target.exists():
        if duplication_policy == "SKIP":
            return "idle", str(source_path)
        if duplication_policy == "RENAME":
            target = _unique_path(target)
        elif duplication_policy == "HASH":
            if target.is_file() and _file_md5(source_path) == _file_md5(target):
                source_path.unlink(missing_ok=True)
                return "completed", str(target)
            target = _unique_path(target)
        elif duplication_policy == "OVERWRITE":
            if target.is_file():
                target.unlink(missing_ok=True)
        else:
            raise RuntimeError(f"unsupported duplication policy: {duplication_policy}")

    shutil.move(str(source_path), str(target))
    return "completed", str(target)
