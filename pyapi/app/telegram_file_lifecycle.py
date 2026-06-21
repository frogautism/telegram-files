from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import FastAPI

from .app_state import (
    EVENT_TYPE_FILE_DOWNLOAD,
    EVENT_TYPE_FILE_STATUS,
    EVENT_TYPE_FILE_UPDATE,
    _build_ws_payload,
    _emit_ws_payload,
    _tdlib_manager_from_app,
)
from .db import (
    cancel_file_download,
    remove_file_download,
    start_file_download,
    toggle_pause_file_download,
)
from .download_jobs import (
    cancel_download_job,
    complete_download_job_for_file,
    mark_download_job_failed,
    mark_download_job_monitoring,
    mark_download_job_starting,
    record_download_job_progress,
    upsert_download_job,
)
from .file_record_ops import upsert_tdlib_file_record
from .route_utils import _file_status_from_file_record, _int_or_default
from .tdlib_downloads import (
    start_tdlib_download_for_message,
    tdlib_cancel_download_fallback,
    tdlib_remove_file_fallback,
    tdlib_toggle_pause_download_fallback,
)

logger = logging.getLogger(__name__)


class FileLifecycleError(RuntimeError):
    pass


class FileLifecycleNotFound(FileLifecycleError):
    pass


@dataclass(frozen=True)
class StartDownload:
    telegram_id: int
    chat_id: int
    message_id: int
    file_id: int
    source: str
    event_session_id: str | None
    monitor_session_id: str


@dataclass(frozen=True)
class FileReference:
    telegram_id: int
    file_id: int
    unique_id: str
    event_session_id: str | None


@dataclass(frozen=True)
class MonitorObservation:
    session_id: str
    telegram_id: int
    file_id: int
    unique_id: str
    file_update: dict[str, Any]
    status: dict[str, Any]
    expected_size: int
    downloaded_size: int
    emit_status: bool


@dataclass(frozen=True)
class LifecycleRuntime:
    account_root_path: Callable[
        [FastAPI, sqlite3.Connection, int, dict[int, str | None] | None],
        str | None,
    ]
    update_tdlib_file_status: Callable[
        [sqlite3.Connection, int, int, str, dict[str, Any], bool],
        None,
    ]
    ensure_monitor: Callable[[FastAPI, str, int, int, str], None]
    stop_monitor: Callable[[str, int, int], None]
    emit_download_aggregate: Callable[[str, int], Awaitable[None]]
    status_payload: Callable[[dict[str, Any]], dict[str, Any]]


class TelegramFileLifecycle:
    """Owns Telegram file lifecycle transitions behind one interface."""

    def __init__(self, app: FastAPI, runtime: LifecycleRuntime) -> None:
        self._app = app
        self._runtime = runtime

    @property
    def _db(self) -> sqlite3.Connection:
        return self._app.state.db

    async def start(
        self,
        command: StartDownload,
        *,
        root_path_cache: dict[int, str | None] | None = None,
    ) -> dict[str, Any]:
        db = self._db
        file_record: dict[str, Any] | None = None
        started_via_tdlib = False
        td_manager = _tdlib_manager_from_app(self._app)
        root_path = (
            self._runtime.account_root_path(
                self._app,
                db,
                command.telegram_id,
                root_path_cache,
            )
            if td_manager is not None
            else None
        )

        if td_manager is not None and root_path is not None:
            upsert_download_job(
                db,
                telegram_id=command.telegram_id,
                chat_id=command.chat_id,
                message_id=command.message_id,
                file_id=command.file_id,
                session_id=command.monitor_session_id,
                source=command.source,
            )
            mark_download_job_starting(
                db,
                telegram_id=command.telegram_id,
                chat_id=command.chat_id,
                message_id=command.message_id,
                file_id=command.file_id,
            )
            try:
                file_record = await asyncio.to_thread(
                    start_tdlib_download_for_message,
                    td_manager,
                    db=db,
                    telegram_id=command.telegram_id,
                    root_path=root_path,
                    chat_id=command.chat_id,
                    message_id=command.message_id,
                    file_id=command.file_id,
                )
                started_via_tdlib = True
                upsert_tdlib_file_record(db, file_payload=file_record)
            except Exception as exc:
                mark_download_job_failed(
                    db,
                    telegram_id=command.telegram_id,
                    chat_id=command.chat_id,
                    message_id=command.message_id,
                    file_id=command.file_id,
                    error=str(exc),
                )
                logger.warning(
                    "TDLib start failed telegram=%s chat=%s message=%s file=%s: %s",
                    command.telegram_id,
                    command.chat_id,
                    command.message_id,
                    command.file_id,
                    exc,
                )
                raise FileLifecycleError(str(exc)) from exc

        if file_record is None:
            file_record = start_file_download(
                db,
                telegram_id=command.telegram_id,
                chat_id=command.chat_id,
                message_id=command.message_id,
                file_id=command.file_id,
            )
            if file_record is None:
                raise FileLifecycleNotFound("File not found")

        await self.emit_file_status(
            self._status_payload(file_record),
            session_id=command.event_session_id,
        )
        if started_via_tdlib:
            self._finish_start(command, file_record)
        return file_record

    async def cancel(
        self,
        reference: FileReference,
        *,
        emit_aggregate: bool = True,
    ) -> dict[str, Any]:
        db = self._db
        result = cancel_file_download(
            db,
            telegram_id=reference.telegram_id,
            file_id=reference.file_id,
            unique_id=reference.unique_id or None,
        )
        if result is None:
            result = await self._tdlib_cancel(reference)

        cancel_download_job(
            db,
            telegram_id=reference.telegram_id,
            file_id=reference.file_id,
            unique_id=str(result.get("uniqueId") or reference.unique_id),
        )
        await self._stop_and_emit(
            reference,
            result,
            emit_aggregate=emit_aggregate,
        )
        return result

    async def toggle_pause(
        self,
        reference: FileReference,
        *,
        is_paused: bool | None,
        emit_aggregate: bool = True,
    ) -> dict[str, Any]:
        db = self._db
        result = toggle_pause_file_download(
            db,
            telegram_id=reference.telegram_id,
            file_id=reference.file_id,
            is_paused=is_paused,
            unique_id=reference.unique_id or None,
        )
        if result is not None:
            await self.emit_file_status(result, session_id=reference.event_session_id)
            return result

        td_manager, root_path = self._tdlib_context(reference.telegram_id)
        try:
            result, should_monitor = await asyncio.to_thread(
                tdlib_toggle_pause_download_fallback,
                td_manager,
                telegram_id=reference.telegram_id,
                root_path=root_path,
                file_id=reference.file_id,
                unique_id=reference.unique_id,
                is_paused=is_paused,
            )
        except Exception as exc:
            raise FileLifecycleError(str(exc)) from exc

        self._runtime.update_tdlib_file_status(
            db,
            reference.telegram_id,
            reference.file_id,
            str(result.get("uniqueId") or reference.unique_id),
            result,
            False,
        )
        monitor_session_id = (
            reference.event_session_id or f"worker:{reference.telegram_id}"
        )
        if should_monitor:
            self._runtime.ensure_monitor(
                self._app,
                monitor_session_id,
                reference.telegram_id,
                reference.file_id,
                str(result.get("uniqueId") or reference.unique_id),
            )
        else:
            self._runtime.stop_monitor(
                monitor_session_id,
                reference.telegram_id,
                reference.file_id,
            )
            if emit_aggregate:
                await self.refresh_download_aggregate(
                    monitor_session_id,
                    reference.telegram_id,
                )
        await self.emit_file_status(result, session_id=reference.event_session_id)
        return result

    async def remove(
        self,
        reference: FileReference,
        *,
        emit_aggregate: bool = True,
    ) -> dict[str, Any]:
        db = self._db
        result = remove_file_download(
            db,
            telegram_id=reference.telegram_id,
            file_id=reference.file_id,
            unique_id=reference.unique_id or None,
        )
        if result is None:
            result = await self._tdlib_remove(reference)

        cancel_download_job(
            db,
            telegram_id=reference.telegram_id,
            file_id=reference.file_id,
            unique_id=str(result.get("uniqueId") or reference.unique_id),
        )
        await self._stop_and_emit(
            reference,
            result,
            emit_aggregate=emit_aggregate,
        )
        return result

    async def observe(self, observation: MonitorObservation) -> None:
        self._runtime.update_tdlib_file_status(
            self._db,
            observation.telegram_id,
            observation.file_id,
            observation.unique_id,
            observation.status,
            False,
        )
        record_download_job_progress(
            self._db,
            telegram_id=observation.telegram_id,
            file_id=observation.file_id,
            unique_id=observation.unique_id,
            downloaded_size=observation.downloaded_size,
            expected_size=observation.expected_size,
            local_path=str(observation.status.get("localPath") or ""),
        )
        await _emit_ws_payload(
            _build_ws_payload(
                EVENT_TYPE_FILE_UPDATE,
                {"file": observation.file_update},
            ),
            session_id=observation.session_id,
        )
        if observation.emit_status:
            await self.emit_file_status(
                observation.status,
                session_id=observation.session_id,
            )
        if str(observation.status.get("downloadStatus") or "") == "completed":
            complete_download_job_for_file(
                self._db,
                telegram_id=observation.telegram_id,
                file_id=observation.file_id,
                unique_id=observation.unique_id,
                local_path=str(observation.status.get("localPath") or ""),
                expected_size=observation.expected_size,
                downloaded_size=observation.downloaded_size,
            )

    async def emit_file_status(
        self,
        payload: dict[str, Any],
        *,
        session_id: str | None,
    ) -> None:
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_STATUS, payload),
            session_id=session_id,
        )

    async def emit_download_aggregate(
        self,
        session_id: str,
        payload: dict[str, Any],
    ) -> None:
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_DOWNLOAD, payload),
            session_id=session_id,
        )

    async def refresh_download_aggregate(
        self,
        session_id: str,
        telegram_id: int,
    ) -> None:
        await self._runtime.emit_download_aggregate(session_id, telegram_id)

    def _finish_start(
        self,
        command: StartDownload,
        file_record: dict[str, Any],
    ) -> None:
        status = str(file_record.get("downloadStatus") or "").strip()
        unique_id = str(file_record.get("uniqueId") or "").strip()
        monitor_file_id = _int_or_default(file_record.get("id"), command.file_id)
        if status == "downloading":
            mark_download_job_monitoring(
                self._db,
                telegram_id=command.telegram_id,
                chat_id=command.chat_id,
                message_id=command.message_id,
                file_id=command.file_id,
                unique_id=unique_id,
                expected_size=_int_or_default(file_record.get("size"), 0),
                downloaded_size=_int_or_default(file_record.get("downloadedSize"), 0),
            )
            self._runtime.ensure_monitor(
                self._app,
                command.monitor_session_id,
                command.telegram_id,
                monitor_file_id,
                unique_id,
            )
        elif status == "completed":
            complete_download_job_for_file(
                self._db,
                telegram_id=command.telegram_id,
                file_id=monitor_file_id,
                unique_id=unique_id,
                local_path=str(file_record.get("localPath") or ""),
                expected_size=_int_or_default(file_record.get("size"), 0),
                downloaded_size=_int_or_default(file_record.get("downloadedSize"), 0),
            )

    async def _tdlib_cancel(self, reference: FileReference) -> dict[str, Any]:
        td_manager, root_path = self._tdlib_context(reference.telegram_id)
        try:
            result = await asyncio.to_thread(
                tdlib_cancel_download_fallback,
                td_manager,
                telegram_id=reference.telegram_id,
                root_path=root_path,
                file_id=reference.file_id,
                unique_id=reference.unique_id,
            )
        except Exception as exc:
            raise FileLifecycleError(str(exc)) from exc
        self._runtime.update_tdlib_file_status(
            self._db,
            reference.telegram_id,
            reference.file_id,
            str(result.get("uniqueId") or reference.unique_id),
            result,
            True,
        )
        return result

    async def _tdlib_remove(self, reference: FileReference) -> dict[str, Any]:
        td_manager, root_path = self._tdlib_context(reference.telegram_id)
        try:
            result = await asyncio.to_thread(
                tdlib_remove_file_fallback,
                td_manager,
                telegram_id=reference.telegram_id,
                root_path=root_path,
                file_id=reference.file_id,
                unique_id=reference.unique_id,
            )
        except Exception as exc:
            raise FileLifecycleError(str(exc)) from exc
        self._runtime.update_tdlib_file_status(
            self._db,
            reference.telegram_id,
            reference.file_id,
            str(result.get("uniqueId") or reference.unique_id),
            result,
            True,
        )
        return result

    def _tdlib_context(self, telegram_id: int) -> tuple[Any, str]:
        td_manager = _tdlib_manager_from_app(self._app)
        root_path = (
            self._runtime.account_root_path(self._app, self._db, telegram_id, None)
            if td_manager is not None
            else None
        )
        if td_manager is None or root_path is None:
            raise FileLifecycleNotFound("File not found")
        return td_manager, root_path

    async def _stop_and_emit(
        self,
        reference: FileReference,
        result: dict[str, Any],
        *,
        emit_aggregate: bool,
    ) -> None:
        monitor_session_id = (
            reference.event_session_id or f"worker:{reference.telegram_id}"
        )
        self._runtime.stop_monitor(
            monitor_session_id,
            reference.telegram_id,
            reference.file_id,
        )
        if emit_aggregate:
            await self.refresh_download_aggregate(
                monitor_session_id,
                reference.telegram_id,
            )
        await self.emit_file_status(result, session_id=reference.event_session_id)

    def _status_payload(self, file_record: dict[str, Any]) -> dict[str, Any]:
        if "messageId" in file_record:
            return _file_status_from_file_record(file_record)
        return self._runtime.status_payload(file_record)


def telegram_file_lifecycle(app: FastAPI) -> TelegramFileLifecycle:
    lifecycle = getattr(app.state, "telegram_file_lifecycle", None)
    if not isinstance(lifecycle, TelegramFileLifecycle):
        raise RuntimeError("Telegram file lifecycle is not initialized")
    return lifecycle
