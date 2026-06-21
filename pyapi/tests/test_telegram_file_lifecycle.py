import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI

from app.db import init_schema
from app.download_jobs import find_download_job, upsert_download_job
from app.file_record_ops import upsert_tdlib_file_record
from app.telegram_file_lifecycle import (
    FileReference,
    LifecycleRuntime,
    MonitorObservation,
    StartDownload,
    TelegramFileLifecycle,
)


class TelegramFileLifecycleTest(unittest.TestCase):
    def _lifecycle(
        self,
        runtime: LifecycleRuntime | None = None,
    ) -> tuple[TelegramFileLifecycle, sqlite3.Connection]:
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        self.addCleanup(conn.close)
        app = FastAPI()
        app.state.db = conn
        app.state.tdlib_manager = None
        lifecycle = TelegramFileLifecycle(
            app,
            runtime
            or LifecycleRuntime(
                account_root_path=lambda *_args, **_kwargs: None,
                update_tdlib_file_status=lambda *_args, **_kwargs: None,
                ensure_monitor=lambda *_args, **_kwargs: None,
                stop_monitor=lambda *_args, **_kwargs: None,
                emit_download_aggregate=AsyncMock(),
                status_payload=lambda payload: payload,
            ),
        )
        app.state.telegram_file_lifecycle = lifecycle
        return lifecycle, conn

    def _insert_file(
        self,
        conn: sqlite3.Connection,
        *,
        status: str = "idle",
        local_path: str = "",
        downloaded_size: int = 0,
    ) -> None:
        upsert_tdlib_file_record(
            conn,
            file_payload={
                "id": 300,
                "telegramId": 1,
                "uniqueId": "unique-300",
                "messageId": 200,
                "chatId": 100,
                "mediaAlbumId": 0,
                "fileName": "file.bin",
                "type": "file",
                "mimeType": "application/octet-stream",
                "size": 4,
                "downloadedSize": downloaded_size,
                "thumbnail": "",
                "downloadStatus": status,
                "date": 1710000000,
                "caption": "",
                "localPath": local_path,
                "hasSensitiveContent": False,
                "startDate": 0,
                "completionDate": 0,
                "transferStatus": "idle",
                "extra": None,
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
            },
        )

    def test_offline_actions_cross_one_lifecycle_interface(self) -> None:
        lifecycle, conn = self._lifecycle()
        self._insert_file(conn)
        emitted: list[dict] = []

        async def _capture(payload, session_id=None):
            emitted.append({"payload": payload, "sessionId": session_id})

        with patch(
            "app.telegram_file_lifecycle._emit_ws_payload",
            side_effect=_capture,
        ):
            started = asyncio.run(
                lifecycle.start(
                    StartDownload(
                        telegram_id=1,
                        chat_id=100,
                        message_id=200,
                        file_id=300,
                        source="manual",
                        event_session_id="session-a",
                        monitor_session_id="session-a",
                    )
                )
            )
            paused = asyncio.run(
                lifecycle.toggle_pause(
                    FileReference(1, 300, "unique-300", "session-a"),
                    is_paused=True,
                )
            )
            cancelled = asyncio.run(
                lifecycle.cancel(FileReference(1, 300, "unique-300", "session-a"))
            )

        self.assertEqual(started["downloadStatus"], "downloading")
        self.assertEqual(paused["downloadStatus"], "paused")
        self.assertEqual(cancelled["downloadStatus"], "idle")
        self.assertEqual(len(emitted), 3)
        self.assertTrue(all(item["sessionId"] == "session-a" for item in emitted))

    def test_monitor_observation_updates_job_and_emits_completion(self) -> None:
        lifecycle, conn = self._lifecycle()
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "file.bin"
            path.write_bytes(b"abcd")
            self._insert_file(
                conn,
                status="downloading",
                local_path=str(path),
                downloaded_size=0,
            )
            upsert_download_job(
                conn,
                telegram_id=1,
                chat_id=100,
                message_id=200,
                file_id=300,
                unique_id="unique-300",
            )
            emitted: list[dict] = []

            async def _capture(payload, session_id=None):
                emitted.append({"payload": payload, "sessionId": session_id})

            with patch(
                "app.telegram_file_lifecycle._emit_ws_payload",
                side_effect=_capture,
            ):
                asyncio.run(
                    lifecycle.observe(
                        MonitorObservation(
                            session_id="session-a",
                            telegram_id=1,
                            file_id=300,
                            unique_id="unique-300",
                            file_update={"id": 300},
                            status={
                                "fileId": 300,
                                "uniqueId": "unique-300",
                                "downloadStatus": "completed",
                                "localPath": str(path),
                                "downloadedSize": 4,
                            },
                            expected_size=4,
                            downloaded_size=4,
                            emit_status=True,
                        )
                    )
                )

        job = find_download_job(
            conn,
            telegram_id=1,
            chat_id=100,
            message_id=200,
            file_id=300,
        )
        self.assertEqual(job["state"], "completed")
        self.assertEqual(job["verificationStatus"], "completed_verified")
        self.assertEqual([item["payload"]["type"] for item in emitted], [3, 5])

    def test_tdlib_start_owns_job_transition_and_monitor_start(self) -> None:
        monitor_calls: list[tuple] = []
        lifecycle, conn = self._lifecycle(
            LifecycleRuntime(
                account_root_path=lambda *_args, **_kwargs: "D:/tdlib/account-1",
                update_tdlib_file_status=lambda *_args, **_kwargs: None,
                ensure_monitor=lambda *args: monitor_calls.append(args),
                stop_monitor=lambda *_args, **_kwargs: None,
                emit_download_aggregate=AsyncMock(),
                status_payload=lambda payload: payload,
            )
        )
        file_record = {
            "id": 300,
            "telegramId": 1,
            "uniqueId": "unique-300",
            "messageId": 200,
            "chatId": 100,
            "mediaAlbumId": 0,
            "fileName": "file.bin",
            "type": "file",
            "mimeType": "application/octet-stream",
            "size": 4,
            "downloadedSize": 1,
            "thumbnail": "",
            "downloadStatus": "downloading",
            "date": 1710000000,
            "caption": "",
            "localPath": "",
            "hasSensitiveContent": False,
            "startDate": 1710000000000,
            "completionDate": 0,
            "transferStatus": "idle",
            "extra": None,
            "threadChatId": 0,
            "messageThreadId": 0,
            "reactionCount": 0,
        }

        with (
            patch(
                "app.telegram_file_lifecycle._tdlib_manager_from_app",
                return_value=object(),
            ),
            patch(
                "app.telegram_file_lifecycle.start_tdlib_download_for_message",
                return_value=file_record,
            ),
            patch(
                "app.telegram_file_lifecycle._emit_ws_payload",
                new=AsyncMock(),
            ),
        ):
            result = asyncio.run(
                lifecycle.start(
                    StartDownload(
                        telegram_id=1,
                        chat_id=100,
                        message_id=200,
                        file_id=300,
                        source="manual",
                        event_session_id="session-a",
                        monitor_session_id="session-a",
                    )
                )
            )

        job = find_download_job(
            conn,
            telegram_id=1,
            chat_id=100,
            message_id=200,
            file_id=300,
        )
        self.assertEqual(result["downloadStatus"], "downloading")
        self.assertEqual(job["state"], "monitoring")
        self.assertEqual(job["downloadedSize"], 1)
        self.assertEqual(monitor_calls[0][1:], ("session-a", 1, 300, "unique-300"))

    def test_tdlib_fallback_actions_keep_effects_behind_interface(self) -> None:
        update_status = Mock()
        ensure_monitor = Mock()
        stop_monitor = Mock()
        emit_aggregate = AsyncMock()
        lifecycle, _ = self._lifecycle(
            LifecycleRuntime(
                account_root_path=lambda *_args, **_kwargs: "D:/tdlib/account-1",
                update_tdlib_file_status=update_status,
                ensure_monitor=ensure_monitor,
                stop_monitor=stop_monitor,
                emit_download_aggregate=emit_aggregate,
                status_payload=lambda payload: payload,
            )
        )
        reference = FileReference(1, 300, "unique-300", "session-a")

        with (
            patch(
                "app.telegram_file_lifecycle._tdlib_manager_from_app",
                return_value=object(),
            ),
            patch(
                "app.telegram_file_lifecycle.cancel_file_download",
                return_value=None,
            ),
            patch(
                "app.telegram_file_lifecycle.remove_file_download",
                return_value=None,
            ),
            patch(
                "app.telegram_file_lifecycle.toggle_pause_file_download",
                return_value=None,
            ),
            patch(
                "app.telegram_file_lifecycle.tdlib_cancel_download_fallback",
                return_value={
                    "uniqueId": "unique-300",
                    "downloadStatus": "idle",
                },
            ),
            patch(
                "app.telegram_file_lifecycle.tdlib_remove_file_fallback",
                return_value={
                    "uniqueId": "unique-300",
                    "downloadStatus": "idle",
                },
            ),
            patch(
                "app.telegram_file_lifecycle.tdlib_toggle_pause_download_fallback",
                side_effect=[
                    (
                        {
                            "uniqueId": "unique-300",
                            "downloadStatus": "downloading",
                        },
                        True,
                    ),
                    (
                        {
                            "uniqueId": "unique-300",
                            "downloadStatus": "paused",
                        },
                        False,
                    ),
                ],
            ),
            patch(
                "app.telegram_file_lifecycle._emit_ws_payload",
                new=AsyncMock(),
            ),
        ):
            asyncio.run(lifecycle.cancel(reference))
            asyncio.run(lifecycle.remove(reference))
            asyncio.run(lifecycle.toggle_pause(reference, is_paused=False))
            asyncio.run(lifecycle.toggle_pause(reference, is_paused=True))

        self.assertEqual(update_status.call_count, 4)
        self.assertEqual(
            [call.args[-1] for call in update_status.call_args_list],
            [True, True, False, False],
        )
        ensure_monitor.assert_called_once()
        self.assertEqual(stop_monitor.call_count, 3)
        self.assertEqual(emit_aggregate.await_count, 3)

    def test_deferred_aggregate_refreshes_once_for_batch_callers(self) -> None:
        emit_aggregate = AsyncMock()
        lifecycle, _ = self._lifecycle(
            LifecycleRuntime(
                account_root_path=lambda *_args, **_kwargs: None,
                update_tdlib_file_status=lambda *_args, **_kwargs: None,
                ensure_monitor=lambda *_args, **_kwargs: None,
                stop_monitor=lambda *_args, **_kwargs: None,
                emit_download_aggregate=emit_aggregate,
                status_payload=lambda payload: payload,
            )
        )

        with (
            patch(
                "app.telegram_file_lifecycle.cancel_file_download",
                return_value={
                    "uniqueId": "unique-300",
                    "downloadStatus": "idle",
                },
            ),
            patch(
                "app.telegram_file_lifecycle._emit_ws_payload",
                new=AsyncMock(),
            ),
        ):
            asyncio.run(
                lifecycle.cancel(
                    FileReference(1, 300, "unique-300", "session-a"),
                    emit_aggregate=False,
                )
            )
            asyncio.run(
                lifecycle.cancel(
                    FileReference(1, 301, "unique-301", "session-a"),
                    emit_aggregate=False,
                )
            )
            asyncio.run(lifecycle.refresh_download_aggregate("session-a", 1))

        emit_aggregate.assert_awaited_once_with("session-a", 1)


if __name__ == "__main__":
    unittest.main()
