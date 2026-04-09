import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app import app_state


class AppStateTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._session_selection = dict(app_state.SESSION_TELEGRAM_SELECTION)
        self._ws_connections = {
            session_id: set(connections)
            for session_id, connections in app_state.WS_CONNECTIONS.items()
        }

    def tearDown(self) -> None:
        app_state.SESSION_TELEGRAM_SELECTION.clear()
        app_state.SESSION_TELEGRAM_SELECTION.update(self._session_selection)
        app_state.WS_CONNECTIONS.clear()
        app_state.WS_CONNECTIONS.update(self._ws_connections)

    def test_tdlib_chat_update_payload_maps_new_message(self) -> None:
        payload = app_state._tdlib_chat_update_payload(
            "1",
            {
                "@type": "updateNewMessage",
                "message": {
                    "id": 200,
                    "chat_id": 100,
                },
            },
        )

        self.assertEqual(
            payload,
            {
                "telegramId": "1",
                "chatId": "100",
                "messageId": 200,
                "updateType": "updateNewMessage",
            },
        )

    def test_tdlib_chat_update_payload_ignores_irrelevant_updates(self) -> None:
        payload = app_state._tdlib_chat_update_payload(
            "1",
            {
                "@type": "updateConnectionState",
            },
        )

        self.assertIsNone(payload)

    async def test_handle_tdlib_update_emits_only_for_selected_sessions(self) -> None:
        app_state.SESSION_TELEGRAM_SELECTION.clear()
        app_state.SESSION_TELEGRAM_SELECTION.update(
            {
                "session-a": "1",
                "session-b": "2",
                "session-c": "1",
            }
        )

        emit_mock = AsyncMock()
        with patch("app.app_state._emit_ws_payload", emit_mock):
            await app_state._handle_tdlib_update(
                object(),
                "1",
                {
                    "@type": "updateChatLastMessage",
                    "chat_id": 100,
                },
            )

        self.assertEqual(emit_mock.await_count, 2)
        targeted_sessions = {
            call.kwargs["session_id"] for call in emit_mock.await_args_list
        }
        self.assertEqual(targeted_sessions, {"session-a", "session-c"})
        first_payload = emit_mock.await_args_list[0].args[0]
        self.assertEqual(first_payload["type"], app_state.EVENT_TYPE_CHAT_UPDATE)
        self.assertEqual(first_payload["data"]["chatId"], "100")

    async def test_handle_tdlib_update_persists_file_records_before_emitting(
        self,
    ) -> None:
        app_state.SESSION_TELEGRAM_SELECTION.clear()
        app_state.SESSION_TELEGRAM_SELECTION.update({"session-a": "1"})

        db = object()
        app = SimpleNamespace(state=SimpleNamespace(db=db))
        file_payload = {
            "id": 7,
            "telegramId": 1,
            "uniqueId": "unique-7",
            "chatId": 100,
            "messageId": 200,
        }

        emit_mock = AsyncMock()
        with (
            patch(
                "app.app_state.td_message_to_file", return_value=file_payload
            ) as map_mock,
            patch("app.app_state.upsert_tdlib_file_record") as upsert_mock,
            patch("app.app_state._emit_ws_payload", emit_mock),
        ):
            await app_state._handle_tdlib_update(
                app,
                "1",
                {
                    "@type": "updateNewMessage",
                    "message": {
                        "id": 200,
                        "chat_id": 100,
                    },
                },
            )

        map_mock.assert_called_once_with(
            1,
            {
                "id": 200,
                "chat_id": 100,
            },
        )
        upsert_mock.assert_called_once_with(db, file_payload=file_payload)
        emit_mock.assert_awaited_once()

    async def test_handle_tdlib_update_persists_file_records_without_active_sessions(
        self,
    ) -> None:
        app_state.SESSION_TELEGRAM_SELECTION.clear()

        db = object()
        app = SimpleNamespace(state=SimpleNamespace(db=db))
        file_payload = {
            "id": 9,
            "telegramId": 1,
            "uniqueId": "unique-9",
            "chatId": 101,
            "messageId": 300,
        }

        emit_mock = AsyncMock()
        with (
            patch("app.app_state.td_message_to_file", return_value=file_payload),
            patch("app.app_state.upsert_tdlib_file_record") as upsert_mock,
            patch("app.app_state._emit_ws_payload", emit_mock),
        ):
            await app_state._handle_tdlib_update(
                app,
                "1",
                {
                    "@type": "updateChatLastMessage",
                    "chat_id": 101,
                    "last_message": {
                        "id": 300,
                        "chat_id": 101,
                    },
                },
            )

        upsert_mock.assert_called_once_with(db, file_payload=file_payload)
        emit_mock.assert_not_awaited()

    async def test_emit_ws_payload_does_not_broadcast_missing_session(self) -> None:
        other_socket = AsyncMock()
        app_state.WS_CONNECTIONS.clear()
        app_state.WS_CONNECTIONS.update({"other-session": {other_socket}})

        await app_state._emit_ws_payload({"type": 1}, session_id="missing-session")

        other_socket.send_json.assert_not_awaited()
