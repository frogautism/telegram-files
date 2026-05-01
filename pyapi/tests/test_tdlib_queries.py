import sqlite3
import unittest
from unittest.mock import patch

from app.db import init_schema
from app.file_record_ops import upsert_tdlib_file_record
from app.tdlib_queries import load_tdlib_chat_files


def _photo_message(message_id: int) -> dict:
    return {
        "@type": "message",
        "id": message_id,
        "chat_id": 100,
        "date": 1710000000 + message_id,
        "message_thread_id": 0,
        "media_album_id": 0,
        "content": {
            "@type": "messagePhoto",
            "caption": {"text": f"photo {message_id}"},
            "has_spoiler": False,
            "photo": {
                "sizes": [
                    {
                        "width": 640,
                        "height": 480,
                        "type": "x",
                        "photo": {
                            "id": 1000 + message_id,
                            "size": 1234,
                            "expected_size": 1234,
                            "local": {
                                "is_downloading_completed": False,
                                "is_downloading_active": False,
                                "downloaded_size": 0,
                                "path": "",
                            },
                            "remote": {
                                "id": f"remote-{message_id}",
                                "unique_id": f"photo-{message_id}",
                            },
                        },
                    }
                ],
                "minithumbnail": {"data": "thumb"},
            },
        },
    }


class _FakeTdlibQueryManager:
    def request(self, account_key: str, payload: dict, timeout_seconds: float):
        request_type = str(payload.get("@type") or "")
        if request_type != "getChatHistory":
            raise AssertionError(f"Unexpected TDLib request: {request_type}")

        return {
            "@type": "messages",
            "messages": [
                {
                    "@type": "message",
                    "id": 200,
                    "chat_id": 100,
                    "date": 1710000000,
                    "message_thread_id": 0,
                    "media_album_id": 0,
                    "content": {
                        "@type": "messagePhoto",
                        "caption": {"text": "same photo again"},
                        "has_spoiler": False,
                        "photo": {
                            "sizes": [
                                {
                                    "width": 640,
                                    "height": 480,
                                    "type": "x",
                                    "photo": {
                                        "id": 321,
                                        "size": 1234,
                                        "expected_size": 1234,
                                        "local": {
                                            "is_downloading_completed": False,
                                            "is_downloading_active": False,
                                            "downloaded_size": 0,
                                            "path": "",
                                        },
                                        "remote": {
                                            "id": "remote-321",
                                            "unique_id": "dup-photo-1",
                                        },
                                    },
                                }
                            ],
                            "minithumbnail": {"data": "thumb"},
                        },
                    },
                }
            ],
        }


class _PagedTdlibQueryManager:
    def request(self, account_key: str, payload: dict, timeout_seconds: float):
        request_type = str(payload.get("@type") or "")
        if request_type != "getChatHistory":
            raise AssertionError(f"Unexpected TDLib request: {request_type}")

        from_message_id = int(payload.get("from_message_id") or 0)
        if from_message_id == 0:
            message_ids = [6, 5, 4, 3, 2, 1]
        else:
            message_ids = [message_id for message_id in [6, 5, 4, 3, 2, 1] if message_id < from_message_id]
        return {
            "@type": "messages",
            "messages": [_photo_message(message_id) for message_id in message_ids],
        }


class TdlibQueriesTest(unittest.TestCase):
    def test_load_chat_files_paginates_from_last_returned_file(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)

        with patch(
            "app.tdlib_queries.load_tdlib_session_for_account", return_value=True
        ):
            first_page = load_tdlib_chat_files(
                _PagedTdlibQueryManager(),
                db=conn,
                telegram_id=1,
                root_path="D:/tdlib/account-1",
                chat_id=100,
                filters={"limit": "2"},
            )
            second_page = load_tdlib_chat_files(
                _PagedTdlibQueryManager(),
                db=conn,
                telegram_id=1,
                root_path="D:/tdlib/account-1",
                chat_id=100,
                filters={
                    "limit": "2",
                    "fromMessageId": str(first_page["nextFromMessageId"]),
                },
            )
            size_filtered = load_tdlib_chat_files(
                _PagedTdlibQueryManager(),
                db=conn,
                telegram_id=1,
                root_path="D:/tdlib/account-1",
                chat_id=100,
                filters={"limit": "2", "sizeRange": "2,3", "sizeUnit": "KB"},
            )

        self.assertEqual([item["messageId"] for item in first_page["files"]], [6, 5])
        self.assertEqual(first_page["nextFromMessageId"], 5)
        self.assertEqual([item["messageId"] for item in second_page["files"]], [4, 3])
        self.assertEqual(second_page["nextFromMessageId"], 3)
        self.assertEqual(size_filtered["files"], [])

    def test_load_chat_files_marks_archive_duplicates_and_filters_them(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        upsert_tdlib_file_record(
            conn,
            file_payload={
                "id": 111,
                "telegramId": 1,
                "uniqueId": "dup-photo-1",
                "messageId": 50,
                "chatId": 100,
                "mediaAlbumId": 0,
                "fileName": "existing.jpg",
                "type": "photo",
                "mimeType": "image/jpeg",
                "size": 1234,
                "downloadedSize": 1234,
                "thumbnail": "",
                "downloadStatus": "completed",
                "date": 1710000000,
                "caption": "existing",
                "localPath": "D:/downloads/existing.jpg",
                "hasSensitiveContent": False,
                "startDate": 0,
                "completionDate": 1710000100000,
                "transferStatus": "idle",
                "extra": {"width": 640, "height": 480, "type": "x"},
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
            },
        )

        with patch(
            "app.tdlib_queries.load_tdlib_session_for_account", return_value=True
        ):
            result = load_tdlib_chat_files(
                _FakeTdlibQueryManager(),
                db=conn,
                telegram_id=1,
                root_path="D:/tdlib/account-1",
                chat_id=100,
                filters={"alreadyDownloaded": "true", "limit": "20"},
            )

        self.assertEqual(result["size"], 1)
        file_item = result["files"][0]
        self.assertTrue(file_item["alreadyDownloaded"])
        self.assertEqual(file_item["downloadStatus"], "completed")
        self.assertEqual(file_item["localPath"], "D:/downloads/existing.jpg")

    def test_load_chat_files_does_not_copy_transfer_status_from_other_file_id(
        self,
    ) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        upsert_tdlib_file_record(
            conn,
            file_payload={
                "id": 111,
                "telegramId": 1,
                "uniqueId": "dup-photo-1",
                "messageId": 50,
                "chatId": 100,
                "mediaAlbumId": 0,
                "fileName": "existing.jpg",
                "type": "photo",
                "mimeType": "image/jpeg",
                "size": 1234,
                "downloadedSize": 1234,
                "thumbnail": "",
                "downloadStatus": "completed",
                "date": 1710000000,
                "caption": "existing",
                "localPath": "D:/downloads/existing.jpg",
                "hasSensitiveContent": False,
                "startDate": 0,
                "completionDate": 1710000100000,
                "transferStatus": "completed",
                "extra": {"width": 640, "height": 480, "type": "x"},
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
            },
        )

        with patch(
            "app.tdlib_queries.load_tdlib_session_for_account", return_value=True
        ):
            result = load_tdlib_chat_files(
                _FakeTdlibQueryManager(),
                db=conn,
                telegram_id=1,
                root_path="D:/tdlib/account-1",
                chat_id=100,
                filters={"limit": "20"},
            )

        self.assertEqual(result["size"], 1)
        file_item = result["files"][0]
        self.assertTrue(file_item["alreadyDownloaded"])
        self.assertEqual(file_item["downloadStatus"], "completed")
        self.assertEqual(file_item["transferStatus"], "idle")


if __name__ == "__main__":
    unittest.main()
