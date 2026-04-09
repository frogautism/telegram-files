import sqlite3
import unittest
from unittest.mock import patch

from app.db import init_schema
from app.file_record_ops import upsert_tdlib_file_record
from app.tdlib_downloads import start_tdlib_download_for_message


class _FakeTdlibManager:
    def __init__(self) -> None:
        self.requests: list[str] = []

    def request(self, account_key: str, payload: dict, timeout_seconds: float):
        self.requests.append(str(payload.get("@type") or ""))
        request_type = str(payload.get("@type") or "")
        if request_type == "getMessage":
            return {
                "@type": "message",
                "id": 200,
                "chat_id": 100,
                "date": 1710000000,
                "message_thread_id": 0,
                "media_album_id": 0,
                "content": {
                    "@type": "messagePhoto",
                    "caption": {"text": "duplicate photo"},
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
        if request_type == "getMessageThread":
            return {
                "@type": "messageThreadInfo",
                "chat_id": 100,
                "message_thread_id": 0,
            }
        raise AssertionError(f"Unexpected TDLib request: {request_type}")


class _FakeTdlibManagerWithStaleRequestId:
    def __init__(self) -> None:
        self.requests: list[tuple[str, int]] = []

    def request(self, account_key: str, payload: dict, timeout_seconds: float):
        request_type = str(payload.get("@type") or "")
        file_id = int(payload.get("file_id") or 0)
        self.requests.append((request_type, file_id))
        if request_type == "getMessage":
            return {
                "@type": "message",
                "id": 200,
                "chat_id": 100,
                "date": 1710000000,
                "message_thread_id": 0,
                "media_album_id": 0,
                "content": {
                    "@type": "messagePhoto",
                    "caption": {"text": "fresh file id"},
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
                                        "unique_id": "fresh-photo-1",
                                    },
                                },
                            }
                        ],
                        "minithumbnail": {"data": "thumb"},
                    },
                },
            }
        if request_type == "getMessageThread":
            return {
                "@type": "messageThreadInfo",
                "chat_id": 100,
                "message_thread_id": 0,
            }
        if request_type == "addFileToDownloads":
            if file_id != 321:
                return {"@type": "error", "message": "File not found"}
            return {"@type": "ok"}
        if request_type == "getFile":
            if file_id != 321:
                return {"@type": "error", "message": "File not found"}
            return {
                "@type": "file",
                "id": 321,
                "size": 1234,
                "expected_size": 1234,
                "local": {
                    "is_downloading_completed": False,
                    "is_downloading_active": True,
                    "downloaded_size": 0,
                    "path": "",
                },
                "remote": {
                    "id": "remote-321",
                    "unique_id": "fresh-photo-1",
                },
            }
        if request_type == "downloadFile":
            return {"@type": "error", "message": "should not use stale file id"}
        raise AssertionError(f"Unexpected TDLib request: {request_type}")


class TdlibDownloadsTest(unittest.TestCase):
    def test_start_download_reuses_completed_duplicate_before_tdlib_download(
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
                "transferStatus": "idle",
                "extra": {"width": 640, "height": 480, "type": "x"},
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
            },
        )

        td_manager = _FakeTdlibManager()
        with patch(
            "app.tdlib_downloads._load_tdlib_session_for_account", return_value=True
        ):
            result = start_tdlib_download_for_message(
                td_manager,
                db=conn,
                telegram_id=1,
                root_path="D:/tdlib/account-1",
                chat_id=100,
                message_id=200,
                file_id=321,
            )

        self.assertEqual(td_manager.requests, ["getMessage", "getMessageThread"])
        self.assertEqual(result["uniqueId"], "dup-photo-1")
        self.assertEqual(result["downloadStatus"], "completed")
        self.assertEqual(result["localPath"], "D:/downloads/existing.jpg")
        self.assertEqual(result["messageId"], 200)

    def test_start_download_uses_live_tdlib_file_id_from_message(self) -> None:
        td_manager = _FakeTdlibManagerWithStaleRequestId()
        with patch(
            "app.tdlib_downloads._load_tdlib_session_for_account", return_value=True
        ):
            result = start_tdlib_download_for_message(
                td_manager,
                db=None,
                telegram_id=1,
                root_path="D:/tdlib/account-1",
                chat_id=100,
                message_id=200,
                file_id=999,
            )

        self.assertEqual(result["id"], 321)
        self.assertEqual(result["uniqueId"], "fresh-photo-1")
        self.assertEqual(result["downloadStatus"], "downloading")
        self.assertIn(("addFileToDownloads", 321), td_manager.requests)
        self.assertIn(("getFile", 321), td_manager.requests)
        self.assertNotIn(("addFileToDownloads", 999), td_manager.requests)
        self.assertNotIn(("downloadFile", 999), td_manager.requests)


if __name__ == "__main__":
    unittest.main()
