import json
import sqlite3
import unittest

from app.db import (
    create_chat_group,
    init_schema,
    update_auto_settings,
    update_chat_group_auto_settings,
)
from app.file_record_ops import upsert_tdlib_file_record
from app.offline_reset import (
    clear_offline_reset_pin,
    has_offline_reset_pin,
    reset_offline_data,
    set_offline_reset_pin,
    verify_offline_reset_pin,
)


class OfflineResetTest(unittest.TestCase):
    def test_set_verify_and_clear_reset_pin(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)

        set_offline_reset_pin(conn, pin="1234")
        self.assertTrue(has_offline_reset_pin(conn))
        self.assertTrue(verify_offline_reset_pin(conn, "1234"))
        self.assertFalse(verify_offline_reset_pin(conn, "9999"))

        with self.assertRaises(PermissionError):
            set_offline_reset_pin(conn, pin="5678", current_pin="0000")

        set_offline_reset_pin(conn, pin="5678", current_pin="1234")
        self.assertTrue(verify_offline_reset_pin(conn, "5678"))
        self.assertFalse(verify_offline_reset_pin(conn, "1234"))

        with self.assertRaises(PermissionError):
            clear_offline_reset_pin(conn, current_pin="0000")

        clear_offline_reset_pin(conn, current_pin="5678")
        self.assertFalse(has_offline_reset_pin(conn))

    def test_reset_offline_data_clears_cache_and_automation_progress(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)

        upsert_tdlib_file_record(
            conn,
            file_payload={
                "id": 1,
                "telegramId": 1,
                "uniqueId": "file-1",
                "messageId": 100,
                "chatId": 200,
                "mediaAlbumId": 0,
                "fileName": "a.jpg",
                "type": "photo",
                "mimeType": "image/jpeg",
                "size": 100,
                "downloadedSize": 100,
                "thumbnail": "",
                "downloadStatus": "completed",
                "date": 1710000000,
                "caption": "cached",
                "localPath": "D:/downloads/a.jpg",
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
        conn.execute(
            "INSERT INTO statistic_record(related_id, type, timestamp, data) VALUES(?, ?, ?, ?)",
            ("1", "speed", 1710000000, "{}"),
        )
        conn.commit()

        update_auto_settings(
            conn,
            telegram_id=1,
            chat_id=200,
            auto_payload={
                "preload": {"enabled": True, "nextFromMessageId": 321},
                "download": {
                    "enabled": True,
                    "nextFileType": "photo",
                    "nextFromMessageId": 654,
                },
                "state": 30,
            },
        )
        create_chat_group(
            conn,
            telegram_id=1,
            group_id="g1",
            name="Group 1",
            chat_ids=[200, 201],
        )
        update_chat_group_auto_settings(
            conn,
            telegram_id=1,
            group_id="g1",
            auto_payload={
                "preload": {"enabled": True},
                "state": 30,
                "progressByChat": {
                    "200": {
                        "state": 30,
                        "preload": {"nextFromMessageId": 123},
                        "download": {"nextFileType": "photo", "nextFromMessageId": 456},
                    },
                    "201": {
                        "state": 30,
                        "preload": {"nextFromMessageId": 789},
                        "download": {"nextFileType": "video", "nextFromMessageId": 999},
                    },
                },
            },
        )

        result = reset_offline_data(conn)

        self.assertEqual(result["filesDeleted"], 1)
        self.assertEqual(result["statisticsDeleted"], 1)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM file_record").fetchone()[0],
            0,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM statistic_record").fetchone()[0],
            0,
        )

        automation_value = conn.execute(
            "SELECT value FROM setting_record WHERE key = 'automation' LIMIT 1"
        ).fetchone()
        parsed_automation = json.loads(str(automation_value["value"] or "{}"))
        self.assertEqual(parsed_automation["automations"][0]["state"], 0)
        self.assertEqual(
            parsed_automation["automations"][0]["preload"]["nextFromMessageId"],
            0,
        )
        self.assertEqual(
            parsed_automation["automations"][0]["download"]["nextFileType"],
            "",
        )
        self.assertEqual(
            parsed_automation["automations"][0]["download"]["nextFromMessageId"],
            0,
        )

        group_value = conn.execute(
            "SELECT auto_settings FROM chat_group_record WHERE id = ? LIMIT 1",
            ("g1",),
        ).fetchone()
        parsed_group = json.loads(str(group_value["auto_settings"] or "{}"))
        self.assertEqual(parsed_group["state"], 0)
        self.assertEqual(parsed_group["progressByChat"]["200"]["state"], 0)
        self.assertEqual(
            parsed_group["progressByChat"]["200"]["preload"]["nextFromMessageId"],
            0,
        )
        self.assertEqual(
            parsed_group["progressByChat"]["200"]["download"]["nextFileType"],
            "",
        )
        self.assertEqual(
            parsed_group["progressByChat"]["200"]["download"]["nextFromMessageId"],
            0,
        )


if __name__ == "__main__":
    unittest.main()
