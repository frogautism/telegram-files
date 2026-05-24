import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

from app.douyin_bridge import _collect_paged_user_items, _organize_downloaded_assets


class FakePagedApi:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int]] = []
        self.pages = {
            0: {
                "items": [{"aweme_id": "1"}, {"aweme_id": "2"}],
                "has_more": True,
                "max_cursor": 20,
            },
            20: {
                "items": [{"aweme_id": "2"}, {"aweme_id": "3"}],
                "has_more": True,
                "max_cursor": 40,
            },
            40: {
                "items": [{"aweme_id": "4"}],
                "has_more": False,
                "max_cursor": 40,
            },
        }

    async def get_user_post(
        self,
        _sec_uid: str,
        max_cursor: int = 0,
        count: int = 20,
    ) -> dict:
        self.calls.append((max_cursor, count))
        return self.pages[max_cursor]


class DouyinBridgeTest(unittest.IsolatedAsyncioTestCase):
    async def test_collect_paged_user_items_paginates_and_deduplicates(self) -> None:
        api = FakePagedApi()

        items = await _collect_paged_user_items(
            api,
            "get_user_post",
            "sec-1",
            limit=10,
            page_size=2,
        )

        self.assertEqual([item["aweme_id"] for item in items], ["1", "2", "3", "4"])
        self.assertEqual(api.calls, [(0, 2), (20, 2), (40, 2)])

    async def test_collect_paged_user_items_stops_at_limit(self) -> None:
        api = FakePagedApi()

        items = await _collect_paged_user_items(
            api,
            "get_user_post",
            "sec-1",
            limit=3,
            page_size=2,
        )

        self.assertEqual([item["aweme_id"] for item in items], ["1", "2", "3"])
        self.assertEqual(api.calls, [(0, 2), (20, 1)])

    def test_organize_downloaded_assets_moves_sidecars_to_author_buckets(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            source_dir = base / "author" / "video" / "2026_title_123"
            source_dir.mkdir(parents=True)
            video = source_dir / "2026_title_123.mp4"
            cover = source_dir / "2026_title_123_cover.jpg"
            music = source_dir / "2026_title_123_music.mp3"
            metadata = source_dir / "2026_title_123_data.json"
            video.write_bytes(b"video")
            cover.write_bytes(b"cover")
            music.write_bytes(b"music")
            metadata.write_text("{}", encoding="utf-8")

            result = _organize_downloaded_assets(base, "123")

            self.assertEqual(result["localPath"], str(base / "author" / "video" / video.name))
            self.assertTrue((base / "author" / "video" / video.name).exists())
            self.assertTrue((base / "author" / "thumbnail" / cover.name).exists())
            self.assertTrue((base / "author" / "music" / music.name).exists())
            self.assertTrue((base / "author" / "json" / metadata.name).exists())


if __name__ == "__main__":
    unittest.main()
