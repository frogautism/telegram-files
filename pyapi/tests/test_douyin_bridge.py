import os
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest import mock

from app.config import AppConfig
from app.douyin_bridge import _collect_paged_user_items, _organize_downloaded_assets
from app.douyin_config import douyin_downloader_path, vendored_downloader_version


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

    def test_organize_downloaded_assets_accepts_live_recording_suffix(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            source_dir = base / "host" / "live" / "2026_live_777"
            source_dir.mkdir(parents=True)
            live = source_dir / "2026_live_777.flv"
            live.write_bytes(b"flv")

            result = _organize_downloaded_assets(base, "777")

            self.assertEqual(result["localPath"], str(base / "host" / "video" / live.name))
            self.assertEqual(result["size"], 3)
            self.assertEqual(result["assets"][0]["bucket"], "video")


def _make_app_config(downloader_path: str = "") -> AppConfig:
    return AppConfig(
        app_root=Path("."),
        db_type="sqlite",
        data_path="data.db",
        version="test",
        telegram_api_id=0,
        telegram_api_hash="",
        telegram_log_level=1,
        tdlib_shared_lib="",
        douyin_downloader_path=downloader_path,
        douyin_path="",
    )


class VendoredDownloaderTest(unittest.TestCase):
    def test_core_imports_without_env_override(self) -> None:
        # The vendored package must import cleanly with no DOUYIN_DOWNLOADER_PATH set
        # and without mutating sys.path.
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DOUYIN_DOWNLOADER_PATH", None)
            from app.vendor.douyin_downloader.core import (  # noqa: F401
                DouyinAPIClient,
                DownloaderFactory,
                URLParser,
            )
            from app.vendor.douyin_downloader.auth import CookieManager  # noqa: F401
            from app.vendor.douyin_downloader.config import ConfigLoader  # noqa: F401
            from app.vendor.douyin_downloader.control import (  # noqa: F401
                QueueManager,
                RateLimiter,
                RetryHandler,
            )
            from app.vendor.douyin_downloader.storage import FileManager  # noqa: F401
            from app.vendor.douyin_downloader.utils.validators import (  # noqa: F401
                is_short_url,
                normalize_short_url,
            )

    def test_vendored_version_is_non_empty(self) -> None:
        self.assertTrue(vendored_downloader_version())

    def test_downloader_path_empty_without_override(self) -> None:
        # No configured override -> vendored package is used (empty path).
        self.assertEqual(douyin_downloader_path(_make_app_config("")), "")
        self.assertEqual(
            douyin_downloader_path(_make_app_config("/nonexistent/path/xyz")), ""
        )

    def test_downloader_path_honours_existing_override(self) -> None:
        # The optional external override still works when the path exists.
        with TemporaryDirectory() as temp_dir:
            config = _make_app_config(temp_dir)
            self.assertEqual(douyin_downloader_path(config), temp_dir)


if __name__ == "__main__":
    unittest.main()
