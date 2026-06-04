from typing import Any, Optional

from app.vendor.douyin_downloader.auth import CookieManager
from app.vendor.douyin_downloader.config import ConfigLoader
from app.vendor.douyin_downloader.control import QueueManager, RateLimiter, RetryHandler
from app.vendor.douyin_downloader.core.api_client import DouyinAPIClient
from app.vendor.douyin_downloader.core.downloader_base import BaseDownloader
from app.vendor.douyin_downloader.core.live_downloader import LiveDownloader
from app.vendor.douyin_downloader.core.mix_downloader import MixDownloader
from app.vendor.douyin_downloader.core.music_downloader import MusicDownloader
from app.vendor.douyin_downloader.core.user_downloader import UserDownloader
from app.vendor.douyin_downloader.core.video_downloader import VideoDownloader
from app.vendor.douyin_downloader.storage import Database, FileManager
from app.vendor.douyin_downloader.utils.logger import setup_logger

logger = setup_logger("DownloaderFactory")


class DownloaderFactory:
    @staticmethod
    def create(
        url_type: str,
        config: ConfigLoader,
        api_client: DouyinAPIClient,
        file_manager: FileManager,
        cookie_manager: CookieManager,
        database: Optional[Database] = None,
        rate_limiter: Optional[RateLimiter] = None,
        retry_handler: Optional[RetryHandler] = None,
        queue_manager: Optional[QueueManager] = None,
        progress_reporter: Optional[Any] = None,
    ) -> Optional[BaseDownloader]:

        common_args = {
            "config": config,
            "api_client": api_client,
            "file_manager": file_manager,
            "cookie_manager": cookie_manager,
            "database": database,
            "rate_limiter": rate_limiter,
            "retry_handler": retry_handler,
            "queue_manager": queue_manager,
            "progress_reporter": progress_reporter,
        }

        if url_type == "video":
            return VideoDownloader(**common_args)
        elif url_type == "user":
            return UserDownloader(**common_args)
        elif url_type == "gallery":
            return VideoDownloader(**common_args)
        elif url_type == "collection":
            return MixDownloader(**common_args)
        elif url_type == "music":
            return MusicDownloader(**common_args)
        elif url_type == "live":
            return LiveDownloader(**common_args)
        elif url_type == "short":
            logger.error(
                "Short URL was not resolved before dispatching. "
                "Please call api_client.resolve_short_url() first."
            )
            return None
        else:
            logger.error("Unsupported URL type: %s", url_type)
            return None
