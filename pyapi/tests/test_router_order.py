from fastapi import FastAPI
from starlette.routing import Match

from app.routers import register_routers


def _matched_endpoint(path: str, method: str) -> str:
    app = FastAPI()
    register_routers(app)
    scope = {"type": "http", "method": method, "path": path, "root_path": ""}
    for route in app.routes:
        match, _ = route.matches(scope)
        if match == Match.FULL:
            return route.endpoint.__name__
    return ""


def test_douyin_download_route_is_not_captured_by_telegram_download_route() -> None:
    assert (
        _matched_endpoint("/douyin/file/start-download", "POST")
        == "douyin_file_start_download"
    )


def test_douyin_preview_route_is_not_captured_by_telegram_file_route() -> None:
    assert _matched_endpoint("/douyin/file/douyin%3A123", "GET") == "douyin_file_preview"
