from __future__ import annotations

from pathlib import Path
from typing import Any

PRIMARY_MEDIA_SUFFIXES = {
    ".mp4",
    ".mov",
    ".m4v",
    ".flv",
    ".m3u8",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
}
MUSIC_SUFFIXES = {".mp3", ".m4a", ".aac", ".wav"}
JSON_SUFFIXES = {".json"}
ASSET_SUFFIXES = PRIMARY_MEDIA_SUFFIXES | MUSIC_SUFFIXES | JSON_SUFFIXES


def douyin_asset_bucket(path: Path) -> str:
    name = path.name.lower()
    suffix = path.suffix.lower()
    if suffix in JSON_SUFFIXES:
        return "json"
    if suffix in MUSIC_SUFFIXES or name.endswith("_music.mp3"):
        return "music"
    if (
        name.endswith("_cover.jpg")
        or name.endswith("_cover.jpeg")
        or name.endswith("_cover.png")
        or name.endswith("_cover.webp")
        or name.endswith("_avatar.jpg")
        or name.endswith("_avatar.jpeg")
        or name.endswith("_avatar.png")
        or name.endswith("_avatar.webp")
    ):
        return "thumbnail"
    return "video"


def is_douyin_asset(path: Path) -> bool:
    return (
        path.is_file()
        and not path.name.endswith(".tmp")
        and path.name != "download_manifest.jsonl"
        and path.suffix.lower() in ASSET_SUFFIXES
    )


def is_primary_asset(path: Path) -> bool:
    name = path.name.lower()
    if path.suffix.lower() not in PRIMARY_MEDIA_SUFFIXES:
        return False
    if name.endswith(("_cover.jpg", "_cover.jpeg", "_cover.png", "_cover.webp")):
        return False
    if "_avatar" in name:
        return False
    return douyin_asset_bucket(path) == "video"


def asset_payload(path: Path) -> dict[str, Any]:
    try:
        size = path.stat().st_size
    except OSError:
        size = 0
    return {"path": str(path), "bucket": douyin_asset_bucket(path), "size": size}


def snapshot_assets(base_path: Path) -> dict[Path, tuple[int, int]]:
    if not base_path.exists():
        return {}
    result: dict[Path, tuple[int, int]] = {}
    for path in base_path.rglob("*"):
        if not is_douyin_asset(path):
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        result[path] = (stat.st_mtime_ns, stat.st_size)
    return result


def changed_assets(
    before: dict[Path, tuple[int, int]],
    after: dict[Path, tuple[int, int]],
) -> list[Path]:
    return [path for path, marker in after.items() if before.get(path) != marker]


def sibling_asset_paths(local_path: str, aweme_id: str = "") -> list[Path]:
    local_path = str(local_path or "").strip()
    if not local_path:
        return []
    primary = Path(local_path)
    paths: list[Path] = [primary]
    author_dir = primary.parent.parent if primary.parent.name in {"video", "live"} else primary.parent
    if not author_dir.exists():
        return paths
    needle = str(aweme_id or "").strip()
    for bucket in ("video", "live", "thumbnail", "music", "json"):
        bucket_dir = author_dir / bucket
        if not bucket_dir.exists():
            continue
        for candidate in bucket_dir.iterdir():
            if not is_douyin_asset(candidate):
                continue
            if needle and needle not in candidate.name:
                continue
            paths.append(candidate)
    seen: set[Path] = set()
    unique: list[Path] = []
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)
    return unique


def unlink_asset_paths(paths: list[Path]) -> None:
    for path in paths:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
