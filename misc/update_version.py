import json
import re
import subprocess
import sys
from pathlib import Path

VERSION_FILE = Path("VERSION")
PYTHON_CONFIG_FILE = Path("pyapi/app/config.py")
PACKAGE_JSON_FILE = Path("web/package.json")
PACKAGE_LOCK_FILE = Path("web/package-lock.json")


def write_version(version: str) -> None:
    VERSION_FILE.write_text(version + "\n", encoding="utf-8")


def update_python_default_version(version: str) -> None:
    content = PYTHON_CONFIG_FILE.read_text(encoding="utf-8")
    updated = re.sub(
        r'version=os\.getenv\("APP_VERSION", ".*?"\)',
        f'version=os.getenv("APP_VERSION", "{version}")',
        content,
    )
    PYTHON_CONFIG_FILE.write_text(updated, encoding="utf-8")


def update_package_json(version: str) -> None:
    payload = json.loads(PACKAGE_JSON_FILE.read_text(encoding="utf-8"))
    payload["version"] = version
    PACKAGE_JSON_FILE.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def update_package_lock(version: str) -> None:
    if not PACKAGE_LOCK_FILE.exists():
        return

    payload = json.loads(PACKAGE_LOCK_FILE.read_text(encoding="utf-8"))
    payload["version"] = version
    packages = payload.get("packages")
    if isinstance(packages, dict) and "" in packages:
        packages[""]["version"] = version
    PACKAGE_LOCK_FILE.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def git_commit_and_tag(version: str, message: str) -> None:
    files = [
        str(VERSION_FILE),
        str(PYTHON_CONFIG_FILE),
        str(PACKAGE_JSON_FILE),
        str(PACKAGE_LOCK_FILE),
    ]
    try:
        subprocess.run(["git", "add", *files], check=True)
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "tag", version], check=True)
        print(f"Committed changes and created tag: {version}")
    except subprocess.CalledProcessError as exc:
        print(f"Git operation failed: {exc}")
        sys.exit(1)


def main() -> None:
    version = sys.argv[1] if len(sys.argv) > 1 else None
    if not version:
        print("Please provide a version, for example: python misc/update_version.py 1.2.3")
        sys.exit(1)

    change_message = input("Release commit message (leave empty to skip commit/tag): ").strip()

    write_version(version)
    update_python_default_version(version)
    update_package_json(version)
    update_package_lock(version)

    print(f"Updated version to: {version}")

    if change_message:
        git_commit_and_tag(version, change_message)


if __name__ == "__main__":
    main()
