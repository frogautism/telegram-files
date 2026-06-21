import sqlite3
import unittest

from app.db import get_settings_by_keys, init_schema
from app.douyin_config import (
    normalize_douyin_cookie_text,
    parse_douyin_cookies,
)
from app.routers.system import douyin_cookies_parse, settings_create


NETSCAPE_COOKIES = """# Netscape HTTP Cookie File
.douyin.com\tTRUE\t/\tTRUE\t1816589737\tttwid\tttwid-value
#HttpOnly_.douyin.com\tTRUE\t/\tTRUE\t1816589737\tsessionid\tsession-value
www.douyin.com\tFALSE\t/\tFALSE\t0\t\tdouyin.com
.example.com\tTRUE\t/\tTRUE\t1816589737\tunrelated\tsecret
"""


class DouyinCookieParserTest(unittest.TestCase):
    def test_parses_douyin_netscape_cookie_export(self) -> None:
        self.assertEqual(
            parse_douyin_cookies(NETSCAPE_COOKIES),
            {
                "ttwid": "ttwid-value",
                "sessionid": "session-value",
            },
        )

    def test_normalizes_json_and_cookie_header(self) -> None:
        self.assertEqual(
            normalize_douyin_cookie_text('{"ttwid": "one", "sessionid": "two"}'),
            "ttwid=one; sessionid=two",
        )
        self.assertEqual(
            parse_douyin_cookies("ttwid=one; sessionid=two"),
            {"ttwid": "one", "sessionid": "two"},
        )


class DouyinCookieEndpointTest(unittest.TestCase):
    def test_parse_endpoint_returns_downloader_cookie_header(self) -> None:
        response = douyin_cookies_parse({"content": NETSCAPE_COOKIES})

        self.assertEqual(response["count"], 2)
        self.assertEqual(
            response["cookieHeader"],
            "ttwid=ttwid-value; sessionid=session-value",
        )

    def test_settings_save_normalizes_cookie_file_content(self) -> None:
        db = sqlite3.connect(":memory:")
        self.addCleanup(db.close)
        db.row_factory = sqlite3.Row
        init_schema(db)

        response = settings_create({"douyinCookies": NETSCAPE_COOKIES}, db)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            get_settings_by_keys(db, ["douyinCookies"])["douyinCookies"],
            "ttwid=ttwid-value; sessionid=session-value",
        )


if __name__ == "__main__":
    unittest.main()
