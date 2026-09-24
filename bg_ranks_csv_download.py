"""Download the BGG board game ranks CSV (zipped) from the data dumps page.

Logs in through BGG's JSON login API with plain requests. The previous Selenium
flow stopped working in 2026-09: Cloudflare shows automated Chrome a "Verify you
are human" challenge on the login page, while the login API and the data dumps
page still answer normal HTTP requests (from a home IP; GitHub runner IPs get
403 on the login API since 2026-08-24).
"""

import os
import re
import sys
import zipfile
from html import unescape

import requests

BGG = "https://boardgamegeek.com"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"
)
DOWNLOAD_LINK_PATTERN = (
    r'<a\s+href="(https://geek-export-stats\.s3\.amazonaws\.com/'
    r'boardgames_export/boardgames_ranks_[^"]+)"'
)


def login(username, password):
    """Return a requests.Session logged in to BGG, or raise RuntimeError."""
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    print("Logging in to BGG...")
    resp = session.post(
        f"{BGG}/login/api/v1",
        json={"credentials": {"username": username, "password": password}},
        headers={
            "Accept": "application/json, text/plain, */*",
            "Origin": BGG,
            "Referer": f"{BGG}/login",
        },
        timeout=30,
    )
    # BGG answers 204 No Content on success (it used to be 200).
    if resp.status_code == 400:
        raise RuntimeError("Login failed - BGG rejected the username/password")
    if resp.status_code == 403:
        raise RuntimeError("Login failed - HTTP 403, blocked by Cloudflare")
    if resp.status_code not in (200, 204):
        raise RuntimeError(f"Login failed - HTTP {resp.status_code}: {resp.text[:200]}")

    # The login response alone doesn't prove the session works; ask BGG.
    current = session.get(f"{BGG}/api/users/current", timeout=30)
    if current.status_code != 200 or not current.json().get("loggedIn"):
        raise RuntimeError(f"Login did not stick (users/current HTTP {current.status_code})")
    print("✅ Login successful")
    return session


def find_download_url(session):
    print("Looking for download link...")
    resp = session.get(f"{BGG}/data_dumps/bg_ranks", timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Data dumps page returned HTTP {resp.status_code}")

    match = re.search(DOWNLOAD_LINK_PATTERN, resp.text)
    if not match:
        with open("download_page_debug.html", "w", encoding="utf-8") as f:
            f.write(resp.text)
        raise RuntimeError("Could not find download link. Saved page to download_page_debug.html")

    url = unescape(match.group(1))
    print(f"✓ Found download URL ({url.split('?')[0].rsplit('/', 1)[-1]})")
    return url


def download_zip(url, save_path):
    print("\nDownloading CSV file...")
    # The link is a pre-signed S3 URL, so it needs no BGG cookies.
    with requests.get(url, stream=True, timeout=(30, 300)) as resp:
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to download zip: {resp.status_code}")

        total_size = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(save_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 16):
                f.write(chunk)
                downloaded += len(chunk)
        if total_size and downloaded != total_size:
            raise RuntimeError(f"Download incomplete: {downloaded:,} of {total_size:,} bytes")

    if not zipfile.is_zipfile(save_path):
        raise RuntimeError(f"{save_path} is not a valid zip file")
    print(f"✅ CSV downloaded successfully: {save_path} ({os.path.getsize(save_path):,} bytes)")


def download_bgg_csv(username, password, save_path="boardgames_ranks.zip"):
    print("=" * 60)
    print("BGG Data Download")
    print("=" * 60)
    session = login(username, password)
    url = find_download_url(session)
    download_zip(url, save_path)
    return True


def main():
    username = os.getenv("BGG_USERNAME") or input("BGG Username: ")
    password = os.getenv("BGG_PASSWORD") or input("BGG Password: ")

    if not username or not password:
        print("❌ Error: Username and password required")
        sys.exit(1)

    download_bgg_csv(username, password)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
