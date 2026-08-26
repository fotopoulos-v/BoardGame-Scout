"""Capture a BGG session for CI.

Cloudflare answers POST /login/api/v1 with HTTP 403 from datacenter IP ranges,
so the GitHub Actions runner cannot log in at all. Ordinary GETs are not
blocked, so the workaround is to log in from an unblocked network (your own
machine), then hand the resulting cookies to the workflow.

Usage:
    python capture_bgg_session.py

Paste the printed JSON into the repository secret BGG_COOKIES:
    Settings -> Secrets and variables -> Actions -> New repository secret
"""

import getpass
import json
import os
import sys
from datetime import datetime, timezone

import requests

LOGIN_URL = "https://boardgamegeek.com/login/api/v1"
WHOAMI_URL = "https://boardgamegeek.com/api/users/current"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def main():
    username = os.getenv("BGG_USERNAME") or input("BGG Username: ")
    password = os.getenv("BGG_PASSWORD") or getpass.getpass("BGG Password: ")

    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://boardgamegeek.com",
        "Referer": "https://boardgamegeek.com/login",
    })

    resp = session.post(
        LOGIN_URL,
        json={"credentials": {"username": username, "password": password}},
        timeout=30,
    )
    if resp.status_code == 403:
        sys.exit("Cloudflare blocked this network too (403). Try a different connection.")
    if resp.status_code not in (200, 204):
        sys.exit(f"Login failed: HTTP {resp.status_code} {resp.text[:200]}")

    whoami = session.get(WHOAMI_URL, timeout=30).json()
    if not whoami.get("loggedIn"):
        sys.exit("Login returned OK but the session is still anonymous.")
    print(f"Logged in as {whoami.get('username')!r}\n")

    cookies = [{"name": c.name, "value": c.value} for c in session.cookies]

    soonest = min(
        (c.expires for c in session.cookies if c.expires),
        default=None,
    )
    if soonest:
        when = datetime.fromtimestamp(soonest, tz=timezone.utc)
        days = (when - datetime.now(timezone.utc)).days
        print(f"Earliest cookie expiry: {when:%Y-%m-%d %H:%M UTC} (~{days} days)")
        print("Re-run this script and update the secret before then.\n")

    print("Set this as the BGG_COOKIES repository secret:\n")
    print(json.dumps(cookies))


if __name__ == "__main__":
    main()
