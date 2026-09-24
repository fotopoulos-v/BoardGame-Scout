#!/usr/bin/env python3
"""Daily BoardGame Scout database update, run from this PC.

Replaces the "Update Board Game Database" GitHub workflow, whose BGG login has
been blocked by Cloudflare for GitHub runner IPs since 2026-08-24. A home IP is
not blocked, so the same steps run here:

  1. Download the BGG ranks CSV (bg_ranks_csv_download.py)
  2. Download the current boardgames_db.zip from the GitHub release
  3. Enrich it with enrich_bgg_db_v3.py
  4. Validate, zip and upload it back to the release
  5. Replace the local boardgames.db with the same file

Secrets come from .env in this folder (see .env for the keys). Work happens in
.update_work/ so a failed run never touches the published or local database.
Logs go to logs/. Failures raise a desktop notification.

Usage:
    python local_update_database.py              # full run
    python local_update_database.py --no-upload  # everything except publishing
"""

import argparse
import fcntl
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import tomllib
import zipfile
from datetime import datetime, UTC
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent
WORK_DIR = PROJECT_DIR / ".update_work"
LOG_DIR = PROJECT_DIR / "logs"
LOCAL_DB = PROJECT_DIR / "boardgames.db"
KEEP_LOGS = 30

REPO = "fotopoulos-v/BoardGame-Scout"
RELEASE_TAG = "boardgame-database"
ASSET_NAME = "boardgames_db.zip"
STAGED_ASSET_NAME = "boardgames_db_new.zip"
PUBLIC_ASSET_URL = f"https://github.com/{REPO}/releases/download/{RELEASE_TAG}/{ASSET_NAME}"
API = f"https://api.github.com/repos/{REPO}"

CSV_DOWNLOAD_TIMEOUT = 15 * 60
ENRICH_TIMEOUT = 4 * 60 * 60

log = logging.getLogger("bgscout-update")


class UpdateError(Exception):
    """A step failed; the message is what the notification shows."""


# -----------------------
# Setup helpers
# -----------------------
def setup_logging() -> Path:
    LOG_DIR.mkdir(exist_ok=True)
    log_file = LOG_DIR / f"update_{datetime.now():%Y-%m-%d_%H%M%S}.log"
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    for handler in (logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)):
        handler.setFormatter(fmt)
        log.addHandler(handler)
    log.setLevel(logging.INFO)

    old_logs = sorted(LOG_DIR.glob("update_*.log"))[:-KEEP_LOGS]
    for old in old_logs:
        old.unlink(missing_ok=True)
    return log_file


def notify(title: str, body: str, urgent: bool = False) -> None:
    """Desktop notification, best-effort.

    The timer runs without a terminal, so notify-send needs to be told which
    session bus to talk to. Never fatal.
    """
    env = dict(os.environ)
    env.setdefault("DBUS_SESSION_BUS_ADDRESS", f"unix:path=/run/user/{os.getuid()}/bus")
    try:
        subprocess.run(
            ["notify-send", "--app-name=BoardGame Scout update",
             f"--urgency={'critical' if urgent else 'normal'}", title, body],
            env=env, timeout=15, check=False,
        )
    except Exception:
        pass


def load_secrets(need_github: bool) -> dict:
    load_dotenv(PROJECT_DIR / ".env")
    secrets = {key: os.getenv(key, "").strip() for key in ("BGG_USERNAME", "BGG_PASSWORD", "BGG_TOKEN", "GITHUB_TOKEN")}

    # The app already keeps BGG_TOKEN in Streamlit secrets; reuse it if .env has none.
    if not secrets["BGG_TOKEN"]:
        secrets_toml = PROJECT_DIR / ".streamlit" / "secrets.toml"
        if secrets_toml.exists():
            with open(secrets_toml, "rb") as f:
                secrets["BGG_TOKEN"] = str(tomllib.load(f).get("BGG_TOKEN", "")).strip()

    required = ["BGG_USERNAME", "BGG_PASSWORD", "BGG_TOKEN"] + (["GITHUB_TOKEN"] if need_github else [])
    missing = [key for key in required if not secrets[key]]
    if missing:
        raise UpdateError(f"Missing in .env: {', '.join(missing)}")
    return secrets


def run_logged(cmd: list, env: dict, timeout: int, step: str) -> None:
    """Run a subprocess in WORK_DIR, streaming its output into the log."""
    log.info(f"$ {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd, cwd=WORK_DIR, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    # A watchdog rather than a check per output line: a hung process prints nothing.
    watchdog = threading.Timer(timeout, proc.kill)
    watchdog.start()
    try:
        for line in proc.stdout:
            log.info(f"  | {line.rstrip()}")
        proc.wait()
    finally:
        timed_out = not watchdog.is_alive()
        watchdog.cancel()
        if proc.poll() is None:
            proc.kill()
    if timed_out:
        raise UpdateError(f"{step} timed out after {timeout // 60} min")
    if proc.returncode != 0:
        raise UpdateError(f"{step} failed (exit code {proc.returncode})")


# -----------------------
# Pipeline steps
# -----------------------
def download_bgg_csv(secrets: dict) -> None:
    log.info("STEP 1/5: Downloading BGG ranks CSV")
    env = dict(os.environ, BGG_USERNAME=secrets["BGG_USERNAME"], BGG_PASSWORD=secrets["BGG_PASSWORD"])
    run_logged(
        [sys.executable, "-u", str(PROJECT_DIR / "bg_ranks_csv_download.py")],
        env, CSV_DOWNLOAD_TIMEOUT, "BGG CSV download",
    )
    with zipfile.ZipFile(WORK_DIR / "boardgames_ranks.zip") as zf:
        zf.extract("boardgames_ranks.csv", WORK_DIR)
    size = (WORK_DIR / "boardgames_ranks.csv").stat().st_size
    log.info(f"  ✓ boardgames_ranks.csv extracted ({size:,} bytes)")


def download_release_db() -> int:
    """Fetch the published database. Returns its game count, for validation."""
    log.info("STEP 2/5: Downloading current database from the GitHub release")
    zip_path = WORK_DIR / ASSET_NAME
    with requests.get(PUBLIC_ASSET_URL, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extract("boardgames.db", WORK_DIR)
    zip_path.unlink()

    games = count_games(WORK_DIR / "boardgames.db")
    log.info(f"  ✓ boardgames.db downloaded ({games:,} games)")
    return games


def enrich_db(secrets: dict) -> None:
    log.info("STEP 3/5: Updating database with new BGG data")
    env = dict(os.environ, BGG_TOKEN=secrets["BGG_TOKEN"])
    run_logged([sys.executable, "-u", str(PROJECT_DIR / "enrich_bgg_db_v3.py")],
               env, ENRICH_TIMEOUT, "Database enrichment")


def count_games(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    finally:
        conn.close()


def validate_and_zip(games_before: int) -> Path:
    log.info("STEP 4/5: Validating and zipping the updated database")
    db_path = WORK_DIR / "boardgames.db"
    conn = sqlite3.connect(db_path)
    try:
        check = conn.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn.close()
    if check != "ok":
        raise UpdateError(f"Updated database failed integrity check: {check}")

    games_after = count_games(db_path)
    if games_after < games_before:
        raise UpdateError(f"Updated database lost games ({games_before:,} → {games_after:,})")
    log.info(f"  ✓ Integrity ok, {games_after:,} games (+{games_after - games_before:,})")

    zip_path = WORK_DIR / ASSET_NAME
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(db_path, arcname="boardgames.db")
    log.info(f"  ✓ {ASSET_NAME} created ({zip_path.stat().st_size:,} bytes)")
    return zip_path


def publish_to_release(zip_path: Path, token: str) -> None:
    """Upload under a temporary name, then swap it in.

    GitHub has no in-place asset replace. Uploading first and deleting + renaming
    afterwards means the app's download URL is missing for about a second, not
    for the whole ~100 MB upload.
    """
    log.info("STEP 5/5: Publishing to the GitHub release")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    r = requests.get(f"{API}/releases/tags/{RELEASE_TAG}", headers=headers, timeout=30)
    if r.status_code == 401:
        raise UpdateError("GitHub token rejected (expired or revoked?)")
    r.raise_for_status()
    release = r.json()
    assets = {a["name"]: a for a in release["assets"]}

    # Leftover from an interrupted earlier run.
    if STAGED_ASSET_NAME in assets:
        delete_asset(assets[STAGED_ASSET_NAME]["id"], headers)

    log.info(f"  Uploading {zip_path.stat().st_size / 1e6:.1f} MB...")
    with open(zip_path, "rb") as f:
        r = requests.post(
            f"https://uploads.github.com/repos/{REPO}/releases/{release['id']}/assets",
            params={"name": STAGED_ASSET_NAME},
            headers={**headers, "Content-Type": "application/zip"},
            data=f, timeout=(30, 1800),
        )
    if r.status_code == 403:
        raise UpdateError("GitHub token lacks Contents: write on the repo")
    r.raise_for_status()
    new_asset_id = r.json()["id"]
    log.info("  ✓ Upload complete")

    if ASSET_NAME in assets:
        delete_asset(assets[ASSET_NAME]["id"], headers)
    r = requests.patch(f"{API}/releases/assets/{new_asset_id}", headers=headers,
                       json={"name": ASSET_NAME}, timeout=30)
    r.raise_for_status()

    stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    r = requests.patch(f"{API}/releases/{release['id']}", headers=headers,
                       json={"body": f"Latest automated database update - {stamp}"}, timeout=30)
    r.raise_for_status()
    log.info(f"  ✓ Release '{RELEASE_TAG}' now serves the new {ASSET_NAME}")


def delete_asset(asset_id: int, headers: dict) -> None:
    r = requests.delete(f"{API}/releases/assets/{asset_id}", headers=headers, timeout=30)
    if r.status_code not in (204, 404):
        r.raise_for_status()


def replace_local_db() -> None:
    os.replace(WORK_DIR / "boardgames.db", LOCAL_DB)
    log.info(f"  ✓ Local {LOCAL_DB.name} replaced")


# -----------------------
# Main
# -----------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-upload", action="store_true",
                        help="stop after validation; leave release and local DB untouched")
    args = parser.parse_args()

    log_file = setup_logging()
    WORK_DIR.mkdir(exist_ok=True)

    # A manual run and the timer must not share the work folder.
    lock = open(WORK_DIR / ".lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.error("Another update is already running — exiting")
        return 1

    started = datetime.now()
    log.info("=" * 60)
    log.info(f"BoardGame Scout database update — {'DRY RUN (no upload)' if args.no_upload else 'full run'}")
    log.info("=" * 60)

    try:
        secrets = load_secrets(need_github=not args.no_upload)

        # Fresh work folder each run, so nothing stale leaks into this one.
        for item in WORK_DIR.iterdir():
            if item.name != ".lock":
                shutil.rmtree(item) if item.is_dir() else item.unlink()

        download_bgg_csv(secrets)
        games_before = download_release_db()
        enrich_db(secrets)
        zip_path = validate_and_zip(games_before)

        if args.no_upload:
            log.info(f"Dry run: stopping here. Result left in {WORK_DIR}")
        else:
            publish_to_release(zip_path, secrets["GITHUB_TOKEN"])
            replace_local_db()

        minutes = (datetime.now() - started).total_seconds() / 60
        log.info(f"✅ Done in {minutes:.1f} min")
        return 0

    except Exception as e:
        if isinstance(e, UpdateError):
            reason = str(e)
            log.error(f"❌ Update failed: {reason}")
        else:
            reason = f"{type(e).__name__}: {e}"
            log.exception(f"❌ Update failed: {reason}")
        notify("BoardGame Scout DB update failed", f"{reason}\nLog: {log_file}", urgent=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
