#!/usr/bin/env python3
"""
enrich_bgg_db.py

Reads a CSV of BGG games (must contain an 'id' column), fetches details from the
BGG XMLAPI2 "thing" endpoint, and stores/enriches a SQLite DB with the requested columns.

Features:
- Batch fetching (20 ids per request)
- Resume support (skips ids already in DB unless --force)
- Progress prints every N games (default 200)
- Exponential backoff & retry on HTTP errors
- last_updated timestamp recorded
"""

import argparse
import csv
import os
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Optional

import requests





# ----------------------
# Configuration defaults
# ----------------------
BGG_API_BASE = "https://boardgamegeek.com/xmlapi2/thing"
BATCH_SIZE = 20            # BGG works well with 20 per batch
SLEEP_BETWEEN_BATCHES = 2  # seconds to wait between batches (rate friendly)
PROGRESS_EVERY = 200       # print progress every N processed

# ----------------------
# Helpers: safe extraction
# ----------------------
def safe_text(node):
    if node is None:
        return None
    # Node.text could contain newlines; keep as-is (strip optional)
    return node.text.strip() if node.text is not None else None

def safe_attr_int(node, attr="value"):
    if node is None:
        return None
    v = node.attrib.get(attr)
    try:
        return int(v) if v not in (None, "") else None
    except Exception:
        return None

def safe_attr_float(node, attr="value"):
    if node is None:
        return None
    v = node.attrib.get(attr)
    try:
        return float(v) if v not in (None, "") else None
    except Exception:
        return None

# ----------------------
# XML parsing for thing
# ----------------------
def parse_thing_item(item: ET.Element) -> Dict:
    """Parse a <item> element from /thing response into a dict of fields."""
    # Title (primary)
    title = None
    for nm in item.findall("name"):
        if nm.attrib.get("type") == "primary":
            title = nm.attrib.get("value")
            break
    if not title:
        nm = item.find("name")
        title = nm.attrib.get("value") if nm is not None else None

    # Basic nodes
    description = safe_text(item.find("description"))
    thumbnail = safe_text(item.find("thumbnail"))
    image = safe_text(item.find("image"))

    # Stats under statistics/ratings
    stats = item.find("statistics/ratings")
    geek_rating = None
    avg_rating = None
    num_voters = None
    complexity = None
    if stats is not None:
        geek_rating = safe_attr_float(stats.find("bayesaverage"))
        avg_rating = safe_attr_float(stats.find("average"))
        num_voters = safe_attr_int(stats.find("usersrated"))
        # averageweight is inside ratings in older schema; try both locations
        complexity = safe_attr_float(stats.find("averageweight"))

    # Publication and gameplay fields
    year_published = safe_attr_int(item.find("yearpublished"))
    min_players = safe_attr_int(item.find("minplayers"))
    max_players = safe_attr_int(item.find("maxplayers"))
    min_playtime = safe_attr_int(item.find("minplaytime"))
    max_playtime = safe_attr_int(item.find("maxplaytime"))
    min_age = safe_attr_int(item.find("minage"))
    playing_time = safe_attr_int(item.find("playingtime"))  # optional

    # Links: categories, designers, artists, publishers, mechanics
    categories = []
    designers = []
    artists = []
    publishers = []
    mechanics = []
    for lk in item.findall("link"):
        t = lk.attrib.get("type")
        v = lk.attrib.get("value", "")
        if t == "boardgamecategory":
            categories.append(v)
        elif t == "boardgamedesigner":
            designers.append(v)
        elif t == "boardgameartist":
            artists.append(v)
        elif t == "boardgamepublisher":
            publishers.append(v)
        elif t == "boardgamemechanic":
            mechanics.append(v)

    # Handle single-value fallback: if only min or max provided, mirror
    if min_players is None and max_players is not None:
        min_players = max_players
    if max_players is None and min_players is not None:
        max_players = min_players
    if min_playtime is None and max_playtime is not None:
        min_playtime = max_playtime
    if max_playtime is None and min_playtime is not None:
        max_playtime = min_playtime

    # id from attribute
    gid = item.attrib.get("id")

    return {
        "id": int(gid) if gid is not None else None,
        "title": title,
        "description": description,
        "thumbnail": thumbnail,
        "image": image,
        "geek_rating": round(geek_rating, 2) if geek_rating is not None else None,
        "avg_rating": round(avg_rating, 2) if avg_rating is not None else None,
        "num_voters": num_voters,
        "year_published": year_published,
        "complexity": round(complexity, 2) if complexity is not None else None,
        "min_players": min_players,
        "max_players": max_players,
        "min_playtime": min_playtime,
        "max_playtime": max_playtime,
        "playing_time": playing_time,
        "min_age": min_age,
        "categories": ", ".join(categories) if categories else None,
        "designers": ", ".join(designers) if designers else None,
        "artists": ", ".join(artists) if artists else None,
        "publishers": ", ".join(publishers) if publishers else None,
        "mechanics": ", ".join(mechanics) if mechanics else None,
    }

# ----------------------
# BGG API fetch function
# ----------------------
def fetch_things_batch(ids: List[str], token: Optional[str] = None, max_retries: int = 4) -> List[Dict]:
    """
    Fetch /thing?id=id1,id2,...&stats=1 for up to BATCH_SIZE ids.
    Returns parsed results as list of dicts.
    """
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    params = {"id": ",".join(ids), "stats": 1}
    attempt = 0
    backoff = 1.0
    while attempt <= max_retries:
        try:
            resp = requests.get(BGG_API_BASE, headers=headers, params=params, timeout=30)
        except Exception as e:
            attempt += 1
            print(f"Network error fetching batch (attempt {attempt}/{max_retries}): {e}. Backing off {backoff}s.")
            time.sleep(backoff)
            backoff *= 2
            continue

        # If queued/processing (some endpoints may return 202) wait & retry
        if resp.status_code == 202:
            attempt += 1
            wait = backoff
            print(f"BGG returned 202 (queued). Waiting {wait}s before retrying (attempt {attempt}/{max_retries}).")
            time.sleep(wait)
            backoff *= 2
            continue

        if resp.status_code != 200:
            attempt += 1
            print(f"BGG returned {resp.status_code} for ids [{ids[0]}...]. Response text starts: {resp.text[:200]}")
            if attempt <= max_retries:
                print(f"Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
                continue
            else:
                raise RuntimeError(f"BGG thing failed with status {resp.status_code}")

        # Successful 200
        root = ET.fromstring(resp.text)
        results = []
        for item in root.findall("item"):
            try:
                parsed = parse_thing_item(item)
                results.append(parsed)
            except Exception as ex:
                print(f"Warning: failed to parse item: {ex}")
        return results

    raise RuntimeError("Exceeded retries while fetching BGG data.")

# ----------------------
# SQLite DB utilities
# ----------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY,
    rank INTEGER,
    title TEXT,
    description TEXT,
    thumbnail TEXT,
    image TEXT,
    geek_rating REAL,
    avg_rating REAL,
    num_voters INTEGER,
    year_published INTEGER,
    complexity REAL,
    min_players INTEGER,
    max_players INTEGER,
    min_playtime INTEGER,
    max_playtime INTEGER,
    playing_time INTEGER,
    min_age INTEGER,
    categories TEXT,
    designers TEXT,
    artists TEXT,
    publishers TEXT,
    mechanics TEXT,
    last_updated TEXT
);
"""

UPSERT_SQL = """
INSERT INTO games (
    id, rank, title, description, thumbnail, image,
    geek_rating, avg_rating, num_voters, year_published, complexity,
    min_players, max_players, min_playtime, max_playtime, playing_time, min_age,
    categories, designers, artists, publishers, mechanics, last_updated
) VALUES (
    :id, :rank, :title, :description, :thumbnail, :image,
    :geek_rating, :avg_rating, :num_voters, :year_published, :complexity,
    :min_players, :max_players, :min_playtime, :max_playtime, :playing_time, :min_age,
    :categories, :designers, :artists, :publishers, :mechanics, :last_updated
)
ON CONFLICT(id) DO UPDATE SET
    rank=excluded.rank,
    title=excluded.title,
    description=excluded.description,
    thumbnail=excluded.thumbnail,
    image=excluded.image,
    geek_rating=excluded.geek_rating,
    avg_rating=excluded.avg_rating,
    num_voters=excluded.num_voters,
    year_published=excluded.year_published,
    complexity=excluded.complexity,
    min_players=excluded.min_players,
    max_players=excluded.max_players,
    min_playtime=excluded.min_playtime,
    max_playtime=excluded.max_playtime,
    playing_time=excluded.playing_time,
    min_age=excluded.min_age,
    categories=excluded.categories,
    designers=excluded.designers,
    artists=excluded.artists,
    publishers=excluded.publishers,
    mechanics=excluded.mechanics,
    last_updated=excluded.last_updated;
"""

def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.executescript(CREATE_TABLE_SQL)
    conn.commit()

# ----------------------
# Main enrichment logic
# ----------------------
def enrich_database(csv_path: str, db_path: str, token: Optional[str] = None,
                    start: int = 0, limit: Optional[int] = None,
                    force: bool = False, progress_every: int = PROGRESS_EVERY):
    # Load ids from CSV
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV input not found: {csv_path}")
    ids_list = []
    ranks = {}   # optional: if the CSV contains rank we keep it
    titles_from_csv = {}

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=',')
        if 'id' not in reader.fieldnames:
            raise RuntimeError("Input CSV must have an 'id' column.")
        # optionally detect rank/title column names
        rank_name = None
        if 'rank' in reader.fieldnames:
            rank_name = 'rank'
        title_name = None
        if 'name' in reader.fieldnames:
            title_name = 'name'
        for row in reader:
            gid = row.get('id') or row.get('ID') or row.get('Id')
            if not gid:
                continue
            try:
                gid_i = str(int(gid))
            except Exception:
                gid_i = gid.strip()
            ids_list.append(gid_i)
            if rank_name and row.get(rank_name):
                try:
                    ranks[gid_i] = int(row.get(rank_name))
                except Exception:
                    ranks[gid_i] = None
            if title_name and row.get(title_name):
                titles_from_csv[gid_i] = row.get(title_name)

    total = len(ids_list)
    if limit is not None:
        ids_list = ids_list[start:start+limit]
    else:
        ids_list = ids_list[start:]

    print(f"Total ids to consider (after start/limit): {len(ids_list)} (original total {total})")

    conn = sqlite3.connect(db_path, timeout=30)
    init_db(conn)
    cur = conn.cursor()

    # determine which ids are already present
    existing_ids = set()
    if not force:
        q = "SELECT id FROM games"
        cur.execute(q)
        existing_ids = set(str(r[0]) for r in cur.fetchall())

    to_process = [gid for gid in ids_list if (force or str(gid) not in existing_ids)]
    print(f"Will process {len(to_process)} ids (skipping {len(ids_list)-len(to_process)} already present)")

    processed = 0
    inserted = 0
    batch = []
    for idx, gid in enumerate(to_process, start=1):
        batch.append(gid)
        # When batch full or last element, fetch
        if len(batch) >= BATCH_SIZE or idx == len(to_process):
            try:
                results = fetch_things_batch(batch, token=token)
            except Exception as e:
                print(f"ERROR fetching batch starting with {batch[0]}: {e}")
                # skip this batch but continue
                batch = []
                time.sleep(SLEEP_BETWEEN_BATCHES)
                continue

            now = datetime.utcnow().isoformat(timespec='seconds') + "Z"
            # Insert/Upsert rows
            with conn:
                for r in results:
                    gid_str = str(r.get("id"))
                    # attach rank/title from CSV when available (rank is optional)
                    db_row = {
                        "id": r.get("id"),
                        "rank": ranks.get(gid_str),
                        "title": r.get("title") or titles_from_csv.get(gid_str),
                        "description": r.get("description"),
                        "thumbnail": r.get("thumbnail"),
                        "image": r.get("image"),
                        "geek_rating": r.get("geek_rating"),
                        "avg_rating": r.get("avg_rating"),
                        "num_voters": r.get("num_voters"),
                        "year_published": r.get("year_published"),
                        "complexity": r.get("complexity"),
                        "min_players": r.get("min_players"),
                        "max_players": r.get("max_players"),
                        "min_playtime": r.get("min_playtime"),
                        "max_playtime": r.get("max_playtime"),
                        "playing_time": r.get("playing_time"),
                        "min_age": r.get("min_age"),
                        "categories": r.get("categories"),
                        "designers": r.get("designers"),
                        "artists": r.get("artists"),
                        "publishers": r.get("publishers"),
                        "mechanics": r.get("mechanics"),
                        "last_updated": now
                    }
                    cur.execute(UPSERT_SQL, db_row)
                    inserted += 1

            processed += len(batch)
            if processed % progress_every == 0 or idx == len(to_process):
                print(f"[{datetime.utcnow().isoformat()}] Processed {processed}/{len(to_process)} (inserted {inserted})")
            # reset batch and sleep
            batch = []
            time.sleep(SLEEP_BETWEEN_BATCHES)

    conn.close()
    print(f"Done. Total processed: {processed}. Total inserted/updated: {inserted}.")

# ----------------------
# CLI
# ----------------------
def main():
    parser = argparse.ArgumentParser(description="Enrich BGG CSV into SQLite DB using XMLAPI2")
    parser.add_argument("--input", "-i", required=True, help="Input CSV (download from BGG: bg_ranks.csv)")
    parser.add_argument("--db", "-d", required=True, help="Output SQLite DB path (will be created if not present)")
    parser.add_argument("--token", "-t", required=False, help="BGG authorization token (optional). If omitted, reads BGG_TOKEN env var.")
    parser.add_argument("--start", type=int, default=0, help="Start offset into CSV (for resuming/testing)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows to process (for testing)")
    parser.add_argument("--force", action="store_true", help="Force refresh even if id already exists in DB")
    parser.add_argument("--progress-every", type=int, default=PROGRESS_EVERY, help="How often to print progress")
    args = parser.parse_args()

    token = args.token or os.environ.get("BGG_TOKEN")
    if not token:
        print("Warning: no BGG token provided. You can still try, but requests may be unauthenticated and may fail if BGG requires tokens for your app.")
    enrich_database(args.input, args.db, token=token, start=args.start, limit=args.limit, force=args.force, progress_every=args.progress_every)

if __name__ == "__main__":
    main()
