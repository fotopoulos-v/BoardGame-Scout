#!/usr/bin/env python3
"""
enrich_bgg_db.py (incremental version)

Updates boardgames.db by:
- Loading boardgames_ranks.csv (must contain id + usersrated)
- Comparing CSV.num_voters with DB.num_voters
- Updating ONLY games where num_voters changed OR game missing in DB

This makes updates fast and suitable for daily GitHub Actions.
"""

import argparse
import csv
import os
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Optional

import requests

# ----------------------
# Configuration
# ----------------------
BGG_API_BASE = "https://boardgamegeek.com/xmlapi2/thing"
BATCH_SIZE = 20
SLEEP_BETWEEN_BATCHES = 2
PROGRESS_EVERY = 200

# ----------------------
# Helpers
# ----------------------
def safe_text(node):
    if node is None:
        return None
    return node.text.strip() if node.text else None

def safe_attr_int(node, attr="value"):
    if node is None:
        return None
    v = node.attrib.get(attr)
    try:
        return int(v) if v not in (None, "") else None
    except:
        return None

def safe_attr_float(node, attr="value"):
    if node is None:
        return None
    v = node.attrib.get(attr)
    try:
        return float(v) if v not in (None, "") else None
    except:
        return None

# ----------------------
# XML parsing
# ----------------------
def parse_thing_item(item: ET.Element) -> Dict:
    title = None
    for nm in item.findall("name"):
        if nm.attrib.get("type") == "primary":
            title = nm.attrib.get("value")
            break
    if not title:
        nm = item.find("name")
        title = nm.attrib.get("value") if nm is not None else None

    description = safe_text(item.find("description"))
    thumbnail = safe_text(item.find("thumbnail"))
    image = safe_text(item.find("image"))

    stats = item.find("statistics/ratings")
    geek_rating = avg_rating = num_voters = complexity = None
    if stats is not None:
        geek_rating = safe_attr_float(stats.find("bayesaverage"))
        avg_rating = safe_attr_float(stats.find("average"))
        num_voters = safe_attr_int(stats.find("usersrated"))
        complexity = safe_attr_float(stats.find("averageweight"))

    year_published = safe_attr_int(item.find("yearpublished"))
    min_players = safe_attr_int(item.find("minplayers"))
    max_players = safe_attr_int(item.find("maxplayers"))
    min_playtime = safe_attr_int(item.find("minplaytime"))
    max_playtime = safe_attr_int(item.find("maxplaytime"))
    playing_time = safe_attr_int(item.find("playingtime"))
    min_age = safe_attr_int(item.find("minage"))

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

    gid = int(item.attrib.get("id"))

    return {
        "id": gid,
        "title": title,
        "description": description,
        "thumbnail": thumbnail,
        "image": image,
        "geek_rating": round(geek_rating, 2) if geek_rating else None,
        "avg_rating": round(avg_rating, 2) if avg_rating else None,
        "num_voters": num_voters,
        "year_published": year_published,
        "complexity": round(complexity, 2) if complexity else None,
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
# BGG batch fetch
# ----------------------
def fetch_things_batch(ids: List[str], token=None, max_retries=4):
    params = {"id": ",".join(ids), "stats": 1}

    backoff = 1.0
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(BGG_API_BASE, params=params, timeout=30)
        except Exception as e:
            time.sleep(backoff)
            backoff *= 2
            continue

        if resp.status_code == 202:
            time.sleep(backoff)
            backoff *= 2
            continue

        if resp.status_code != 200:
            if attempt == max_retries:
                raise RuntimeError(f"BGG returned {resp.status_code}")
            time.sleep(backoff)
            backoff *= 2
            continue

        root = ET.fromstring(resp.text)
        return [parse_thing_item(item) for item in root.findall("item")]

    raise RuntimeError("Exceeded retries")

# ----------------------
# SQLite
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

# ----------------------
# Incremental logic
# ----------------------
def enrich_database(csv_path, db_path):

    # --- Load CSV: id + rank + usersrated ---
    csv_ids = []
    csv_ranks = {}
    csv_votes = {}

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = row["id"]
            csv_ids.append(gid)

            # rank may be absent
            rank_val = row.get("rank")
            csv_ranks[gid] = int(rank_val) if rank_val and rank_val.isdigit() else None

            # IMPORTANT: this determines if update required
            ur = row.get("usersrated") or row.get("num_voters") or row.get("users_rated")
            try:
                csv_votes[gid] = int(ur)
            except:
                csv_votes[gid] = None

    # --- Load DB values ---
    conn = sqlite3.connect(db_path, timeout=30)
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    cur.execute("SELECT id, num_voters FROM games")
    db_data = {str(r[0]): r[1] for r in cur.fetchall()}

    # --- Determine which IDs to update ---
    to_update = []
    for gid in csv_ids:
        csv_v = csv_votes.get(gid)
        db_v = db_data.get(gid)

        # new game or changed voters?
        if db_v is None or csv_v != db_v:
            to_update.append(gid)

    print(f"Total games in CSV: {len(csv_ids)}")
    print(f"Games needing update: {len(to_update)}")

    # --- Fetch and update DB ---
    processed = 0
    for i in range(0, len(to_update), BATCH_SIZE):
        batch = to_update[i:i+BATCH_SIZE]
        results = fetch_things_batch(batch)

        now = datetime.utcnow().isoformat(timespec='seconds') + "Z"

        with conn:
            for r in results:
                gid_str = str(r["id"])
                row = {
                    "id": r["id"],
                    "rank": csv_ranks.get(gid_str),
                    "title": r["title"],
                    "description": r["description"],
                    "thumbnail": r["thumbnail"],
                    "image": r["image"],
                    "geek_rating": r["geek_rating"],
                    "avg_rating": r["avg_rating"],
                    "num_voters": r["num_voters"],
                    "year_published": r["year_published"],
                    "complexity": r["complexity"],
                    "min_players": r["min_players"],
                    "max_players": r["max_players"],
                    "min_playtime": r["min_playtime"],
                    "max_playtime": r["max_playtime"],
                    "playing_time": r["playing_time"],
                    "min_age": r["min_age"],
                    "categories": r["categories"],
                    "designers": r["designers"],
                    "artists": r["artists"],
                    "publishers": r["publishers"],
                    "mechanics": r["mechanics"],
                    "last_updated": now
                }
                cur.execute(UPSERT_SQL, row)

        processed += len(batch)
        if processed % PROGRESS_EVERY == 0:
            print(f"Updated {processed}/{len(to_update)}")

        time.sleep(SLEEP_BETWEEN_BATCHES)

    conn.close()
    print("Done.")

# ----------------------
# CLI
# ----------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", required=True)
    parser.add_argument("--db", "-d", required=True)
    args = parser.parse_args()

    enrich_database(args.input, args.db)


if __name__ == "__main__":
    main()
