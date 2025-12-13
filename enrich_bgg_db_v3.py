import sqlite3
import requests
import time
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd
import os

# -----------------------
# CONFIG
# -----------------------
CSV_PATH = "boardgames_ranks.csv"
DB_PATH = "boardgames.db"
BATCH_SIZE = 20
SLEEP_BETWEEN_BATCHES = 2
PROGRESS_EVERY = 200

# Get token from environment (GitHub Actions) or Streamlit secrets (local)
def get_bgg_token():
    """Get BGG token from environment or Streamlit secrets."""
    # Try environment variable first (GitHub Actions)
    token = os.getenv("BGG_TOKEN")
    if token:
        return token
    
    # Try Streamlit secrets (local development)
    try:
        import streamlit as st
        return st.secrets.get("BGG_TOKEN", "")
    except:
        return ""

token = get_bgg_token()


def parse_thing_item(item):
    def text_or_none(elem, attr=None):
        if elem is None:
            return None
        if attr:
            return elem.get(attr)
        return elem.text

    data = {}
    data["id"] = int(item.get("id"))
    data["title"] = text_or_none(item.find("name[@type='primary']"), "value")
    data["description"] = text_or_none(item.find("description"))
    data["thumbnail"] = text_or_none(item.find("thumbnail"))
    data["image"] = text_or_none(item.find("image"))
    data["year_published"] = int(text_or_none(item.find("yearpublished"), "value") or 0)

    stats = item.find("statistics/ratings")
    if stats is not None:
        data["geek_rating"] = float(text_or_none(stats.find("bayesaverage"), "value") or 0)
        data["avg_rating"] = float(text_or_none(stats.find("average"), "value") or 0)
        data["num_voters"] = int(text_or_none(stats.find("usersrated"), "value") or 0)
        data["complexity"] = float(text_or_none(stats.find("averageweight"), "value") or 0)
    else:
        data["geek_rating"] = 0
        data["avg_rating"] = 0
        data["num_voters"] = 0
        data["complexity"] = 0

    data["min_players"] = int(text_or_none(item.find("minplayers"), "value") or 0)
    data["max_players"] = int(text_or_none(item.find("maxplayers"), "value") or 0)
    data["min_playtime"] = int(text_or_none(item.find("minplaytime"), "value") or 0)
    data["max_playtime"] = int(text_or_none(item.find("maxplaytime"), "value") or 0)
    data["playing_time"] = int(text_or_none(item.find("playingtime"), "value") or 0)
    data["min_age"] = int(text_or_none(item.find("minage"), "value") or 0)

    def get_links(type_name):
        return ", ".join([e.get("value") for e in item.findall(f"link[@type='{type_name}']")])

    data["categories"] = get_links("boardgamecategory")
    data["mechanics"] = get_links("boardgamemechanic")
    data["designers"] = get_links("boardgamedesigner")
    data["artists"] = get_links("boardgameartist")
    data["publishers"] = get_links("boardgamepublisher")

    return data


# -----------------------
# FETCH BGG DETAILS (BATCH)
# -----------------------
def fetch_bgg_details(game_ids: list, token="", max_retries: int = 5) -> list:
    """
    Fetch a batch of games from BGG XMLAPI2 /thing endpoint.
    Returns a list of dicts with game details.
    """
    headers = {"User-Agent": "BoardGame Scout/1.0", "Accept": "application/xml"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    params = {"id": ",".join(map(str, game_ids)), "stats": 1}
    attempt = 0
    backoff = 2

    while attempt <= max_retries:
        try:
            r = requests.get("https://boardgamegeek.com/xmlapi2/thing", headers=headers, params=params, timeout=30)
        except Exception as e:
            attempt += 1
            print(f"Network error on batch {game_ids[:3]}...: {e}. Backing off {backoff}s.")
            time.sleep(backoff)
            backoff *= 2
            continue

        if r.status_code == 202:
            print(f"BGG queued batch {game_ids[:3]}..., waiting {backoff}s.")
            time.sleep(backoff)
            backoff *= 2
            attempt += 1
            continue

        if r.status_code == 429:
            print(f"BGG rate limit for batch {game_ids[:3]}..., retrying in {backoff}s.")
            time.sleep(backoff)
            backoff *= 2
            attempt += 1
            continue

        if r.status_code != 200:
            print(f"BGG error {r.status_code} for batch {game_ids[:3]}... Response: {r.text[:100]}")
            attempt += 1
            time.sleep(backoff)
            backoff *= 2
            continue

        # Parse XML
        root = ET.fromstring(r.content)
        results = []
        for item in root.findall("item"):
            try:
                results.append(parse_thing_item(item))
            except Exception as ex:
                print(f"Warning: failed to parse item {item.attrib.get('id')}: {ex}")
        return results

    raise RuntimeError(f"Exceeded max retries for batch {game_ids[:3]}...")


# -----------------------
# MAIN UPDATE FUNCTION
# -----------------------
def main(token=None):
    if token is None:
        token = get_bgg_token()
    
    print("============================================")
    print("  Starting incremental BoardGame DB update")
    start_time = time.time()
    print(f"  Start time: {datetime.now()}")
    print("============================================\n")

    # Load CSV
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
    
    df = pd.read_csv(CSV_PATH)
    df.rename(columns={"usersrated": "csv_num_voters"}, inplace=True)

    # Load DB
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database file not found: {DB_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Build current voter counts map
    db_voters = {row[0]: row[1] for row in c.execute("SELECT id, num_voters FROM games")}

    # Separate new games and games to update
    new_game_rows = {}
    to_update_rows = {}

    for _, row in df.iterrows():
        game_id = int(row["id"])
        csv_voters = int(row["csv_num_voters"])
        if game_id not in db_voters:
            new_game_rows[game_id] = row
        elif csv_voters != db_voters[game_id]:
            to_update_rows[game_id] = row

    new_game_ids = list(new_game_rows.keys())
    to_update_ids = list(to_update_rows.keys())

    print(f"New games to insert: {len(new_game_ids)}")
    print(f"Games to update (voters changed): {len(to_update_ids)}\n")

    inserted_count = 0
    updated_count = 0

    # -----------------------
    # Insert new games (batched)
    # -----------------------
    for i in range(0, len(new_game_ids), BATCH_SIZE):
        batch_ids = new_game_ids[i:i+BATCH_SIZE]

        try:
            batch_results = fetch_bgg_details(batch_ids, token=token)
        except Exception as e:
            print(f"Skipping new-game batch {batch_ids[:3]} due to error: {e}")
            continue

        now = datetime.utcnow().isoformat(timespec='seconds') + "Z"
        for game in batch_results:
            game_id = game["id"]
            row = new_game_rows[game_id]

            c.execute("""
                INSERT INTO games (
                    id, rank, title, description, thumbnail, image,
                    geek_rating, avg_rating, num_voters, year_published,
                    complexity, min_players, max_players, min_playtime,
                    max_playtime, playing_time, min_age, categories,
                    designers, artists, publishers, mechanics, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                game_id, row.get("rank"),
                game["title"], game["description"], game["thumbnail"], game["image"],
                game["geek_rating"], game["avg_rating"], game["num_voters"],
                game["year_published"], game["complexity"], game["min_players"],
                game["max_players"], game["min_playtime"], game["max_playtime"],
                game["playing_time"], game["min_age"], game["categories"],
                game["designers"], game["artists"], game["publishers"],
                game["mechanics"], now
            ))
            inserted_count += 1

        if (i // BATCH_SIZE + 1) % (PROGRESS_EVERY // BATCH_SIZE) == 0:
            print(f"Inserted {min(i+BATCH_SIZE, len(new_game_ids))}/{len(new_game_ids)} new games...")
        time.sleep(SLEEP_BETWEEN_BATCHES)

    conn.commit()

    # -----------------------
    # Update existing games (batched)
    # -----------------------
    for i in range(0, len(to_update_ids), BATCH_SIZE):
        batch_ids = to_update_ids[i:i+BATCH_SIZE]

        try:
            batch_results = fetch_bgg_details(batch_ids, token=token)
        except Exception as e:
            print(f"Skipping update batch {batch_ids[:3]} due to error: {e}")
            continue

        now = datetime.utcnow().isoformat(timespec='seconds') + "Z"
        for game in batch_results:
            game_id = game["id"]
            row = to_update_rows[game_id]

            c.execute("""
                UPDATE games SET
                    rank = ?, title = ?, description = ?, thumbnail = ?, image = ?,
                    geek_rating = ?, avg_rating = ?, num_voters = ?, year_published = ?,
                    complexity = ?, min_players = ?, max_players = ?, min_playtime = ?,
                    max_playtime = ?, playing_time = ?, min_age = ?, categories = ?,
                    designers = ?, artists = ?, publishers = ?, mechanics = ?,
                    last_updated = ?
                WHERE id = ?
            """, (
                row.get("rank"),
                game["title"], game["description"], game["thumbnail"], game["image"],
                game["geek_rating"], game["avg_rating"], game["num_voters"],
                game["year_published"], game["complexity"], game["min_players"],
                game["max_players"], game["min_playtime"], game["max_playtime"],
                game["playing_time"], game["min_age"], game["categories"],
                game["designers"], game["artists"], game["publishers"],
                game["mechanics"], now, game_id
            ))
            updated_count += 1

        if (i // BATCH_SIZE + 1) % (PROGRESS_EVERY // BATCH_SIZE) == 0 or i + BATCH_SIZE >= len(to_update_ids):
            print(f"Processed {min(i+BATCH_SIZE, len(to_update_ids))}/{len(to_update_ids)} updates...")
        time.sleep(SLEEP_BETWEEN_BATCHES)

    conn.commit()
    conn.close()

    end_time = time.time()
    duration = round(end_time - start_time, 2)

    print("\n============================================")
    print(f"Update complete!")
    print(f"Inserted games: {inserted_count}")
    print(f"Updated games: {updated_count}")
    print(f"End time: {datetime.now()}")
    print(f"Total duration: {duration} seconds ({duration/60:.2f} min)")
    print("============================================")


if __name__ == "__main__":
    main()