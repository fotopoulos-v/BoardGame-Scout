"""
BGG Guild Members Fetcher
Fetches all members from a BGG guild and optionally their ratings

NOTE: Requires BGG_TOKEN in .streamlit/secrets.toml
"""

import requests
import xml.etree.ElementTree as ET
import sqlite3
import time
import os
from typing import List, Dict
import toml



BGG_TOKEN = os.getenv("BGG_TOKEN")

if not BGG_TOKEN:
    raise RuntimeError(
        "BGG_TOKEN environment variable not set. "
        "Set it before running the script."
    )





# def fetch_guild_members(guild_id: int = 119) -> list[str]:
#     """
#     Fetch *all* guild members via the official XML-API2 paginator
#     with polite rate-limit handling.
#     """
#     members: list[str] = []
#     page = 1
#     headers = {
#         "Authorization": f"Bearer {BGG_TOKEN}",
#         "User-Agent": "BoardGame-Scout/1.0",
#         "Accept": "application/xml",
#     }

#     while True:
#         url = f"https://boardgamegeek.com/xmlapi2/guild?id={guild_id}&members=1&page={page}"
#         print(f"  requesting page {page} …")

#         # ── request with back-off on 429 ──────────────────────────────────────
#         for attempt in range(1, 6):                       # max 5 attempts
#             try:
#                 r = requests.get(url, headers=headers, timeout=30)
#                 if r.status_code == 202:                  # still building
#                     print("    BGG not ready – waiting 5 s")
#                     time.sleep(5)
#                     continue
#                 if r.status_code == 429:                  # rate-limit
#                     wait = 2 ** attempt                   # 2, 4, 8, 16, 32 s
#                     print(f"    429 – backing off {wait} s")
#                     time.sleep(wait)
#                     continue
#                 r.raise_for_status()
#                 break                                     # success

#             except Exception as exc:
#                 print(f"    error: {exc} – retrying page")
#                 time.sleep(5)
#                 continue
#         else:
#             print("    too many retries – aborting")
#             return members

#         # ── parse page --------------------------------------------------------
#         try:
#             root = ET.fromstring(r.content)
#         except ET.ParseError as exc:
#             print(f"    XML error: {exc} – aborting")
#             break

#         page_members = [m.attrib["name"] for m in root.findall(".//member") if m.attrib.get("name")]
#         if not page_members:            # empty page → finished
#             break
#         members.extend(page_members)

#         page += 1
#         time.sleep(3)                   # polite pause between pages

#     print(f"  received {len(members)} unique members")
#     return members
def fetch_guild_members(guild_id: int = 119) -> list[str]:
    members: list[str] = []
    page = 1
    headers = {
        "Authorization": f"Bearer {BGG_TOKEN}",
        "User-Agent": "BoardGame-Scout/1.0",
        "Accept": "application/xml",
    }

    while True:
        url = f"https://boardgamegeek.com/xmlapi2/guild?id={guild_id}&members=1&page={page}"
        print(f"  requesting page {page} …")

        for attempt in range(1, 6):
            try:
                r = requests.get(url, headers=headers, timeout=30)

                if r.status_code == 202:
                    print("    BGG not ready – waiting 5 s")
                    time.sleep(5)
                    continue

                if r.status_code == 429:
                    wait = 2 ** attempt
                    print(f"    429 – backing off {wait} s")
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                break

            except Exception as exc:
                print(f"    error: {exc} – retrying page")
                time.sleep(5)
                continue
        else:
            print("    too many retries – skipping page")
            page += 1
            continue

        try:
            root = ET.fromstring(r.content)
        except ET.ParseError as exc:
            print(f"    XML error: {exc} – skipping page")
            page += 1
            continue

        page_members = [
            m.attrib["name"]
            for m in root.findall(".//member")
            if m.attrib.get("name")
        ]

        if not page_members:
            break

        members.extend(page_members)
        page += 1
        time.sleep(3)

    members = list(dict.fromkeys(members))
    print(f"  received {len(members)} unique members")
    return members











# def fetch_user_ratings(username: str, max_retries: int = 3) -> List[Dict]:
#     """
#     Fetch all game ratings from a user.
    
#     Args:
#         username: BGG username
#         max_retries: Number of retry attempts
    
#     Returns:
#         List of dicts with game_id, rating, game_name
#     """
#     url = f"https://boardgamegeek.com/xmlapi2/collection?username={username}&rated=1&stats=1&subtype=boardgame"
    
#     headers = {
#         "Authorization": f"Bearer {BGG_TOKEN}",
#         "User-Agent": "BoardGame Scout/1.0",
#         "Accept": "application/xml"
#     }
    
#     for attempt in range(max_retries):
#         try:
#             # Add small delay before first request
#             if attempt == 0:
#                 time.sleep(1)
            
#             response = requests.get(url, headers=headers, timeout=20)
            
#             if response.status_code == 202:
#                 # BGG is queuing the request
#                 print(f"  Collection queued, waiting...")
#                 time.sleep(3)
#                 continue
            
#             if response.status_code == 401:
#                 print(f"  ❌ Unauthorized - check BGG token")
#                 return []
            
#             if response.status_code != 200:
#                 return []
            
#             root = ET.fromstring(response.content)
            
#             # Check if collection is empty
#             items = root.findall("item")
#             if not items:
#                 return []
            
#             ratings = []
#             for item in items:
#                 game_id = item.attrib.get("objectid")
                
#                 # Get game name
#                 name_elem = item.find("name")
#                 game_name = name_elem.text if name_elem is not None else "Unknown"
                
#                 # Get rating - check multiple possible locations
#                 rating_value = None
                
#                 # Method 1: stats/rating element
#                 stats = item.find("stats")
#                 if stats is not None:
#                     rating_elem = stats.find("rating")
#                     if rating_elem is not None:
#                         # Check rating attribute
#                         rating_value = rating_elem.attrib.get("value")
                        
#                         # Check value child element
#                         if not rating_value or rating_value == "N/A":
#                             value_elem = rating_elem.find("value")
#                             if value_elem is not None:
#                                 rating_value = value_elem.attrib.get("value")
#                                 if not rating_value:
#                                     rating_value = value_elem.text
                
#                 # Try to convert to float
#                 if rating_value and rating_value != "N/A":
#                     try:
#                         rating = float(rating_value)
#                         if rating > 0:  # Only include actual ratings (not 0)
#                             ratings.append({
#                                 "game_id": int(game_id),
#                                 "game_name": game_name,
#                                 "rating": rating
#                             })
#                     except (ValueError, TypeError):
#                         continue
            
#             return ratings
        
#         except Exception as e:
#             if attempt == max_retries - 1:
#                 print(f"  Error fetching ratings: {e}")
#                 return []
#             time.sleep(2)
    
#     return []
def fetch_user_ratings(username: str, max_retries: int = 6) -> List[Dict]:
    """
    Fetch all rated boardgames for a user from BGG.
    NOTE: BGG collection API is NOT paginated.
    """
    url = (
        "https://boardgamegeek.com/xmlapi2/collection"
        f"?username={username}&rated=1&stats=1&subtype=boardgame"
    )

    headers = {
        "Authorization": f"Bearer {BGG_TOKEN}",
        "User-Agent": "BoardGame-Scout/1.0",
        "Accept": "application/xml",
    }

    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                wait = min(30, 2 ** attempt)
                time.sleep(wait)

            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 202:
                time.sleep(5)
                continue

            if response.status_code == 429:
                time.sleep(10)
                continue

            if response.status_code != 200:
                return []

            root = ET.fromstring(response.content)
            items = root.findall("item")

            if not items:
                return []

            ratings = []
            for item in items:
                game_id = item.attrib.get("objectid")
                name_elem = item.find("name")
                game_name = name_elem.text if name_elem is not None else "Unknown"

                stats = item.find("stats")
                if stats is None:
                    continue

                rating_elem = stats.find("rating")
                if rating_elem is None:
                    continue

                value = rating_elem.attrib.get("value")
                if not value or value == "N/A":
                    continue

                try:
                    rating = float(value)
                    if rating > 0:
                        ratings.append({
                            "game_id": int(game_id),
                            "game_name": game_name,
                            "rating": rating
                        })
                except ValueError:
                    continue

            return ratings

        except Exception:
            if attempt == max_retries:
                return []

    return []






def create_ratings_database(db_path: str = "user_ratings.db"):
    """Create SQLite database for user ratings."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            ratings_count INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            game_id INTEGER,
            game_name TEXT,
            rating REAL,
            FOREIGN KEY (username) REFERENCES users(username),
            UNIQUE(username, game_id)
        )
    """)
    
    # Create indices for faster queries
    cur.execute("CREATE INDEX IF NOT EXISTS idx_game_id ON ratings(game_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_username ON ratings(username)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rating ON ratings(rating)")
    
    conn.commit()
    conn.close()
    print(f"Database created: {db_path}")


def save_user_ratings(username: str, ratings: List[Dict], db_path: str = "user_ratings.db"):
    """Save user ratings to database."""
    if not ratings:
        return
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    try:
        # Insert/update user
        cur.execute("""
            INSERT OR REPLACE INTO users (username, ratings_count, last_updated)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, (username, len(ratings)))
        
        # Insert ratings
        for r in ratings:
            cur.execute("""
                INSERT OR REPLACE INTO ratings (username, game_id, game_name, rating)
                VALUES (?, ?, ?, ?)
            """, (username, r["game_id"], r["game_name"], r["rating"]))
        
        conn.commit()
    except Exception as e:
        print(f"Error saving ratings for {username}: {e}")
        conn.rollback()
    finally:
        conn.close()


def build_ratings_database_from_guild(
    guild_id: int = 119,
    db_path: str = "user_ratings.db",
    delay_between_users: float = 2.0,
    max_users: int = None
):
    """
    Main function to build ratings database from guild members.
    
    Args:
        guild_id: BGG guild ID
        db_path: Path to SQLite database
        delay_between_users: Seconds to wait between API calls (be nice to BGG!)
        max_users: Limit number of users to process (None = all)
    """
    print("="*60)
    print("BGG User Ratings Database Builder")
    print("="*60)
    
    # Step 1: Create database
    create_ratings_database(db_path)
    
    # Step 2: Fetch guild members
    members = fetch_guild_members(guild_id)
    
    if not members:
        print("No members found!")
        return
    
    if max_users:
        members = members[:max_users]
        print(f"Processing first {max_users} members...")
    
    # Step 3: Fetch ratings for each member
    total = len(members)
    successful = 0
    
    for i, username in enumerate(members, 1):
        print(f"\n[{i}/{total}] Processing: {username}")
        
        ratings = fetch_user_ratings(username)
        
        if ratings:
            save_user_ratings(username, ratings, db_path)
            successful += 1
            print(f"  ✓ Saved {len(ratings)} ratings")
        else:
            print(f"  ✗ No ratings found")
        
        # Be nice to BGG servers
        if i < total:
            time.sleep(delay_between_users)
    
    print("\n" + "="*60)
    print(f"COMPLETE! Processed {successful}/{total} users")
    print(f"Database saved: {db_path}")
    print("="*60)


if __name__ == "__main__":
    # To process ALL users (will take ~3.5 hours with 3s delay):
    build_ratings_database_from_guild(
        guild_id=119,
        db_path="greek_user_ratings.db",
        delay_between_users=3,
        max_users=None
    )