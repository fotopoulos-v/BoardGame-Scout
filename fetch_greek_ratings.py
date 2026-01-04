"""
BGG Greek Ratings Fetcher - Rotating 3-Day Update Cycle
Updates 1/3 of users each day based on oldest date_updated
"""
import requests
import xml.etree.ElementTree as ET
import time
import sqlite3
import os
from typing import List, Dict
from datetime import datetime, timedelta

BGG_TOKEN = os.getenv("BGG_TOKEN")
if not BGG_TOKEN:
    raise RuntimeError("BGG_TOKEN environment variable not set.")

# Configuration
USERS_PER_RUN = 850
consecutive_429s = 0
current_delay = 10.0
MIN_DELAY = 10.0
MAX_DELAY = 15.0


def adjust_delay_after_429():
    """Increase delay after getting rate limited."""
    global current_delay, consecutive_429s
    consecutive_429s += 1
    current_delay = min(MAX_DELAY, current_delay * 1.2)
    print(f"  ⚠️  Rate limited! Increasing delay to {current_delay:.1f}s")


def adjust_delay_after_success():
    """Gradually decrease delay after successful requests."""
    global current_delay, consecutive_429s
    consecutive_429s = 0
    current_delay = max(MIN_DELAY, current_delay * 0.98)


def fetch_guild_members(guild_id: int = 119) -> List[str]:
    """Fetch all members from a BGG guild."""
    members = []
    page = 1
    headers = {
        "Authorization": f"Bearer {BGG_TOKEN}",
        "User-Agent": "BoardGame-Scout/1.0",
        "Accept": "application/xml",
    }
    
    print("Fetching guild members...")
    
    while True:
        url = f"https://boardgamegeek.com/xmlapi2/guild?id={guild_id}&members=1&page={page}"
        
        for attempt in range(5):
            try:
                r = requests.get(url, headers=headers, timeout=30)
                
                if r.status_code == 202:
                    time.sleep(5)
                    continue
                
                if r.status_code == 429:
                    time.sleep(10)
                    continue
                
                r.raise_for_status()
                break
            except:
                time.sleep(5)
                continue
        else:
            page += 1
            continue
        
        try:
            root = ET.fromstring(r.content)
        except:
            page += 1
            continue
        
        page_members = [m.attrib["name"] for m in root.findall(".//member") if m.attrib.get("name")]
        
        if not page_members:
            break
        
        members.extend(page_members)
        page += 1
        time.sleep(3)
    
    members = list(dict.fromkeys(members))
    print(f"Found {len(members)} unique members\n")
    return members


def fetch_user_ratings(username: str, max_retries: int = 5) -> List[Dict]:
    """Fetch all game ratings from a user."""
    global consecutive_429s
    
    url = f"https://boardgamegeek.com/xmlapi2/collection?username={username}&rated=1&stats=1&subtype=boardgame"
    headers = {
        "Authorization": f"Bearer {BGG_TOKEN}",
        "User-Agent": "BoardGame-Scout/1.0",
        "Accept": "application/xml",
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 202:
                time.sleep(5)
                continue
            
            if response.status_code == 429:
                adjust_delay_after_429()
                wait_time = 30 if consecutive_429s > 3 else 20
                time.sleep(wait_time)
                continue
            
            if response.status_code == 401:
                print(f"  ❌ Unauthorized")
                return []
            
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
                except (ValueError, TypeError):
                    continue
            
            adjust_delay_after_success()
            return ratings
            
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"  Error: {e}")
                return []
            time.sleep(3)
    
    return []


def initialize_database(db_path: str, all_users: List[str]):
    """Initialize database with all users if not exists."""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cursor = conn.cursor()
    
    # Create ratings table with date_updated column
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
            username TEXT,
            game_id INTEGER,
            game_name TEXT,
            rating REAL,
            date_updated TIMESTAMP,
            PRIMARY KEY (username, game_id)
        )
    """)
    
    # Create index on date_updated for faster queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_date_updated ON ratings(username, date_updated)
    """)
    
    # Create users tracking table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users_tracking (
            username TEXT PRIMARY KEY,
            date_updated TIMESTAMP,
            ratings_count INTEGER DEFAULT 0
        )
    """)
    
    # Insert all users if they don't exist (with NULL date_updated)
    for username in all_users:
        cursor.execute("""
            INSERT OR IGNORE INTO users_tracking (username, date_updated, ratings_count)
            VALUES (?, NULL, 0)
        """, (username,))
    
    conn.commit()
    conn.close()
    print(f"✅ Database initialized with {len(all_users)} users\n")


def get_users_to_update(db_path: str, limit: int) -> List[str]:
    """Get list of users that need updating (oldest date_updated first)."""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cursor = conn.cursor()
    
    # Get users ordered by date_updated (NULL first, then oldest)
    cursor.execute("""
        SELECT username 
        FROM users_tracking 
        ORDER BY 
            CASE WHEN date_updated IS NULL THEN 0 ELSE 1 END,
            date_updated ASC
        LIMIT ?
    """, (limit,))
    
    users = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    return users


def save_ratings_to_db(username: str, ratings: List[Dict], db_path: str):
    """Save ratings to database and update timestamp."""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cursor = conn.cursor()
    
    try:
        current_time = datetime.now().isoformat()
        
        # Delete existing ratings for this user first
        cursor.execute("DELETE FROM ratings WHERE username = ?", (username,))
        
        # Insert new ratings with current timestamp
        for rating_data in ratings:
            cursor.execute("""
                INSERT OR REPLACE INTO ratings (username, game_id, game_name, rating, date_updated)
                VALUES (?, ?, ?, ?, ?)
            """, (username, rating_data['game_id'], rating_data['game_name'], 
                  rating_data['rating'], current_time))
        
        # Update users_tracking table
        cursor.execute("""
            INSERT OR REPLACE INTO users_tracking (username, date_updated, ratings_count)
            VALUES (?, ?, ?)
        """, (username, current_time, len(ratings)))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"  DB Error: {e}")
        # Don't raise - just continue
    finally:
        conn.close()


def mark_user_updated_no_ratings(username: str, db_path: str):
    """Mark user as updated even if they have no ratings."""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cursor = conn.cursor()
    
    current_time = datetime.now().isoformat()
    
    cursor.execute("""
        INSERT OR REPLACE INTO users_tracking (username, date_updated, ratings_count)
        VALUES (?, ?, 0)
    """, (username, current_time))
    
    conn.commit()
    conn.close()


def get_update_stats(db_path: str) -> dict:
    """Get statistics about database update status."""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM users_tracking")
    total_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM users_tracking WHERE date_updated IS NOT NULL")
    updated_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT username) FROM ratings")
    users_with_ratings = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM ratings")
    total_ratings = cursor.fetchone()[0]
    
    # Get oldest update date
    cursor.execute("SELECT MIN(date_updated) FROM users_tracking WHERE date_updated IS NOT NULL")
    oldest_update = cursor.fetchone()[0]
    
    # Get newest update date
    cursor.execute("SELECT MAX(date_updated) FROM users_tracking WHERE date_updated IS NOT NULL")
    newest_update = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'total_users': total_users,
        'updated_users': updated_users,
        'never_updated': total_users - updated_users,
        'users_with_ratings': users_with_ratings,
        'total_ratings': total_ratings,
        'oldest_update': oldest_update,
        'newest_update': newest_update
    }


if __name__ == "__main__":
    db_path = "greek_user_ratings.db"
    
    print(f"{'='*70}")
    print(f"BGG Greek Ratings - Rotating 3-Day Update Cycle")
    print(f"Updates {USERS_PER_RUN} users per run")
    print(f"{'='*70}\n")
    
    # Step 1: Fetch all guild members
    all_users = fetch_guild_members(guild_id=119)
    
    # Step 2: Initialize database with all users
    initialize_database(db_path, all_users)
    
    # Step 3: Get current stats
    stats = get_update_stats(db_path)
    print(f"📊 Current Database Stats:")
    print(f"   Total users: {stats['total_users']}")
    print(f"   Updated: {stats['updated_users']}")
    print(f"   Never updated: {stats['never_updated']}")
    print(f"   Users with ratings: {stats['users_with_ratings']}")
    print(f"   Total ratings: {stats['total_ratings']}")
    if stats['oldest_update']:
        print(f"   Oldest update: {stats['oldest_update'][:10]}")
    if stats['newest_update']:
        print(f"   Newest update: {stats['newest_update'][:10]}")
    print()
    
    # Step 4: Get users to update (oldest first)
    users_to_update = get_users_to_update(db_path, USERS_PER_RUN)
    
    print(f"📋 Will update {len(users_to_update)} users in this run\n")
    print(f"{'='*70}\n")
    
    # Step 5: Process users
    start_time = time.time()
    successful = 0
    failed = 0
    
    for i, username in enumerate(users_to_update, 1):
        print(f"[{i}/{len(users_to_update)}] Processing: {username} (delay: {current_delay:.1f}s)", end="", flush=True)
        
        ratings = fetch_user_ratings(username)
        
        if ratings:
            save_ratings_to_db(username, ratings, db_path)
            print(f" ✓ {len(ratings)} ratings", flush=True)
            successful += 1
        else:
            mark_user_updated_no_ratings(username, db_path)
            print(f" ✗ No ratings", flush=True)
            failed += 1
        
        # Adaptive delay
        if i < len(users_to_update):
            time.sleep(current_delay)
        
        # Progress report every 100 users
        if i % 100 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed * 60
            eta = (len(users_to_update) - i) / rate
            print(f"\n📊 Progress: {i}/{len(users_to_update)} | Rate: {rate:.1f}/min | ETA: {eta:.0f}min\n")
    
    # Final stats
    elapsed = time.time() - start_time
    final_stats = get_update_stats(db_path)
    
    print(f"\n{'='*70}")
    print(f"✅ Run Complete!")
    print(f"⏱️  Time: {elapsed/60:.1f} minutes ({elapsed:.0f} seconds)")
    print(f"✓ Successful: {successful}")
    print(f"✗ No ratings: {failed}")
    print(f"\n📊 Updated Database Stats:")
    print(f"   Total users: {final_stats['total_users']}")
    print(f"   Updated: {final_stats['updated_users']}")
    print(f"   Never updated: {final_stats['never_updated']}")
    print(f"   Users with ratings: {final_stats['users_with_ratings']}")
    print(f"   Total ratings: {final_stats['total_ratings']}")
    print(f"{'='*70}\n")