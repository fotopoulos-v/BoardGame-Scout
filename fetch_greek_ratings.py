import requests
import xml.etree.ElementTree as ET
import time
import sqlite3
from typing import List, Dict
from datetime import datetime
import os
import toml

BGG_TOKEN = os.getenv("BGG_TOKEN")

# Global rate limiting state
consecutive_429s = 0
current_delay = 2.0  # Start with 2 seconds between requests
MIN_DELAY = 1.5
MAX_DELAY = 10.0


def adjust_delay_after_429():
    """Increase delay after getting rate limited."""
    global current_delay, consecutive_429s
    consecutive_429s += 1
    current_delay = min(MAX_DELAY, current_delay * 1.5)
    print(f"  ⚠️  Rate limited! Increasing delay to {current_delay:.1f}s")


def adjust_delay_after_success():
    """Gradually decrease delay after successful requests."""
    global current_delay, consecutive_429s
    consecutive_429s = 0
    # Slowly reduce delay if we're being successful
    current_delay = max(MIN_DELAY, current_delay * 0.95)


def fetch_user_ratings(username: str, max_retries: int = 5) -> List[Dict]:
    """
    Fetch all game ratings from a user.
    """
    global consecutive_429s
    
    url = f"https://boardgamegeek.com/xmlapi2/collection?username={username}&rated=1&stats=1&subtype=boardgame"
    
    # Try WITHOUT Authorization header first
    headers = {
        "Authorization": f"Bearer {BGG_TOKEN}",
        "User-Agent": "BoardGame-Scout/1.0",
        "Accept": "application/xml",
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 202:
                # BGG is queuing the request
                time.sleep(5)
                continue
            
            if response.status_code == 429:
                adjust_delay_after_429()
                # If we're getting hammered with 429s, wait even longer
                wait_time = 30 if consecutive_429s > 3 else 20
                time.sleep(wait_time)
                continue
            
            if response.status_code == 401:
                print(f"  ❌ Unauthorized - this shouldn't happen for public collections!")
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
            
            # Success! Gradually reduce delay
            adjust_delay_after_success()
            return ratings
            
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"  Error: {e}")
                return []
            time.sleep(3)
    
    return []


def save_ratings_to_db(username: str, ratings: List[Dict], db_path: str):
    """Save ratings to database."""
    if not ratings:
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
            username TEXT,
            game_id INTEGER,
            game_name TEXT,
            rating REAL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (username, game_id)
        )
    """)
    
    cursor.execute("DELETE FROM ratings WHERE username = ?", (username,))
    
    for rating_data in ratings:
        cursor.execute("""
            INSERT OR REPLACE INTO ratings (username, game_id, game_name, rating, last_updated)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (username, rating_data['game_id'], rating_data['game_name'], rating_data['rating']))
    
    conn.commit()
    conn.close()


def fetch_guild_members(guild_id: int = 119) -> List[str]:
    """Fetch all members from guild."""
    members = []
    page = 1
    headers = {
        "Authorization": f"Bearer {BGG_TOKEN}",
        "User-Agent": "BoardGame-Scout/1.0",
        "Accept": "application/xml",
    }
    
    print(f"Fetching guild members...")
    
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
    
    # Remove duplicates
    members = list(dict.fromkeys(members))
    print(f"Found {len(members)} unique members\n")
    return members


if __name__ == "__main__":
    greek_users = fetch_guild_members(guild_id=119)
    
    db_path = "greek_user_ratings.db"
    total = len(greek_users)
    successful = 0
    failed = 0
    start_time = time.time()
    
    print(f"{'='*70}")
    print(f"Starting sequential fetch of {total} users")
    print(f"Initial delay: {current_delay:.1f}s between requests")
    print(f"{'='*70}\n")
    
    for i, username in enumerate(greek_users, 1):
        print(f"[{i}/{total}] Processing: {username} (delay: {current_delay:.1f}s)", end="")
        
        ratings = fetch_user_ratings(username)
        
        if ratings:
            save_ratings_to_db(username, ratings, db_path)
            print(f" ✓ {len(ratings)} ratings")
            successful += 1
        else:
            print(f" ✗ No ratings")
            failed += 1
        
        # Adaptive delay before next request
        if i < total:
            time.sleep(current_delay)
        
        # Progress report every 100 users
        if i % 100 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed * 60
            eta = (total - i) / rate
            print(f"\n📊 Progress: {i}/{total} | Rate: {rate:.1f}/min | ETA: {eta:.0f}min | 429s: {consecutive_429s}\n")
    
    elapsed = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"✅ Complete!")
    print(f"⏱️  Time: {elapsed/60:.1f} minutes")
    print(f"✓ Successful: {successful}")
    print(f"✗ Failed: {failed}")
    print(f"📈 Success rate: {successful/(successful+failed)*100:.1f}%")
    print(f"{'='*70}")