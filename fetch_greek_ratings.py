"""
BGG Guild Members Fetcher and Ratings Database Builder
Fetches all members from a BGG guild and their ratings in parallel
NOTE: Requires BGG_TOKEN environment variable (only for guild member fetch)
"""
import requests
import xml.etree.ElementTree as ET
import time
import sqlite3
import os
import json
import threading
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

BGG_TOKEN = os.getenv("BGG_TOKEN")
if not BGG_TOKEN:
    raise RuntimeError(
        "BGG_TOKEN environment variable not set. "
        "Set it before running the script."
    )

CHECKPOINT_FILE = "greek_ratings_checkpoint.json"
db_lock = threading.Lock()  # Thread-safe database writes


def fetch_guild_members(guild_id: int = 119) -> List[str]:
    """Fetch all members from a BGG guild, removing duplicates."""
    members: List[str] = []
    page = 1
    headers = {
        "Authorization": f"Bearer {BGG_TOKEN}",
        "User-Agent": "BoardGame-Scout/1.0",
        "Accept": "application/xml",
    }
    
    print(f"Fetching members from guild {guild_id}...")
    
    while True:
        url = f"https://boardgamegeek.com/xmlapi2/guild?id={guild_id}&members=1&page={page}"
        print(f"  Requesting page {page}...")
        
        for attempt in range(1, 6):
            try:
                r = requests.get(url, headers=headers, timeout=30)
                
                if r.status_code == 202:
                    print("    BGG not ready – waiting 5s")
                    time.sleep(5)
                    continue
                
                if r.status_code == 429:
                    wait = 2 ** attempt
                    print(f"    429 – backing off {wait}s")
                    time.sleep(wait)
                    continue
                
                r.raise_for_status()
                break
                
            except Exception as exc:
                print(f"    Error: {exc} – retrying page")
                time.sleep(5)
                continue
        else:
            print("    Too many retries – skipping page")
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
        print(f"    Found {len(page_members)} members on this page")
        page += 1
        time.sleep(3)
    
    # Remove duplicates while preserving order
    original_count = len(members)
    members = list(dict.fromkeys(members))
    
    if original_count != len(members):
        print(f"⚠️  Removed {original_count - len(members)} duplicate usernames")
    
    print(f"\n✅ Total unique members: {len(members)}\n")
    return members


def fetch_user_ratings(username: str, max_retries: int = 6) -> Tuple[List[Dict], float]:
    """
    Fetch all rated boardgames for a user from BGG.
    Returns: (ratings_list, elapsed_time_seconds)
    """
    start_time = time.time()
    
    url = (
        "https://boardgamegeek.com/xmlapi2/collection"
        f"?username={username}&rated=1&stats=1&subtype=boardgame"
    )
    headers = {
        "User-Agent": "BoardGame-Scout/1.0",
        "Accept": "application/xml",
    }
    # NO Authorization header - collection API is public!
    
    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                wait = min(20, 3 ** (attempt - 1))  # 1s, 3s, 9s, 20s, 20s, 20s
                time.sleep(wait)
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 202:
                # BGG is processing - wait longer
                time.sleep(8)
                continue
            
            if response.status_code == 429:
                # Rate limited - back off significantly
                time.sleep(20)
                continue
            
            if response.status_code == 400:
                # Bad request - user might not exist or have no collection
                elapsed = time.time() - start_time
                return [], elapsed
            
            if response.status_code != 200:
                elapsed = time.time() - start_time
                return [], elapsed
            
            root = ET.fromstring(response.content)
            items = root.findall("item")
            
            if not items:
                elapsed = time.time() - start_time
                return [], elapsed
            
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
            
            elapsed = time.time() - start_time
            return ratings, elapsed
            
        except requests.exceptions.Timeout:
            if attempt == max_retries:
                elapsed = time.time() - start_time
                return [], elapsed
            time.sleep(10)
            
        except Exception as e:
            if attempt == max_retries:
                elapsed = time.time() - start_time
                return [], elapsed
    
    elapsed = time.time() - start_time
    return [], elapsed


def process_single_user(username: str, user_idx: int, total_users: int) -> Tuple[str, List[Dict], float, int]:
    """
    Process a single user and return results.
    Returns: (username, ratings, elapsed_time, user_index)
    """
    ratings, elapsed = fetch_user_ratings(username)
    return username, ratings, elapsed, user_idx


def load_checkpoint() -> set:
    """Load previously processed users from checkpoint file."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                return set(data.get('processed', []))
        except:
            return set()
    return set()


def save_checkpoint(processed_users: set):
    """Save checkpoint of processed users."""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({
            'processed': list(processed_users),
            'timestamp': datetime.now().isoformat()
        }, f)


def save_ratings_to_db(username: str, ratings: List[Dict], db_path: str):
    """Save ratings to SQLite database with thread safety."""
    if not ratings:
        return
    
    # Use lock to prevent concurrent writes
    with db_lock:
        conn = sqlite3.connect(db_path, timeout=30.0)
        cursor = conn.cursor()
        
        try:
            # Create table if not exists (changed from user_ratings to ratings)
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
            
            # Delete existing ratings for this user (prevents duplicates)
            cursor.execute("DELETE FROM ratings WHERE username = ?", (username,))
            
            # Insert new ratings
            for rating_data in ratings:
                cursor.execute("""
                    INSERT INTO ratings (username, game_id, game_name, rating)
                    VALUES (?, ?, ?, ?)
                """, (username, rating_data['game_id'], rating_data['game_name'], rating_data['rating']))
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise
        finally:
            conn.close()


def fetch_all_greek_ratings_parallel(
    greek_users: List[str],
    db_path: str,
    max_workers: int = 5,
    checkpoint_interval: int = 50,
    batch_delay: float = 3.0
):
    """
    Fetch ratings for all Greek users in parallel with checkpoint support.
    
    Args:
        greek_users: List of BGG usernames
        db_path: Path to SQLite database
        max_workers: Number of parallel workers (default 5)
        checkpoint_interval: Save checkpoint every N users (default 50)
        batch_delay: Seconds to wait after each batch completes (default 3.0)
    """
    
    # Load checkpoint
    processed = load_checkpoint()
    remaining = [u for u in greek_users if u not in processed]
    
    total_users = len(greek_users)
    already_processed = len(processed)
    
    print(f"\n{'='*70}")
    print(f"🚀 Starting parallel fetch with {max_workers} workers")
    print(f"📊 Total users: {total_users}")
    print(f"✓ Already processed: {already_processed}")
    print(f"⏳ Remaining: {len(remaining)}")
    print(f"⏱️  Batch delay: {batch_delay}s between batches")
    print(f"{'='*70}\n")
    
    if not remaining:
        print("✅ All users already processed!")
        return
    
    start_time = time.time()
    successful = 0
    failed = 0
    total_ratings = 0
    batch_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(process_single_user, user, already_processed + idx, total_users): user
            for idx, user in enumerate(remaining, 1)
        }
        
        # Process completed tasks as they finish
        for future in as_completed(futures):
            username = futures[future]
            
            try:
                username, ratings, elapsed, user_idx = future.result()
                
                # Print progress with timing
                print(f"[{user_idx}/{total_users}] {username:20s} | {elapsed:5.1f}s", end="")
                
                if ratings:
                    save_ratings_to_db(username, ratings, db_path)
                    print(f" | ✓ {len(ratings):3d} ratings")
                    successful += 1
                    total_ratings += len(ratings)
                else:
                    print(f" | ✗ No ratings")
                    failed += 1
                
                processed.add(username)
                batch_count += 1
                
                # Add delay every max_workers completions (one "batch")
                if batch_count % max_workers == 0:
                    time.sleep(batch_delay)
                
                # Save checkpoint periodically
                if len(processed) % checkpoint_interval == 0:
                    save_checkpoint(processed)
                    elapsed_total = time.time() - start_time
                    rate = len(processed) / elapsed_total * 60
                    print(f"\n💾 Checkpoint: {len(processed)}/{total_users} users | {rate:.1f} users/min\n")
            
            except Exception as e:
                print(f"[ERROR] {username}: {str(e)[:50]}")
                failed += 1
                processed.add(username)  # Mark as processed even if failed to avoid retry loops
    
    # Final checkpoint save
    save_checkpoint(processed)
    
    # Summary
    elapsed_total = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"✅ Parallel fetch complete!")
    print(f"⏱️  Total time: {elapsed_total/60:.1f} minutes ({elapsed_total:.0f} seconds)")
    print(f"✓ Successful: {successful} users")
    print(f"✗ Failed: {failed} users")
    print(f"📊 Total ratings collected: {total_ratings}")
    if (successful + failed) > 0:
        print(f"📈 Success rate: {successful/(successful+failed)*100:.1f}%")
        print(f"⚡ Average rate: {(successful + failed) / elapsed_total * 60:.1f} users/minute")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    # Step 1: Fetch guild members
    greek_users = fetch_guild_members(guild_id=119)
    
    # Step 2: Fetch ratings in parallel with conservative settings
    fetch_all_greek_ratings_parallel(
        greek_users=greek_users,
        db_path="greek_user_ratings.db",
        max_workers=5,          # Conservative: 5 parallel workers
        checkpoint_interval=50,  # Save progress every 50 users
        batch_delay=3.0         # Wait 3 seconds between batches
    )