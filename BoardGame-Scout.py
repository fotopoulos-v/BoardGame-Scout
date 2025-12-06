# app.py
# BoardGame Scout - DB-backed version with filters + pagination
import streamlit as st
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
import os
import sqlite3
import base64
import time

# --- page config ---
st.set_page_config(
    page_title="BoardGame Scout",
    page_icon="./assets/images/scout_logo.png",
    layout="wide"
)



import os
import zipfile

DB_PATH = "boardgames.db"
ZIP_PATH = "boardgames_db.zip"

# Extract the DB from zip only if needed
if not os.path.exists(DB_PATH):
    if os.path.exists(ZIP_PATH):
        with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
            zip_ref.extractall(".")
        print("📦 Extracted boardgames.db from boardgames_db.zip")
    else:
        raise FileNotFoundError(
            f"❌ Neither {DB_PATH} nor {ZIP_PATH} were found. Cannot load the database."
        )



# -------------------------
# CSS (unchanged except small additions for API messages)
# -------------------------
st.markdown("""
    <style>
    
    /* Sidebar background */
    section[data-testid="stSidebar"] { background-color: #2B060A; color: #FFB703; }

    /* Main background */
    .stApp { background-color: #02021A !important; }
    input, textarea, select, div[data-baseweb="input"] input { color: rgba(200, 200, 200, 0.45) !important; }
    ::placeholder { color: rgba(200, 200, 200, 0.35) !important; }
    input:focus, textarea:focus, select:focus, div[data-baseweb="input"] input:focus { color: #FAFAFA !important; }

            

    /* Force multiselect to match text input height */
    [data-testid="stExpander"] .stMultiSelect {
        margin: 0 !important;
    }
    [data-testid="stExpander"] .stMultiSelect > div {
        min-height: 38px !important;
    }
    [data-testid="stExpander"] .stMultiSelect [data-baseweb="select"] {
        min-height: 38px !important;
    }
    [data-testid="stExpander"] .stMultiSelect [data-baseweb="select"] > div {
        min-height: 38px !important;
        padding-top: 4px !important;
        padding-bottom: 4px !important;
    }
            
       
        /* Remove text cursor from multiselect - more specific */
        div[data-testid="stMultiSelect"] input,
        div[data-testid="stMultiSelect"] input:hover,
        div[data-testid="stMultiSelect"] input:focus,
        div[data-testid="stMultiSelect"] div,
        div[data-testid="stMultiSelect"] [data-baseweb="select"],
        div[data-testid="stMultiSelect"] [data-baseweb="select"] > div {
            cursor: pointer !important;
        }   

            
    /* Large buttons */
    div.stButton > button {
        width: 120px !important;
        height: 48px !important;
        font-size: 28px !important;
        font-weight: bold !important;
        border-radius: 10px !important;
        color: white !important;
        transition: all 0.2s ease-in-out;
    }
    /* Green search button */
    div.stButton > button[kind="primary"] { background-color: #1B5E20 !important; }
    div.stButton > button[kind="primary"]:hover { background-color: #00850C !important; transform: scale(1.05); }
    /* Hot Games button - Purple (tertiary) */
    div.stButton > button[kind="tertiary"] { background-color: #7C4794 !important; }
    div.stButton > button[kind="tertiary"]:hover { background-color: #5F047D !important; transform: scale(1.05); }
    /* Clear/Reset button - Red (secondary or default) */
    div.stButton > button[kind="secondary"],
    div[data-testid="stButton"] > button:not([kind="primary"]):not([kind="tertiary"]) { background-color: #991D12 !important; }
    div.stButton > button[kind="secondary"]:hover,
    div[data-testid="stButton"] > button:not([kind="primary"]):not([kind="tertiary"]):hover { background-color: #F70202 !important; transform: scale(1.05); }


            
    /* Narrow search input container */
    div.stTextInput[data-testid="stTextInput"] { max-width: 400px !important; margin-left:0 !important; }



 /* Compact filters inside the expander */
    [data-testid="stExpander"] [data-testid="stVerticalBlock"] { 
        gap: 0.1rem !important;
    }
    [data-testid="stExpander"] [data-testid="stVerticalBlock"] > div {
        padding: 0; 
        margin: 0; 
    }

    /* Compact widget spacing */
    [data-testid="stExpander"] .stNumberInput,
    [data-testid="stExpander"] .stSlider,
    [data-testid="stExpander"] .stSelectbox,
    [data-testid="stExpander"] .stTextInput {
        margin: 0 !important;
    }

    /* Label spacing */
    [data-testid="stExpander"] .stMarkdown { 
        margin-bottom: -1.4rem !important;
        margin-top: 1.2rem !important;
    }

    /* First element in each column */
    [data-testid="stExpander"] .stColumn > div > div:first-child .stMarkdown {
        margin-top: 0 !important;
    }

    /* Reduce space between input → next label */
    [data-testid="stExpander"] .stNumberInput + div .stMarkdown,
    [data-testid="stExpander"] .stSlider + div .stMarkdown,
    [data-testid="stExpander"] .stSelectbox + div .stMarkdown {
        margin-top: 0.7rem !important;
    }

    /* Active filter highlight */
    .filter-label-active {
        color: #00FFFF !important;
        font-weight: bold;
    }

/* Labels inside the expander columns */
[data-testid="stExpander"] .stColumn div[style*="margin: 0px"] {
    margin-bottom: 0.3rem !important;  /* add a bit of space below the label */
}

/* Number input fields immediately following those labels */
[data-testid="stExpander"] .stColumn input[data-testid="stNumberInputField"] {
    margin-top: 0.65rem !important;  /* push input slightly down */
}

            
    </style>
""", unsafe_allow_html=True)

# ---------------------------
# Sidebar: Buy Me a Coffee
# ---------------------------
with st.sidebar:
    st.markdown("---")
    st.markdown(
        """
        <p style="color:#FCF2D9; font-size:16px;">
        💰 Support me!<br>
        Your support helps me maintain and improve the app.
        </p>
        """, unsafe_allow_html=True
    )

    st.markdown(
        """
        <style>
        .bmc-button {
            background-color:#3679AD;
            color:white;
            border:none;
            border-radius:8px;
            padding:10px 20px;
            font-size:16px;
            font-weight:bold;
            cursor:pointer;
            margin-top:5px;
            margin-bottom:18px;
            transition: all 0.3s ease;
        }
        .bmc-button:hover { background-color:#003AAB; transform: scale(1.05); }
        </style>
        <a href="https://buymeacoffee.com/vasileios" target="_blank">
            <button class="bmc-button">☕ Buy Me a Coffee</button>
        </a>
        """, unsafe_allow_html=True
    )

    # -------------------------
    # Sidebar: Powered by BGG (required attribution)
    # -------------------------
    st.markdown("---")

    # Path to your logo file
    img_path = "assets/images/powered_by_logo_01_SM.jpg"

    # Check existence
    if not os.path.exists(img_path):
        st.sidebar.warning("⚠️ Logo not found.")
    else:
        # Convert the image to base64 so it works anywhere (local or cloud)
        with open(img_path, "rb") as f:
            img_bytes = f.read()
            img_base64 = base64.b64encode(img_bytes).decode()

        # CSS for hover scaling
        st.markdown("""
            <style>
            .bgg-logo-container img {
                border-radius: 8px;
                margin-top:-20px;
                transition: transform 0.2s ease-in-out;
            }
            .bgg-logo-container img:hover {
                transform: scale(1.05);
            }
            </style>
        """, unsafe_allow_html=True)

        # Render clickable image with hover effect
        st.sidebar.markdown(
            f"""
            <div class="bgg-logo-container" style="text-align:center; margin-top:10px;">
                <a href="https://boardgamegeek.com" target="_blank" rel="noopener">
                    <img src="data:image/jpeg;base64,{img_base64}" width="160" alt="Powered by BGG">
                </a>
            </div>
            """,
            unsafe_allow_html=True
        )





# ---------------------------
# App header
# ---------------------------
col1, col2 = st.columns([1, 6], gap="small")
with col1:
    if os.path.exists("assets/images/scout_logo.png"):
        st.image("assets/images/scout_logo.png", width=120)
with col2:
    st.markdown("<h1 style='color:#FAFAFA; margin-top: 10px; margin-left: -10px;'>BoardGame Scout</h1>", unsafe_allow_html=True)

st.write("Find board games using name or filters!")

# -------------------------
# Keep these flags in session_state
if "show_hot_games" not in st.session_state:
    st.session_state["show_hot_games"] = False
if "show_search_results" not in st.session_state:
    st.session_state["show_search_results"] = False
if "db_page" not in st.session_state:
    st.session_state["db_page"] = 0
if "bgg_page" not in st.session_state:
    st.session_state["bgg_page"] = 0
if "user_sub_view" not in st.session_state:   # values: None | "collection" | "recommendations"
    st.session_state["user_sub_view"] = None

PAGE_SIZE = 50
DB_PATH = "boardgames.db"   



# -----------------------------
# Mechanics Extraction Function
@st.cache_data
def get_unique_mechanics():
    """Extract all unique mechanics from the database."""
    if not os.path.exists(DB_PATH):
        return []
    
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT DISTINCT mechanics FROM games WHERE mechanics IS NOT NULL;"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Split comma-separated mechanics and flatten
    all_mechanics = set()
    for mechanics_str in df['mechanics'].dropna():
        mechanics_list = [m.strip() for m in mechanics_str.split(',')]
        all_mechanics.update(mechanics_list)
    
    return sorted(list(all_mechanics))



# -----------------------------
# Type / Category Extraction Function
@st.cache_data
def get_unique_categories():
    """Extract all unique categories from the database."""
    if not os.path.exists(DB_PATH):
        return []
    
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT DISTINCT categories FROM games WHERE categories IS NOT NULL;"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Split comma-separated categories and flatten
    all_categories = set()
    for categories_str in df['categories'].dropna():
        categories_list = [c.strip() for c in categories_str.split(',')]
        all_categories.update(categories_list)
    
    return sorted(list(all_categories))





# -------------------------
# Hot games function (kept as-is per your request)
@st.cache_data(ttl=600)
def fetch_hot_games():
    """Fetch the hot board games from BGG with detailed stats."""

    def safe_int(val):
        try:
            return int(val) if val not in [None, ""] else None
        except:
            return None

    def safe_float(val):
        try:
            return float(val) if val not in [None, ""] else None
        except:
            return None

    try:
        # 1️⃣ Get hot list
        hot_url = "https://boardgamegeek.com/xmlapi2/hot"
        params = {"type": "boardgame"}
        headers = {"Authorization": f"Bearer {st.secrets.get('BGG_TOKEN', '')}"}
        resp = requests.get(hot_url, headers=headers, params=params, timeout=15)

        if resp.status_code != 200:
            st.warning(f"Failed to fetch hot games: {resp.status_code}")
            return pd.DataFrame()

        root = ET.fromstring(resp.text)
        game_ids = [item.attrib.get("id") for item in root.findall("item")]
        hot_games = []

        # 2️⃣ Fetch details in batches
        batch_size = 20
        for i in range(0, len(game_ids), batch_size):
            batch = game_ids[i:i + batch_size]
            thing_url = "https://boardgamegeek.com/xmlapi2/thing"
            params = {"id": ",".join(batch), "stats": 1}

            resp2 = requests.get(thing_url, headers=headers, params=params, timeout=20)
            if resp2.status_code != 200:
                st.warning(f"Failed to fetch details for batch {i//batch_size + 1}")
                continue

            root2 = ET.fromstring(resp2.text)

            for item in root2.findall("item"):

                # ---- Title ----
                title = None
                for nm in item.findall("name"):
                    if nm.attrib.get("type") == "primary":
                        title = nm.attrib.get("value")
                        break
                if not title:
                    title = "N/A"

                # ---- Stats ----
                stats = item.find("statistics/ratings")
                geek_rating = safe_float(stats.find("bayesaverage").attrib.get("value")) if stats is not None and stats.find("bayesaverage") is not None else None
                avg_rating = safe_float(stats.find("average").attrib.get("value")) if stats is not None and stats.find("average") is not None else None
                voters = safe_int(stats.find("usersrated").attrib.get("value")) if stats is not None and stats.find("usersrated") is not None else None
                complexity = safe_float(stats.find("averageweight").attrib.get("value")) if stats is not None and stats.find("averageweight") is not None else None

                # ---- Basic details from attributes ----
                year = safe_int(item.find("yearpublished").attrib.get("value")) if item.find("yearpublished") is not None else None
                min_players = safe_int(item.find("minplayers").attrib.get("value")) if item.find("minplayers") is not None else None
                max_players = safe_int(item.find("maxplayers").attrib.get("value")) if item.find("maxplayers") is not None else None
                min_time = safe_int(item.find("minplaytime").attrib.get("value")) if item.find("minplaytime") is not None else None
                max_time = safe_int(item.find("maxplaytime").attrib.get("value")) if item.find("maxplaytime") is not None else None
                min_age = safe_int(item.find("minage").attrib.get("value")) if item.find("minage") is not None else None

                # ---- Categories, Designers, Artists, Publishers, Mechanics ----
                categories = [x.attrib.get("value") for x in item.findall("link") if x.attrib.get("type") == "boardgamecategory"]
                designers = [x.attrib.get("value") for x in item.findall("link") if x.attrib.get("type") == "boardgamedesigner"]
                artists = [x.attrib.get("value") for x in item.findall("link") if x.attrib.get("type") == "boardgameartist"]
                publishers = [x.attrib.get("value") for x in item.findall("link") if x.attrib.get("type") == "boardgamepublisher"]
                mechanics = [x.attrib.get("value") for x in item.findall("link") if x.attrib.get("type") == "boardgamemechanic"]

                hot_games.append({
                    "Title": title,
                    "Geek Rating": geek_rating,
                    "Average Rating": avg_rating,
                    "Number of Voters": voters,
                    "Year": year,
                    "Complexity": complexity,
                    "Min Players": min_players,
                    "Max Players": max_players,
                    "Min Time": min_time,
                    "Max Time": max_time,
                    "Min Age": min_age,
                    "Type / Category": ", ".join(categories) if categories else None,
                    "Designers": ", ".join(designers) if designers else None,
                    "Artists": ", ".join(artists) if artists else None,
                    "Publishers": ", ".join(publishers) if publishers else None,
                    "Mechanics": ", ".join(mechanics) if mechanics else None
                })

        df = pd.DataFrame(hot_games)

        # Round numeric stats to 2 decimals
        for col in ["Geek Rating", "Average Rating", "Complexity"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: round(x, 2) if isinstance(x, (float, int)) else x)

        return df

    except Exception as e:
        st.error(f"Error fetching hot games: {e}")
        return pd.DataFrame()

# -------------------------
# Search input field (full width)
search_query = st.text_input("", placeholder="Enter a board game name (optional)", key="search_query")





# -------------------------
# Defaults & session state initialization (safe)
defaults = {
    "search_query": "",
    "min_players": None,
    "max_players": None,
    "min_duration": None,
    "max_duration": None,
    "min_year": None,
    "max_year": None,
    "complexity_slider": 5.0,
    "game_type": "Select type...",
    # text-based column filters:
    "f_title": "",
    # "f_category": "",
    "f_category": [],
    "f_designers": "",
    "f_artists": "",
    "f_publishers": "",
    # "f_mechanics": [],
    "max_results": 0,  # 0 -> unlimited
    "db_page": 0
}
for k, v in defaults.items():
    st.session_state.setdefault(k, v)

# -------------------------
# Filters UI (compact + the additional text inputs requested)
# -------------------------
with st.expander("🎚️ Filters"):
    c1, c2, c3, c4 = st.columns(4, gap="small")

    # -------------------------------------------------
    # COLUMN 1 — Players + Year
    # -------------------------------------------------
    with c1:
        # PLAYERS
        for key in ["min_players", "max_players"]:
            if key not in st.session_state:
                st.session_state[key] = None

        min_p_active = st.session_state["min_players"] is not None
        st.markdown(
            f'<div style="color:{("#00FFFF" if min_p_active else "inherit")}; margin:0;">'
            f'{"Players (From) ✅" if min_p_active else "Players (From)"}</div>',
            unsafe_allow_html=True
        )
        st.number_input("", min_value=1, max_value=20, key="min_players")

        max_p_active = st.session_state["max_players"] is not None
        st.markdown(
            f'<div style="color:{("#00FFFF" if max_p_active else "inherit")}; margin:0;">'
            f'{"Players (To) ✅" if max_p_active else "Players (To)"}</div>',
            unsafe_allow_html=True
        )
        st.number_input("", min_value=1, max_value=20, key="max_players")

        # YEAR
        for key in ["min_year", "max_year"]:
            if key not in st.session_state:
                st.session_state[key] = None

        min_y_active = st.session_state["min_year"] is not None
        st.markdown(
            f'<div style="color:{("#00FFFF" if min_y_active else "inherit")}; margin:0;">'
            f'{"Year (From) ✅" if min_y_active else "Year (From)"}</div>',
            unsafe_allow_html=True
        )
        st.number_input("", min_value=1900, max_value=2100, key="min_year")

        max_y_active = st.session_state["max_year"] is not None
        st.markdown(
            f'<div style="color:{("#00FFFF" if max_y_active else "inherit")}; margin:0;">'
            f'{"Year (To) ✅" if max_y_active else "Year (To)"}</div>',
            unsafe_allow_html=True
        )
        st.number_input("", min_value=1900, max_value=2100, key="max_year")

    # -------------------------------------------------
    # COLUMN 2 — Duration + Category + Mechanics
    # -------------------------------------------------
    with c2:
        for key in ["min_duration", "max_duration"]:
            if key not in st.session_state:
                st.session_state[key] = None

        # MIN DURATION
        min_d_active = st.session_state["min_duration"] is not None
        st.markdown(
            f'<div style="color:{("#00FFFF" if min_d_active else "inherit")}; margin:0;">'
            f'{"Min duration (min) ✅" if min_d_active else "Min duration (min)"}</div>',
            unsafe_allow_html=True
        )
        st.number_input("", min_value=1, max_value=600, key="min_duration")

        # MAX DURATION
        max_d_active = st.session_state["max_duration"] is not None
        st.markdown(
            f'<div style="color:{("#00FFFF" if max_d_active else "inherit")}; margin:0;">'
            f'{"Max duration (min) ✅" if max_d_active else "Max duration (min)"}</div>',
            unsafe_allow_html=True
        )
        st.number_input("", min_value=1, max_value=600, key="max_duration")

        # CATEGORY
        def labeled_text_filter(key, label):
            if key not in st.session_state:
                st.session_state[key] = ""
            active = st.session_state[key].strip() != ""
            st.markdown(
                f'<div style="color:{("#00FFFF" if active else "inherit")}; margin:0;">'
                f'{label + " ✅" if active else label}</div>',
                unsafe_allow_html=True
            )
            st.text_input("", key=key)

        # labeled_text_filter("f_category", "Type / Category")
        # CATEGORY multiselect
        if "f_category" not in st.session_state:
            st.session_state["f_category"] = []

        category_options = get_unique_categories()
        category_active = len(st.session_state["f_category"]) > 0

        st.markdown(
            f'<div style="color:{("#00FFFF" if category_active else "inherit")}; margin:0;">'
            f'{"Type / Category ✅" if category_active else "Type / Category"}</div>',
            unsafe_allow_html=True
        )
        st.multiselect("", category_options, key="f_category", label_visibility="collapsed", placeholder="Select categories...")



        # labeled_text_filter("f_mechanics", "Mechanics")
        # MECHANICS multiselect
        if "f_mechanics" not in st.session_state:
            st.session_state["f_mechanics"] = []

        mechanics_options = get_unique_mechanics()
        mechanics_active = len(st.session_state["f_mechanics"]) > 0

        st.markdown(
            f'<div style="color:{("#00FFFF" if mechanics_active else "inherit")}; margin:0;">'
            f'{"Mechanics ✅" if mechanics_active else "Mechanics"}</div>',
            unsafe_allow_html=True
        )
        st.multiselect("", mechanics_options, key="f_mechanics", label_visibility="collapsed", placeholder="Select mechanics...")

    # -------------------------------------------------
    # COLUMN 3 — Designers + Artists + Publishers
    # -------------------------------------------------
    with c3:
        labeled_text_filter("f_designers", "Designers")
        labeled_text_filter("f_artists", "Artists")
        labeled_text_filter("f_publishers", "Publishers")

    # -------------------------------------------------
    # COLUMN 4 — Complexity + Age + Max Results
    # -------------------------------------------------
    with c4:
        # COMPLEXITY
        if "complexity_slider" not in st.session_state:
            st.session_state["complexity_slider"] = 5.0

        comp_active = st.session_state["complexity_slider"] != 5.0
        st.markdown(
            f'<div style="color:{("#00FFFF" if comp_active else "inherit")}; margin:0;">'
            f'{"Max Complexity (1-5) ✅" if comp_active else "Max Complexity (1-5)"}</div>',
            unsafe_allow_html=True
        )
        st.slider("", min_value=1.0, max_value=5.0, step=0.01, key="complexity_slider")

        # MIN AGE
        if "min_age" not in st.session_state:
            st.session_state["min_age"] = None

        age_active = st.session_state["min_age"] not in [None, 0]
        st.markdown(
            f'<div style="color:{("#00FFFF" if age_active else "inherit")}; margin:0;">'
            f'{"Min Age ✅" if age_active else "Min Age"}</div>',
            unsafe_allow_html=True
        )
        st.number_input("", min_value=0, max_value=99, key="min_age")

        # MAX RESULTS
        if "max_results" not in st.session_state:
            st.session_state["max_results"] = 0

        mr_active = st.session_state["max_results"] not in [0, None]
        st.markdown(
            f'<div style="color:{("#00FFFF" if mr_active else "inherit")}; margin:0;">'
            f'{"Max Results ⚠️" if mr_active else "Max Results (0 = unlimited)"}</div>',
            unsafe_allow_html=True
        )
        st.number_input("", min_value=0, max_value=1_000_000, key="max_results")








def fetch_bgg_collection(username, filter_flag, max_retries=10, delay=3):
    """
    Fetch a BGG collection (owned, rated, wishlist) using API v2.
    Requires BGG API token as of 2024.
    """
    # Build URL directly as a string
    url = f"https://boardgamegeek.com/xmlapi2/collection?username={username}&stats=1&subtype=boardgame&{filter_flag}=1"
    
    # Get your BGG token from secrets or environment
    bgg_token = st.secrets.get("BGG_TOKEN", "")
    
    if not bgg_token:
        return None, "⚠️ BGG API Token is missing! Please add BGG_API_TOKEN to your secrets.\n\nTo get a token:\n1. Go to https://boardgamegeek.com/manage/applications\n2. Register your app (free for non-commercial)\n3. Create a token\n4. Add it to .streamlit/secrets.toml as:\nBGG_API_TOKEN = 'your_token_here'"
    
    headers = {
        "Authorization": f"Bearer {bgg_token}",
        "User-Agent": "BoardGame Scout/1.0",
        "Accept": "application/xml"
    }
    
    for i in range(max_retries):
        try:
            print(f"Attempt {i+1}: Requesting {url}")
            
            # Add small delay before first request
            if i == 0:
                time.sleep(2)
            
            r = requests.get(url, headers=headers, timeout=15)
            
            print(f"Response status: {r.status_code}")
            
            # Handle 202 (queued response - BGG needs to process collection)
            if r.status_code == 202:
                print("Collection is queued, waiting...")
                if i < max_retries - 1:
                    time.sleep(delay)
                    continue
                else:
                    return None, "BGG collection is still processing. Try again in a moment."
            
            # Handle 401 - unauthorized (bad/missing token)
            if r.status_code == 401:
                return None, f"❌ Unauthorized (401): Your BGG API token is invalid or expired.\n\nPlease:\n1. Check your token at https://boardgamegeek.com/manage/applications\n2. Generate a new token if needed\n3. Update your secrets.toml file"
            
            # Handle other errors
            if r.status_code != 200:
                return None, f"BGG API error: HTTP {r.status_code}"
            
            # Success! Parse the XML
            try:
                root = ET.fromstring(r.text)
                
                # Check if collection is empty
                items = root.findall("item")
                if not items:
                    return None, f"No {filter_flag} games found for user '{username}'"
                
                # DEBUG: Print first item structure
                if items and len(items) > 0:
                    print("DEBUG - First item XML structure:")
                    print(ET.tostring(items[0], encoding='unicode')[:500])
                
                # Parse games into list
                games = []
                for item in items:
                    game_data = {
                        "Title": item.find("name").text if item.find("name") is not None else "N/A"
                    }
                    
                    # Get year published
                    year = item.find("yearpublished")
                    game_data["Year"] = year.text if year is not None else "N/A"
                    
                    # Get stats if available
                    stats = item.find("stats")
                    if stats is not None:
                        rating_elem = stats.find("rating")
                        if rating_elem is not None:
                            # BGG average rating
                            avg = rating_elem.find("average")
                            game_data["BGG Rating"] = avg.attrib.get("value", "N/A") if avg is not None else "N/A"
                            
                            # YOUR rating - check both attribute and value element
                            your_rating = rating_elem.attrib.get("value")
                            if not your_rating or your_rating == "N/A":
                                value_elem = rating_elem.find("value")
                                if value_elem is not None:
                                    your_rating = value_elem.attrib.get("value")
                                    if not your_rating:
                                        your_rating = value_elem.text
                            
                            game_data["Your Rating"] = your_rating if your_rating and your_rating != "N/A" else "Not Rated"
                    else:
                        game_data["BGG Rating"] = "N/A"
                        game_data["Your Rating"] = "Not Rated"
                    
                    # Get play stats
                    numplays = item.find("numplays")
                    game_data["Plays"] = numplays.text if numplays is not None else "0"
                    
                    games.append(game_data)
                
                df = pd.DataFrame(games)
                
                # Format numeric columns
                if "BGG Rating" in df.columns:
                    df["BGG Rating"] = pd.to_numeric(df["BGG Rating"], errors='coerce').round(2)
                
                return df, None
                
            except ET.ParseError as e:
                return None, f"XML parsing error: {e}"
                
        except requests.exceptions.Timeout:
            if i < max_retries - 1:
                time.sleep(delay)
                continue
            return None, "Request timed out after multiple attempts."
        
        except requests.exceptions.RequestException as e:
            return None, f"Request error: {e}"
    
    return None, "Failed after maximum retries."



# CSS to make these 4 buttons smaller and style them with colors
st.markdown("""
    <style>
    /* Override the main button styles for the 4-button row specifically */
    div.st-key-search_btn button,
    div.st-key-clear_btn button,
    div.st-key-hot_games_btn button,
    div.st-key-your_games_btn button {
        width: 220px !important;
        height: 44px !important;
        font-size: 22px !important;
        padding: 0 10px !important;
        white-space: nowrap !important;
    }
    
    /* Your Games button - Gold - Ultra-specific selector */
    div.stElementContainer.st-key-your_games_btn div.stButton > button {
        background-color: #A34612 !important;
        color: white !important;
    }
    div.stElementContainer.st-key-your_games_btn div.stButton > button:hover {
        background-color: #FF5D00 !important;
        transform: scale(1.05);
    }
    </style>
""", unsafe_allow_html=True)

# -------------------------
# Buttons: Search, Reset, Hot Games & Your Games (all in one row)
# -------------------------
col_search, col_clear, col_hot, col_yours = st.columns([1, 1, 1, 1], gap="small")
with col_search:
    search_clicked = st.button("🔍 Search", key="search_btn", type="primary")
with col_clear:
    clear_clicked = st.button("🧹 Reset", key="clear_btn")
with col_hot:
    hot_games_clicked = st.button("🔥 Hot Games", key="hot_games_btn", type="tertiary")
with col_yours:
    your_games_clicked = st.button("🎮 Your Games!", key="your_games_btn")

# -------------------------
# Button logic handlers
# -------------------------
# Hot Games button logic
if hot_games_clicked:
    st.session_state["show_hot_games"] = True
    st.session_state["show_search_results"] = False
    st.session_state["show_user_section"] = False
    st.session_state["hot_games_df"] = fetch_hot_games()
    st.session_state["user_sub_view"] = None

# Your Games button logic
if your_games_clicked:
    st.session_state["show_user_section"] = True
    st.session_state["show_hot_games"] = False
    st.session_state["show_search_results"] = False

# Clear button logic
if clear_clicked:
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    # explicitly reset the text_input
    st.session_state["search_query"] = ""
    st.rerun()

# Search button logic (launch DB search)
if search_clicked:
    st.session_state["show_search_results"] = True
    st.session_state["show_hot_games"] = False
    st.session_state["show_user_section"] = False
    st.session_state["db_page"] = 0
    st.session_state["user_sub_view"] = None



# ========== USER-BASED COLLABORATIVE FILTERING ==========
DB_RATINGS = "greek_user_ratings.db"   # the db your scraper built
MIN_OVERLAP = 5                        # at least 5 co-rated games to trust similarity
NEIGHBOURS  = 25                       # top-k Greek users whose taste we trust
RECOMMEND_COUNT = 20                   # how many titles to show in the UI

@st.cache_data(show_spinner=False)
def build_user_similarity_matrix() -> pd.DataFrame:
    """
    Offline step: load the ratings table, mean-centre, compute cosine
    similarity between every pair of users that share >= MIN_OVERLAP games.
    Returns a tall DataFrame: columns = [user_1, user_2, similarity]
    """
    import sqlite3, pandas as pd, numpy as np
    from sklearn.metrics.pairwise import cosine_similarity

    conn = sqlite3.connect(DB_RATINGS)
    ratings = pd.read_sql("SELECT username, game_id, rating FROM ratings", conn)
    conn.close()

    # mean-centre ratings per user
    user_mean = ratings.groupby("username")["rating"].mean()
    ratings["rating_c"] = ratings["rating"] - ratings["username"].map(user_mean)

    # pivot to user×game sparse matrix  (index = username, columns = game_id)
    pivot = ratings.pivot(index="username", columns="game_id", values="rating_c").fillna(0)

    # cosine similarity matrix  (username × username)
    sim = cosine_similarity(pivot)
    sim = pd.DataFrame(sim, index=pivot.index, columns=pivot.index)

    # ----  overlap matrix (how many games each pair co-rated)  ----
    overlap = (pivot != 0).astype(int)
    overlap_counts = overlap @ overlap.T          # username × username

    # convert to tall format
    sim_df = (sim.reset_index()
                 .melt(id_vars="username", var_name="other_user", value_name="similarity"))
    overlap_df = (overlap_counts.reset_index()
                               .melt(id_vars="username", var_name="other_user", value_name="overlap"))

    # merge and filter
    sim_df = sim_df.merge(overlap_df, on=["username", "other_user"])
    sim_df = sim_df[sim_df["username"] != sim_df["other_user"]]          # drop self
    sim_df = sim_df[sim_df["overlap"] >= MIN_OVERLAP]
    sim_df = sim_df.sort_values(["username", "similarity"], ascending=[True, False])

    return sim_df[["username", "other_user", "similarity"]]

@st.cache_data(show_spinner=False)
def recommend_games(username: str, n: int = RECOMMEND_COUNT) -> pd.DataFrame:
    """
    Produce recommendations for a *single* username.
    Returns DataFrame with columns:
    [game_id, title, predicted_rating, avg_greek_rating, reason]
    ready for st.dataframe.
    """
    import sqlite3, pandas as pd, numpy as np

    # 1. load similarity matrix (already cached)
    sim_df = build_user_similarity_matrix()
    neighbours = sim_df[sim_df["username"] == username].head(NEIGHBOURS)
    if neighbours.empty:
        return pd.DataFrame()   # not enough neighbours

    # 2. load active user ratings + all Greek ratings
    conn = sqlite3.connect(DB_RATINGS)
    user_rates = pd.read_sql("SELECT game_id, rating FROM ratings WHERE username = ?", conn, params=(username,))
    all_rates = pd.read_sql("SELECT username, game_id, rating FROM ratings", conn)
    conn.close()

    seen = set(user_rates["game_id"].tolist())

    # 3. build neighbourhood rating matrix (only games not seen by active user)
    neigh_rates = all_rates[all_rates["username"].isin(neighbours["other_user"]) & ~all_rates["game_id"].isin(seen)]

    # 4. aggregate neighbour scores (weighted average by similarity)
    neigh_rates = neigh_rates.merge(neighbours, left_on="username", right_on="other_user")
    neigh_rates["weighted"] = neigh_rates["rating"] * neigh_rates["similarity"]

    recs = (neigh_rates.groupby(["game_id"])
                       .agg(w_sum=("weighted", "sum"),
                            sim_sum=("similarity", "sum"),
                            count=("rating", "count"))
                       .reset_index())
    recs = recs[recs["count"] >= 3]                    # at least 3 neighbours rated it
    recs["pred"] = recs["w_sum"] / recs["sim_sum"]
    recs = recs.sort_values("pred", ascending=False).head(n)

    # 5. enrich with game titles + average Greek rating
    conn_bg = sqlite3.connect("boardgames.db")
    titles = pd.read_sql("SELECT id, title FROM games", conn_bg)
    greek_avg = pd.read_sql("SELECT game_id, AVG(rating) avg_greek FROM ratings GROUP BY game_id", sqlite3.connect(DB_RATINGS))
    conn_bg.close()

    recs = recs.merge(titles, left_on="game_id", right_on="id", how="left")
    recs = recs.merge(greek_avg, on="game_id", how="left")
    recs["avg_greek"] = recs["avg_greek"].round(2)

    # 6. build a short textual reason
    top_neigh = neighbours.head(5)
    reason = f"Loved by {len(recs)} Greek users most similar to you (top neighbours: {', '.join(top_neigh['other_user'].head(3).tolist())})"
    recs["reason"] = reason

    return recs[["game_id", "title", "pred", "avg_greek", "reason"]].rename(columns={"pred": "Predicted Rating"})





# -------------------------
# Show Your BGG Collection Section
# -------------------------
st.markdown("""
    <style>
    /* Reveal button - Blue - Maximum specificity to override secondary defaults */
    div.stElementContainer.st-key-reveal_btn div.stButton > button,
    div.stElementContainer.st-key-reveal_btn div.stButton > button[kind="secondary"],
    div.stElementContainer.st-key-reveal_btn button[kind="secondary"],
    div.stElementContainer.st-key-reveal_btn div.stButton > button:active,
    div.stElementContainer.st-key-reveal_btn div.stButton > button:focus,
    div.stElementContainer.st-key-reveal_btn button:active,
    div.stElementContainer.st-key-reveal_btn button:focus {
        background-color: #172E61 !important;
        color: white !important;
        width: 190px !important;
        height: 38px !important;
        margin-top: 0px !important;
        border: 1px solid #0F1F47 !important;
        transform: none !important;
    }
    div.stElementContainer.st-key-reveal_btn div.stButton > button:hover,
    div.stElementContainer.st-key-reveal_btn button:hover {
        background-color: #0445DB !important;
        transform: scale(1.05) !important;
        border: 1px solid #0336B8 !important;
    }


    /* =====  RECOMMENDED GAMES BUTTON  ===== */
    div.stElementContainer.st-key-rec_btn div.stButton > button,
    div.stElementContainer.st-key-rec_btn div.stButton > button[kind="secondary"],
    div.stElementContainer.st-key-rec_btn button[kind="secondary"],
    div.stElementContainer.st-key-rec_btn div.stButton > button:active,
    div.stElementContainer.st-key-rec_btn div.stButton > button:focus,
    div.stElementContainer.st-key-rec_btn button:active,
    div.stElementContainer.st-key-rec_btn button:focus {
        background-color: #4F1022 !important;   /* your favourite purple */
        color: white !important;
        width  : 190px !important;              /* make it wider */
        height : 38px !important;
        margin-top: 0px !important;
        border: 1px solid #4F1022 !important;
        transform: none !important;
    }
    div.stElementContainer.st-key-rec_btn div.stButton > button:hover,
    div.stElementContainer.st-key-rec_btn button:hover {
        background-color: #B30034 !important;
        transform: scale(1.05) !important;
        border: 1px solid #7B2CBF !important;
    }
            
    </style>
""", unsafe_allow_html=True)



if st.session_state.get("show_user_section", False):
    st.subheader("🎮 Retrieve Your BGG Collection")
    
    col_username, col_list, col_reveal, col_rec = st.columns([2, 1.5, 1.4, 2], gap='small')

    with col_username:
        username = st.text_input("Enter your BoardGameGeek username:", label_visibility="collapsed", placeholder="BGG username")
    with col_list:
        option = st.selectbox("Choose list:", ["Owned Games", "Rated Games", "Wishlist"], label_visibility="collapsed")
    with col_reveal:
        reveal_clicked = st.button("See your games", key="reveal_btn")
    with col_rec:
        recommend_clicked = st.button("Recommended for you!", key="rec_btn")

    # ---------- logic ----------
    if recommend_clicked:
        if not username:
            st.error("Please enter a username.")
        else:
            st.session_state["user_sub_view"] = "recommendations"   # ← mark active view
            with st.spinner("Building your personal Greek-guild top list..."):
                rec_df = recommend_games(username, RECOMMEND_COUNT)
                st.session_state["rec_df"] = rec_df          # ← store DF under expected key
            if rec_df.empty:
                st.warning("Not enough ratings yet (need ≥ 10 rated games and ≥ 25 Greek neighbours).")
            else:
                st.success(f"🎯 Here are {len(rec_df)} games you might love!")
    
    # Map display names to API flags
    flag_map = {
        "Owned Games": "own",
        "Rated Games": "rated",
        "Wishlist": "wishlist",
    }
    
    if reveal_clicked:
        if not username:
            st.error("Please enter a username.")
        else:
            st.session_state["user_sub_view"] = "collection"   # ← mark active view
            with st.spinner(f"Fetching {option}..."):
                df, error = fetch_bgg_collection(username, flag_map[option])
            if error:
                st.error(error)
            else:
                st.session_state["bgg_collection_df"] = df
                st.session_state["bgg_page"] = 0
                st.success(f"✅ Found {len(df)} games!")
        
    # Display paginated results if data exists
    # ----------  SHOW THE RIGHT TABLE  ----------
    sub_view = st.session_state.get("user_sub_view")
    bgg_page = st.session_state.get("bgg_page", 0) 


    if sub_view == "collection" and "bgg_collection_df" in st.session_state:
        # =====  COLLECTION PAGINATION  =====
        df_full   = st.session_state["bgg_collection_df"]
        bgg_page  = st.session_state.get("bgg_page", 0)
        total_rows= len(df_full)
        start_idx = bgg_page * PAGE_SIZE + 1
        end_idx   = min(bgg_page * PAGE_SIZE + PAGE_SIZE, total_rows)
        st.markdown(f"**Results: {start_idx}–{end_idx} from {total_rows:,}**")
        df_page   = df_full.iloc[(start_idx-1):end_idx].copy()
        df_page.index = range(start_idx, end_idx + 1)
        df_page.index.name = "No."
        st.dataframe(df_page, use_container_width=True)

        # Prev / Next buttons  (INSIDE the collection branch)
        col_spacer_left, col_pagination, col_spacer_right = st.columns([2, 1.5, 2])
        with col_pagination:
            cprev_bgg, cnext_bgg = st.columns([1, 1])
            with cprev_bgg:
                if bgg_page > 0:
                    if st.button("◀ Prev", key="prev_bgg_btn", type="secondary"):
                        st.session_state["bgg_page"] = bgg_page - 1
                        st.rerun()
            with cnext_bgg:
                if (bgg_page + 1) * PAGE_SIZE < total_rows:
                    if st.button("Next ▶", key="next_bgg_btn", type="secondary"):
                        st.session_state["bgg_page"] = bgg_page + 1
                        st.rerun()

    elif sub_view == "recommendations" and "rec_df" in st.session_state:
        # =====  RECOMMENDATIONS  =====
        rec_df = st.session_state["rec_df"].copy()   # work on a copy
        # 1. drop the internal game_id column
        rec_df = rec_df.drop(columns=["game_id"])
        # 2. round the predicted rating to 1 decimal
        rec_df["Predicted Rating"] = rec_df["Predicted Rating"].round(1)
        # 3. pretty index
        rec_df.index = range(1, len(rec_df)+1)
        rec_df.index.name = "No."
        st.dataframe(rec_df, use_container_width=True)
        
        # Pagination button styling (NOT NEEDED IF THE RECOMMENDATIONS ARE < 51)
        st.markdown("""
            <style>
            /* BGG Prev button */
            div.stElementContainer.st-key-prev_bgg_btn div.stButton > button,
            div.stElementContainer.st-key-prev_bgg_btn button[kind="secondary"] {
                background-color: #212B45 !important;   
                color: white !important;
                border: 1px solid #333 !important;
                width: 120px !important;
                height: 38px !important;
                font-size: 16px !important;
            }
            div.stElementContainer.st-key-prev_bgg_btn div.stButton > button:hover,
            div.stElementContainer.st-key-prev_bgg_btn button[kind="secondary"]:hover {
                background-color: #041D5C !important;
                transform: scale(1.05);
            }
            
            /* BGG Next button */
            div.stElementContainer.st-key-next_bgg_btn div.stButton > button,
            div.stElementContainer.st-key-next_bgg_btn button[kind="secondary"] {
                background-color: #212B45 !important;   
                color: white !important;
                border: 1px solid #333 !important;
                width: 120px !important;
                height: 38px !important;
                font-size: 16px !important;
            }
            div.stElementContainer.st-key-next_bgg_btn div.stButton > button:hover,
            div.stElementContainer.st-key-next_bgg_btn button[kind="secondary"]:hover {
                background-color: #041D5C !important;
                transform: scale(1.05);
            }
            </style>
        """, unsafe_allow_html=True)









# -------------------------
# Helper: build SQL WHERE clause + params
def build_where_and_params():
    where = []
    params = []

    # Title (top search_query has priority; otherwise use f_title)
    title_val = (st.session_state.get("search_query") or "").strip()
    if title_val:
        where.append("lower(title) LIKE ?")
        params.append(f"%{title_val.lower()}%")
    elif st.session_state.get("f_title"):
        t = st.session_state["f_title"].strip()
        if t:
            where.append("lower(title) LIKE ?")
            params.append(f"%{t.lower()}%")

    # Text fields that search their own columns
    # Category filter - require ALL selected categories (AND logic)
    category_selected = st.session_state.get("f_category", [])
    if category_selected:
        # Build AND conditions - game must have all selected categories
        for category in category_selected:
            where.append("lower(categories) LIKE ?")
            params.append(f"%{category.lower()}%")

    # Mechanics filter - require ALL selected mechanics (AND logic)
    mechanics_selected = st.session_state.get("f_mechanics", [])
    if mechanics_selected:
        # Build AND conditions - game must have all selected mechanics
        for mechanic in mechanics_selected:
            where.append("lower(mechanics) LIKE ?")
            params.append(f"%{mechanic.lower()}%")

    # Text fields that still use text input
    txt_filters = [
        ("f_designers", "designers"),
        ("f_artists", "artists"),
        ("f_publishers", "publishers"),
    ]
    for sk, col in txt_filters:
        v = st.session_state.get(sk, "").strip()
        if v:
            where.append(f"lower({col}) LIKE ?")
            params.append(f"%{v.lower()}%")

            


    # Numeric filters - players
    players_from = st.session_state.get("min_players")
    if players_from not in [None, ""]:
        where.append("min_players IS NOT NULL AND min_players <= ?")
        params.append(int(players_from))

    players_to = st.session_state.get("max_players")
    if players_to not in [None, ""]:
        # Game supports at most Y players
        where.append("max_players IS NOT NULL AND max_players <= ?")
        params.append(int(players_to))

    # time filters (min_playtime / max_playtime)
    if st.session_state.get("min_duration") not in [None, ""]:
        where.append("min_playtime IS NOT NULL AND min_playtime >= ?")
        params.append(int(st.session_state["min_duration"]))
    if st.session_state.get("max_duration") not in [None, ""]:
        where.append("max_playtime IS NOT NULL AND max_playtime <= ?")
        params.append(int(st.session_state["max_duration"]))

    # year filters
    if st.session_state.get("min_year") not in [None, ""]:
        where.append("year_published IS NOT NULL AND year_published >= ?")
        params.append(int(st.session_state["min_year"]))
    if st.session_state.get("max_year") not in [None, ""]:
        where.append("year_published IS NOT NULL AND year_published <= ?")
        params.append(int(st.session_state["max_year"]))

    # complexity - slider is max complexity (5.0 default means no filter)
    if float(st.session_state.get("complexity_slider", 5.0)) != 5.0:
        where.append("complexity IS NOT NULL AND complexity <= ?")
        params.append(float(st.session_state["complexity_slider"]))

    # min_age
    if st.session_state.get("min_age") not in [None, ""]:
        where.append("min_age IS NOT NULL AND min_age >= ?")
        params.append(int(st.session_state["min_age"]))

    if where:
        return " WHERE " + " AND ".join(where), params
    else:
        return "", params

# -------------------------
# Database query function (count + fetch page)
def query_db_page(page: int = 0, page_size: int = PAGE_SIZE):
    if not os.path.exists(DB_PATH):
        st.error(f"Database not found at {DB_PATH}. Please create/populate it first.")
        return 0, pd.DataFrame()

    where_clause, params = build_where_and_params()

    # Count total matching rows
    count_sql = f"SELECT COUNT(*) FROM games {where_clause};"
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(count_sql, params)
        total_matching = cur.fetchone()[0] or 0
    except Exception as e:
        conn.close()
        st.error(f"DB count error: {e}")
        return 0, pd.DataFrame()

    # Respect max_results cap if set (>0)
    max_results_cap = int(st.session_state.get("max_results", 0) or 0)
    if max_results_cap > 0:
        total_effective = min(total_matching, max_results_cap)
    else:
        total_effective = total_matching

    # If no results, return
    if total_effective == 0:
        conn.close()
        return 0, pd.DataFrame()

    # Calculate LIMIT/OFFSET for requested page
    offset = page * page_size
    if offset >= total_effective:
        # page out of range -> return empty
        conn.close()
        return total_effective, pd.DataFrame()

    # How many rows to fetch on this page
    rows_to_fetch = min(page_size, total_effective - offset)

    # Build select SQL using the exact DB column names
    select_cols = [
        "id",  # Fetch it but won't display
        "title",
        "geek_rating",
        "avg_rating",
        "num_voters",
        "year_published",
        "complexity",
        "min_players",
        "max_players",
        "min_playtime",
        "max_playtime",
        "min_age",
        "categories",
        "designers",
        "artists",
        "publishers",
        "mechanics"
    ]
    select_sql = (
        "SELECT " + ", ".join(select_cols) +
        f" FROM games {where_clause} " +
        # Put NULL geek_rating rows last, then sort desc
        " ORDER BY (geek_rating IS NULL), geek_rating DESC " +
        " LIMIT ? OFFSET ?;"
    )

    try:
        exec_params = params + [rows_to_fetch, offset]
        df = pd.read_sql_query(select_sql, conn, params=exec_params)
    except Exception as e:
        conn.close()
        st.error(f"DB fetch error: {e}")
        return total_effective, pd.DataFrame()
    conn.close()

    # Rename to human-friendly columns for display
    col_rename = {
        "id": "BGG_ID",  
        "title": "Title",
        "geek_rating": "Geek Rating",
        "avg_rating": "Average Rating",
        "num_voters": "Number of Voters",
        "year_published": "Year",
        "complexity": "Complexity",
        "min_players": "Min Players",
        "max_players": "Max Players",
        "min_playtime": "Min Time",
        "max_playtime": "Max Time",
        "min_age": "Min Age",
        "categories": "Type / Category",
        "designers": "Designers",
        "artists": "Artists",
        "publishers": "Publishers",
        "mechanics": "Mechanics"
    }
    df.rename(columns=col_rename, inplace=True)
    return total_effective, df

# -------------------------
# Show filtered DB results (pagination UI)
if st.session_state.get("show_search_results"):
    # Query page
    page = st.session_state.get("db_page", 0)
    total, df_page = query_db_page(page, PAGE_SIZE)

    st.subheader("🎯 Search Results")
    if total == 0:
        st.warning("No results found with current filters.")
    else:
        # compute display range numbers
        start_idx = page * PAGE_SIZE + 1
        end_idx = page * PAGE_SIZE + len(df_page)
        st.markdown(f"**Results: {start_idx}–{end_idx} from {total:,}**")

        # show dataframe (first 50 - our page)
        # ensure consistent column order:
        display_cols = ["Title", "Geek Rating", "Average Rating", "Number of Voters",
                        "Year", "Complexity", "Min Players", "Max Players",
                        "Min Time", "Max Time", "Min Age", "Type / Category",
                        "Designers", "Artists", "Publishers", "Mechanics"]
        # --------------------------------------------
        # Continuous row numbering across pages
        global_start_number = page * PAGE_SIZE
        df_display = df_page.reindex(columns=display_cols).copy()
        df_display.index = df_display.index + global_start_number + 1
        df_display.index.name = "No."
        # --------------------------------------------


        st.dataframe(df_display, use_container_width=True)


        # Pagination controls - centered container
        col_spacer_left, col_pagination, col_spacer_right = st.columns([2, 1.5, 2])
        
        with col_pagination:
            cprev, cnext = st.columns([1, 1])

        with cprev:
            if page > 0:
                if st.button("◀ Prev", key="prev_page_final", type="secondary"):
                    st.session_state["db_page"] = page - 1
                    st.rerun()

        with cnext:
            if (page + 1) * PAGE_SIZE < total:
                if st.button("Next ▶", key="next_page_final", type="secondary"):
                    st.session_state["db_page"] = page + 1
                    st.rerun()

        # Pagination button styling - must override secondary button defaults
        st.markdown("""
            <style>
            /* Ultra-specific selectors to override secondary button styles */
            div.stElementContainer.st-key-prev_page_final div.stButton > button,
            div.stElementContainer.st-key-prev_page_final button[kind="secondary"] {
                background-color: #212B45 !important;   
                color: white !important;
                border: 1px solid #333 !important;
                width: 120px !important;
                height: 38px !important;
                font-size: 16px !important;
            }
            div.stElementContainer.st-key-prev_page_final div.stButton > button:hover,
            div.stElementContainer.st-key-prev_page_final button[kind="secondary"]:hover {
                background-color: #041D5C !important;
                transform: scale(1.05);
            }

            div.stElementContainer.st-key-next_page_final div.stButton > button,
            div.stElementContainer.st-key-next_page_final button[kind="secondary"] {
                background-color: #212B45 !important;   
                color: white !important;
                border: 1px solid #333 !important;
                width: 120px !important;
                height: 38px !important;
                font-size: 16px !important;
            }
            div.stElementContainer.st-key-next_page_final div.stButton > button:hover,
            div.stElementContainer.st-key-next_page_final button[kind="secondary"]:hover {
                background-color: #041D5C !important;
                transform: scale(1.05);
            }
            </style>
        """, unsafe_allow_html=True)


# -------------------------
# Show Hot Games (leave as-is)
elif st.session_state.get("show_hot_games"):
    df_hot = st.session_state.get("hot_games_df", pd.DataFrame())
    if not df_hot.empty:
        df_hot.index = df_hot.index + 1
        df_hot.index.name = "No."
        st.subheader("🔥 Hot Board Games")
        st.caption("💡 Board games trending right now! (Data powered by BGG XML API)")
        st.dataframe(df_hot, use_container_width=True)
    else:
        st.warning("No hot games found right now.")
