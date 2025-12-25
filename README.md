<p align="center">
  <img src="assets/images/scout_logo.png" alt="BoardGame Scout Logo" width="110"/>
</p>

<h1 align="center">BoardGame Scout</h1>

**Discover. Explore. Track. Get Recommendations!**  
A Streamlit web app that connects to the official BoardGameGeek API.  
Search and filter board games, see the current hot games, view your owned/rated/wishlist games, and receive personalized recommendations.

---

## 🚀 Try It Live

👉 **[Launch BoardGame Scout on Streamlit →](https://boardgame-scout.streamlit.app/)**  

If you enjoy it, consider showing your support — it helps keep the app online and improving!  
☕ **[Buy Me a Coffee](https://buymeacoffee.com/vasileios)**  

---

## 💡 Features

✅ Search board games by name or filters  
✅ Browse current **hot games** trending on BoardGameGeek  
✅ Enter your BGG username to view your **owned, rated, and wishlist games**  
✅ Get **personalized recommendations** based on your collection  
✅ Local database (`boardgames_db.zip`) speeds up search and filtering  
✅ Clean, modern interface built with Streamlit  

---

## 🔄 Automated Database Updates

BoardGame Scout uses **two automated GitHub Actions** to keep its data fresh and reliable.

### 📦 Board games database
- A GitHub Action runs **daily**
- It fetches and processes BoardGameGeek data
- The resulting SQLite database is published as a **GitHub Release asset**
- The Streamlit app downloads it automatically when needed

### 🇬🇷 Greek user ratings database
- A separate GitHub Action runs **every 3 days**
- It rebuilds a database of ratings from Greek BoardGameGeek guild members
- The database is published as a **GitHub Release asset**
- Used for **collaborative-filtering recommendations**

### ✅ Why this approach?
- No heavy data files stored in the repository
- No manual updates required
- No API rate-limit issues at runtime
- Fast app startup and consistent data freshness

All downloads are handled transparently by the app in the background.

---

## 🧩 How It Works

<p align="justify">
BoardGame Scout uses the official <b>BoardGameGeek API</b> to retrieve game data and your user collection.  
It also maintains a local SQLite database for fast filtering and searching, which is extracted automatically from the included ZIP on first run.</p>

Here’s what happens behind the scenes:

1. **Search or filter** board games by name, mechanics, category, player count, or difficulty.  
2. **Hot games** are retrieved in real-time from BGG.  
3. Users enter their **BGG username** to view owned games, ratings, and wishlist.  
4. **Recommendations** are generated based on user data and BGG stats.  
5. The local database speeds up repeated queries without overloading the BGG API.  

---

## 🛠️ Tech Stack

| Component | Library / API |
|------------|----------------|
| Web app | [Streamlit](https://streamlit.io/) |
| Data storage | SQLite (local `boardgames_db.zip`) |
| API | [BoardGameGeek XML API2](https://boardgamegeek.com/wiki/page/BGG_XML_API2) |
| Data handling | Pandas, Requests |
| Database extraction | Python `zipfile`, `os` |


