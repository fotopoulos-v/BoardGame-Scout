import pandas as pd
import requests
import xml.etree.ElementTree as ET

API_BASE = "https://boardgamegeek.com/xmlapi2/"

def search_boardgame(name):
    """
    Searches for a board game by name (mocked for now).
    Later will use BoardGameGeek API with Authorization token.
    """
    try:
        # Mock data while waiting for approval
        return get_mock_data(name)

        # Uncomment this once your token is approved
        """
        headers = {"Authorization": "Bearer YOUR_TOKEN_HERE"}
        params = {"query": name}
        url = API_BASE + "search"
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            games = []
            for item in root.findall("item"):
                game_id = item.attrib.get("id")
                title = item.find("name").attrib.get("value") if item.find("name") is not None else "N/A"
                year = item.find("yearpublished")
                year_val = year.attrib.get("value") if year is not None else "-"
                games.append({"ID": game_id, "Title": title, "Year": year_val})
            return pd.DataFrame(games)
        else:
            print("Error:", response.status_code)
            return pd.DataFrame()
        """
    except Exception as e:
        print("Error:", e)
        return pd.DataFrame()

def get_mock_data(name):
    """Temporary mock results for development"""
    data = [
        {"ID": "1", "Title": "Catan", "Year": "1995"},
        {"ID": "2", "Title": "Catan: Seafarers", "Year": "1997"},
        {"ID": "3", "Title": "Catan: Cities & Knights", "Year": "1998"},
    ]
    df = pd.DataFrame(data)
    return df[df["Title"].str.contains(name, case=False)]
