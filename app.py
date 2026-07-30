import streamlit as st
import pandas as pd
import crud
from database import get_db
import summary_stats as statistics
import data_loader
from modules import home, players, members
from modules import transfers as transfers_page
from modules import teams as teams_page
from modules import head_to_head
from modules import admin

# Initialize database connection
db = get_db()

# Configure Streamlit page
st.set_page_config(
    page_title="Comunio App",
    page_icon=":money_with_wings:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar navigation
st.sidebar.title("Comunio App")
page = st.sidebar.radio(
    "Navigation",
    [
        "Home",
        "Transfers",
        "Players",
        "Members",
        "Teams",
        "Statistics",
        "Head-to-Head",
        "Daten aktualisieren",
    ],
)

# Main title and season selector
st.title("Comunio App")
# Dropdown wird datengetrieben aus crud.base.SEASON_DATE_RANGES befüllt —
# damit neue Saisons automatisch in der UI auftauchen, sobald sie dort
# ergänzt werden.
spielzeit = st.selectbox("Spielzeit", list(crud.SEASON_DATE_RANGES.keys()), index=0)

# Get current date for caching (hourly granularity)
date = pd.to_datetime("today").hour

# Load data — all via @st.cache_data, keyed by (spielzeit, date)
# No session_state needed: cache_data handles deduplication per season
transfers_data = data_loader.load_transfers(db, spielzeit, date)
players_points = data_loader.load_player_points(db, spielzeit, date)
player_data_combined = data_loader.load_player_data_combined(db, spielzeit, date)

# Route to appropriate page
if page == "Statistics":
    statistics.show(db, spielzeit)
elif page == "Home":
    home.show(db, transfers_data, spielzeit)
elif page == "Players":
    players.show(player_data_combined)
elif page == "Members":
    members.show(transfers_data)
elif page == "Transfers":
    transfers_page.show(db, transfers_data, spielzeit)
elif page == "Teams":
    teams_page.show()
elif page == "Head-to-Head":
    head_to_head.show(transfers_data, spielzeit)
elif page == "Daten aktualisieren":
    admin.show(db, transfers_data, spielzeit)
