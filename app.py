import streamlit as st
import pandas as pd
from database import get_db
import summary_stats as statistics
import data_loader
from pages import home, players, members
from pages import transfers as transfers_page
from pages import teams as teams_page

# Initialize database connection
db = get_db()

# Configure Streamlit page
st.set_page_config(
    page_title="Comunio App",
    page_icon=":money_with_wings:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Sidebar navigation
st.sidebar.title("Comunio App")
page = st.sidebar.radio(
    "",
    ["Home", "Transfers", "Players", "Members", "Teams", "Statistics"],
)

# Main title and season selector
st.title("Comunio App")
spielzeit = st.selectbox("Spielzeit", ["2025/2026","2024/2025", "2023/2024"], index=0)

# Get current date for caching
date = pd.to_datetime("today").hour

# Load data
transfers_data = data_loader.load_transfers(db, spielzeit, date)

# Load player data in the background
if "players_points" not in st.session_state:
    st.session_state.players_points = data_loader.load_player_points(db, date)

if "player_data_combined" not in st.session_state:
    st.session_state.player_data_combined = data_loader.load_player_data_combined(db, date)

# Route to appropriate page
if page == "Statistics":
    statistics.show(db, spielzeit)
elif page == "Home":
    home.show(db, transfers_data, spielzeit)
elif page == "Players":
    players.show(st.session_state.player_data_combined)
elif page == "Members":
    members.show(transfers_data)
elif page == "Transfers":
    transfers_page.show()
elif page == "Teams":
    teams_page.show()
