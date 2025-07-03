"""
Data loading utilities for the Comunio app.
Contains cached functions for loading data from the database.
"""

import streamlit as st
import pandas as pd
import time
import crud


@st.cache_data
def load_transfers(_db, spielzeit, date) -> pd.DataFrame:
    """Load transfers data for the specified season"""
    start_time = time.time()
    transfers = crud.get_transfers(_db, spielzeit)
    end_time = time.time()
    print(f"Loaded transfers in {end_time - start_time:.2f} seconds")
    return transfers


@st.cache_resource
def load_player_points(_db, date):
    """Load player points data"""
    start_time = time.time()
    player_points = crud.get_player_points_df(_db)
    end_time = time.time()
    print(f"Loaded player points in {end_time - start_time:.2f} seconds")
    return player_points


@st.cache_resource
def load_player_data_combined(_db, date):
    """Load both player points and current market values in one query"""
    start_time = time.time()
    player_data = crud.get_player_points_with_market_value_df(_db)
    end_time = time.time()
    print(f"Loaded combined player data in {end_time - start_time:.2f} seconds")
    return player_data
