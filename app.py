import streamlit as st
import crud
from database import get_db
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder
import utils

db = get_db()

st.set_page_config(
    page_title="Comunio App",
    page_icon=":money_wiht_wings:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.sidebar.title("Comunio App")
page = st.sidebar.radio(
    "",
    ["Home", "Transfers", "Players", "Members", "Teams", "Statistics"],
)

st.title("Comunio App")


# configure the grid
col1, col2, col3 = st.columns([2, 6, 2])
with col1:
    spielzeit = st.selectbox("Spielzeit", ["2024/2025", "2023/2024"], index=0)
    group_by_column = st.selectbox(
        "Gruppieren nach", ["Kein", "Mitspieler", "Von", "An"]
    )
with col3:
    search_value = st.text_input("Suche")

aggregations = {
    "Kaufpreis": "sum",
    "Verkaufspreis": "sum",
    "Gewinn/Verlust": "sum",
    "Gewinn %": "sum",
    "Gewinn/Verlust pro Tag": "sum",
}

transfers = crud.get_transfers(db, spielzeit)

if group_by_column != "Kein":
    grouped_transfers = (
        transfers.groupby(group_by_column).agg(aggregations).reset_index()
    )
else:
    grouped_transfers = transfers

if search_value:
    filtered_grouped_transfers = grouped_transfers.apply(
        lambda x: search_value.lower() in x.astype(str).str.lower().str.cat(sep=" "),
        axis=1,
    )
    print(filtered_grouped_transfers)
    transfers_to_display = grouped_transfers[filtered_grouped_transfers]
else:
    transfers_to_display = grouped_transfers

gb = GridOptionsBuilder.from_dataframe(transfers_to_display)
gb.configure_column(
    "Kaufpreis",
    type=["numericColumn", "numberColumnFilter", "customNumericFormat"],
    valueFormatter="data.Kaufpreis.toLocaleString('de-DE') + ' €';",
)

gb.configure_column(
    "Verkaufspreis",
    type=["numericColumn", "numberColumnFilter", "customNumericFormat"],
    valueFormatter="data.Verkaufspreis.toLocaleString('de-DE') + ' €';",
)

gb.configure_column(
    "Gewinn/Verlust",
    type=["numericColumn", "numberColumnFilter", "customNumericFormat"],
    valueFormatter="data['Gewinn/Verlust'].toLocaleString('de-DE') + ' €';",
)

gb.configure_column(
    "Gewinn %",
    type=["numericColumn", "numberColumnFilter", "customNumericFormat"],
    valueFormatter="data['Gewinn %'].toLocaleString('de-DE') + ' %';",
)

gb.configure_column(
    "Gewinn/Verlust pro Tag",
    type=["numericColumn", "numberColumnFilter", "customNumericFormat"],
    valueFormatter="data['Gewinn/Verlust pro Tag'].toLocaleString('de-DE') + ' €';",
)
gb.configure_selection("single")
grid_options = gb.build()

response = AgGrid(
    transfers_to_display,
    gridOptions=grid_options,
    enable_enterprise_modules=True,
    update_mode="MODEL_CHANGED",
)

if response:
    selected_row = response["selected_rows"]
    if selected_row is not None:
        if not selected_row.empty:
            player_id = selected_row["ID"].values[0] if not selected_row.empty else None
            player_market_value = crud.get_player_market_value(db, player_id)
            player_points = crud.get_player_points(db, player_id)
            utils.plot_player_market_value(
                player_market_value=player_market_value,
                player_points=player_points,
                player_name=selected_row["Spieler"].values[0],
                buy_date=selected_row["Kaufdatum"].values[0],
                sell_date=selected_row["Verkaufsdatum"].values[0],
            )
