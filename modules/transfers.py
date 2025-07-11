import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import crud
import utils


def show(db, transfers, spielzeit):
    """Display the Home page with transfers grid and filtering options"""
    
    # configure the grid
    col1, col2, col3 = st.columns([2, 6, 2])
    with col1:
        group_by_column = st.selectbox(
            "Gruppieren nach", ["Kein", "Mitspieler", "Von", "An"]
        )
    with col2:
        # add a date filter input
        date_range_standard = [
            transfers["Kaufdatum"].min(),
            transfers["Kaufdatum"].max(),
        ]
        date_range = st.date_input("Von - Bis", date_range_standard)
        # st.date_input returns a single date if not used as a range, so check type
        if not isinstance(date_range, (list, tuple)) or len(date_range) < 2:
            date_range = date_range_standard
        date_range = [pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])]

    with col3:
        search_value = st.text_input("Suche")

    aggregations = {
        "Kaufpreis": "sum",
        "Verkaufspreis": "sum",
        "Gewinn/Verlust": "sum",
        "Gewinn/Verlust pro Tag": "sum",
    }

    if date_range:
        transfers_to_display = transfers[
            (transfers["Kaufdatum"] >= date_range[0])
            & (transfers["Kaufdatum"] <= date_range[1])
        ]
    else:
        transfers_to_display = transfers

    if search_value:
        filtered_grouped_transfers = transfers_to_display.apply(
            lambda x: search_value.lower()
            in x.astype(str).str.lower().str.cat(sep=" "),
            axis=1,
        )
        transfers_to_display = transfers_to_display[filtered_grouped_transfers]
    else:
        transfers_to_display = transfers_to_display

    if group_by_column != "Kein":
        transfers_to_display = (
            transfers_to_display.groupby(group_by_column)
            .agg(aggregations)
            .reset_index()
        )
    else:
        transfers_to_display = transfers_to_display
    
    # Format dates to German format (dd.mm.yyyy) - only if not grouped
    if group_by_column == "Kein":
        # Format Kaufdatum
        transfers_to_display = transfers_to_display.copy()
        transfers_to_display['Kaufdatum'] = transfers_to_display['Kaufdatum'].apply(
            lambda x: x.strftime('%d.%m.%Y') if pd.notna(x) else ''
        )
        # Format Verkaufsdatum  
        transfers_to_display['Verkaufsdatum'] = transfers_to_display['Verkaufsdatum'].apply(
            lambda x: x.strftime('%d.%m.%Y') if pd.notna(x) else ''
        )

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
        "Gewinn/Verlust pro Tag",
        type=["numericColumn", "numberColumnFilter", "customNumericFormat"],
        valueFormatter="data['Gewinn/Verlust pro Tag'].toLocaleString('de-DE') + ' €';",
    )
    
    # Configure date columns as text columns for German formatting
    gb.configure_column("Kaufdatum", type=["textColumn"])
    gb.configure_column("Verkaufsdatum", type=["textColumn"])
    
    gb.configure_selection("single")
    grid_options = gb.build()

    response = AgGrid(
        transfers_to_display,
        gridOptions=grid_options,
        enable_enterprise_modules=True,
        update_mode=GridUpdateMode.MODEL_CHANGED,
    )

    if response:
        print(f"spielzeit: {spielzeit}")
        selected_row = response["selected_rows"]
        if selected_row is not None and not selected_row.empty:
            player_id = selected_row["ID"].values[0]
            if player_id:
                player_market_value = crud.get_player_market_value(db, str(player_id))
                player_points = crud.get_player_points(db, str(player_id))
                if player_market_value is not None and player_points is not None:
                    utils.plot_player_market_value(
                        player_market_value=player_market_value,
                        player_points=player_points,
                        player_name=selected_row["Spieler"].values[0],
                        buy_date=selected_row["Kaufdatum"].values[0],
                        sell_date=selected_row["Verkaufsdatum"].values[0],
                        sell_price=selected_row["Verkaufspreis"].values[0],
                        spielzeit=spielzeit,
                    )
