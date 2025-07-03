import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import crud
import utils


def show(db, transfers, spielzeit):
    """Display the Home page with current team overview"""
    
    st.header("Mein aktuelles Team")
    
    # User selection
    col1, col2 = st.columns([3, 7])
    with col1:
        # Get list of users from transfers data
        users = transfers['Mitspieler'].unique()
        selected_user = st.selectbox("Benutzer auswählen:", users)
    
    with col2:
        st.info(f"Team von: **{selected_user}** | Saison: **{spielzeit}**")
    
    # Get current team for selected user
    current_team = get_current_team(db, selected_user, spielzeit)
    
    if current_team.empty:
        st.warning(f"Keine aktuellen Spieler für {selected_user} in der Saison {spielzeit} gefunden.")
        return
    
    # Display team statistics
    display_team_stats(current_team)
    
    # Configure and display the team grid
    display_team_grid(db, current_team, spielzeit)


def get_current_team(db, user_name, spielzeit):
    """Get current team for a specific user in a specific season"""
    
    # Get all transfers for the user in this season
    all_transfers = crud.get_transfers(db, spielzeit)
    user_transfers = all_transfers[all_transfers['Mitspieler'] == user_name]
    
    # Get players that were bought but not sold (current team)
    current_players = user_transfers[user_transfers['Verkaufsdatum'].isna()]
    
    if current_players.empty:
        return pd.DataFrame()
    
    # Get current market values for these players
    player_ids = current_players['ID'].astype(str).tolist()
    current_market_values = {}
    
    for player_id in player_ids:
        market_value_df = crud.get_player_market_value(db, player_id)
        if market_value_df is not None and not market_value_df.empty:
            # Get the most recent market value
            current_market_values[int(player_id)] = market_value_df['Marktwert'].iloc[-1]
        else:
            current_market_values[int(player_id)] = 0
    
    # Prepare team data
    team_data = []
    for _, player in current_players.iterrows():
        player_id = int(player['ID'])
        current_value = current_market_values.get(player_id, 0)
        buy_price = player['Kaufpreis']
        difference = current_value - buy_price
        
        team_data.append({
            'ID': player_id,
            'Spieler': player['Spieler'],
            'Kaufdatum': player['Kaufdatum'],
            'Kaufpreis': buy_price,
            'Aktueller_Marktwert': current_value,
            'Differenz': difference,
            'Differenz_Prozent': round((difference / buy_price * 100), 1) if buy_price > 0 else 0,
            'Von': player['Von']
        })
    
    return pd.DataFrame(team_data)


def display_team_stats(team_df):
    """Display team statistics in metrics"""
    
    total_investment = team_df['Kaufpreis'].sum()
    current_value = team_df['Aktueller_Marktwert'].sum()
    total_difference = current_value - total_investment
    percentage_change = (total_difference / total_investment * 100) if total_investment > 0 else 0
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Anzahl Spieler", len(team_df))
    
    with col2:
        st.metric("Gesamtinvestition", f"{total_investment:,.0f} €")
    
    with col3:
        st.metric("Aktueller Wert", f"{current_value:,.0f} €")
    
    with col4:
        delta_color = "normal" if total_difference >= 0 else "inverse"
        st.metric("Gewinn/Verlust", f"{total_difference:,.0f} €", delta=f"{percentage_change:+.1f}%")
    
    with col5:
        avg_value = current_value / len(team_df) if len(team_df) > 0 else 0
        st.metric("Ø Spielerwert", f"{avg_value:,.0f} €")


def display_team_grid(db, team_df, spielzeit):
    """Display the team in an interactive grid"""
    
    # Sort by current market value descending
    team_df = team_df.sort_values('Aktueller_Marktwert', ascending=False)
    
    # Configure the grid
    gb = GridOptionsBuilder.from_dataframe(team_df)
    
    # Configure columns with proper formatting
    gb.configure_column(
        "Kaufpreis",
        type=["numericColumn", "numberColumnFilter", "customNumericFormat"],
        valueFormatter="data.Kaufpreis.toLocaleString('de-DE') + ' €';"
    )
    
    gb.configure_column(
        "Aktueller_Marktwert",
        type=["numericColumn", "numberColumnFilter", "customNumericFormat"],
        valueFormatter="data.Aktueller_Marktwert.toLocaleString('de-DE') + ' €';"
    )
    
    gb.configure_column(
        "Differenz",
        type=["numericColumn", "numberColumnFilter", "customNumericFormat"],
        valueFormatter="data.Differenz.toLocaleString('de-DE') + ' €';",
        cellStyle={
            "styleConditions": [
                {
                    "condition": "params.value >= 0",
                    "style": {"color": "green"}
                },
                {
                    "condition": "params.value < 0", 
                    "style": {"color": "red"}
                }
            ]
        }
    )
    
    gb.configure_column(
        "Differenz_Prozent",
        header_name="Differenz %",
        type=["numericColumn", "numberColumnFilter", "customNumericFormat"],
        valueFormatter="data.Differenz_Prozent.toLocaleString('de-DE') + ' %';",
        cellStyle={
            "styleConditions": [
                {
                    "condition": "params.value >= 0",
                    "style": {"color": "green"}
                },
                {
                    "condition": "params.value < 0",
                    "style": {"color": "red"}
                }
            ]
        }
    )
    
    gb.configure_column("Kaufdatum", type=["dateColumnFilter", "customDateTimeFormat"])
    gb.configure_selection("single")
    
    grid_options = gb.build()
    
    # Display the grid
    response = AgGrid(
        team_df,
        gridOptions=grid_options,
        enable_enterprise_modules=True,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        height=400
    )
    
    # Handle row selection for detailed player view
    if response:
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
                        sell_date=None,  # No sell date for current players
                        sell_price=None,  # No sell price for current players
                        spielzeit=spielzeit,
                    )