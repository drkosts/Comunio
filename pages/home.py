import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import crud
import utils
import plotly.express as px

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
    
    # Add portfolio timeline section
    st.subheader("📈 Portfolio Entwicklung")
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📊 Aktuelles Team", "📈 Portfolio Timeline"])
    
    with tab1:
        # Configure and display the team grid
        display_team_grid(db, current_team, spielzeit)
    
    with tab2:
        display_portfolio_timeline(db, selected_user, spielzeit)

def display_portfolio_timeline(db, user_name, spielzeit):
    """Display portfolio timeline analysis"""
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.info("📊 **Investment Timeline**\nZeigt Käufe und Verkäufe über die Zeit")
    
    with col2:
        st.info("💰 **Marktwert Timeline**\nZeigt die Entwicklung des Portfolio-Marktwerts")
    
    # Get timeline data
    with st.spinner("Lade Portfolio Timeline..."):
        investment_timeline = crud.get_portfolio_timeline(db, user_name, spielzeit)
        market_value_timeline = crud.get_portfolio_current_value_timeline(db, user_name, spielzeit)
    
    if investment_timeline.empty and market_value_timeline.empty:
        st.warning("Keine Timeline-Daten verfügbar für den ausgewählten Benutzer.")
        return
    
    # Create and display the timeline chart
    timeline_chart = utils.plot_portfolio_timeline(
        investment_timeline, 
        market_value_timeline, 
        user_name, 
        spielzeit
    )
    
    st.plotly_chart(timeline_chart, use_container_width=True)
    
    # Display timeline statistics
    if not investment_timeline.empty or not market_value_timeline.empty:
        display_timeline_stats(investment_timeline, market_value_timeline)

def display_timeline_stats(investment_timeline, market_value_timeline):
    """Display timeline statistics"""
    
    st.subheader("📊 Timeline Statistiken")
    
    STARTING_BUDGET = 40_000_000
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if not investment_timeline.empty:
            # Count actual transactions (exclude the starting point)
            transactions = investment_timeline[investment_timeline['Event_Type'] != 'start']
            total_transactions = len(transactions)
            st.metric("Transaktionen", total_transactions)
    
    with col2:
        if not investment_timeline.empty:
            buy_events = investment_timeline[investment_timeline['Event_Type'] == 'buy'] if 'Event_Type' in investment_timeline.columns else pd.DataFrame()
            sell_events = investment_timeline[investment_timeline['Event_Type'] == 'sell'] if 'Event_Type' in investment_timeline.columns else pd.DataFrame()
            st.metric("Käufe / Verkäufe", f"{len(buy_events)} / {len(sell_events)}")
    
    with col3:
        if not investment_timeline.empty and 'Verfuegbares_Cash' in investment_timeline.columns:
            current_cash = investment_timeline['Verfuegbares_Cash'].iloc[-1]
            st.metric("Verfügbares Cash", f"{current_cash:,.0f} €")
    
    with col4:
        if not investment_timeline.empty and 'Anzahl_Spieler' in investment_timeline.columns:
            max_players = investment_timeline['Anzahl_Spieler'].max()
            st.metric("Max. Spieler", max_players)
    
    # Performance metrics
    if not investment_timeline.empty and 'Gesamtwert' in investment_timeline.columns:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("💰 Performance Metriken")
            
            # Check if user has made any transactions
            has_transactions = len(investment_timeline[investment_timeline['Event_Type'] != 'start']) > 0
            
            if has_transactions:
                # Calculate total performance vs starting budget using CURRENT market values
                current_total_value = investment_timeline['Gesamtwert'].iloc[-1]
                total_gain_loss = current_total_value - STARTING_BUDGET
                total_gain_loss_pct = (total_gain_loss / STARTING_BUDGET * 100)
                
                st.metric(
                    "Gesamt Performance vs. Startbudget", 
                    f"{total_gain_loss:+,.0f} €",
                    delta=f"{total_gain_loss_pct:+.1f}%"
                )
                
                # Show unrealized gains/losses if available
                if 'Portfolio_Wert_Kaufpreis' in investment_timeline.columns and 'Portfolio_Wert_Aktuell' in investment_timeline.columns:
                    current_investment = investment_timeline['Portfolio_Wert_Kaufpreis'].iloc[-1]
                    current_market_value = investment_timeline['Portfolio_Wert_Aktuell'].iloc[-1]
                    
                    if current_investment > 0:  # Only show if actually invested
                        unrealized_gain = current_market_value - current_investment
                        unrealized_gain_pct = (unrealized_gain / current_investment * 100)
                        
                        st.metric(
                            "Unrealisierter Gewinn/Verlust", 
                            f"{unrealized_gain:+,.0f} €",
                            delta=f"{unrealized_gain_pct:+.1f}%"
                        )
                    else:
                        st.info("Noch keine Spieler im Portfolio")
            else:
                st.metric(
                    "Gesamt Performance vs. Startbudget", 
                    "0 €",
                    delta="Keine Transaktionen"
                )
        
        with col2:
            st.subheader("📈 Budget Allocation")
            
            has_transactions = len(investment_timeline[investment_timeline['Event_Type'] != 'start']) > 0
            
            if has_transactions and 'Portfolio_Wert_Kaufpreis' in investment_timeline.columns and 'Verfuegbares_Cash' in investment_timeline.columns:
                current_investment = investment_timeline['Portfolio_Wert_Kaufpreis'].iloc[-1]
                current_cash = investment_timeline['Verfuegbares_Cash'].iloc[-1]
                
                investment_pct = (current_investment / STARTING_BUDGET * 100)
                cash_pct = (current_cash / STARTING_BUDGET * 100)
                
                st.metric("% in Spielern investiert", f"{investment_pct:.1f}%")
                st.metric("% als Cash verfügbar", f"{cash_pct:.1f}%")
                
                # Show if over-invested (borrowed money)
                if current_cash < 0:
                    st.error(f"⚠️ Überzogen: {abs(current_cash):,.0f} € über budget!")
                
                # Efficiency metrics
                if len(investment_timeline) >= 2 and 'Gesamtwert' in investment_timeline.columns:
                    value_trend = investment_timeline['Gesamtwert'].iloc[-1] - investment_timeline['Gesamtwert'].iloc[-2]
                    if value_trend > 0:
                        st.success(f"📈 Portfolio Trend: +{value_trend:,.0f} €")
                    elif value_trend < 0:
                        st.error(f"📉 Portfolio Trend: {value_trend:,.0f} €")
                    else:
                        st.info("➡️ Portfolio stabil")
            elif not has_transactions:
                st.info("💰 Vollständig in Cash (100%)\n\nNoch keine Investitionen getätigt")
            else:
                st.warning("Keine Allokationsdaten verfügbar")


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
                        sell_date="",  # No sell date for current players
                        sell_price="",  # No sell price for current players
                        spielzeit=spielzeit,
                    )