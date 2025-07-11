import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from unidecode import unidecode
from crud import get_date_range
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def normalize_string(s: str) -> str:
    return unidecode(s).lower()


def plot_player_market_value(
    player_market_value: pd.DataFrame,
    player_points: pd.DataFrame,
    player_name: str,
    buy_date: str,
    sell_date: str = "",
    sell_price: str = "",
    spielzeit: str = "2024/2025",
) -> None:
    date_from, date_to = get_date_range(spielzeit)
    print(spielzeit)
    print(date_from, date_to)
    buy_date = pd.to_datetime(buy_date).date()
    sell_date = pd.to_datetime(sell_date).date() if sell_date else None
    # take only values from player_market_value for spielzeit
    player_market_value["Datum"] = pd.to_datetime(
        player_market_value["Datum"], utc=True
    ).dt.tz_convert(None)
    player_market_value = player_market_value[
        (player_market_value["Datum"] >= pd.to_datetime(date_from))
        & (player_market_value["Datum"] <= pd.to_datetime(date_to))
    ]
    converted_date = pd.to_datetime(player_points["Datum"], utc=True).dt.tz_convert(
        None
    )
    player_points = player_points[
        (converted_date >= pd.to_datetime(date_from))
        & (converted_date <= pd.to_datetime(date_to))
    ]

    if player_market_value is not None:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=player_market_value["Datum"],
                y=player_market_value["Marktwert"],
                mode="lines",
                name=player_name,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[buy_date, buy_date],
                y=[
                    player_market_value["Marktwert"].min(),
                    player_market_value["Marktwert"].max(),
                ],
                mode="lines",
                name="Kaufdatum",
                line=dict(color="red", dash="dash"),
            )
        )
        if sell_date:
            fig.add_trace(
                go.Scatter(
                    x=[sell_date, sell_date],
                    y=[
                        player_market_value["Marktwert"].min(),
                        player_market_value["Marktwert"].max(),
                    ],
                    mode="lines",
                    name="Verkaufsdatum",
                    line=dict(color="green", dash="dash"),
                )
            )
        if sell_price:
            sell_date_minus_three_days = pd.to_datetime(sell_date) - pd.Timedelta(
                days=3
            )
            fig.add_trace(
                go.Scatter(
                    x=[sell_date_minus_three_days, sell_date],
                    y=[sell_price, sell_price],
                    mode="lines",
                    name="Verkaufspreis",
                    line=dict(color="black", dash="dash"),
                )
            )
        if player_points is not None:
            # create bars with the points
            fig.add_trace(
                go.Bar(
                    x=player_points["Datum"],
                    y=player_points["Punkte"],
                    name="Punkte",
                    marker_color="black",
                    yaxis="y2",
                    text=player_points["Spieltag"].apply(
                        lambda x: f"Spieltag: {x}"
                    ),  # Format the Spieltag information as hover text
                    hoverinfo="y+text",
                )
            )
            # Iterate through player_points to find and annotate 0 values
            for i, row in player_points.iterrows():
                if row["Punkte"] == 0:
                    fig.add_annotation(
                        x=row["Datum"],
                        y=0,
                        text="★",  # Special symbol
                        showarrow=False,
                        font=dict(
                            family="Courier New, monospace", size=12, color="red"
                        ),
                        ax=0,
                        ay=40,
                        yref="y2",
                    )
        fig.update_layout(
            title="Marktwertverlauf",
            yaxis=dict(
                title="Marktwert",
                showgrid=False,
                range=[0, player_market_value["Marktwert"].max() + 2],
            ),
            xaxis_title="Datum",
            template="plotly_white",
            yaxis2=dict(
                title="Punkte",
                overlaying="y",
                side="right",
                showgrid=False,
            ),
        )
        st.plotly_chart(fig)
    else:
        st.text("Für diesen Spieler sind keine Daten vorhanden.")


def plot_total_points_vs_price(
    player_points: pd.DataFrame, search_value: str = ""
) -> None:

    # Create the scatter plot
    fig = go.Figure()

    # Add the main scatter plot trace
    fig.add_trace(
        go.Scatter(
            x=player_points["Preis"],
            y=player_points["Punkte"],
            mode="markers",
            text=player_points["Spieler"],
            marker=dict(
                size=12,
                color=player_points["Preis"],
                colorscale="Viridis",
                showscale=True,
            ),
            name="Players",
            showlegend=False,
        )
    )

    # Highlight the searched player
    if search_value:
        normalized_search_value = normalize_string(search_value)
        highlighted_points = player_points[
            player_points["Spieler"]
            .apply(normalize_string)
            .str.contains(normalized_search_value, case=False, na=False)
        ]
        fig.add_trace(
            go.Scatter(
                x=highlighted_points["Preis"],
                y=highlighted_points["Punkte"],
                mode="markers",
                text=highlighted_points["Spieler"],
                marker=dict(
                    size=12,
                    color="red",
                    symbol="circle",
                ),
                name="Highlighted Player",
                showlegend=False,
            )
        )

    fig.update_layout(
        title="Punkte vs Preis",
        xaxis_title="Preis",
        yaxis_title="Punkte",
        template="plotly_white",
        legend=None,
    )
    st.plotly_chart(fig)


def plot_average_points_vs_price(
    player_points: pd.DataFrame, search_value: str = ""
) -> None:
    # make scatter plot of Punkte vs Preis
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=player_points["Preis"],
            y=player_points["PpS"],
            mode="markers",
            text=player_points["Spieler"],
            marker=dict(
                size=12,
                color=player_points["Preis"],
                colorscale="Viridis",
                showscale=True,
            ),
            showlegend=False,
        )
    )

    # Highlight the searched player
    if search_value:
        normalized_search_value = normalize_string(search_value)
        highlighted_points = player_points[
            player_points["Spieler"]
            .apply(normalize_string)
            .str.contains(normalized_search_value, case=False, na=False)
        ]
        fig.add_trace(
            go.Scatter(
                x=highlighted_points["Preis"],
                y=highlighted_points["PpS"],
                mode="markers",
                text=highlighted_points["Spieler"],
                marker=dict(
                    size=12,
                    color="red",
                    symbol="circle",
                ),
                name="Highlighted Player",
                showlegend=False,
            )
        )

    fig.update_layout(
        title="Avg Punkte vs Preis",
        xaxis_title="Preis",
        yaxis_title="PpS",
        template="plotly_white",
    )
    st.plotly_chart(fig)


def plot_profit_by_price_buckets(member_transfers):
    # Define custom bin edges
    bin_edges = [0, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 30000000]
    bin_labels = ["0M-1M", "1M-2M", "2M-5M", "5M-10M", "10M-15M", "15M-20M", "20M-30M"]

    # Create a new column for the custom bins
    member_transfers["Kaufpreis_bins"] = pd.cut(
        member_transfers["Kaufpreis"], bins=bin_edges, labels=bin_labels
    )

    # Aggregate Gewinn/Verlust by the custom bins
    bin_data = (
        member_transfers.groupby("Kaufpreis_bins")["Gewinn/Verlust"].sum().reset_index()
    )

    # Create the histogram
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=bin_data["Kaufpreis_bins"],
            y=bin_data["Gewinn/Verlust"],
        )
    )
    fig.update_layout(
        xaxis_title="Kaufpreis",
        yaxis_title="Gewinn/Verlust",
        template="plotly_white",
        width=800,  # Adjust the width to match the table size
        height=300,  # Adjust the height to match the table size
        margin=dict(l=0, r=0, t=0, b=0),  # Remove the space around the plot
        xaxis=dict(tickvals=bin_data["Kaufpreis_bins"], ticktext=bin_labels),
    )
    st.plotly_chart(fig)


def plot_portfolio_timeline(investment_timeline, market_value_timeline, user_name, spielzeit):
    """Create an interactive portfolio timeline chart"""
    
    # Check if we have any data to plot
    if investment_timeline.empty and market_value_timeline.empty:
        # Return empty figure
        fig = go.Figure()
        fig.add_annotation(
            text="Keine Daten verfügbar",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False, font_size=20
        )
        return fig
    
    # Create subplots
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=(
            f'Portfolio Entwicklung - {user_name} ({spielzeit})',
            'Cash vs. Investment Allocation',
            'Anzahl Spieler im Portfolio'
        ),
        specs=[[{"secondary_y": False}],
               [{"secondary_y": False}],
               [{"secondary_y": False}]],
        vertical_spacing=0.08
    )
    
    # Plot 1: Total Portfolio Value vs Starting Budget
    if not investment_timeline.empty and 'Gesamtwert' in investment_timeline.columns:
        # Total portfolio value (cash + current market values)
        fig.add_trace(
            go.Scatter(
                x=investment_timeline['Datum'],
                y=investment_timeline['Gesamtwert'],
                mode='lines+markers',
                name='Gesamtwert (Cash + Aktuelle Marktwerte)',
                line=dict(color='blue', width=3),
                marker=dict(size=6),
                hovertemplate='<b>Datum:</b> %{x}<br>' +
                              '<b>Gesamtwert:</b> €%{y:,.0f}<br>' +
                              '<extra></extra>'
            ),
            row=1, col=1
        )
        
        # Starting budget reference line
        fig.add_hline(
            y=40_000_000, 
            line_dash="dash", 
            line_color="gray",
            annotation_text="Startbudget: €40.000.000",
            row=1, col=1
        )
        
        # Add purchase value line for comparison
        if 'Portfolio_Wert_Kaufpreis' in investment_timeline.columns and 'Verfuegbares_Cash' in investment_timeline.columns:
            purchase_total_value = investment_timeline['Portfolio_Wert_Kaufpreis'] + investment_timeline['Verfuegbares_Cash']
            fig.add_trace(
                go.Scatter(
                    x=investment_timeline['Datum'],
                    y=purchase_total_value,
                    mode='lines',
                    name='Gesamtwert (Cash + Kaufpreise)',
                    line=dict(color='lightblue', width=2, dash='dot'),
                    hovertemplate='<b>Datum:</b> %{x}<br>' +
                                  '<b>Gesamtwert (Kaufpreise):</b> €%{y:,.0f}<br>' +
                                  '<extra></extra>'
                ),
                row=1, col=1
            )
    
    # Plot 2: Cash vs Investment Allocation
    if not investment_timeline.empty:
        if 'Verfuegbares_Cash' in investment_timeline.columns:
            fig.add_trace(
                go.Scatter(
                    x=investment_timeline['Datum'],
                    y=investment_timeline['Verfuegbares_Cash'],
                    mode='lines+markers',
                    name='Verfügbares Cash',
                    line=dict(color='orange', width=2),
                    marker=dict(size=6),
                    fill='tonexty',
                    hovertemplate='<b>Datum:</b> %{x}<br>' +
                                  '<b>Cash:</b> €%{y:,.0f}<br>' +
                                  '<extra></extra>',
                    showlegend=True
                ),
                row=2, col=1
            )
        
        # Show both purchase value and current market value
        if 'Portfolio_Wert_Kaufpreis' in investment_timeline.columns:
            fig.add_trace(
                go.Scatter(
                    x=investment_timeline['Datum'],
                    y=investment_timeline['Portfolio_Wert_Kaufpreis'],
                    mode='lines+markers',
                    name='Investiert (Kaufpreise)',
                    line=dict(color='purple', width=2, dash='dot'),
                    marker=dict(size=4),
                    hovertemplate='<b>Datum:</b> %{x}<br>' +
                                  '<b>Investiert (Kaufpreis):</b> €%{y:,.0f}<br>' +
                                  '<extra></extra>',
                    showlegend=True
                ),
                row=2, col=1
            )
        
        if 'Portfolio_Wert_Aktuell' in investment_timeline.columns:
            fig.add_trace(
                go.Scatter(
                    x=investment_timeline['Datum'],
                    y=investment_timeline['Portfolio_Wert_Aktuell'],
                    mode='lines+markers',
                    name='Investiert (Aktuelle Marktwerte)',
                    line=dict(color='green', width=2),
                    marker=dict(size=6),
                    fill='tozeroy',
                    hovertemplate='<b>Datum:</b> %{x}<br>' +
                                  '<b>Investiert (Marktwert):</b> €%{y:,.0f}<br>' +
                                  '<extra></extra>',
                    showlegend=True
                ),
                row=2, col=1
            )
    
    # Plot 3: Number of Players
    if not investment_timeline.empty and 'Anzahl_Spieler' in investment_timeline.columns:
        fig.add_trace(
            go.Scatter(
                x=investment_timeline['Datum'],
                y=investment_timeline['Anzahl_Spieler'],
                mode='lines+markers',
                name='Anzahl Spieler',
                line=dict(color='red', width=2),
                marker=dict(size=6),
                hovertemplate='<b>Datum:</b> %{x}<br>' +
                              '<b>Spieler:</b> %{y}<br>' +
                              '<extra></extra>',
                showlegend=False
            ),
            row=3, col=1
        )
    
    # Add buy/sell markers to the main chart
    if not investment_timeline.empty and all(col in investment_timeline.columns for col in ['Event_Type', 'Event_Player', 'Event_Price', 'Gesamtwert']):
        # Filter events (exclude 'start' events)
        events_only = investment_timeline[investment_timeline['Event_Type'].isin(['buy', 'sell'])]
        
        if not events_only.empty:
            # Group transactions by date and event type to handle multiple transactions per day
            buy_events = events_only[events_only['Event_Type'] == 'buy']
            sell_events = events_only[events_only['Event_Type'] == 'sell']
            
            # Group buy events by date
            if not buy_events.empty:
                buy_grouped = buy_events.groupby('Datum').agg({
                    'Event_Player': lambda x: list(x),
                    'Event_Price': lambda x: list(x),
                    'Gesamtwert': 'first'  # Take the portfolio value after all transactions on that day
                }).reset_index()
                
                # Create hover text for grouped transactions
                buy_hover_text = []
                buy_hover_customdata = []
                for _, row in buy_grouped.iterrows():
                    players = row['Event_Player']
                    prices = row['Event_Price']
                    
                    if len(players) == 1:
                        # Single transaction
                        buy_hover_text.append(players[0])
                        buy_hover_customdata.append(prices[0])
                    else:
                        # Multiple transactions - create list
                        player_price_list = [f"{player} (€{price:,.0f})" for player, price in zip(players, prices)]
                        buy_hover_text.append('<br>'.join(player_price_list))
                        buy_hover_customdata.append(sum(prices))  # Total spent that day
                
                fig.add_trace(
                    go.Scatter(
                        x=buy_grouped['Datum'],
                        y=buy_grouped['Gesamtwert'],
                        mode='markers',
                        name='Kauf',
                        marker=dict(color='green', size=12, symbol='triangle-up'),
                        hovertemplate='<b>Gekauft:</b><br>%{text}<br>' +
                                      '<b>Gesamt:</b> €%{customdata:,.0f}<br>' +
                                      '<extra></extra>',
                        text=buy_hover_text,
                        customdata=buy_hover_customdata
                    ),
                    row=1, col=1
                )
            
            # Group sell events by date
            if not sell_events.empty:
                sell_grouped = sell_events.groupby('Datum').agg({
                    'Event_Player': lambda x: list(x),
                    'Event_Price': lambda x: list(x),
                    'Gesamtwert': 'first'  # Take the portfolio value after all transactions on that day
                }).reset_index()
                
                # Create hover text for grouped transactions
                sell_hover_text = []
                sell_hover_customdata = []
                for _, row in sell_grouped.iterrows():
                    players = row['Event_Player']
                    prices = row['Event_Price']
                    
                    if len(players) == 1:
                        # Single transaction
                        sell_hover_text.append(players[0])
                        sell_hover_customdata.append(prices[0])
                    else:
                        # Multiple transactions - create list
                        player_price_list = [f"{player} (€{price:,.0f})" for player, price in zip(players, prices)]
                        sell_hover_text.append('<br>'.join(player_price_list))
                        sell_hover_customdata.append(sum(prices))  # Total earned that day
                
                fig.add_trace(
                    go.Scatter(
                        x=sell_grouped['Datum'],
                        y=sell_grouped['Gesamtwert'],
                        mode='markers',
                        name='Verkauf',
                        marker=dict(color='red', size=12, symbol='triangle-down'),
                        hovertemplate='<b>Verkauft:</b><br>%{text}<br>' +
                                      '<b>Gesamt:</b> €%{customdata:,.0f}<br>' +
                                      '<extra></extra>',
                        text=sell_hover_text,
                        customdata=sell_hover_customdata
                    ),
                    row=1, col=1
                )
    
    # Update layout
    fig.update_layout(
        height=800,
        showlegend=True,
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Update axes
    fig.update_xaxes(title_text="Datum", row=1, col=1)
    fig.update_xaxes(title_text="Datum", row=2, col=1)
    fig.update_xaxes(title_text="Datum", row=3, col=1)
    fig.update_yaxes(title_text="Gesamtwert (€)", row=1, col=1)
    fig.update_yaxes(title_text="Betrag (€)", row=2, col=1)
    fig.update_yaxes(title_text="Anzahl Spieler", row=3, col=1)
    
    return fig


