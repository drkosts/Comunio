import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def plot_player_market_value(
    player_market_value: pd.DataFrame,
    player_points: pd.DataFrame,
    player_name: str,
    buy_date: str,
    sell_date: str = None,
    sell_price: str = None,
    spielzeit: str = "2024/2025",
) -> None:
    if spielzeit == "2024/2025":
        date_from = "2024-07-01"
        date_to = "2025-06-30"
    elif spielzeit == "2023/2024":
        date_from = "2023-06-01"
        date_to = "2024-06-30"
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
