import streamlit as st
import utils


def show(transfers):
    """Display the Members page with member statistics and profit analysis"""
    
    members = transfers["Mitspieler"].unique()
    # Sort members by Gewinn/Verlust
    sorted_members = (
        transfers.groupby("Mitspieler")["Gewinn/Verlust"]
        .sum()
        .sort_values(ascending=False, na_position="last")
        .index
    )

    for member in sorted_members:
        st.write(f"### {member}")
        member_transfers = transfers[transfers["Mitspieler"] == member]
        member_transfers = member_transfers.sort_values(
            by="Gewinn/Verlust", ascending=False, na_position="last"
        )

        # Select specific columns to display
        member_transfers = member_transfers[
            [
                "Spieler",
                "Kaufpreis",
                "Verkaufspreis",
                "Gewinn/Verlust",
            ]
        ]

        col1, col2 = st.columns([1, 2])
        with col1:
            # Display the 5 best Gewinn/Verlust pro Tag for each member
            st.write(
                f"Gesamt: {transfers[transfers['Mitspieler'] == member]['Gewinn/Verlust'].sum():,.0f} € ({transfers[transfers['Mitspieler']==member]['Gewinn/Verlust'].count():,.0f} Trades)"
            )
            st.write(member_transfers.head(5))
        with col2:
            # Plot histogram of Gewinn/Verlust by Kaufpreis buckets
            utils.plot_profit_by_price_buckets(member_transfers)
