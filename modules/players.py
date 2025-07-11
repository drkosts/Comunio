import streamlit as st
import utils


def show(player_data_combined):
    """Display the Players page with point analysis and market values"""
    
    if player_data_combined is not None:        
        search_value = st.text_input("Suche Spieler", key="search_player")
        col1, col2 = st.columns([5, 5])
        with col1:
            utils.plot_total_points_vs_price(
                player_data_combined, search_value
            )
        with col2:
            utils.plot_average_points_vs_price(
                player_data_combined, search_value
            )
            
        # Display a sample of the data with current market values
        st.subheader("Spielerdaten mit aktuellem Marktwert")
        display_data = player_data_combined[["Spieler", "Punkte", "Spiele", "PpS", "Preis", "Aktueller_Marktwert"]].head(10)
        st.dataframe(display_data)
    else:
        st.write("Loading player data...")
