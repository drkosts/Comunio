import streamlit as st
import crud


def show(db, spielzeit):
    print(spielzeit)
    col1, col2 = st.columns([1, 1])
    second_bids = crud.count_second_bids(db, spielzeit)
    col1.table(second_bids.reset_index(drop=True))
    col2.table(crud.count_transfers_buys(db, spielzeit).reset_index(drop=True))
