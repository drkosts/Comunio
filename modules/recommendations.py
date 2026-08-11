"""
Empfehlungs-Seite: passwortgeschützt, zeigt Sell/Hold für Hansi Flicks
Kader und Buy-Empfehlungen für den aktuellen Transfermarkt.

Datenquelle: recommendation_engine.run_recommendations() — wird on-demand
beim ersten Aufruf ausgeführt, dann 1h gecached.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

import auth
import live_data_utils
import recommendation_engine


CACHE_TTL_SECONDS = 3600
TARGET_MEMBER_NAME = "Hansi Flick"
SELL_THRESHOLD = 70  # Score >= 70 → SELL, sonst HOLD


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Empfehlungen werden berechnet …")
def _get_recommendations_cached(_db, _token):
    """Cache-Wrapper um die Engine. Argumente mit _ prefix werden nicht gehasht."""
    return recommendation_engine.run_recommendations(_db, _token)


def show(db, token):
    """Die Seite. Wird aus app.py aufgerufen."""
    auth.require_auth()

    st.title("Empfehlungen")
    st.caption(
        "Vorschläge, keine Auto-Trades. Bid ≥ Mindestgebot (Comunio-Pflicht). "
        "Letzte Berechnung wird 1 Stunde gecached."
    )

    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("🔄 Aktualisieren", use_container_width=True):
            _get_recommendations_cached.clear()
            st.rerun()

    result = _get_recommendations_cached(db, token)
    market_recs = result["market"]
    own_squad_recs = [
        r for r in result["own_squad"] if r.owner_name == TARGET_MEMBER_NAME
    ]

    st.caption(
        f"{len(market_recs)} Markt-Spieler · {len(own_squad_recs)} in {TARGET_MEMBER_NAME}s Kader"
    )

    tab_squad, tab_market = st.tabs([
        f"{TARGET_MEMBER_NAME}s Kader",
        "Markt",
    ])

    with tab_squad:
        _render_squad(own_squad_recs)

    with tab_market:
        _render_market(market_recs)


def _render_squad(recs):
    if not recs:
        st.info(f"{TARGET_MEMBER_NAME} hat aktuell keine Spieler im Kader.")
        return

    rows = []
    for r in sorted(recs, key=lambda x: x.sell_score, reverse=True):
        rows.append({
            "Spieler": r.player_name,
            "Verein": r.club_name,
            "Aktueller MW": _format_eur(r.quoted_price),
            "Sell-Score": r.sell_score,
            "Empfehlung": "🔴 SELL" if r.sell_score >= SELL_THRESHOLD else "🟢 HOLD",
            "Vorgeschlagener Ask": _format_eur(r.suggested_ask_price),
            "Begründung": " · ".join(r.sell_reasons) or "—",
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _render_market(recs):
    if not recs:
        st.info("Markt ist aktuell leer.")
        return

    min_score = st.slider("Min Buy-Score", 0, 100, 40, key="min_buy_score")
    filtered = [r for r in recs if r.buy_score >= min_score]

    if not filtered:
        st.info(f"Keine Spieler mit Buy-Score ≥ {min_score}.")
        return

    rows = []
    for r in sorted(filtered, key=lambda x: x.buy_score, reverse=True):
        rows.append({
            "Spieler": r.player_name,
            "Verein": r.club_name,
            "Aktueller MW": _format_eur(r.quoted_price),
            "Mindestgebot": _format_eur(r.minimum_bid),
            "Buy-Score": r.buy_score,
            "Vorgeschlagener Bid": _format_eur(r.suggested_bid_price),
            "Begründung": " · ".join(r.buy_reasons) or "—",
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _format_eur(value):
    if value is None:
        return "—"
    return f"{value:,.0f} €".replace(",", ".")
