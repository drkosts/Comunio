import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

FARBE_S1 = "#1f77b4"  # Blau
FARBE_S2 = "#ff7f0e"  # Orange


def _abgeschlossene_trades(transfers: pd.DataFrame, spieler: str) -> pd.DataFrame:
    """Gibt abgeschlossene Trades (Kauf + Verkauf vorhanden) eines Spielers zurück."""
    df = transfers[transfers["Mitspieler"] == spieler].copy()
    df = df[df["Verkaufsdatum"].notna()].copy()
    df["Gewinn"] = df["Verkaufspreis"] - df["Kaufpreis"]
    df["Haltedauer"] = (
        pd.to_datetime(df["Verkaufsdatum"]) - pd.to_datetime(df["Kaufdatum"])
    ).dt.days
    return df


def _alle_trades(transfers: pd.DataFrame, spieler: str) -> pd.DataFrame:
    """Alle Trades (gekauft und/oder verkauft) eines Spielers."""
    return transfers[transfers["Mitspieler"] == spieler].copy()


def _kennzahlen(df_abg: pd.DataFrame, df_alle: pd.DataFrame) -> dict:
    """Berechnet Kennzahlen für einen Spieler."""
    if df_abg.empty:
        return {
            "gesamtgewinn": 0,
            "anzahl_trades": len(df_alle),
            "gewinnquote": 0.0,
            "avg_gewinn": 0.0,
            "bester_trade_name": "–",
            "bester_trade_wert": 0,
            "schlechtester_trade_name": "–",
            "schlechtester_trade_wert": 0,
            "avg_haltedauer": 0.0,
        }

    gesamtgewinn = df_abg["Gewinn"].sum()
    anzahl_trades = len(df_alle)
    positive = df_abg[df_abg["Gewinn"] > 0]
    gewinnquote = len(positive) / len(df_abg) * 100 if len(df_abg) > 0 else 0.0
    avg_gewinn = df_abg["Gewinn"].mean()

    idx_best = df_abg["Gewinn"].idxmax()
    idx_worst = df_abg["Gewinn"].idxmin()

    bester_name = df_abg.loc[idx_best, "Spieler"] if "Spieler" in df_abg.columns else "–"
    bester_wert = df_abg.loc[idx_best, "Gewinn"]
    schlechtester_name = df_abg.loc[idx_worst, "Spieler"] if "Spieler" in df_abg.columns else "–"
    schlechtester_wert = df_abg.loc[idx_worst, "Gewinn"]

    avg_haltedauer = df_abg["Haltedauer"].mean() if "Haltedauer" in df_abg.columns else 0.0

    return {
        "gesamtgewinn": gesamtgewinn,
        "anzahl_trades": anzahl_trades,
        "gewinnquote": gewinnquote,
        "avg_gewinn": avg_gewinn,
        "bester_trade_name": bester_name,
        "bester_trade_wert": bester_wert,
        "schlechtester_trade_name": schlechtester_name,
        "schlechtester_trade_wert": schlechtester_wert,
        "avg_haltedauer": avg_haltedauer if not pd.isna(avg_haltedauer) else 0.0,
    }


def _format_euro(wert: float) -> str:
    return f"{wert:+,.0f} €".replace(",", ".")


def show(transfers_data: pd.DataFrame, spielzeit: str):
    st.header("⚔️ Head-to-Head Vergleich")

    if transfers_data is None or transfers_data.empty:
        st.warning("Keine Transfer-Daten verfügbar.")
        return

    if "Mitspieler" not in transfers_data.columns:
        st.error("Spalte 'Mitspieler' nicht gefunden.")
        return

    spieler_liste = sorted(transfers_data["Mitspieler"].dropna().unique().tolist())

    if len(spieler_liste) < 2:
        st.warning("Nicht genug Spieler für einen Vergleich.")
        return

    col1, col2 = st.columns(2)
    with col1:
        spieler1 = st.selectbox("🔵 Spieler 1", spieler_liste, index=0, key="h2h_s1")
    with col2:
        default_idx = 1 if len(spieler_liste) > 1 else 0
        spieler2 = st.selectbox("🟠 Spieler 2", spieler_liste, index=default_idx, key="h2h_s2")

    if spieler1 == spieler2:
        st.info("Bitte zwei verschiedene Spieler auswählen.")
        return

    df1_abg = _abgeschlossene_trades(transfers_data, spieler1)
    df2_abg = _abgeschlossene_trades(transfers_data, spieler2)
    df1_alle = _alle_trades(transfers_data, spieler1)
    df2_alle = _alle_trades(transfers_data, spieler2)

    kz1 = _kennzahlen(df1_abg, df1_alle)
    kz2 = _kennzahlen(df2_abg, df2_alle)

    # ── Kennzahlen-Vergleich ────────────────────────────────────────────────
    st.subheader("📊 Kennzahlen-Vergleich")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"### 🔵 {spieler1}")
        st.metric("Gesamtgewinn/-verlust", _format_euro(kz1["gesamtgewinn"]))
        st.metric("Anzahl Trades", kz1["anzahl_trades"])
        st.metric("Gewinnquote", f"{kz1['gewinnquote']:.1f} %")
        st.metric("Ø Gewinn pro Trade", _format_euro(kz1["avg_gewinn"]))
        st.metric(
            "Bester Trade",
            f"{kz1['bester_trade_name']} ({_format_euro(kz1['bester_trade_wert'])})",
        )
        st.metric(
            "Schlechtester Trade",
            f"{kz1['schlechtester_trade_name']} ({_format_euro(kz1['schlechtester_trade_wert'])})",
        )
        st.metric("Ø Haltedauer", f"{kz1['avg_haltedauer']:.1f} Tage")

    with c2:
        st.markdown(f"### 🟠 {spieler2}")
        st.metric("Gesamtgewinn/-verlust", _format_euro(kz2["gesamtgewinn"]))
        st.metric("Anzahl Trades", kz2["anzahl_trades"])
        st.metric("Gewinnquote", f"{kz2['gewinnquote']:.1f} %")
        st.metric("Ø Gewinn pro Trade", _format_euro(kz2["avg_gewinn"]))
        st.metric(
            "Bester Trade",
            f"{kz2['bester_trade_name']} ({_format_euro(kz2['bester_trade_wert'])})",
        )
        st.metric(
            "Schlechtester Trade",
            f"{kz2['schlechtester_trade_name']} ({_format_euro(kz2['schlechtester_trade_wert'])})",
        )
        st.metric("Ø Haltedauer", f"{kz2['avg_haltedauer']:.1f} Tage")

    # ── Gewinn-Vergleich Bar Chart ──────────────────────────────────────────
    st.subheader("📈 Gewinn/Verlust pro Trade")

    if not df1_abg.empty or not df2_abg.empty:
        df1_plot = df1_abg.copy()
        df2_plot = df2_abg.copy()

        spieler_col = "Spieler" if "Spieler" in transfers_data.columns else None

        if spieler_col:
            df1_plot = df1_plot.sort_values("Verkaufsdatum")
            df2_plot = df2_plot.sort_values("Verkaufsdatum")

            fig_bar = go.Figure()
            if not df1_abg.empty:
                fig_bar.add_trace(go.Bar(
                    x=df1_plot[spieler_col].astype(str) + " (" + df1_plot["Verkaufsdatum"].astype(str).str[:10] + ")",
                    y=df1_plot["Gewinn"],
                    name=spieler1,
                    marker_color=FARBE_S1,
                ))
            if not df2_abg.empty:
                fig_bar.add_trace(go.Bar(
                    x=df2_plot[spieler_col].astype(str) + " (" + df2_plot["Verkaufsdatum"].astype(str).str[:10] + ")",
                    y=df2_plot["Gewinn"],
                    name=spieler2,
                    marker_color=FARBE_S2,
                ))

            fig_bar.update_layout(
                barmode="group",
                xaxis_title="Trade",
                yaxis_title="Gewinn/Verlust (€)",
                legend_title="Spieler",
                height=400,
                xaxis_tickangle=-45,
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Spalte 'Spieler' nicht gefunden – Bar Chart nicht möglich.")
    else:
        st.info("Keine abgeschlossenen Trades vorhanden.")

    # ── Kumulative Gewinnkurve ──────────────────────────────────────────────
    st.subheader("📉 Kumulative Gewinnkurve")

    if not df1_abg.empty or not df2_abg.empty:
        fig_line = go.Figure()

        if not df1_abg.empty:
            df1_sorted = df1_abg.sort_values("Verkaufsdatum").copy()
            df1_sorted["Kumuliert"] = df1_sorted["Gewinn"].cumsum()
            fig_line.add_trace(go.Scatter(
                x=pd.to_datetime(df1_sorted["Verkaufsdatum"]),
                y=df1_sorted["Kumuliert"],
                mode="lines+markers",
                name=spieler1,
                line=dict(color=FARBE_S1, width=2),
            ))

        if not df2_abg.empty:
            df2_sorted = df2_abg.sort_values("Verkaufsdatum").copy()
            df2_sorted["Kumuliert"] = df2_sorted["Gewinn"].cumsum()
            fig_line.add_trace(go.Scatter(
                x=pd.to_datetime(df2_sorted["Verkaufsdatum"]),
                y=df2_sorted["Kumuliert"],
                mode="lines+markers",
                name=spieler2,
                line=dict(color=FARBE_S2, width=2),
            ))

        fig_line.update_layout(
            xaxis_title="Verkaufsdatum",
            yaxis_title="Kumulierter Gewinn (€)",
            legend_title="Spieler",
            height=400,
        )
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("Keine abgeschlossenen Trades für die Kurve vorhanden.")

    # ── Gemeinsame Spieler ──────────────────────────────────────────────────
    st.subheader("🤝 Gemeinsam gehandelte Spieler")

    spieler_col = "Spieler" if "Spieler" in transfers_data.columns else None
    if spieler_col and not df1_abg.empty and not df2_abg.empty:
        gemeinsame = set(df1_abg[spieler_col].unique()) & set(df2_abg[spieler_col].unique())

        if gemeinsame:
            rows = []
            for sp in sorted(gemeinsame):
                g1 = df1_abg[df1_abg[spieler_col] == sp]["Gewinn"].sum()
                g2 = df2_abg[df2_abg[spieler_col] == sp]["Gewinn"].sum()
                winner = spieler1 if g1 > g2 else (spieler2 if g2 > g1 else "Unentschieden")
                rows.append({
                    "Spieler": sp,
                    f"{spieler1} (€)": f"{g1:+,.0f}".replace(",", "."),
                    f"{spieler2} (€)": f"{g2:+,.0f}".replace(",", "."),
                    "Gewinner": winner,
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("Keine gemeinsam gehandelten Spieler gefunden.")
    elif not spieler_col:
        st.info("Spalte 'Spieler' nicht gefunden.")
    else:
        st.info("Nicht genug Daten für den Vergleich gemeinsamer Spieler.")
