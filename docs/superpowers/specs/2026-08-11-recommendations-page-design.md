# Design: Passwortgeschützte Empfehlungs-Seite im Streamlit-Dashboard

**Datum:** 2026-08-11
**Status:** Approved (Brainstorming abgeschlossen, Spec self-review durch)
**Owner:** Samuel

## Ziel

Im bestehenden Streamlit-Dashboard (`comunio_datenverarbeitung/`) eine neue, passwortgeschützte Seite „Empfehlungen" hinzufügen, die:

1. Hansi Flicks aktuellen Kader mit **Sell/Hold-Empfehlungen** anzeigt
2. Den aktuellen Transfermarkt mit **Buy-Empfehlungen und Marktwert-Prognosen** anzeigt

Die Empfehlungs-Heuristik existiert bereits in `backend/recommendation_engine.py` und wird heute über `recommend_cli.py` ausgespielt. Diese Engine wird in den Dashboard-Code dupliziert und hinter einem Passwort-Gate angerichtet.

## Auth

- **Mechanismus:** bcrypt-Hash in Streamlit-Secrets (`st.secrets["page_password_hash"]`), clientseitiger Vergleich in `auth.py:check_password()`.
- **UX:** Single-Password, kein Username, keine Mehr-Faktor-Lösung.
- **Session-State:** `st.session_state.auth_ok = True` nach erfolgreicher Eingabe; bleibt für die Streamlit-Session aktiv.
- **Fehlerpfad:** Falsches Passwort → Inline-Fehler „Falsches Passwort". Leeres Feld → `st.stop()`.
- **Konfiguration:** Wenn `page_password_hash` fehlt → harter Error mit Anleitung.

## Architektur

**Neue Dateien in `comunio_datenverarbeitung/`:**

| Datei | Zweck |
|---|---|
| `auth.py` | `check_password()`, `require_auth()`, Hash-Lookup via `st.secrets` |
| `recommendation_engine.py` | Kopie von `backend/recommendation_engine.py` mit angepassten Imports |
| `live_endpoints.py` | Kopie von `backend/live_endpoints.py`, gestrippt auf `get_exchangemarket()` + `get_squad()` |
| `live_data_utils.py` | `process_exchangemarket_snapshot()` (für `_ensure_live_snapshot`-Logik) |
| `club_mapping.py` | Mapping Comunio-Club → OpenLigaDB-Team-ID |
| `seasonality_analysis.py` | Wird von Engine referenziert, brauchen wir 1:1 |
| `modules/recommendations.py` | Die neue Seite (Auth + 2 Tabs) |

**Geänderte Dateien:**

| Datei | Änderung |
|---|---|
| `app.py` | Neuer Eintrag `"Empfehlungen"` in Sidebar-Radio + neuer `elif`-Branch, der `token` und die Cache-Funktion weitergibt |
| `requirements.txt` | `bcrypt` ergänzen |
| `.streamlit/secrets.toml` | Eintrag `page_password_hash = "..."` |
| Streamlit-Cloud-Secrets | derselbe Eintrag online setzen |

**Datenfluss beim Seitenaufruf:**

```
require_auth()
    ↓ wenn auth_ok
get_recommendations(db, token)   [st.cache_data, ttl=3600]
    ↓ ruft intern
recommendation_engine.run_recommendations(db, token)
    ↓ holt frischen Snapshot wenn > 6h alt
live_data_utils.process_exchangemarket_snapshot(db, token)
    ↓ Comunio-API
get_exchangemarket(token) + get_squad(token)
    ↓
result = {"market": [...], "own_squad": [...]}
    ↓ gefiltert + sortiert
zwei Tabellen rendern
```

## Komponenten

### `auth.py`

```python
def check_password(plain: str) -> bool: ...
def require_auth() -> None:
    # zeigt st.text_input(type="password"), vergleicht, st.session_state.auth_ok, st.stop()
```

### `recommendation_engine.py` (Kopie)

Inhalt identisch zu `backend/recommendation_engine.py`. Imports ändern sich:
- `from database.base import get_db` → `from database import get_db` (anderer Pfad im Streamlit-Projekt)
- `from services import login_to_comunio` → ersetzen durch direkten Aufruf von `live_endpoints` (oder Login-Helper hier reinkopieren)

### `live_endpoints.py` (Kopie)

Nur die zwei Funktionen, die die Engine braucht:
- `get_exchangemarket(token)` → Liste der Markt-Items mit `_embedded.player` und `quotedPrice`
- `get_squad(token)` → Liste der Squad-Items

### `modules/recommendations.py`

```python
def show(db, token, get_recommendations):
    require_auth()

    st.title("Empfehlungen")
    st.caption("Hinweis: Vorschläge, keine Auto-Trades.")

    # Refresh-Button rechts
    if st.button("🔄 Aktualisieren"):
        get_recommendations.clear()

    with st.spinner("Empfehlungen werden berechnet …"):
        result = get_recommendations(db, token)
        market = result["market"]
        own_squad = [r for r in result["own_squad"] if r.member_name == "Hansi Flick"]

    tab1, tab2 = st.tabs(["Hansi Flicks Kader", "Markt"])

    with tab1:
        _render_own_squad(own_squad)

    with tab2:
        _render_market(market)


def _render_own_squad(recs):
    if not recs:
        st.info("Hansi Flick hat aktuell keine Spieler im Kader.")
        return
    df = pd.DataFrame([
        {
            "Spieler": r.player_name,
            "Verein": r.club_name,
            "Aktueller MW": r.quoted_price,
            "Sell-Score": r.sell_score,
            "Empfehlung": "SELL" if r.sell_score >= 70 else "HOLD",
            "Vorgeschlagener Ask": r.suggested_ask_price,
            "Begründung": " · ".join(r.sell_reasons),
        }
        for r in sorted(recs, key=lambda x: x.sell_score, reverse=True)
    ])
    st.dataframe(df, use_container_width=True, hide_index=True)


def _render_market(recs):
    if not recs:
        st.info("Markt ist aktuell leer.")
        return
    min_score = st.slider("Min Buy-Score", 0, 100, 40)
    filtered = [r for r in recs if r.buy_score >= min_score]
    df = pd.DataFrame([
        {
            "Spieler": r.player_name,
            "Verein": r.club_name,
            "Aktueller MW": r.quoted_price,
            "Mindestgebot": r.minimum_bid,
            "Buy-Score": r.buy_score,
            "Vorgeschlagener Bid": r.suggested_bid_price,
            "Begründung": " · ".join(r.buy_reasons),
        }
        for r in sorted(filtered, key=lambda x: x.buy_score, reverse=True)
    ])
    st.dataframe(df, use_container_width=True, hide_index=True)
```

## Datenfluss & Caching

**Caching-Strategie:** `@st.cache_data(ttl=3600)` auf `get_recommendations(db, token)`. Cache-Key enthält `db` und `token`, beides stabil pro Session.

**Refresh:**
- Manuell via Button → `get_recommendations.clear()`
- Automatisch nach 1h TTL
- `token` muss vor `get_recommendations` einmal geholt werden via `login_to_comunio(...)` (aus `update_jobs.py` reimportieren)

**Login-Token:** `update_jobs.login_to_comunio(username, password)` — wird bereits im `admin.py` verwendet. Credentials kommen aus `st.secrets["comunio"]` oder UI-Eingabe.

## Fehlerbehandlung

| Szenario | Verhalten |
|---|---|
| Comunio-Login schlägt fehl | `st.error()` + Hinweis, Refresh-Button sichtbar. Keine Tabelle. |
| Engine-Crash (einzelner Spieler) | Spieler wird mit `buy_score=0` und leerer Begründung gerendert. |
| Engine-Crash (komplett) | `st.error("Empfehlungs-Engine fehlgeschlagen")` + Trace. |
| Hansi Flick hat keinen Squad | Tab 1 zeigt Hinweistext. |
| Markt ist leer | Tab 2 zeigt Hinweistext. |
| Passwort-Feld leer | `st.stop()` |
| Passwort falsch | Inline-Fehler „Falsches Passwort" |
| `page_password_hash` fehlt | Harter Error mit Anleitung („setze `page_password_hash` in Streamlit-Secrets") |
| Cache wirft beim Refresh | Fällt durch zu Live-Berechnung. |

## Testing / Verifikation

1. **Lokal vor Commit:**
   - `python -c "from recommendation_engine import run_recommendations; from database import get_db; ..."` läuft
   - `streamlit run app.py` lokal, Passwort eingeben, beide Tabs füllen sich
2. **Plausibilität:** Vergleiche Top-3 Sell-Empfehlungen der Seite mit `backend/recommend_cli.py` Output (gleicher Spieler oben)
3. **Streamlit Cloud:** Secret `page_password_hash` setzen, App rebooten, Seite aufrufen

## Out of Scope (bewusst)

- ML-Modell-Training (`backend/ml_pipeline/`) — separate Codebase, andere Verantwortlichkeit
- Auto-Trades — explizit ausgeschlossen (USER-Vorgabe)
- Mehrere User-Accounts — eine Seite, ein Passwort reicht
- Push-Notifications / E-Mail — YAGNI
- Portfolio-Timeline auf Empfehlungsseite — gibt's schon im Home-Tab
- Eigene Hansi-Flick-Identität vs. Login-User — hartcodiert auf `member_name == "Hansi Flick"`

## Offene Punkte für Implementation

- [ ] bcrypt-Hash generieren für das gewählte Passwort (Hash wird generiert, nicht Plain-Text-Store)
- [ ] `bcrypt` zu `requirements.txt` hinzufügen
- [ ] `recommendation_engine.py`, `live_endpoints.py`, `live_data_utils.py`, `club_mapping.py`, `seasonality_analysis.py` aus `backend/` kopieren mit angepassten Imports
- [ ] `auth.py` schreiben
- [ ] `modules/recommendations.py` schreiben
- [ ] `app.py` erweitern (Radio + elif)
- [ ] `.streamlit/secrets.toml` und Streamlit-Cloud-Secrets mit `page_password_hash` aktualisieren
- [ ] Test: lokal ausführen + auf Streamlit Cloud deployen
