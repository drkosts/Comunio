# Recommendations Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a password-protected "Empfehlungen" page to the Streamlit dashboard that shows Hansi Flick's squad with sell/hold recommendations and the current transfer market with buy recommendations + market value forecasts.

**Architecture:** Reuse the existing heuristic recommendation engine from `backend/` by duplicating it into `comunio_datenverarbeitung/`. Gate the page behind a bcrypt-hashed password stored in `st.secrets`. Render two tabs: "Hansi Flicks Kader" (sell/hold) and "Markt" (buy). 1h `st.cache_data` TTL on the engine result, manual refresh button.

**Tech Stack:** Streamlit 1.36, bcrypt, pymongo, requests, pandas. No new ML dependencies — heuristic only.

---

## File Structure

**Create (in `comunio_datenverarbeitung/`):**

| File | Responsibility | Source |
|---|---|---|
| `auth.py` | Password gate via bcrypt + `st.secrets` | New |
| `live_endpoints.py` | `get_exchangemarket()`, `get_squad()` Comunio-API wrappers | Stripped copy of `backend/live_endpoints.py` |
| `live_data_utils.py` | `process_exchangemarket_snapshot()` for cache refresh | Stripped copy of `backend/live_data_utils.py` |
| `club_mapping.py` | Comunio-club → OpenLigaDB-team mapping (data only) | 1:1 copy of `backend/club_mapping.py` |
| `seasonality_analysis.py` | Seasonal multiplier helper used by engine | 1:1 copy of `backend/seasonality_analysis.py` (imports adjusted) |
| `recommendation_engine.py` | Heuristic engine: features → scores → recommendations | 1:1 copy of `backend/recommendation_engine.py` (imports adjusted) |
| `modules/recommendations.py` | The page itself (auth + 2 tabs + caching) | New |
| `tests/test_auth.py` | Unit tests for `auth.check_password` and `require_auth` stub | New |

**Modify:**

| File | Change |
|---|---|
| `app.py` | Add "Empfehlungen" to sidebar radio + new `elif` branch |
| `requirements.txt` | Add `bcrypt==4.2.0` |
| `.streamlit/secrets.toml` (local) | Add `page_password_hash = "<generated>"` |
| Streamlit Cloud secrets | Add `page_password_hash` (manual step in Task 9) |

---

## Task 1: Copy `club_mapping.py` 1:1

**Files:**
- Create: `comunio_datenverarbeitung/club_mapping.py`

- [ ] **Step 1: Copy file**

```bash
cp /home/samuel/OneDrive/Comunio/backend/club_mapping.py /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung/club_mapping.py
```

- [ ] **Step 2: Verify file is in place**

```bash
test -f /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung/club_mapping.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git add club_mapping.py
git commit -m "feat(recommendations): copy club_mapping.py from backend"
```

---

## Task 2: Create stripped `live_endpoints.py`

**Files:**
- Create: `comunio_datenverarbeitung/live_endpoints.py`

- [ ] **Step 1: Write file**

Create `comunio_datenverarbeitung/live_endpoints.py` with **only** the two functions the engine needs. The Comunio-API login is already done in `update_jobs.py` — we don't need `_get` helpers from `services.py`. Use `requests` directly:

```python
"""
Stripped Comunio-API-Helfer für die Empfehlungs-Seite.

Nur die zwei Endpunkte, die recommendation_engine braucht:
  - get_exchangemarket()  — aktueller Transfermarkt
  - get_squad()           — eigener Kader

Login passiert upstream in update_jobs.login_to_comunio(); diese Datei
erwartet nur einen gültigen Bearer-Token.
"""

import requests

COMMUNITY_ID = "857661"
USER_ID = "5763843"

BASE_URL = "https://www.comunio.de/api"


def _get(url: str, token: str) -> dict:
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    return response.json()


def get_squad(token: str, user_id: str = USER_ID, state: str = "lineup") -> dict:
    """Holt den aktuellen Kader eines Users."""
    url = f"{BASE_URL}/users/{user_id}/squad?state={state}"
    return _get(url, token)


def get_exchangemarket(
    token: str, community_id: str = COMMUNITY_ID, user_id: str = USER_ID
) -> dict:
    """Holt den aktuellen Transfermarkt."""
    url = f"{BASE_URL}/communities/{community_id}/users/{user_id}/exchangemarket"
    return _get(url, token)
```

- [ ] **Step 2: Verify syntax**

```bash
python -c "import ast; ast.parse(open('/home/samuel/OneDrive/Comunio/comunio_datenverarbeitung/live_endpoints.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git add live_endpoints.py
git commit -m "feat(recommendations): stripped live_endpoints.py with get_exchangemarket/get_squad"
```

---

## Task 3: Create stripped `live_data_utils.py`

**Files:**
- Create: `comunio_datenverarbeitung/live_data_utils.py`

- [ ] **Step 1: Write file**

Only `process_exchangemarket_snapshot` is needed by the engine's `_ensure_live_snapshot`:

```python
"""
Snapshot-Helfer für den Transfermarkt — minimaler Stand für die Empfehlungs-
Engine. Die Engine ruft process_exchangemarket_snapshot(), wenn der letzte
Snapshot älter als 6h ist (siehe _ensure_live_snapshot in recommendation_engine).
"""

import logging
from datetime import datetime, timezone

import live_endpoints

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def process_exchangemarket_snapshot(db, token: str) -> int:
    """Holt den aktuellen Transfermarkt und schreibt jeden Eintrag als Snapshot.

    Returns:
        Anzahl der geschriebenen Snapshot-Datensätze.
    """
    market = live_endpoints.get_exchangemarket(token)
    items = market.get("items", [])
    if not items:
        logger.info("Transfermarkt-Snapshot: leer (Saisonpause?)")
        return 0

    fetched_at = {"fetched_at": _now()}
    docs = [{**entry, **fetched_at} for entry in items]

    collection = db.get_collection("LiveExchangemarket")
    collection.insert_many(docs)
    logger.info("Transfermarkt-Snapshot: %d Angebote geschrieben", len(docs))
    return len(docs)
```

- [ ] **Step 2: Verify syntax**

```bash
python -c "import ast; ast.parse(open('/home/samuel/OneDrive/Comunio/comunio_datenverarbeitung/live_data_utils.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git add live_data_utils.py
git commit -m "feat(recommendations): stripped live_data_utils.py with exchangemarket snapshot"
```

---

## Task 4: Copy `seasonality_analysis.py` with adjusted import

**Files:**
- Create: `comunio_datenverarbeitung/seasonality_analysis.py`

- [ ] **Step 1: Copy file**

```bash
cp /home/samuel/OneDrive/Comunio/backend/seasonality_analysis.py /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung/seasonality_analysis.py
```

- [ ] **Step 2: Adjust the import for `get_db`**

Open the file and change the import line. Find:

```python
from database.base import get_db
```

Replace with:

```python
from database import get_db
```

(Im Streamlit-Projekt gibt es `database.py` statt `database/base.py`.)

- [ ] **Step 3: Verify syntax + import works**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
python -c "import seasonality_analysis; print('OK')"
```

Expected: `OK`

- [ ] **Step 4: Commit**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git add seasonality_analysis.py
git commit -m "feat(recommendations): copy seasonality_analysis.py with import fix"
```

---

## Task 5: Copy `recommendation_engine.py` with adjusted imports

**Files:**
- Create: `comunio_datenverarbeitung/recommendation_engine.py`

- [ ] **Step 1: Copy file**

```bash
cp /home/samuel/OneDrive/Comunio/backend/recommendation_engine.py /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung/recommendation_engine.py
```

- [ ] **Step 2: Adjust imports**

The engine imports `services` (for login) and `database.base` (for get_db). In the Streamlit project, replace:

- `from services import login_to_comunio` → **remove this import** (login happens upstream in `update_jobs.py`; we receive an already-authenticated `token` parameter)
- `from database.base import get_db` → `from database import get_db`
- `import live_endpoints` → unchanged (we have a local copy now)
- `import live_data_utils` → unchanged (we have a local copy now)

The body of `_login_token()` references `login_to_comunio` and `os.environ[...]`. Since we no longer import it, **delete the function** (it's not used when `token` is passed in):

Find:

```python
def _login_token() -> str:
    """Loggt sich ein und gibt den Token zurück. Erwartet env-Vars."""
    return login_to_comunio(os.environ["COMUNIO_USERNAME"], COMUNIO_PASSWORD])
```

Replace with: *(delete the entire function)*

- [ ] **Step 3: Verify syntax + import works**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
python -c "import recommendation_engine; print('OK')"
```

Expected: `OK`

- [ ] **Step 4: Commit**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git add recommendation_engine.py
git commit -m "feat(recommendations): copy recommendation_engine.py with import adjustments"
```

---

## Task 6: Add `bcrypt` to `requirements.txt`

**Files:**
- Modify: `comunio_datenverarbeitung/requirements.txt`

- [ ] **Step 1: Append bcrypt**

```bash
echo "bcrypt==4.2.0" >> /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung/requirements.txt
```

- [ ] **Step 2: Verify**

```bash
grep bcrypt /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung/requirements.txt
```

Expected: `bcrypt==4.2.0`

- [ ] **Step 3: Install locally**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
source venv/bin/activate
pip install bcrypt==4.2.0
```

Expected: `Successfully installed bcrypt-4.2.0`

- [ ] **Step 4: Commit**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git add requirements.txt
git commit -m "feat(recommendations): add bcrypt for password hashing"
```

---

## Task 7: Generate bcrypt hash and save to secrets

**Files:**
- Modify: `comunio_datenverarbeitung/.streamlit/secrets.toml` (local)
- Modify: Streamlit Cloud secrets (manual, see Task 9)

- [ ] **Step 1: Ask user for password**

Ask the user: "Welches Passwort soll ich hashen und in `secrets.toml` eintragen?"

The user will provide a plaintext password. Do NOT log it, do NOT commit it. Only the bcrypt hash goes into secrets.

- [ ] **Step 2: Generate hash**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
source venv/bin/activate
python -c "import bcrypt; print(bcrypt.hashpw(b'USER_PASSWORD_HERE', bcrypt.gensalt()).decode())"
```

Replace `USER_PASSWORD_HERE` with the plaintext the user provided. Capture the output (it will look like `$2b$12$...`).

- [ ] **Step 3: Write hash to `secrets.toml`**

If `.streamlit/secrets.toml` doesn't exist yet, create it. Otherwise, add the entry. The file should look like:

```toml
# Bcrypt-Hash für die passwortgeschützte Empfehlungs-Seite.
# Plaintext-Passwort ist NUR in den Streamlit-Cloud-Secrets
# (siehe docs/HANDOFF.md) — diese Datei wird versioniert.
page_password_hash = "$2b$12$...<generated>..."
```

- [ ] **Step 4: Verify**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
source venv/bin/activate
python -c "
import bcrypt, os, toml
cfg = toml.load('.streamlit/secrets.toml')
hash_bytes = cfg['page_password_hash'].encode()
test = bcrypt.checkpw(b'USER_PASSWORD_HERE', hash_bytes)
print('verify:', test)
"
```

Expected: `verify: True`

- [ ] **Step 5: Commit `secrets.toml` if file was created (otherwise skip)**

Only if `.streamlit/secrets.toml` was newly created. If it was already in `.gitignore` (which is the case for this project — verify with `cat .gitignore | grep secrets`), do NOT commit.

---

## Task 8: Write `auth.py` with tests

**Files:**
- Create: `comunio_datenverarbeitung/tests/test_auth.py`
- Create: `comunio_datenverarbeitung/auth.py`

- [ ] **Step 1: Write failing tests**

Create `comunio_datenverarbeitung/tests/test_auth.py`:

```python
"""Tests für auth.py — bcrypt-Hash-Vergleich und require_auth()-Logik."""
import bcrypt
import pytest

import auth


# ---------------------------------------------------------------------------
# Fixture: bekannter Hash für die Tests
# ---------------------------------------------------------------------------

@pytest.fixture
def known_hash():
    """Hash für 'correct-password' mit fixem Salt (deterministisch)."""
    return bcrypt.hashpw(b"correct-password", bcrypt.gensalt(rounds=4))


# ---------------------------------------------------------------------------
# check_password
# ---------------------------------------------------------------------------

def test_check_password_returns_true_for_correct(monkeypatch, known_hash):
    monkeypatch.setattr(auth, "_get_hash", lambda: known_hash)
    assert auth.check_password("correct-password") is True


def test_check_password_returns_false_for_wrong(monkeypatch, known_hash):
    monkeypatch.setattr(auth, "_get_hash", lambda: known_hash)
    assert auth.check_password("wrong-password") is False


def test_check_password_returns_false_for_empty(monkeypatch, known_hash):
    monkeypatch.setattr(auth, "_get_hash", lambda: known_hash)
    assert auth.check_password("") is False


def test_check_password_returns_false_for_none(monkeypatch, known_hash):
    monkeypatch.setattr(auth, "_get_hash", lambda: known_hash)
    assert auth.check_password(None) is False  # type: ignore[arg-type]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
source venv/bin/activate
python -m pytest tests/test_auth.py -v
```

Expected: `ModuleNotFoundError: No module named 'auth'` (or similar import error)

- [ ] **Step 3: Write minimal `auth.py`**

Create `comunio_datenverarbeitung/auth.py`:

```python
"""
Passwort-Gate für die Empfehlungs-Seite.

Vergleicht eingegebenes Passwort gegen einen bcrypt-Hash aus
st.secrets["page_password_hash"]. Verwendet bcrypt direkt (nicht
passlib) — kleinere Dependency-Footprint.

Public API:
    check_password(plain: str) -> bool
    require_auth() -> None      # stoppt die App wenn nicht authentifiziert
"""

import bcrypt
import streamlit as st


def _get_hash() -> bytes:
    """Holt den konfigurierten Hash aus st.secrets.

    Raises:
        stoppt die App mit klarer Anleitung wenn der Secret fehlt.
    """
    raw = st.secrets.get("page_password_hash")
    if not raw:
        st.error(
            "page_password_hash ist nicht konfiguriert.\n\n"
            "Lokal: in `.streamlit/secrets.toml` eintragen.\n"
            "Streamlit Cloud: Settings → Secrets."
        )
        st.stop()
    return raw.encode() if isinstance(raw, str) else raw


def check_password(plain: str) -> bool:
    """Vergleicht Klartext-Passwort gegen den konfigurierten Hash."""
    if not plain:
        return False
    try:
        return bcrypt.checkpw(plain.encode(), _get_hash())
    except (ValueError, TypeError):
        return False


def require_auth() -> None:
    """Zeigt Passwort-Eingabe, hält App an wenn nicht authentifiziert.

    Nach erfolgreicher Eingabe wird st.session_state.auth_ok gesetzt und
    die App neu gerendert. Bleibt für die Session aktiv.
    """
    if st.session_state.get("auth_ok"):
        return

    pwd = st.text_input("Passwort", type="password", key="page_pwd")
    if not pwd:
        st.stop()

    if check_password(pwd):
        st.session_state.auth_ok = True
        st.rerun()
    else:
        st.error("Falsches Passwort")
        st.stop()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
source venv/bin/activate
python -m pytest tests/test_auth.py -v
```

Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git add auth.py tests/test_auth.py
git commit -m "feat(recommendations): auth.py with bcrypt check and require_auth gate"
```

---

## Task 9: Write `modules/recommendations.py`

**Files:**
- Create: `comunio_datenverarbeitung/modules/recommendations.py`

- [ ] **Step 1: Write file**

```python
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
        r for r in result["own_squad"] if r.member_name == TARGET_MEMBER_NAME
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
```

**Hinweis:** `Recommendation.member_name` ist im aktuellen `recommendation_engine.py` **nicht** als Feld definiert — der Empfehlungs-Dataclass hat nur `player_id, player_name, club_name, quoted_price, ...`. Wir müssen also beim Filtern anders vorgehen: nicht auf `member_name` matchen (existiert nicht), sondern auf `club_name` der eigenen Squad-Items die zu Hansi Flick gehören. Das passiert implizit, weil `run_recommendations` für jedes eigene Squad-Item eine Recommendation baut — aber die Recommendation trägt den Spielername + Verein, nicht den Owner.

**Korrektur:** Statt nach `member_name == "Hansi Flick"` zu filtern, müssen wir beim Iterieren der eigenen Squad den Owner-Namen aus der API-Antwort mitführen. Dafür ist die einfachste Lösung: in `recommendation_engine.py` dem Dataclass `Recommendation` ein optionales Feld `owner_name: str | None = None` hinzufügen, und in `run_recommendations` dieses Feld setzen.

- [ ] **Step 2: Patch `recommendation_engine.py` to carry `owner_name`**

Open `comunio_datenverarbeitung/recommendation_engine.py` and:

a) Add `owner_name: str | None = None` to the `Recommendation` dataclass (after `price_bucket`).

b) In `run_recommendations`, when building the `out_own` list, the loop iterates `squad_items`. We need `squad_items` to also expose the owner name. Comunio's squad endpoint returns items where the player is in `_embedded.player` and the owner is the requesting user (`USER_ID` in `live_endpoints.py`). For our purposes, **all `own_squad` recommendations belong to the user who is logged in** — i.e. `USER_ID = "5763843"`.

So when building the Recommendation in the own-squad loop, set `owner_name = "Hansi Flick"` (or read from `USER_ID`). Simplest: hardcode the constant.

Add at the top of `recommendation_engine.py`:

```python
# Der User-ID, dessen Squad geladen wird. Kommt aus live_endpoints.USER_ID.
# Hier hartcodiert als Display-Name; im Dashboard hartcodiert auf Hansi Flick.
OWNER_DISPLAY_NAME = "Hansi Flick"
```

In the `out_own` loop, add `owner_name=OWNER_DISPLAY_NAME` to both `Recommendation(...)` constructions.

- [ ] **Step 3: Update `modules/recommendations.py` filter**

Change the filter from `r.member_name == TARGET_MEMBER_NAME` to `r.owner_name == TARGET_MEMBER_NAME`.

- [ ] **Step 4: Verify engine still works**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
source venv/bin/activate
python -c "
from database import get_db
from live_endpoints import get_squad, get_exchangemarket
import recommendation_engine as eng
db = get_db()
# Just check imports + dataclass field
import inspect
sig = inspect.signature(eng.Recommendation)
print('owner_name in fields:', 'owner_name' in sig.parameters)
"
```

Expected: `owner_name in fields: True`

- [ ] **Step 5: Verify syntax of recommendations.py**

```bash
python -c "import ast; ast.parse(open('/home/samuel/OneDrive/Comunio/comunio_datenverarbeitung/modules/recommendations.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git add modules/recommendations.py recommendation_engine.py
git commit -m "feat(recommendations): page module with auth gate + 2 tabs + owner_name tracking"
```

---

## Task 10: Wire into `app.py`

**Files:**
- Modify: `comunio_datenverarbeitung/app.py`

- [ ] **Step 1: Add "Empfehlungen" to radio options**

Open `app.py`. Find:

```python
page = st.sidebar.radio(
    "Navigation",
    [
        "Home",
        "Transfers",
        "Players",
        "Members",
        "Teams",
        "Statistics",
        "Head-to-Head",
        "Daten aktualisieren",
    ],
)
```

Replace with:

```python
page = st.sidebar.radio(
    "Navigation",
    [
        "Home",
        "Transfers",
        "Players",
        "Members",
        "Teams",
        "Statistics",
        "Head-to-Head",
        "Daten aktualisieren",
        "Empfehlungen",
    ],
)
```

- [ ] **Step 2: Add `elif` branch**

Find:

```python
elif page == "Daten aktualisieren":
    admin.show(db, transfers_data, spielzeit)
```

Add after it:

```python
elif page == "Empfehlungen":
    from modules import recommendations as rec_page
    token = os.environ.get("COMUNIO_TOKEN") or _get_comunio_token()
    rec_page.show(db, token)
```

- [ ] **Step 3: Add helper to get a Comunio token**

Add at the top of `app.py` (after `db = get_db()`):

```python
import os
from dotenv import load_dotenv
load_dotenv()

from update_jobs import login_to_comunio

_cached_token = {"value": None, "expires_at": None}


def _get_comunio_token() -> str:
    """Holt einen frischen Comunio-Token aus st.secrets oder .env und cached 50min."""
    now = datetime.now(timezone.utc)
    if _cached_token["value"] and _cached_token["expires_at"] > now:
        return _cached_token["value"]

    user = st.secrets.get("comunio", {}).get("username") or os.environ.get("COMUNIO_USERNAME")
    pw = st.secrets.get("comunio", {}).get("password") or os.environ.get("COMUNIO_PASSWORD")
    if not user or not pw:
        st.error("Comunio-Credentials fehlen (st.secrets['comunio'] oder .env).")
        st.stop()
    _cached_token["value"] = login_to_comunio(user, pw)
    _cached_token["expires_at"] = now + timedelta(minutes=50)
    return _cached_token["value"]
```

And at the very top of `app.py`, add:

```python
from datetime import datetime, timedelta, timezone
```

- [ ] **Step 4: Verify syntax**

```bash
python -c "import ast; ast.parse(open('/home/samuel/OneDrive/Comunio/comunio_datenverarbeitung/app.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 5: Commit**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git add app.py
git commit -m "feat(recommendations): wire page into app.py navigation"
```

---

## Task 11: Local verification

**Files:** None modified.

- [ ] **Step 1: Run pytest**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
source venv/bin/activate
python -m pytest tests/ -v
```

Expected: All tests pass.

- [ ] **Step 2: Smoke-test engine**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
source venv/bin/activate
python -c "
from database import get_db
from update_jobs import login_to_comunio
import os
token = login_to_comunio(os.environ['COMUNIO_USERNAME'], os.environ['COMUNIO_PASSWORD'])
result = __import__('recommendation_engine').run_recommendations(get_db(), token)
print('market:', len(result['market']))
print('own_squad:', len(result['own_squad']))
print('hansi in own:', sum(1 for r in result['own_squad'] if getattr(r, 'owner_name', None) == 'Hansi Flick'))
"
```

Expected: non-zero numbers for all three counts.

- [ ] **Step 3: Start Streamlit**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
source venv/bin/activate
streamlit run app.py
```

Open browser to `http://localhost:8501`.

- [ ] **Step 4: Manual checks**

- [ ] Sidebar shows "Empfehlungen" entry
- [ ] Clicking it shows password input
- [ ] Wrong password → "Falsches Passwort" + stops
- [ ] Correct password → two tabs appear
- [ ] Tab 1 shows Hansi Flick's squad (if any) with SELL/HOLD labels
- [ ] Tab 2 shows market with min-score slider
- [ ] Refresh button works (recomputes)

- [ ] **Step 5: Stop Streamlit**

```bash
# In the terminal where streamlit runs:
Ctrl+C
```

---

## Task 12: Deploy to Streamlit Cloud

**Files:**
- Modify: GitHub remote (push)

- [ ] **Step 1: Push**

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git push origin main
```

Expected: Push succeeds.

- [ ] **Step 2: Set Cloud secret**

Manual step in browser:
1. Open https://share.streamlit.io → deine App
2. Settings → Secrets
3. Add entry:

```toml
page_password_hash = "$2b$12$...<generated>..."
```

(plus `comunio.username` / `comunio.password` if not already set)

4. Save → App rebootet automatisch

- [ ] **Step 3: Verify on Cloud**

Open the deployed app. Same manual checks as Task 11, Step 4, but in the cloud deployment.

- [ ] **Step 4: Cleanup local secrets.toml (optional)**

If you committed `.streamlit/secrets.toml` by mistake (Task 7 Step 5 should have prevented this), remove it:

```bash
cd /home/samuel/OneDrive/Comunio/comunio_datenverarbeitung
git rm .streamlit/secrets.toml
git commit -m "chore: remove accidentally-committed secrets.toml"
```

---

## Self-Review Checklist

- [x] Each spec section has a matching task
- [x] No "TBD" / "TODO" / "fill in" placeholders in the plan itself
- [x] All file paths exact
- [x] All commands with expected output
- [x] Type names consistent (Recommendation dataclass has `owner_name` field added in Task 9, used in Task 9 filter, used by Task 11 smoke-test)
- [x] Function names consistent: `_get_recommendations_cached` defined in Task 9, called from same task and Task 10 wiring; `show(db, token)` signature in Task 9 matches caller in Task 10
