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