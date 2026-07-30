"""Admin-Seite: Daten direkt aus dem Dashboard aktualisieren.

Spiegelt die bisherigen Backend-Skripte (``structure_data.py`` /
``fetch_live_data.py``) — läuft jetzt direkt hier, kein Laptop nötig.

Credentials werden bevorzugt aus ``st.secrets["comunio"]`` gelesen
(Streamlit-Secrets-Management); fallback auf UI-Eingabe pro Sitzung.

Timeout-Hinweis: ``refresh_players`` macht ~2400 sequenzielle API-Calls
und kann auf Streamlit Cloud je nach Tier an Timeout-Grenzen stoßen.
"""

import streamlit as st

import update_jobs


def _secrets_credentials() -> tuple[str | None, str | None]:
    """Liest Comunio-Credentials aus Streamlit-Secrets, falls vorhanden.

    Erwartetes Format in ``.streamlit/secrets.toml``::

        [comunio]
        username = "..."
        password = "..."
    """
    try:
        cfg = st.secrets.get("comunio", {})  # type: ignore[attr-defined]
    except Exception:
        cfg = {}
    return cfg.get("username"), cfg.get("password")


def show(db, transfers, spielzeit):  # spielzeit nur für Konsistenz mit anderen Pages
    st.header("Daten aktualisieren")
    st.caption(
        "Spiegelt die Backend-Skripte (`structure_data.py` / `fetch_live_data.py`) "
        "und läuft jetzt direkt hier — kein Laptop mehr nötig."
    )

    secrets_user, secrets_pw = _secrets_credentials()
    using_secrets = bool(secrets_user and secrets_pw)

    if using_secrets:
        username, password = secrets_user, secrets_pw
        st.success("✓ Comunio-Credentials aus Streamlit-Secrets geladen.")
    else:
        if "comunio_creds" not in st.session_state:
            st.session_state.comunio_creds = ("", "")
        username, password = st.session_state.comunio_creds

        st.warning(
            "Keine Comunio-Credentials in `st.secrets['comunio']` gefunden. "
            "Entweder in `.streamlit/secrets.toml` hinterlegen oder hier eingeben "
            "(bleibt nur für diese Session):"
        )
        col1, col2 = st.columns(2)
        with col1:
            username = st.text_input(
                "Comunio-Benutzername", value=username, key="admin_user"
            )
        with col2:
            password = st.text_input(
                "Comunio-Passwort", value=password, type="password", key="admin_pw"
            )
        if username and password:
            st.session_state.comunio_creds = (username, password)

    if not username or not password:
        st.stop()

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("1. Spieler aktualisieren")
        st.caption(
            "Holt Player-Liste + Price/Point-History (~800 Spieler, "
            "~2400 sequenzielle Requests). **Dauert mehrere Minuten.**"
        )
        run_players = st.button(
            "🔄 Spieler aktualisieren", use_container_width=True, key="run_players"
        )
    with col2:
        st.subheader("2. Transfers aktualisieren")
        st.caption(
            "Wertet neue Einträge aus `TRANSACTION_TRANSFER` aus. "
            "Kein Login nötig — schnell (Sekunden)."
        )
        run_transfers = st.button(
            "🔄 Transfers aktualisieren", use_container_width=True, key="run_transfers"
        )

    log_box = st.empty()
    progress = st.progress(0.0, text="Bereit.")

    def log(msg: str) -> None:
        log_box.text(msg)

    if run_players:
        try:
            with st.status("Logge bei Comunio ein …", expanded=True) as status:
                log("Login läuft …")
                token = update_jobs.login_to_comunio(username, password)
                log("Login OK. Starte Player-Update …")
                progress.progress(0.05, text="Player-Update läuft …")
                status.update(label="Player-Update läuft …")

                # Kleiner Hook: Fortschritt in der Progress-Bar anzeigen,
                # ohne die Funktion in update_jobs zu verändern.
                original_log = log

                def hooked(msg: str) -> None:
                    original_log(msg)
                    # Messages sehen aus wie "  Players: 25/800 verarbeitet"
                    try:
                        if "/" in msg and "verarbeitet" in msg:
                            tail = msg.split(":")[-1].strip()
                            num, denom = tail.split("/")[0], tail.split("/")[1].split()[0]
                            pct = int(num) / int(denom)
                            progress.progress(min(pct, 1.0), text=msg)
                    except Exception:
                        pass

                update_jobs.refresh_players(db, token, log=hooked)
                progress.progress(1.0, text="Player-Update fertig ✅")
                status.update(label="Player-Update fertig ✅", state="complete")
        except Exception as e:
            st.error(f"Player-Update fehlgeschlagen: {e}")

    if run_transfers:
        try:
            with st.status("Transfer-Update läuft …", expanded=True) as status:
                progress.progress(0.3, text="Transfer-Update läuft …")
                update_jobs.refresh_transfers(db, log=log)
                progress.progress(1.0, text="Transfer-Update fertig ✅")
                status.update(label="Transfer-Update fertig ✅", state="complete")
        except Exception as e:
            st.error(f"Transfer-Update fehlgeschlagen: {e}")
