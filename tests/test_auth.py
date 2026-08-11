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