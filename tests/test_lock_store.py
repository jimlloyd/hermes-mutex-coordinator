"""Tests for the mutex-coordinator plugin."""

import json
import tempfile
import threading
import time
from pathlib import Path

import pytest

from mutex_coordinator.lock_store import GRACE_PERIOD_MS, LockStore


@pytest.fixture
def store():
    """Create a LockStore backed by a temp file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = Path(f.name)
    store = LockStore(path)
    yield store
    path.unlink(missing_ok=True)


@pytest.fixture
def store_fast_ttl():
    """LockStore with 100ms TTL for timeout testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = Path(f.name)
    store = LockStore(path, ttl_ms=100)
    yield store
    path.unlink(missing_ok=True)


# ── claim_channel tests ──────────────────────────────────────────────────


def test_claim_when_free(store):
    result = store.claim_channel("discord:1", "taliesin")
    assert result["status"] == "acquired"
    assert result["fence"] == 1
    assert result["consecutive_timeouts"] == 0


def test_claim_when_locked_by_other(store):
    store.claim_channel("discord:1", "taliesin")
    result = store.claim_channel("discord:1", "gwydion")
    assert result["status"] == "locked"
    assert result["by"] == "taliesin"


def test_claim_after_expiry(store_fast_ttl):
    store_fast_ttl.claim_channel("discord:1", "taliesin")
    time.sleep(0.2)  # exceeds 100ms TTL
    result = store_fast_ttl.claim_channel("discord:1", "gwydion")
    assert result["status"] == "acquired"
    assert result["fence"] == 2
    # taliesin should have a timeout incremented
    timeouts = store_fast_ttl._get_timeouts("discord:1", "taliesin")
    assert timeouts == 1


def test_claim_when_already_holding(store):
    store.claim_channel("discord:1", "taliesin")
    result = store.claim_channel("discord:1", "taliesin")
    assert result["status"] == "acquired"
    assert result["fence"] == 1  # same fence, renewal


# ── release_channel tests ────────────────────────────────────────────────


def test_release_with_correct_fence(store):
    store.claim_channel("discord:1", "taliesin")
    result = store.release_channel("discord:1", "taliesin", 1, "100")
    assert result["status"] == "released"


def test_release_with_stale_fence(store):
    store.claim_channel("discord:1", "taliesin")
    result = store.release_channel("discord:1", "taliesin", 99, "100")
    assert result["status"] == "stale_fence"


def test_release_resets_timeouts(store_fast_ttl):
    store_fast_ttl.claim_channel("discord:1", "taliesin")
    time.sleep(0.2)
    store_fast_ttl.claim_channel("discord:1", "gwydion")  # increments taliesin's timeouts
    store_fast_ttl.release_channel("discord:1", "gwydion", 2, "100")

    timeouts = store_fast_ttl._get_timeouts("discord:1", "gwydion")
    assert timeouts == 0


# ── verify_lock tests ────────────────────────────────────────────────────


def test_verify_within_ttl(store):
    store.claim_channel("discord:1", "taliesin")
    assert store.verify_lock("discord:1", "taliesin", 1) is True


def test_verify_after_expiry_within_grace(store_fast_ttl):
    store_fast_ttl.claim_channel("discord:1", "taliesin")
    time.sleep(0.15)  # expired (100ms) but within grace (10s)
    assert store_fast_ttl.verify_lock("discord:1", "taliesin", 1) is True


def test_verify_wrong_fence(store):
    store.claim_channel("discord:1", "taliesin")
    assert store.verify_lock("discord:1", "taliesin", 99) is False


def test_verify_after_grace_expired(store_fast_ttl):
    """verify_lock returns false after expiry AND grace period."""
    store_fast_ttl.claim_channel("discord:1", "taliesin")
    time.sleep(0.2)  # expired (100ms), but still within 10s grace
    assert store_fast_ttl.verify_lock("discord:1", "taliesin", 1) is True
    # simulate clock past grace
    original_now = store_fast_ttl._now_ms
    store_fast_ttl._now_ms = lambda: original_now() + 11_000
    assert store_fast_ttl.verify_lock("discord:1", "taliesin", 1) is False
    store_fast_ttl._now_ms = original_now


def test_same_claimant_re_claim_extends_ttl(store_fast_ttl):
    """Re-claiming while holding the lock extends expires_at."""
    store_fast_ttl.claim_channel("discord:1", "taliesin")
    time.sleep(0.05)  # 50ms, not expired
    store_fast_ttl.claim_channel("discord:1", "taliesin")  # re-claim extends
    time.sleep(0.08)  # would have expired without the re-claim extension
    assert store_fast_ttl.verify_lock("discord:1", "taliesin", 1) is True


# ── renew_lease tests ────────────────────────────────────────────────────


def test_renew_with_correct_fence(store):
    store.claim_channel("discord:1", "taliesin")
    result = store.renew_lease("discord:1", "taliesin", 1)
    assert result["status"] == "renewed"


def test_renew_with_stale_fence(store_fast_ttl):
    store_fast_ttl.claim_channel("discord:1", "taliesin")
    time.sleep(0.2)  # expired
    store_fast_ttl.claim_channel("discord:1", "gwydion")  # gwydion steals
    result = store_fast_ttl.renew_lease("discord:1", "taliesin", 1)
    assert result["status"] == "expired"
    assert result["by"] == "gwydion"


# ── concurrency tests ────────────────────────────────────────────────────


def test_two_claimant_race(store):
    """Two threads claim simultaneously — exactly one wins."""
    results = []
    db_path = store.db_path

    def claim(name):
        s = LockStore(db_path)
        results.append(s.claim_channel("discord:1", name))

    t1 = threading.Thread(target=claim, args=("taliesin",))
    t2 = threading.Thread(target=claim, args=("gwydion",))
    t1.start(); t2.start(); t1.join(); t2.join()

    winners = [r for r in results if r["status"] == "acquired"]
    assert len(winners) == 1
    assert winners[0]["fence"] == 1


def test_claim_during_active_lock(store):
    store.claim_channel("discord:1", "taliesin")
    db_path = store.db_path
    results = []

    def claim():
        s = LockStore(db_path)
        results.append(s.claim_channel("discord:1", "gwydion"))

    t = threading.Thread(target=claim)
    t.start(); t.join()

    assert results[0]["status"] == "locked"


def test_expired_claim_race(store_fast_ttl):
    """Lock expires, two threads race expired path."""
    store_fast_ttl.claim_channel("discord:1", "taliesin")
    db_path = store_fast_ttl.db_path
    time.sleep(0.2)
    results = []

    def claim(name):
        s = LockStore(db_path)
        results.append(s.claim_channel("discord:1", name))

    t1 = threading.Thread(target=claim, args=("gwydion",))
    t2 = threading.Thread(target=claim, args=("vera",))
    t1.start(); t2.start(); t1.join(); t2.join()

    winners = [r for r in results if r["status"] == "acquired"]
    assert len(winners) == 1
    assert winners[0]["fence"] >= 2


# ── consecutive_timeouts tests ───────────────────────────────────────────


def test_consecutive_timeouts_accumulate(store_fast_ttl):
    store_fast_ttl.claim_channel("discord:1", "taliesin")
    time.sleep(0.2)
    store_fast_ttl.claim_channel("discord:1", "gwydion")
    time.sleep(0.2)
    store_fast_ttl.claim_channel("discord:1", "vera")

    timeouts = store_fast_ttl._get_timeouts("discord:1", "taliesin")
    assert timeouts == 1  # only incremented once (when gwydion took over)


def test_consecutive_timeouts_reset_on_release(store_fast_ttl):
    """A profile with timeouts gets them reset on successful release."""
    # gwydion expires, taliesin claims → gwydion=1.
    # gwydion expires again, taliesin claims → gwydion=2.
    # gwydion finally succeeds and releases → gwydion=0.
    store_fast_ttl.claim_channel("discord:1", "gwydion")
    time.sleep(0.2)
    store_fast_ttl.claim_channel("discord:1", "taliesin")  # gwydion=1
    time.sleep(0.2)
    store_fast_ttl.claim_channel("discord:1", "gwydion")   # gwydion reclaims
    time.sleep(0.2)
    store_fast_ttl.claim_channel("discord:1", "taliesin")  # gwydion=2
    time.sleep(0.2)
    store_fast_ttl.claim_channel("discord:1", "gwydion")   # gwydion reclaims
    assert store_fast_ttl._get_timeouts("discord:1", "gwydion") == 2
    store_fast_ttl.release_channel("discord:1", "gwydion", 5, "100")
    assert store_fast_ttl._get_timeouts("discord:1", "gwydion") == 0


# ── cursor tests ─────────────────────────────────────────────────────────


def test_cursor_updated_on_release(store):
    store.claim_channel("discord:1", "taliesin")
    store.release_channel("discord:1", "taliesin", 1, "105")

    cursor = store.get_cursor("discord:1", "taliesin")
    assert cursor == "105"


def test_cursor_none_for_new_channel(store):
    cursor = store.get_cursor("discord:99", "taliesin")
    assert cursor is None
