"""SQLite-backed channel lock store."""

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

GRACE_PERIOD_MS = 10_000
RELEASE_MAX_RETRIES = 3


class LockStore:
    def __init__(self, db_path: Path, ttl_ms: int = 60_000) -> None:
        self.db_path = db_path
        self.ttl_ms = ttl_ms
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()
        self._load_ttl()

    def _init_db(self) -> None:
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("""CREATE TABLE IF NOT EXISTS channel_locks (
            channel_id TEXT PRIMARY KEY, claimant TEXT NOT NULL,
            fence INTEGER NOT NULL, claimed_at INTEGER NOT NULL, expires_at INTEGER NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS channel_cursors (
            channel_id TEXT NOT NULL, profile_name TEXT NOT NULL,
            last_message_id TEXT, consecutive_timeouts INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (channel_id, profile_name))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS mutex_config (
            key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
        conn.execute("INSERT OR IGNORE INTO mutex_config (key, value) VALUES ('ttl_ms', ?)",
                     (str(self.ttl_ms),))
        conn.commit()
        self._conn = conn

    def _load_ttl(self) -> None:
        row = self._conn.execute(
            "SELECT value FROM mutex_config WHERE key = 'ttl_ms'").fetchone()
        if row:
            self.ttl_ms = int(row[0])

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    # ── claim_channel ────────────────────────────────────────────────────

    def claim_channel(self, channel_id: str, claimant: str) -> Dict[str, Any]:
        now = self._now_ms()
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            cursor = self._conn.execute(
                """INSERT INTO channel_locks (channel_id, claimant, fence, claimed_at, expires_at)
                   VALUES (?, ?, 1, ?, ?)
                   ON CONFLICT(channel_id) DO NOTHING""",
                (channel_id, claimant, now, now + self.ttl_ms))

            if cursor.rowcount == 1:
                return {"status": "acquired", "fence": 1,
                        "consecutive_timeouts": self._get_timeouts(channel_id, claimant)}

            # Row exists — read it
            row = self._conn.execute(
                "SELECT claimant, fence, expires_at FROM channel_locks WHERE channel_id = ?",
                (channel_id,)).fetchone()
            current_claimant, fence, expires_at = row

            if expires_at <= now:
                self._conn.execute(
                    "UPDATE channel_locks SET claimant = ?, fence = fence + 1, "
                    "claimed_at = ?, expires_at = ? WHERE channel_id = ?",
                    (claimant, now, now + self.ttl_ms, channel_id))
                self._increment_timeouts(channel_id, current_claimant)
                return {"status": "acquired", "fence": fence + 1,
                        "consecutive_timeouts": self._get_timeouts(channel_id, claimant)}

            if current_claimant == claimant:
                self._conn.execute(
                    "UPDATE channel_locks SET expires_at = ? WHERE channel_id = ?",
                    (now + self.ttl_ms, channel_id))
                return {"status": "acquired", "fence": fence,
                        "consecutive_timeouts": self._get_timeouts(channel_id, claimant)}

            return {"status": "locked", "by": current_claimant}

    # ── verify_lock ──────────────────────────────────────────────────────

    def verify_lock(self, channel_id: str, claimant: str, fence: int) -> bool:
        row = self._conn.execute(
            "SELECT claimant, fence, expires_at FROM channel_locks WHERE channel_id = ?",
            (channel_id,)).fetchone()
        if row is None:
            return False
        return (row[0] == claimant and row[1] == fence
                and row[2] + GRACE_PERIOD_MS > self._now_ms())

    # ── renew_lease ──────────────────────────────────────────────────────

    def renew_lease(self, channel_id: str, claimant: str, fence: int) -> Dict[str, Any]:
        now = self._now_ms()
        with self._conn:
            cursor = self._conn.execute(
                "UPDATE channel_locks SET expires_at = ? "
                "WHERE channel_id = ? AND claimant = ? AND fence = ?",
                (now + self.ttl_ms, channel_id, claimant, fence))
            if cursor.rowcount == 0:
                row = self._conn.execute(
                    "SELECT claimant FROM channel_locks WHERE channel_id = ?",
                    (channel_id,)).fetchone()
                return {"status": "expired", "by": row[0] if row else "unknown"}
            return {"status": "renewed"}

    # ── release_channel ──────────────────────────────────────────────────

    def release_channel(
        self, channel_id: str, claimant: str, fence: int, last_message_id: str
    ) -> Dict[str, Any]:
        for attempt in range(RELEASE_MAX_RETRIES):
            try:
                with self._conn:
                    cursor = self._conn.execute(
                        "DELETE FROM channel_locks WHERE channel_id = ? "
                        "AND claimant = ? AND fence = ?",
                        (channel_id, claimant, fence))
                    if cursor.rowcount == 0:
                        return {"status": "stale_fence"}

                    self._conn.execute(
                        """INSERT INTO channel_cursors (channel_id, profile_name,
                           last_message_id, consecutive_timeouts, updated_at)
                           VALUES (?, ?, ?, 0, ?)
                           ON CONFLICT(channel_id, profile_name) DO UPDATE
                           SET last_message_id = excluded.last_message_id,
                               consecutive_timeouts = 0,
                               updated_at = excluded.updated_at""",
                        (channel_id, claimant, last_message_id, self._now_ms()))
                    return {"status": "released"}

            except sqlite3.OperationalError:
                if attempt == RELEASE_MAX_RETRIES - 1:
                    logger.exception(
                        "release_failed channel=%s claimant=%s fence=%s",
                        channel_id, claimant, fence)
                    raise
                time.sleep(0.1 * (2 ** attempt))

        return {"status": "release_error"}

    # ── cursor helpers ───────────────────────────────────────────────────

    def _get_timeouts(self, channel_id: str, profile: str) -> int:
        row = self._conn.execute(
            "SELECT consecutive_timeouts FROM channel_cursors "
            "WHERE channel_id = ? AND profile_name = ?",
            (channel_id, profile)).fetchone()
        return row[0] if row else 0

    def _increment_timeouts(self, channel_id: str, profile: str) -> None:
        self._conn.execute(
            """INSERT INTO channel_cursors (channel_id, profile_name,
               last_message_id, consecutive_timeouts, updated_at)
               VALUES (?, ?, NULL, 1, ?)
               ON CONFLICT(channel_id, profile_name) DO UPDATE
               SET consecutive_timeouts = consecutive_timeouts + 1,
                   updated_at = excluded.updated_at""",
            (channel_id, profile, self._now_ms()))

    def get_cursor(self, channel_id: str, profile: str) -> Optional[str]:
        row = self._conn.execute(
            "SELECT last_message_id FROM channel_cursors "
            "WHERE channel_id = ? AND profile_name = ?",
            (channel_id, profile)).fetchone()
        return row[0] if row and row[0] else None
