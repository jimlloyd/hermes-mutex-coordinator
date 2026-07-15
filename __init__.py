"""Mutex Coordinator plugin for Hermes Agent."""

import asyncio
import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

GRACE_PERIOD_MS = 10_000
MAX_BUFFER_SIZE = 500
RELEASE_MAX_RETRIES = 3

# ── module-level state ───────────────────────────────────────────────────────

_lock_store = None
_buffer = None
_buffer_lock = None
_profile_name = None
_last_message_id = None

# ── tool schemas ─────────────────────────────────────────────────────────────

VERIFY_LOCK_SCHEMA = {
    "name": "verify_lock",
    "description": "Check whether we still hold the channel lock before sending a response.",
    "parameters": {
        "type": "object",
        "properties": {
            "channel_id": {"type": "string", "description": "Channel ID with platform prefix, e.g. discord:123"},
            "fence": {"type": "integer", "description": "Fence token from the claim response"},
        },
        "required": ["channel_id", "fence"],
    },
}

RENEW_LEASE_SCHEMA = {
    "name": "renew_lease",
    "description": "Extend our channel lock TTL mid-processing. Call when the turn is taking longer than expected.",
    "parameters": {
        "type": "object",
        "properties": {
            "channel_id": {"type": "string", "description": "Channel ID with platform prefix"},
            "fence": {"type": "integer", "description": "Fence token from the claim response"},
        },
        "required": ["channel_id", "fence"],
    },
}

RELEASE_CHANNEL_SCHEMA = {
    "name": "release_channel",
    "description": "Release the channel lock after responding or passing.",
    "parameters": {
        "type": "object",
        "properties": {
            "channel_id": {"type": "string", "description": "Channel ID with platform prefix"},
            "fence": {"type": "integer", "description": "Fence token from the claim response"},
            "last_message_id": {"type": "string", "description": "Discord snowflake ID of last message processed"},
        },
        "required": ["channel_id", "fence", "last_message_id"],
    },
}

# ── LockStore ────────────────────────────────────────────────────────────────

class LockStore:
    """SQLite-backed channel lock store."""

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

    def verify_lock(self, channel_id: str, claimant: str, fence: int) -> bool:
        row = self._conn.execute(
            "SELECT claimant, fence, expires_at FROM channel_locks WHERE channel_id = ?",
            (channel_id,)).fetchone()
        if row is None:
            return False
        return (row[0] == claimant and row[1] == fence
                and row[2] + GRACE_PERIOD_MS > self._now_ms())

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


# ── MessageBuffer ────────────────────────────────────────────────────────────

class MessageBuffer:
    """Per-channel message buffer with overflow protection."""

    def __init__(self) -> None:
        self._buffers: Dict[str, List[dict]] = {}

    def append(self, channel_id: str, event: dict) -> None:
        if channel_id not in self._buffers:
            self._buffers[channel_id] = []
        buf = self._buffers[channel_id]
        if len(buf) >= MAX_BUFFER_SIZE:
            discarded = buf.pop(0)
            logger.warning("buffer_overflow channel=%s discarded=%s",
                           channel_id, discarded.get("message_id", "unknown"))
        buf.append(event)

    def flush(self, channel_id: str) -> str:
        buf = self._buffers.pop(channel_id, [])
        if not buf:
            return ""
        messages = [
            f"@{e.get('user_name', e.get('user_id', 'unknown'))}: {e.get('text', '')}"
            for e in buf
        ]
        return "[Recent channel messages]\n" + "\n".join(messages) + "\n\n"


# ── registration ─────────────────────────────────────────────────────────────

def register(ctx):
    global _lock_store, _buffer, _buffer_lock, _profile_name

    _profile_name = ctx.profile_name

    try:
        from hermes_constants import get_default_hermes_root
        db_path = get_default_hermes_root() / "coordination.db"
    except ImportError:
        db_path = Path.home() / ".hermes" / "coordination.db"

    _lock_store = LockStore(db_path)
    _buffer = MessageBuffer()
    _buffer_lock = asyncio.Lock()

    ctx.register_hook("pre_gateway_dispatch", on_pre_gateway_dispatch)
    ctx.register_tool(name="verify_lock", toolset="mutex-coordinator",
                      schema=VERIFY_LOCK_SCHEMA, handler=verify_lock_tool)
    ctx.register_tool(name="renew_lease", toolset="mutex-coordinator",
                      schema=RENEW_LEASE_SCHEMA, handler=renew_lease_tool)
    ctx.register_tool(name="release_channel", toolset="mutex-coordinator",
                      schema=RELEASE_CHANNEL_SCHEMA, handler=release_channel_tool)

    skill_path = Path(__file__).parent / "skills" / "mutex-coordinator" / "SKILL.md"
    ctx.register_skill("mutex-coordinator", skill_path)

    logger.info("mutex-coordinator registered profile=%s db=%s", _profile_name, db_path)


# ── hook handler ─────────────────────────────────────────────────────────────

async def on_pre_gateway_dispatch(event, gateway, session_store, **kwargs):
    global _last_message_id

    channel_id = f"discord:{event.source.chat_id}"
    _last_message_id = event.message_id

    result = _lock_store.claim_channel(channel_id, _profile_name)

    if result["status"] == "acquired":
        async with _buffer_lock:
            buf = _buffer.flush(channel_id)

        timeouts = result.get("consecutive_timeouts", 0)
        preamble = f"[consecutive_timeouts: {timeouts}]\n\n" if timeouts > 0 else ""

        if buf:
            text = f"{preamble}{buf}[New message]\n{event.user_name or event.user_id}: {event.text}"
        else:
            text = f"{preamble}@{event.user_name or event.user_id}: {event.text}"

        logger.info("lock_acquired channel=%s claimant=%s fence=%d timeouts=%d",
                     channel_id, _profile_name, result["fence"], timeouts)
        return {"action": "rewrite", "text": text}

    elif result["status"] == "locked":
        async with _buffer_lock:
            _buffer.append(channel_id, {
                "user_name": event.user_name, "user_id": event.user_id,
                "text": event.text, "message_id": event.message_id,
            })
        return {"action": "skip"}

    return {"action": "allow"}


# ── tool handlers ────────────────────────────────────────────────────────────

def verify_lock_tool(args, **kwargs):
    ok = _lock_store.verify_lock(args["channel_id"], _profile_name, args["fence"])
    return json.dumps({"valid": ok})


def renew_lease_tool(args, **kwargs):
    result = _lock_store.renew_lease(args["channel_id"], _profile_name, args["fence"])
    if result["status"] == "expired":
        logger.warning("renew_stolen channel=%s by=%s", args["channel_id"], result.get("by"))
    return json.dumps(result)


def release_channel_tool(args, **kwargs):
    result = _lock_store.release_channel(
        args["channel_id"], _profile_name, args["fence"], args["last_message_id"])
    if result["status"] == "released":
        logger.info("lock_released channel=%s cursor=%s",
                     args["channel_id"], args["last_message_id"])
    return json.dumps(result)
