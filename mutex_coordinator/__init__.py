"""Mutex Coordinator plugin for Hermes Agent."""

import asyncio
import json
import logging
from pathlib import Path

from .buffer import MessageBuffer
from .lock_store import LockStore

logger = logging.getLogger(__name__)

_lock_store = None
_buffer = None
_buffer_lock = None
_profile_name = None
_last_message_id = None

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
    ctx.register_tool(name="verify_lock", toolset="mutex-coordinator", schema=VERIFY_LOCK_SCHEMA, handler=verify_lock_tool)
    ctx.register_tool(name="renew_lease", toolset="mutex-coordinator", schema=RENEW_LEASE_SCHEMA, handler=renew_lease_tool)
    ctx.register_tool(name="release_channel", toolset="mutex-coordinator", schema=RELEASE_CHANNEL_SCHEMA, handler=release_channel_tool)

    skill_path = Path(__file__).parent.parent / "skills" / "mutex-coordinator" / "SKILL.md"
    ctx.register_skill("mutex-coordinator", skill_path)

    logger.info("mutex-coordinator registered profile=%s db=%s", _profile_name, db_path)


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


def verify_lock_tool(args, **kwargs):
    ok = _lock_store.verify_lock(args["channel_id"], _profile_name, args["fence"])
    return json.dumps({"valid": ok})


def renew_lease_tool(args, **kwargs):
    result = _lock_store.renew_lease(args["channel_id"], _profile_name, args["fence"])
    if result["status"] == "expired":
        logger.warning("renew_stolen channel=%s by=%s", args["channel_id"], result.get("by"))
    return json.dumps(result)


def release_channel_tool(args, **kwargs):
    result = _lock_store.release_channel(args["channel_id"], _profile_name, args["fence"], args["last_message_id"])
    if result["status"] == "released":
        logger.info("lock_released channel=%s cursor=%s", args["channel_id"], args["last_message_id"])
    return json.dumps(result)
