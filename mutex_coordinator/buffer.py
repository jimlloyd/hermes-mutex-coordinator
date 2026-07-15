"""In-memory message buffer for the mutex-coordinator plugin."""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

MAX_BUFFER_SIZE = 500


class MessageBuffer:
    def __init__(self) -> None:
        self._buffers: Dict[str, List[dict]] = {}

    def append(self, channel_id: str, event: dict) -> None:
        if channel_id not in self._buffers:
            self._buffers[channel_id] = []
        buf = self._buffers[channel_id]
        if len(buf) >= MAX_BUFFER_SIZE:
            discarded = buf.pop(0)
            logger.warning("buffer_overflow channel=%s discarded=%s", channel_id, discarded.get("message_id", "unknown"))
        buf.append(event)

    def flush(self, channel_id: str) -> str:
        buf = self._buffers.pop(channel_id, [])
        if not buf:
            return ""
        messages = [f"@{e.get('user_name', e.get('user_id', 'unknown'))}: {e.get('text', '')}" for e in buf]
        return "[Recent channel messages]\n" + "\n".join(messages) + "\n\n"

    def __len__(self) -> int:
        return sum(len(buf) for buf in self._buffers.values())
