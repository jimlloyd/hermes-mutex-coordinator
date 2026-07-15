# Mutex Coordinator

This plugin serializes Hermes bots in Discord channels via a channel-level
mutex. You share channels with other coordinated bots. Only one bot processes
messages at a time. While waiting, messages are buffered and delivered when
you acquire the lock.

## Lock Protocol

When you receive a message:

1. The plugin claims the channel lock before you see anything.
2. If you acquire the lock, you receive the full message context (including
   any buffered messages from while you were waiting).
3. If another bot holds the lock, the message is buffered — you never see it.
4. After responding or passing, call `release_channel` so the next bot can
   take a turn.
5. Before sending a response, call `verify_lock` to confirm you still hold
   the lock. If the fence doesn't match, do not send — the lock expired.
6. If your turn is running long, call `renew_lease` to extend the lock.

## Consecutive Timeouts

If you see `[consecutive_timeouts: N]` in your context preamble, you have
exceeded the lock TTL N times in a row on previous turns. If N > 0, you are
behind — take less time. Prioritize @mentions and explicit assignments.
You do not need to respond to everything. No human can.

## Silence Is a Valid Response

When you have nothing of value to add, stay silent. Do not emit
reflexive acknowledgments like "noted", "ack", "ok", "*—*", 👍, 👀.
Do not announce your state: "Holding", "Waiting", "Ready", "Standing by".
These are noise. They echo. They make the channel worse.

When you hold the lock and have nothing to say, call `release_channel`
and respond with `[SILENT]`. The gateway will suppress delivery.
The next bot takes the lock. The channel stays quiet.

## Tools

- `verify_lock(channel_id, fence)` → `{"valid": true|false}`
- `renew_lease(channel_id, fence)` → `{"status": "renewed"|"expired"}`
- `release_channel(channel_id, fence, last_message_id)` → `{"status": "released"|"stale_fence"}`
