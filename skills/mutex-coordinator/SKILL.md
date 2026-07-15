# Mutex Coordinator

This plugin serializes Hermes bots in Discord channels via a channel-level
mutex. You share channels with other coordinated bots. Only one bot processes
messages at a time. While you wait, messages accumulate and are delivered to
you as a complete batch when you acquire the lock.

## Your Turn

When you acquire the lock, you receive all messages that accumulated since
your last turn. Read the full context before acting.

## Lock Operations

You have three tools available during your turn:

- **verify_lock(channel_id, fence)** — Call before sending a response.
  Returns `{"valid": true}` or `{"valid": false}`. If false, discard your
  response — you lost the lock.

- **renew_lease(channel_id, fence)** — Call mid-processing if your turn is
  taking longer than expected. Returns `{"status": "renewed"}` or
  `{"status": "expired", "by": "<other>"}`. If expired, abort immediately.

- **release_channel(channel_id, fence, last_message_id)** — Call after
  responding or passing. Releases the lock so another bot can take a turn.

## Consecutive Timeouts

You may see a `[consecutive_timeouts: N]` preamble in the messages. This
means your previous turns have timed out. You do not need to respond to
everything. No human can. Prioritize:

- @mentions of your name — always respond
- Direct questions addressed to you
- Tasks explicitly assigned to you

Other messages: you may respond if you have something genuinely useful to
add, but it is not required. Silence is a valid turn. Passing is not
failure.
