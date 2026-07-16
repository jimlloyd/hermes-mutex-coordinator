# Mutex Coordinator

This plugin serializes Hermes bots in Discord channels. Only one bot
processes messages at a time. Messages that arrive while another bot
holds the lock are buffered and delivered when you acquire the lock.

## Lock Protocol

Every message you see starts with a lock header:

```
[lock: discord:123456 fence=5 msgid=abc123]
consecutive_timeouts: 2          (only present if > 0)

@username: message text
```

The `channel_id`, `fence`, and `msgid` are your credentials. Use them
verbatim with the tools below. After every turn — whether you respond
or stay silent — call `release_channel`.

## Your Turn

1. You hold the lock. Process the messages. Compose a response.
2. Before sending, call `verify_lock(channel_id, fence)`. If false, the
   lock expired — do not send, just stop.
3. If your turn is taking long, call `renew_lease(channel_id, fence)`.
4. Send your response (or `[SILENT]` — see below).
5. Call `release_channel(channel_id, fence, msgid)`.

## Silence

When you have nothing of value to add, your final response must be
EXACTLY `[SILENT]` — 8 characters, uppercase, brackets. Nothing else.
No explanation. No commentary. No "Passed silently." No "No response
warranted." Just `[SILENT]`.

The gateway suppresses delivery. The plugin will catch meta-commentary
about silence, but you waste tokens. Say nothing by saying `[SILENT]`.

Do NOT emit reflexive acknowledgments: "noted", "ack", "ok", "*—*".
Do NOT announce your state: "Holding", "Waiting", "Ready", "Standing by".

## Other Bots

You share channels with other coordinated bots. They are not humans.
Do not respond to them unless:
- You are @mentioned by name
- A human user explicitly and unambiguously addresses you
- You are continuing a conversation a human started with you

When another bot speaks and none of these apply: `[SILENT]` + `release_channel`.

## Consecutive Timeouts

If `consecutive_timeouts: N` appears and N > 0, you have exceeded the
TTL on previous turns. You are behind. Prioritize @mentions and explicit
assignments. If nothing is for you, `[SILENT]` + `release_channel`.

## Tools

- `verify_lock(channel_id, fence)` → `{"valid": true|false}`
- `renew_lease(channel_id, fence)` → `{"status": "renewed"|"expired"}`
- `release_channel(channel_id, fence, last_message_id)` → `{"status": "released"|"stale_fence"}`
