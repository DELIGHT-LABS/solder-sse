# solder-sse

Resumable Server-Sent Events for Rust, on `http` / `http-body` — no web framework in the core.
Adapters are a newtype each: `solder-sse-axum` today. A Rust client with the same profile lives in
`solder-sse-client`; the browser package `solder-sse` speaks it from the other side.

A stream can be cut at any moment. With this crate a cut costs nothing:

| Profile point | Where |
| --- | --- |
| Monotonic `id:` on every transition | `Event::seq`, assigned by `EventLog::append` |
| `Last-Event-ID` / `?last_event_id=` replay of `seq > cursor` | `Resume::from_parts`, `resumable()` |
| `resync` when the cursor cannot be served (empty `id:` resets the browser) | `Resumed::resync`, `Event::resync` |
| `ping` at connect and every 15 s; jittered `retry:` | `SseResponseBuilder` |
| `503 + Retry-After` instead of an empty 200 stream | `unavailable()` |
| Rotation: end a healthy stream after `max_age` (±10 %, at a frame boundary), announced in `ping` | `SseResponseBuilder::max_age`, `max_age_jitter` |
| Bounded replay buffer | `MemoryLog` (per topic, count + TTL); implement `EventLog` for SQL / Redis |
| A log write failed after the state changed | `EventLog::mark_gap` — every earlier cursor answers `resync`; the publisher broadcasts the event unsequenced (no `id:`) so live viewers stay current and nobody misses it silently |
| Broadcast fan-out that does not drop `Lagged` on the floor | `broadcast::bridge` |
| A spec-faithful parser (clients, tests) | `Parser` |

```rust
let resume = Resume::from_parts(&parts);
// resumable() calls the closure FIRST (subscribe), then reads the log.
let r = resumable(&*log, "topic", resume, || broadcast::bridge(bus.subscribe()), 500).await;
let response = SseResponseBuilder::new()
    .resync(r.resync)
    .build(r.stream.map(|p| p.map(|p| Event::named("tick").seq(p.seq).data(p.event))))
    .into_http();                                           // http::Response<SseBody>
```

The wire contract is `spec/profile-v1.md`; `spec/vectors/*.sse` are golden frames shared with the
browser package. License: MIT.
