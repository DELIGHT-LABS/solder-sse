# solder-sse-server

Resumable Server-Sent Events for Rust servers, on `http` / `http-body` — no web framework in the
core. Adapters are a newtype each: `solder-sse-axum` today. The wire it speaks is `solder-sse`;
the client half for Rust programs is `solder-sse-client`.

A stream can be cut at any moment. With this crate a cut costs nothing:

| Profile point | Where |
| --- | --- |
| An `id:` on every transition — a `Cursor` the log issues, opaque to the client | `EventLog::append`, `Event::cursor` |
| `Last-Event-ID` / `?last_event_id=` replay of everything after the cursor, no overlap with live | `Resume::from_parts`, `resumable()` |
| `resync` when the cursor cannot be served (empty `id:` resets the browser) | `Resumed::resync`, `Event::resync` |
| `ping` at connect and every 15 s; jittered `retry:` | `SseResponseBuilder` |
| Rotation: end a healthy stream after `max_age` (±10 %, at a frame boundary), announced in `ping` | `SseResponseBuilder::max_age`, `max_age_jitter` |
| `503 + Retry-After` instead of an empty 200 stream | `unavailable()` |
| Bounded replay buffer | `MemoryLog` (per topic, count + TTL; cursors are `<generation>-<sequence>`, so a restart answers `resync` and never a stale replay); implement `EventLog` for SQL / Redis with cursors of its own |
| A log write failed after the state changed | `EventLog::mark_gap` — every earlier cursor answers `resync`; the publisher broadcasts the event unsequenced (no `id:`) so live viewers stay current and nobody misses it silently |
| Broadcast fan-out that does not drop `Lagged` on the floor | `broadcast::bridge` |

```rust
let resume = Resume::from_parts(&parts);
// resumable() calls the closure FIRST (subscribe), then reads the log.
let r = resumable(&*log, "topic", resume, || broadcast::bridge(bus.subscribe()), 500).await;
let response = SseResponseBuilder::new()
    .max_age(Duration::from_secs(20)) // rotate on the server's terms
    .resync(r.resync)
    .build(r.stream.map(|p| p.map(|p| Event::named("tick").cursor(p.cursor).data(p.event))))
    .into_http();                                           // http::Response<SseBody>
```

The contract is `spec/profile-v1.md` at the repository root. License: MIT or Apache-2.0, at your
option.
