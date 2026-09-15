# solder-sse-axum

axum adapter for `solder-sse-server`. Two newtypes, because Rust's orphan rule forbids
implementing axum's traits for another crate's types:

- `Resume(pub solder_sse_server::Resume)` — an extractor reading `Last-Event-ID` or `?last_event_id=`.
- `Sse(pub solder_sse_server::SseResponse<S>)` — `IntoResponse`.
- `unavailable(retry_after)` — the `503 + Retry-After` response.

```rust
async fn stream(Resume(resume): Resume, State(app): State<App>) -> Response {
    let r = resumable(&*app.log, "topic", resume, || broadcast::bridge(app.bus.subscribe()), 500).await;
    Sse(SseResponseBuilder::new().resync(r.resync).build(r.stream.map(to_event))).into_response()
}
```

Tracks axum 0.8 (`axum-core` 0.5). License: MIT or Apache-2.0, at your option.
