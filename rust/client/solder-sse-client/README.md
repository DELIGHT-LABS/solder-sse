# solder-sse-client

The client half of the resumable SSE profile for Rust programs — canaries, CLIs, services — on
`reqwest`. Because it is fetch-based it can do what a browser `EventSource` cannot: read
`Retry-After`, set headers, and resume with `Last-Event-ID` on every reconnect it makes itself.

| Behaviour | Default |
| --- | --- |
| `Last-Event-ID` on reconnect | on |
| Healthy stream ended → reopen at once | ≥ 5 s live, ≤ 250 ms jitter |
| Stream ended sooner → server `retry:` hint | 3 s when absent |
| Transport error / non-200 → full-jitter backoff | 1 s → 30 s, floor 200 ms; `Retry-After` is the floor |
| Dead-man after a `ping` has been seen | `Deadman::Auto` (default): 35 s, or `2 × every + 5 s` once the `ping` announces its interval; `Fixed(d)`; `Off` |
| `resync` | surfaced as `Message::Resync`; the cursor is dropped |
| Server rotation (`ping` announces `max_age`) | kept in `Subscription::server_hints()`; the end itself is the healthy-stream reopen above, `Cause::Ended` |

```rust
let mut sub = subscribe(reqwest::Client::new(), url, Options::default());
while let Some(msg) = sub.recv().await {
    match msg {
        Message::Event(frame) => println!("{:?} {} {:?}", frame.name, frame.data, frame.id),
        Message::Resync { .. } => reload_snapshot().await,
        Message::Ping | Message::Status(_) => {}
    }
}
```

`tests/resume.rs` is the existence proof: a server that ends every stream after five events,
rotates it on a timer, or resets the connection outright (a proxy timeout in miniature), a producer
that never pauses — and the client still sees every sequence exactly once; a server restart is
one `resync`, not a silent gap. License: MIT.
