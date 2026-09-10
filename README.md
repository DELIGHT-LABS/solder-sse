# solder-sse

Resumable Server-Sent Events. A small profile on top of the SSE wire format, and both halves of
it — a server and a client for Rust, a client for the browser — so that a cut stream costs
nothing: the client reconnects at once, the server replays the gap, and when it cannot, it says so.

## The profile

| Rule | What it buys |
| --- | --- |
| Every state change carries a monotonic `id:` | A cursor to resume from |
| `Last-Event-ID` / `?last_event_id=` replays from a bounded log | The server closes the gap, not a poll |
| `resync` when the cursor cannot be served | A silent gap becomes "reload a snapshot" |
| A named `ping` at connect and while quiet | A dead-man timer catches a half-open connection |
| A jittered `retry:` hint | A fleet does not reconnect in lockstep |
| `503 + Retry-After` instead of an empty `200` | The browser stops its own loop; the client backs off |
| Rotation: a healthy stream ends on purpose after `max_age` | Connections turn over on the server's terms, at a frame boundary, announced in `ping` |

`spec/profile-v1.md` is the contract. `spec/vectors/*.sse` are golden frames every implementation
encodes and decodes.

## Packages

| Path | Registry | Role |
| --- | --- | --- |
| `crates/solder-sse` | crates.io `solder-sse` | Server core on `http` / `http-body`: `Event`, `SseResponseBuilder`, `EventLog` (`MemoryLog`), `resumable()`, `Parser`. No web framework |
| `crates/solder-sse-axum` | crates.io `solder-sse-axum` | axum adapter: a `Resume` extractor, an `IntoResponse` newtype |
| `crates/solder-sse-client` | crates.io `solder-sse-client` | Rust client on `reqwest`: resume, retry hints, jittered backoff with `Retry-After` as the floor, dead-man, `resync`, the server's hints |
| `packages/solder-sse` | npm `solder-sse` | Browser client: one shared `EventSource` per URL, watchdog, eager reconnect, dead-man, resume, `resync`, and a link-state judgement for what a screen shows |
| `packages/solder-sse-solid` | npm `solder-sse-solid` | SolidJS 2.0 adapter: owner-bound subscriptions with a reactive status |

## Quick start

Server (axum):

```rust
use solder_sse::{broadcast, resumable, Event, SseResponseBuilder};
use solder_sse_axum::{Resume, Sse};

async fn stream(Resume(resume): Resume, State(app): State<App>) -> Response {
    // Subscribe first (the closure), then read the log: nothing in between is lost.
    let r = resumable(&*app.log, "topic", resume, || broadcast::bridge(app.bus.subscribe()), 500).await;
    Sse(SseResponseBuilder::new()
        .max_age(Duration::from_secs(20)) // rotate on the server's terms, ±10 %
        .resync(r.resync)
        .build(r.stream.map(|p| p.map(|p| Event::named("tick").seq(p.seq).data(p.event)))))
    .into_response()
}
```

Browser:

```ts
import { createSolder } from 'solder-sse';

const solder = createSolder();
solder.subscribe(
	'/topic/stream',
	{
		onEvent: (frame) => console.log(frame.name, frame.json(), frame.lastEventId),
		onLink: (link) => show(link), // 'connecting' | 'live' | 'reconnecting' | 'offline'
		onReconnect: (resumed) => { if (!resumed) refetch(); }, // a resumed reopen was replayed
		onResync: () => refetch() // the server could not replay: reload a snapshot
	},
	{ events: ['tick'] }
);
```

Rust client:

```rust
let mut sub = solder_sse_client::subscribe(reqwest::Client::new(), url, Options::default());
while let Some(msg) = sub.recv().await {
    match msg {
        Message::Event(frame) => { /* frame.name, frame.data, frame.id */ }
        Message::Resync { .. } => reload_snapshot().await,
        Message::Status(_) | Message::Ping => {}
    }
}
```

## Development

```bash
cargo test --workspace        # Rust cores, adapters, the client's end-to-end proofs, the golden vectors
npm --prefix packages test    # browser core, adapter, the same golden vectors
```

APIs may change before 1.0.

## License

MIT or Apache-2.0, at your option — see `LICENSE-MIT` and `LICENSE-APACHE`. Unless you state
otherwise, a contribution you submit is licensed the same way, without additional terms.
