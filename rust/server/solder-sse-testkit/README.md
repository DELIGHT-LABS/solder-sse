# solder-sse-testkit

A server that ends streams every way an intermediary can, so a client — in any language — can
prove the profile against it. One topic, numbered events, and a way of ending each connection:

| `Cut` | Looks like |
| --- | --- |
| `After(n)` | A clean end after `n` events |
| `Abort(n)` | A reset with no terminating chunk — a proxy timeout, a dropped network |
| `Rotate(age)` | The server's own rotation (`max_age`) |
| `Never` | Nothing, until the client leaves |

A restart (`restart_on_connection(n)`, or `POST /restart`) swaps the log for a fresh one of a new
generation: every old cursor is unknown, one `resync` follows, and no new cursor can be mistaken
for an old one. `GET /down` answers `503 + Retry-After`.

As a library, in a test:

```rust
let chaos = Chaos::new(Cut::Rotate(Duration::from_secs(1)));
let served = chaos.clone().serve().await;           // http://127.0.0.1:<port>
chaos.publish(1).await;                             // logged under a cursor, replayed on resume
let mut sub = subscribe(reqwest::Client::new(), format!("{}/stream", served.base_url), Options::default());
```

As a binary, for a browser or a foreign client:

```bash
solder-sse-testkit --port 8090 --cut rotate:8000 --produce-every 400
# GET  /stream      the stream (id: <cursor>, event: tick, data: <the number>)
# GET  /down        503 + Retry-After
# POST /publish     the event as decimal text; answers its cursor
# POST /restart     the server forgets its log
```

License: MIT or Apache-2.0, at your option.
