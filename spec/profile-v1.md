# Resumable SSE profile, version 1

A small profile on top of the WHATWG Server-Sent Events wire format so that a
cut stream costs nothing: the client reconnects at once, the server replays
what was missed, and when it cannot, it says so. Every implementation in this
repository speaks it; the golden frames in `vectors/` are shared by all of
them, and `profile.json` carries the constants for tooling.

Key words MUST, SHOULD and MAY are to be read as in RFC 2119.

## 1. Cursor

- Every event that changes state MUST carry `id: <cursor>`. A cursor is a token
  the server's log issues; it is **opaque to the client**, which never orders,
  compares or parses cursors — it remembers the last one and hands it back.
- On the wire a cursor is 1 to 128 bytes of the URL-unreserved alphabet
  (`A–Z a–z 0–9 - . _ ~`), so it travels in a header, a query string and a JSON
  string without encoding. Within that alphabet its shape is the log's
  business: a sequence number, `<generation>-<sequence>`, a stream id, a hex
  digest. A server MUST be able to tell a cursor it issued from any other
  token — across restarts, deployments and replicas.
- A frame that does not change state (a connect snapshot, `ping`) MUST NOT
  carry an `id:` line, so it never moves the client's cursor.
- A client MUST remember the last `id` it saw and present it on reconnect as
  the `Last-Event-ID` header (browsers do this by themselves) or, when it
  cannot set headers, as the `last_event_id` query parameter. When both are
  present the header wins. An empty value counts as absent.

## 2. Connect

On every connection the server sends, in this order:

1. `retry: <ms>` — a hint for the client's own retry, drawn per connection from
   a window (recommended 500–1000 ms) so a fleet does not reconnect in lockstep.
2. One keep-alive frame (`event: ping`, §4), so the client learns at once that
   this server sends pings and can arm its dead-man timer.
3. Then, depending on the cursor:
   - no cursor → the connect snapshot (implementation-defined), then live events;
   - a cursor the server can serve → every retained event **after** it, in
     order, then live events, with no overlap and **no snapshot**;
   - a cursor the server cannot serve → `event: resync` (§3), then the connect
     snapshot, then live events.

A server MUST NOT resend the event the cursor names, and MUST NOT deliver an
event twice across the replay/live boundary.

## 3. Resync

```
id:
event: resync
data: {"reason":"expired","earliest":"7f3a9c2e-47900"}
```

- `reason` is `expired` (the cursor was issued by this server but can no longer
  be replayed from: older than what it retains, a gap wider than its replay
  limit, or a write it lost since) or `unknown` (the cursor was never issued by
  this server — a restart, another deployment, or a value that is not a cursor
  at all).
- `earliest` is the oldest cursor the server could still have replayed from on
  this stream, or `null` when it has none to name. It is information for a log
  line; a client cannot act on it.
- The frame MUST carry an **empty** `id:` line: the specification resets the
  browser's last event id on an empty id, so its own next retry does not resend
  the rejected cursor.
- A client receiving `resync` MUST drop its cursor and SHOULD reload a snapshot
  through whatever non-stream path it has (a poll).

## 4. Keep-alive and dead-man

- The server MUST send `event: ping` once at connect and whenever the stream has
  been quiet for the keep-alive interval (recommended 15 s — under every common
  proxy idle timeout).
- The `ping` payload carries the keep-alive interval in seconds,
  `{"every":15}`, and MAY carry `max_age` in whole seconds,
  `{"every":15,"max_age":30}`: the lifetime after which this server ends a
  healthy stream on purpose (§8). Unknown fields MUST be ignored; an absent
  `every` means the recommended interval.
- A client that has seen a `ping` **on the current connection** SHOULD treat
  silence longer than `2 × every + 5 s` (35 s at the recommended interval) as a
  half-open connection and reopen. A client that has not seen a `ping` on the
  connection MUST NOT apply the dead-man rule (a plain SSE server sends no
  pings), and a `ping` on an earlier connection proves nothing about this one.
- A client MAY use `max_age` to expect the end — a canary can check the
  cadence, a console can say so — but MUST NOT change its reconnect policy for
  it: §9 already reopens a cut healthy stream at once.
- Comment lines (`: …`) MAY be sent but are invisible to `EventSource` and do
  not count as keep-alive for this profile.

## 5. Failure to open

When the stream cannot be opened (the upstream is down), the server MUST answer
with a non-200 status — `503` with `Retry-After: <seconds>` recommended — rather
than an empty `200` stream. A non-200 makes the browser stop its own retry
loop; the client's watchdog takes over with jittered exponential backoff
(recommended 1 s → 30 s, full jitter, a floor around 200 ms).

## 6. Frames and headers

- Response headers: `Content-Type: text/event-stream; charset=utf-8`,
  `Cache-Control: no-cache, no-transform`, `X-Accel-Buffering: no`.
- `id:` and `event:` are single-line fields; an encoder MUST NOT let a line
  break inside a value forge a second field.
- `data` MAY contain any text. The encoder emits one `data:` line per line,
  treating LF, CR LF and a bare CR alike as line breaks — the three the
  parser side of the specification recognises — so a payload's CR cannot end a
  line early and pose as another field. A client joins the lines with LF, as
  the specification says; a payload's CR LF and bare CR therefore arrive as
  LF.

## 7. Retention

The server keeps a bounded log per topic (recommended: at least the client's
maximum backoff several times over — e.g. 10 minutes or 2 000 events). A
cursor below the retained window answers `resync(expired)`; a cursor the
server never issued answers `resync(unknown)`. A server that loses a write
after the state changed MUST make every cursor issued up to then answer
`resync(expired)`, so no client silently misses what was lost.

## 8. Rotation

A server MAY end a healthy stream after a lifetime (`max_age`) so that
connections rotate: across replicas after a deploy or a scale-out, ahead of a
load balancer's response timeout, and within a graceful shutdown. The end is an
ordinary end of the response — at a frame boundary, after every connect frame
of §2, never inside a frame — and not an error. A server that rotates:

- SHOULD draw the lifetime per connection from a window around the nominal
  value (±10 % recommended), so a fleet does not reconnect in lockstep;
- SHOULD keep it below any intermediary's response timeout, so the server —
  not the proxy — ends the stream, cleanly and at a frame boundary;
- MUST retain (§7) at least one lifetime plus the client's reconnect, so every
  rotation resumes without `resync`;
- SHOULD announce the nominal lifetime in `ping` (§4, `max_age`).

A client treats the end as §9 says: a stream live for at least 5 s reopens
within 250 ms with its cursor; one that ended sooner follows the `retry:`
hint. Whether a rotation is visible to people is the client's link judgement
(a grace before a drop is reported), not the server's concern.

## 9. Client reconnect policy (recommended)

| Situation | Action |
| --- | --- |
| Stream ended after ≥ 5 s live | reopen within ≤ 250 ms (jittered); do not wait for the native retry |
| Stream ended sooner | the native retry (`retry:` hint) |
| Non-200 / transport error | full-jitter exponential backoff; `Retry-After` is the floor when readable |
| Silence past the dead-man window (after a `ping` on this connection) | reopen at once; while the page is hidden, judge again when visible |
| Network or page returns (`online`, `visibilitychange`) | collapse a pending backoff into an immediate retry |
| `resync` | drop the cursor; reload a snapshot |

A reconnect that carried a cursor is replayed by the server and costs the
consumer nothing; one that carried none (a first connection, the reopen after
a `resync`) is the consumer's cue to reload through its non-stream path.

## 10. Golden vectors

`vectors/*.sse` are exact wire bytes. An implementation's encoder MUST produce
them from the described events, and its parser MUST decode them to the
described events. See `rust/core/solder-sse/tests/vectors.rs` (Rust) and
`js/client/solder-sse/src/profile.test.ts` (browser) for the expectations.
