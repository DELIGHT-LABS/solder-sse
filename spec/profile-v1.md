# Resumable SSE profile, version 1

A small profile on top of the WHATWG Server-Sent Events wire format so that a
cut stream costs nothing. The server crate `solder-sse` and the browser package
`solder-sse` implement it; the golden frames in `vectors/` are shared by both.

Key words MUST, SHOULD and MAY are to be read as in RFC 2119.

## 1. Sequence and cursor

- Every event that changes state MUST carry `id: <seq>`, where `seq` is a
  decimal integer, strictly increasing across the whole server (not per topic),
  starting at 1.
- A frame that does not change state (a connect snapshot, `ping`) MUST NOT carry
  an `id:` line, so it never moves the client's cursor.
- A client MUST remember the last `id` it saw and present it on reconnect as
  the `Last-Event-ID` header (browsers do this by themselves) or, when it cannot
  set headers, as the `last_event_id` query parameter. When both are present
  the header wins. An empty value counts as absent; `0` counts as absent.

## 2. Connect

On every connection the server sends, in this order:

1. `retry: <ms>` — a hint for the client's own retry, drawn per connection from
   a window (recommended 500–1000 ms) so a fleet does not reconnect in lockstep.
2. One keep-alive frame (`event: ping`, see §4), so the client learns at once
   that this server sends pings and can arm its dead-man timer.
3. Then, depending on the cursor:
   - no cursor → the connect snapshot (implementation-defined), then live events;
   - a cursor the server can serve → every retained event with `seq > cursor`,
     ascending, then live events, with no overlap and **no snapshot**;
   - a cursor the server cannot serve → `event: resync` (§3), then the connect
     snapshot, then live events.

The boundary is strictly `seq > cursor`; a server MUST NOT resend the event the
cursor names.

## 3. Resync

```
id:
event: resync
data: {"reason":"expired","earliest_seq":47900}
```

- `reason` is `expired` (the cursor is older than what the server retains, or
  the gap exceeds the replay limit) or `unknown` (the cursor was never issued —
  a restart, another deployment, a malformed value).
- The frame MUST carry an **empty** `id:` line: the specification resets the
  browser's last event id on an empty id, so its own next retry does not resend
  the rejected cursor.
- A client receiving `resync` MUST drop its cursor and SHOULD reload a snapshot
  through whatever non-stream path it has (a poll).

## 4. Keep-alive and dead-man

- The server MUST send `event: ping` once at connect and whenever the stream has
  been quiet for the keep-alive interval (recommended 15 s — under every common
  proxy idle timeout).
- **v1.1**: The `ping` payload MAY carry the keep-alive interval in seconds:
  `{"every": <seconds>}` (e.g. `{"every":15}`). A client receiving this adjusts
  its dead-man window to `2 × every + 5s`. Profile v1.0 clients (and servers
  sending `{}`) retain the default dead-man window (35 s).
- **v1.2**: The `ping` payload MAY also carry `max_age` in whole seconds
  (`{"every":15,"max_age":30}`): the lifetime after which this server ends a
  healthy stream on purpose (§10). A client MAY use it to expect the end — a
  canary can check the cadence, a console can say so — but MUST NOT change its
  reconnect policy for it: §8 already reopens a cut healthy stream at once.
  Unknown fields in the `ping` payload MUST be ignored.
- A client that has seen a `ping` on a connection SHOULD treat silence longer
  than roughly two intervals plus a margin (recommended 35 s, or `2 × every + 5s`
  per v1.1) as a half-open connection and reopen. A client that has never seen a
  `ping` MUST NOT apply the dead-man rule (a plain SSE server sends no pings).
- Comment lines (`: …`) MAY be sent but are invisible to `EventSource` and do
  not count as keep-alive for this profile.

## 5. Failure to open

When the stream cannot be opened (the upstream is down), the server MUST answer
with a non-200 status — `503` with `Retry-After: <seconds>` recommended — rather
than an empty `200` stream. A non-200 makes the browser stop its own retry
loop; the client's watchdog takes over with jittered exponential backoff
(recommended 1 s → 30 s, full jitter, a floor around 200 ms).

## 6. Response headers

`Content-Type: text/event-stream; charset=utf-8`,
`Cache-Control: no-cache, no-transform`, `X-Accel-Buffering: no`.

## 7. Retention

The server keeps a bounded log per topic (recommended: at least the client's
maximum backoff several times over — e.g. 10 minutes or 2 000 events). A cursor
below the retained window answers `resync(expired)`; a cursor above the last
issued sequence answers `resync(unknown)`.

## 8. Client reconnect policy (recommended)

| Situation | Action |
| --- | --- |
| Stream ended after ≥ 5 s live | reopen within ≤ 250 ms (jittered); do not wait for the native retry |
| Stream ended sooner | the native retry (`retry:` hint) |
| Non-200 / transport error | full-jitter exponential backoff; `Retry-After` is the floor when readable |
| Silence past the dead-man window (after a `ping`) | reopen at once; while the page is hidden, judge again when visible |
| Network or page returns (`online`, `visibilitychange`) | collapse a pending backoff into an immediate retry |
| `resync` | drop the cursor; reload a snapshot |

## 9. Golden vectors

`vectors/*.sse` are exact wire bytes. An implementation's encoder MUST produce
them from the described events, and its parser MUST decode them to the
described events. See `tests/vectors.rs` (Rust) for the expectations.

## 10. Rotation (v1.2)

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

A client treats the end as §8 says: a stream live for at least 5 s reopens
within 250 ms with its cursor; one that ended sooner follows the `retry:`
hint. Whether a rotation is visible to people is the client's link judgement
(a grace before a drop is reported), not the server's concern.
