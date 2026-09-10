# solder-sse

Drop-tolerant, resumable Server-Sent Events for the browser. Framework-agnostic; adapters
(`solder-sse-solid`) are one file each.

A stream can be cut at any moment: load-balancer idle timeouts, gRPC deadlines, mobile
networks, a laptop lid. `solder-sse` keeps one `EventSource` per URL alive for every
subscriber, reopens it quickly and politely, resumes from the last event id so a server that
speaks the same profile replays the gap, and surfaces the two things a consumer must react to.

## What it does

| Concern                       | Behaviour                                                                                                                       |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Sharing                       | One source per URL, refcounted; a source lingers 60s after its last subscriber leaves.                                          |
| The browser gave up (non-200) | Watchdog recreates the source with full-jitter exponential backoff, 1s → 30s, floor 200ms.                                      |
| A healthy stream was cut      | Reopened within 250ms instead of after the browser's 3s / 5s native retry.                                                      |
| A connect that hangs          | Killed after 20s and retried.                                                                                                   |
| Half-open connection          | Once the server has sent `ping`, 35s of silence reopens the stream (deferred while hidden).                                     |
| Network / tab returns         | `online` and `visibilitychange` collapse a pending backoff into an immediate retry.                                             |
| Resume                        | The last event id is remembered; a watchdog reopen appends `?last_event_id=`.                                                   |
| Resync                        | A server `resync` event is delivered and the rejected cursor is cleared.                                                        |
| Reconnect edge                | `onReconnect(resumed)`: a fresh reopen (no cursor) is the consumer's cue to poll once; a resumed one is replayed by the server. |
| Rotation (profile v1.2)       | A server that ends healthy streams on purpose announces the age in `ping` (`max_age`); kept as `maxAgeMs`, policy unchanged.    |

Everything the core touches (`EventSource`, `document`, `window`, clock, randomness) is
injectable, so it runs in workers and tests.

## Usage

```ts
import { createSolder } from 'solder-sse';

const solder = createSolder();
const off = solder.subscribe(
	'/api/v1/screens/a/stream',
	{
		// One Frame per event for every subscriber; json() parses the body once.
		onEvent: (frame) => {
			if (frame.name !== 'new_message') return;
			const event = frame.json<{ seq: number }>();
			console.log(frame.lastEventId, event.seq);
		},
		onStatus: (s) => {}, // 'connecting' | 'live' | 'retrying' — what the transport did
		onLink: (l) => {}, // 'connecting' | 'live' | 'reconnecting' | 'offline' — what to show
		onReconnect: (resumed) => {
			if (!resumed) refetchPolls(); // no cursor to resume from: close the gap by the poll
		},
		onResync: () => refetchPolls() // the server could not replay: reload a snapshot
	},
	{ events: ['new_message', 'now_displaying'] }
);
solder.inspect('/api/v1/screens/a/stream'); // status, link, cursor, opens, deadmanMs, maxAgeMs …
solder.dispose(); // hot-replacement / test teardown
```

The poll is the truth and the stream is a freshness hint: a consumer patches what the stream
carries, discards nothing on a drop, and polls once only when the server cannot have replayed
the gap — a fresh reconnect or a `resync`. A resumed reconnect (every rotation, every cut inside
retention) costs it nothing.

## Layout

| Module       | Role                                                                                                       |
| ------------ | ---------------------------------------------------------------------------------------------------------- |
| `registry`   | subscribers, refcounted event names, linger, one `online`/`visibilitychange` listener pair for all streams |
| `connection` | the per-URL state machine: watchdog, eager reconnect, connect timeout, dead-man, resume, resync            |
| `protocol`   | the wire profile (`ping`, `resync`, `last_event_id`) and pure helpers (`makeFrame`, `deadmanFor`)          |
| `env`        | injectable globals (`EventSource`, visibility, online, clock, randomness)                                  |
| `link`       | the link-state judgement: grace before a drop is reported, offline window                                  |

## Dead-man window

Armed once a connection has shown a `ping`. It starts at the profile's 35 s (two 15 s
intervals plus a margin) and follows the interval a `ping` body announces (`{"every":5}` →
`2 × 5 + 5 = 15 s`), so changing the server's keep-alive needs no client change. Pass
`deadmanMs` to fix it, `false` to disable. Silence is judged by a timestamp per event and one
interval per connection, not a timer reset per event.

## The profile

The server side lives in the Rust crate `solder-sse`: every event carries a monotonic `id:`,
the server honours `Last-Event-ID` (header) or `last_event_id` (query) by replaying
`seq > id`, answers `event: resync` when it cannot, sends `event: ping` at connect and every
15 s, a jittered `retry:` hint, and `503 + Retry-After` when the stream cannot be opened. A
plain SSE server works too; the resume and dead-man features simply stay dormant.

## Building

`exports` serve the TypeScript source under the `development` condition (Vite dev, Vitest)
and `dist/` otherwise; `npm run build` in this package emits `dist/` with declarations.
