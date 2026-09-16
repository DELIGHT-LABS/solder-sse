// solder-sse — the client half of the resumable SSE profile.
//
// A stream can be cut at any moment (load-balancer idle timeouts, gRPC
// deadlines, mobile networks, a laptop lid). This package keeps ONE
// EventSource per URL alive for every subscriber, reopens it quickly and
// politely, resumes from the last event id so the server can replay the
// gap, and surfaces the two things a consumer must react to: a fresh
// reconnect (no cursor to resume from — poll once) and a `resync` (the
// server could not replay — reload a snapshot); a resumed reconnect is
// replayed by the server and costs the consumer nothing. For what a
// surface shows there is the link state: the transport status with
// judgement, so a stream the far end cuts on a timer — or rotates on
// purpose — never flashes "reconnecting" (`link`).
//
// Layout: `registry` (subscribers, linger, one environment listener pair)
// → `connection` (the per-URL state machine) → `protocol` (the wire
// profile and pure helpers) and `env` (injectable globals).
export { createSolder } from './registry.js';
export type { Handlers, Solder, SolderOptions, StreamInfo, SubscribeOptions } from './registry.js';
export type { ConnectionInfo, Policy, StreamStatus } from './connection.js';
export { DEFAULT_DEADMAN_MS } from './connection.js';
export {
	DEFAULT_LINK,
	linkAt,
	nextDeadline,
	trackStatus,
	type LinkPolicy,
	type LinkState,
	type LinkTrack
} from './link.js';
export type { Environment, OnlineLike, VisibilityLike } from './env.js';
export {
	deadmanFor,
	makeFrame,
	parsePing,
	parseResync,
	PING,
	RESUME_QUERY,
	RESYNC,
	withResume,
	type Frame,
	type PingHints,
	type ResyncInfo
} from './protocol.js';
