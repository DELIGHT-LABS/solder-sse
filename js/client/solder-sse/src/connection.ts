// One self-healing connection to one URL: the state machine behind a
// registry entry. It owns the EventSource, its timers and the cursor; the
// registry owns subscribers and fan-out (the `Sink`).
//
// EventSource reconnects on its own after a transport drop, but it gives up
// PERMANENTLY when the server answers with an HTTP error — exactly what a
// 502 during a redeploy looks like — and its native retry (3s Chromium, 5s
// Firefox) sits between a server-side cut and the next event. This machine
// recreates a source that gave up (jittered exponential backoff), reopens a
// healthy stream the far end cut at once (eager reconnect), caps a connect
// that hangs, watches for a half-open connection once the server has shown
// it sends `ping` (dead-man), collapses a pending wait the moment the
// environment says "try now", and resumes from the last event id so the
// server can replay the gap.

import type { ResolvedEnv } from './env.js';
import {
	deadmanFor,
	makeFrame,
	parsePing,
	parseResync,
	PING,
	RESYNC,
	withResume,
	type Frame,
	type ResyncInfo
} from './protocol.js';

export type StreamStatus = 'connecting' | 'live' | 'retrying';

/** `EventSource.readyState` values, per the specification. Read from the
 * instance and compared to these, never to a constructor's statics: an
 * injected `EventSource` need not carry them. */
const CONNECTING = 0;
const CLOSED = 2;

export interface Policy {
	/** Watchdog backoff after the source gave up: full jitter over
	 * `min(maxMs, baseMs·2^n)`, never below `floorMs`. */
	backoff: { baseMs: number; maxMs: number; floorMs: number };
	/** A connect that has not opened by then is killed and retried. */
	connectTimeoutMs: number;
	/** A stream live for at least `healthyMs` that drops is reopened within
	 * `jitterMs` instead of after the browser's native retry. */
	eager: { healthyMs: number; jitterMs: number } | null;
	/** Silence after a `ping` has been seen that means half-open. `null`
	 * disables. `'auto'` starts at the profile default and follows the
	 * interval a `ping` body announces (`{"every":s}` → `2·every + 5s`). */
	deadmanMs: number | 'auto' | null;
	/** Resume: the query parameter carrying the cursor on a watchdog open
	 * (the browser's own reconnect sends the `Last-Event-ID` header). */
	resume: { query: string } | null;
}

/** The profile's recommended dead-man window when no `ping` announces an
 * interval: two 15s intervals plus a 5s margin. */
export const DEFAULT_DEADMAN_MS = deadmanFor(15_000);

/** Where a connection reports. Implemented by the registry (fan-out). */
export interface Sink {
	status(status: StreamStatus): void;
	event(frame: Frame): void;
	ping(): void;
	resync(info: ResyncInfo): void;
}

/** A read-only view of the connection, for debugging and tests. */
export interface ConnectionInfo {
	status: StreamStatus;
	/** The cursor in force; undefined before any. */
	lastEventId: string | undefined;
	/** Sources opened so far (1 = never reconnected). */
	opens: number;
	/** Whether the server has shown it sends `ping` (dead-man armed). */
	pingSeen: boolean;
	/** The dead-man window in force (derived from `ping` when it announces
	 * one); undefined when the policy disables it. */
	deadmanMs: number | undefined;
	/** The rotation age the server's `ping` announces: it ends a healthy
	 * stream on purpose after about this long. Information for a console or
	 * a test; the reconnect policy does not change for it. */
	maxAgeMs: number | undefined;
	/** Consecutive failed opens (the backoff exponent). */
	attempt: number;
	/** Whether the current source opened with a cursor, so a server speaking
	 * the profile replays the gap: an owned open that carried the resume
	 * query, or the browser's own reconnect while a cursor was in force (it
	 * sends `Last-Event-ID` by itself, whatever the resume policy). */
	resumed: boolean;
}

export interface Connection {
	info(): ConnectionInfo;
	/** Collapse a pending backoff (or a deferred dead-man verdict) into an
	 * immediate reconnect. A live stream ignores it. */
	nudge(): void;
	/** Listen for a named event on the current and every future source. */
	listen(name: string): void;
	/** Stop listening for a name (the last subscriber for it left). */
	unlisten(name: string): void;
	close(): void;
}

/** Names the connection wires itself, for every source and for as long as
 * it lives: a subscriber neither claims nor releases them. */
const RESERVED = new Set([PING, RESYNC, 'error', 'message']);

/** True for the names the connection always wires: the profile's own
 * (`ping`, `resync`), `error`, and the unnamed `message`. */
export function isReserved(name: string): boolean {
	return RESERVED.has(name);
}

export function openConnection(
	url: string,
	names: Iterable<string>,
	policy: Policy,
	env: ResolvedEnv,
	sink: Sink
): Connection {
	const wanted = new Set<string>([...names].filter((n) => !isReserved(n)));
	const state: ConnectionInfo = {
		status: 'connecting',
		lastEventId: undefined,
		opens: 0,
		pingSeen: false,
		deadmanMs: policy.deadmanMs === 'auto' ? DEFAULT_DEADMAN_MS : (policy.deadmanMs ?? undefined),
		maxAgeMs: undefined,
		attempt: 0,
		resumed: false
	};
	let source: EventSource | undefined;
	let closed = false;
	let liveSinceMs: number | undefined;
	/** The current source was opened with a resume query. After a resync a
	 * native retry would resend it and loop, so the next drop reopens with a
	 * clean URL instead. */
	let openedWithQuery = false;
	let staleQuery = false;
	/** `open` events of the current source: the first is the open this
	 * machine made, every later one a reconnect the browser made itself. */
	let sourceOpens = 0;
	let reviveTimer: ReturnType<typeof setTimeout> | undefined;
	let connectTimer: ReturnType<typeof setTimeout> | undefined;
	// Dead-man: one interval per connection, not a timer reset per event —
	// events only stamp `lastActivityMs`.
	let deadmanTicker: ReturnType<typeof setInterval> | undefined;
	let lastActivityMs = env.now();
	let deadmanDue = false;
	/** Listeners on the current source, by name, so they can be removed. */
	const listeners = new Map<string, (e: Event) => void>();

	const setStatus = (status: StreamStatus) => {
		if (state.status === status) return;
		state.status = status;
		sink.status(status);
	};

	const scheduleRevive = () => {
		if (closed || reviveTimer != null) return;
		const cap = Math.min(policy.backoff.maxMs, policy.backoff.baseMs * 2 ** state.attempt);
		state.attempt += 1;
		const delay = Math.max(policy.backoff.floorMs, env.random() * cap);
		reviveTimer = setTimeout(() => {
			reviveTimer = undefined;
			open();
		}, delay);
	};

	// Caps ANY connecting window — the watchdog's own opens and the browser's
	// native reconnects alike.
	const armConnectTimer = () => {
		clearTimeout(connectTimer);
		connectTimer = setTimeout(() => {
			connectTimer = undefined;
			if (!closed && source?.readyState === CONNECTING) {
				source.close();
				setStatus('retrying');
				scheduleRevive();
			}
		}, policy.connectTimeoutMs);
	};

	/** Reopen now (after `delayMs`): a healthy stream the far end cut, a
	 * half-open one the dead-man caught, or a stale resume query. The
	 * backoff is untouched — should THIS open fail, it starts from its base
	 * like any other death. */
	const reopen = (delayMs: number) => {
		if (closed || reviveTimer != null) return;
		clearTimeout(connectTimer);
		connectTimer = undefined;
		source?.close();
		setStatus('retrying');
		if (delayMs <= 0) {
			open();
			return;
		}
		reviveTimer = setTimeout(() => {
			reviveTimer = undefined;
			open();
		}, delayMs);
	};

	const stopDeadman = () => {
		clearInterval(deadmanTicker);
		deadmanTicker = undefined;
	};
	const touch = () => {
		lastActivityMs = env.now();
	};
	/** Runs once the server has shown it sends pings. */
	const startDeadman = () => {
		if (closed || deadmanTicker != null || state.deadmanMs === undefined || !state.pingSeen) return;
		const window = state.deadmanMs;
		deadmanTicker = setInterval(
			() => {
				if (closed || env.now() - lastActivityMs < window) return;
				// A hidden tab's timers are throttled to once a minute; a
				// missed ping there proves nothing. Judge again when visible.
				if (env.hidden()) {
					deadmanDue = true;
					return;
				}
				stopDeadman();
				reopen(0);
			},
			Math.max(1_000, Math.floor(window / 4))
		);
	};

	const deliver = (name: string, e: MessageEvent) => {
		if (typeof e.lastEventId === 'string' && e.lastEventId !== '')
			state.lastEventId = e.lastEventId;
		touch();
		sink.event(makeFrame(name, String(e.data), state.lastEventId));
	};
	const listenOn = (src: EventSource, name: string) => {
		if (listeners.has(name)) return;
		const fn = (e: Event) => {
			if (e instanceof MessageEvent) deliver(name, e);
		};
		listeners.set(name, fn);
		src.addEventListener(name, fn);
	};
	const wire = (src: EventSource) => {
		listeners.clear();
		for (const name of wanted) listenOn(src, name);
		listenOn(src, 'message');
		src.addEventListener(PING, (e) => {
			touch();
			state.pingSeen = true;
			if (e instanceof MessageEvent) {
				const hints = parsePing(String(e.data));
				state.maxAgeMs = hints.maxAgeMs;
				if (
					policy.deadmanMs === 'auto' &&
					hints.everyMs !== undefined &&
					deadmanFor(hints.everyMs) !== state.deadmanMs
				) {
					state.deadmanMs = deadmanFor(hints.everyMs);
					stopDeadman();
				}
			}
			startDeadman();
			sink.ping();
		});
		src.addEventListener(RESYNC, (e) => {
			if (!(e instanceof MessageEvent)) return;
			touch();
			// The cursor the server rejected must not be sent again.
			state.lastEventId = undefined;
			if (openedWithQuery) staleQuery = true;
			sink.resync(parseResync(String(e.data)));
		});
		// A server-sent event NAMED "error" (it carries data) is the caller's;
		// the transport error (no data) is the watchdog's.
		src.addEventListener('error', (e) => {
			if (e instanceof MessageEvent && typeof e.data === 'string') {
				deliver('error', e);
				return;
			}
			setStatus('retrying');
			// The timer guard keeps a double error burst from stacking revivals.
			if (closed || reviveTimer != null) return;
			if (src.readyState === CLOSED) {
				// EventSource gave up (HTTP error) — take over.
				clearTimeout(connectTimer);
				connectTimer = undefined;
				stopDeadman();
				scheduleRevive();
			} else if (src.readyState === CONNECTING) {
				if (staleQuery) {
					reopen(0);
				} else if (
					policy.eager &&
					liveSinceMs !== undefined &&
					env.now() - liveSinceMs >= policy.eager.healthyMs
				) {
					// A healthy stream the far end cut on a timer: do not sit
					// through the native retry. The jitter spreads a fleet.
					reopen(env.random() * policy.eager.jitterMs);
				} else if (connectTimer == null) {
					// Native reconnect in flight — cap it like an owned open.
					armConnectTimer();
				}
			}
		});
	};

	const open = () => {
		const target =
			policy.resume && state.lastEventId !== undefined
				? withResume(url, policy.resume.query, state.lastEventId)
				: url;
		let next: EventSource;
		try {
			const ES = env.eventSource();
			if (!ES) throw new Error('[solder-sse] no EventSource in this environment; inject one');
			next = new ES(target);
		} catch (error) {
			// A throwing constructor (an invalid URL, a missing global) must
			// not leave the connection stuck: report and keep retrying.
			console.error('[solder-sse] open failed', error);
			setStatus('retrying');
			scheduleRevive();
			return;
		}
		source = next;
		openedWithQuery = target !== url;
		staleQuery = false;
		sourceOpens = 0;
		state.opens += 1;
		liveSinceMs = undefined;
		stopDeadman();
		touch();
		setStatus('connecting');
		armConnectTimer();
		wire(next);
		// Fires for this open and again for every reconnect the browser makes
		// on the same source.
		next.addEventListener('open', () => {
			const native = sourceOpens > 0;
			sourceOpens += 1;
			// The browser's own reconnect sends `Last-Event-ID` whenever a
			// cursor is in force (an empty `id:` — resync — clears it, and
			// this machine mirrors that); an owned open carried the query.
			state.resumed = native ? state.lastEventId !== undefined : openedWithQuery;
			// The dead-man belongs to a connection: it is armed by a ping on
			// THIS one, at the window this one announces.
			state.pingSeen = false;
			state.maxAgeMs = undefined;
			if (policy.deadmanMs === 'auto') state.deadmanMs = DEFAULT_DEADMAN_MS;
			stopDeadman();
			clearTimeout(connectTimer);
			connectTimer = undefined;
			state.attempt = 0;
			liveSinceMs = env.now();
			touch();
			setStatus('live');
		});
	};

	open();

	return {
		info: () => ({ ...state }),
		nudge() {
			if (closed) return;
			if (deadmanDue && !env.hidden()) {
				deadmanDue = false;
				stopDeadman();
				reopen(0);
				return;
			}
			if (reviveTimer == null) return;
			clearTimeout(reviveTimer);
			reviveTimer = undefined;
			source?.close();
			open();
		},
		listen(name) {
			if (RESERVED.has(name) || wanted.has(name)) return;
			wanted.add(name);
			if (source) listenOn(source, name);
		},
		unlisten(name) {
			if (!wanted.delete(name)) return;
			const fn = listeners.get(name);
			if (fn && source) {
				source.removeEventListener(name, fn);
				listeners.delete(name);
			}
		},
		close() {
			closed = true;
			clearTimeout(reviveTimer);
			clearTimeout(connectTimer);
			stopDeadman();
			source?.close();
		}
	};
}
