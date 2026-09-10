// The registry: one shared connection per URL for every subscriber, kept
// alive briefly after the last one leaves (linger), with ONE pair of
// environment listeners (`online`, `visibilitychange`) fanning "try now"
// out to every connection.

import { resolveEnv, type Environment } from './env.ts';
import {
	isReserved,
	openConnection,
	type Connection,
	type ConnectionInfo,
	type Policy,
	type StreamStatus
} from './connection.ts';
import { RESUME_QUERY, type Frame, type ResyncInfo } from './protocol.ts';
import {
	DEFAULT_LINK,
	linkAt,
	nextDeadline,
	trackStatus,
	type LinkPolicy,
	type LinkState,
	type LinkTrack
} from './link.ts';

export interface Handlers {
	/** A named event (or an unnamed `message`). `frame.json()` parses the
	 * body once for every subscriber of the stream. */
	onEvent?: (frame: Frame) => void;
	onStatus?: (status: StreamStatus) => void;
	/** The link state — the transport status with judgement (`./link.ts`):
	 * a drop is reported only once it outlasts the grace, `offline` once it
	 * outlasts the offline window, nothing at all before the first verdict.
	 * What a surface shows; `onStatus` is what the transport did. */
	onLink?: (link: LinkState) => void;
	/** The server could not replay the gap: reload a snapshot (poll once). */
	onResync?: (info: ResyncInfo) => void;
	/** The retrying → live edge — never the first open, never the status
	 * replay to a late joiner. `resumed` is true when the source reopened
	 * with the cursor, so a server speaking the profile replays the gap
	 * (and says `resync` when it cannot); a consumer that trusts replay
	 * catches up — polls once — only when `!resumed`. */
	onReconnect?: (resumed: boolean) => void;
	onPing?: () => void;
}

export interface SubscribeOptions {
	/** Named events to listen for. `message`, `ping` and `resync` are always
	 * wired; a server-sent `error` event that carries data is delivered as
	 * a frame named `error`. Names are refcounted across subscribers. */
	events?: readonly string[];
}

export interface SolderOptions extends Environment {
	/** Watchdog backoff after the source gave up: full jitter over
	 * `min(maxMs, baseMs·2^n)`, never below `floorMs`. */
	backoff?: Partial<Policy['backoff']>;
	/** A connect that has not opened by then is killed and retried. */
	connectTimeoutMs?: number;
	/** A stream live for at least `healthyMs` that drops is reopened within
	 * `jitterMs` instead of after the browser's native retry. `false` disables. */
	eager?: Partial<NonNullable<Policy['eager']>> | false;
	/** Silence after a `ping` has been seen that means half-open. Omit to
	 * start at the profile's 35s and follow the interval a `ping` announces;
	 * a number fixes it; `false` disables. */
	deadmanMs?: number | false;
	/** Keep a source open this long after its last subscriber leaves, so a
	 * route hop re-attaches instead of reconnecting. */
	lingerMs?: number;
	/** Resume: send the last event id as this query parameter when the
	 * watchdog opens a fresh source. `false` disables. */
	resume?: { query?: string } | false;
	/** Link-state judgement: the grace a drop must outlast before it is
	 * reported, and the window after which it is `offline`. */
	link?: Partial<LinkPolicy>;
}

/** A read-only view of one shared stream, for debugging and tests. */
export interface StreamInfo extends ConnectionInfo {
	/** The current link verdict (`null` before the first). */
	link: LinkState | null;
	subscribers: number;
	/** Named events currently wired. */
	events: string[];
}

export interface Solder {
	/** Subscribe to `url`. Subscribers of the same URL share one source. The
	 * current status is replayed to a late joiner. Returns unsubscribe. */
	subscribe(url: string, handlers: Handlers, options?: SubscribeOptions): () => void;
	/** Collapse a pending backoff into an immediate reconnect (all streams,
	 * or one). A live stream ignores it. */
	nudge(url?: string): void;
	inspect(url: string): StreamInfo | null;
	/** Close every stream now (no linger) and drop the environment
	 * listeners — for module hot-replacement and test teardown. Subscribers
	 * are not notified; the registry stays usable afterwards. */
	dispose(): void;
}

const DEFAULTS = {
	backoff: { baseMs: 1_000, maxMs: 30_000, floorMs: 200 },
	connectTimeoutMs: 20_000,
	eager: { healthyMs: 5_000, jitterMs: 250 },
	lingerMs: 60_000,
	resume: { query: RESUME_QUERY }
} as const;

interface Entry {
	conn: Connection;
	/** Each subscriber with the event names it asked for. */
	subscribers: Map<Handlers, readonly string[]>;
	/** Refcount per wired name, so a name is unwired when its last
	 * subscriber leaves. */
	names: Map<string, number>;
	linger?: ReturnType<typeof setTimeout>;
	/** The link track and its last verdict; `deadline` re-evaluates when a
	 * grace or offline window passes without a status report. */
	track: LinkTrack | null;
	link: LinkState | null;
	deadline?: ReturnType<typeof setTimeout>;
	/** A drop has been reported and the stream is not back yet: the next
	 * `live` is a reconnect. */
	down: boolean;
}

export function createSolder(options: SolderOptions = {}): Solder {
	const env = resolveEnv(options);
	const policy: Policy = {
		backoff: { ...DEFAULTS.backoff, ...options.backoff },
		connectTimeoutMs: options.connectTimeoutMs ?? DEFAULTS.connectTimeoutMs,
		eager: options.eager === false ? null : { ...DEFAULTS.eager, ...options.eager },
		deadmanMs: options.deadmanMs === false ? null : (options.deadmanMs ?? 'auto'),
		resume: options.resume === false ? null : { ...DEFAULTS.resume, ...options.resume }
	};
	const lingerMs = options.lingerMs ?? DEFAULTS.lingerMs;
	const linkPolicy: LinkPolicy = { ...DEFAULT_LINK, ...options.link };

	const entries = new Map<string, Entry>();

	// One listener pair for the whole registry: installed with the first
	// connection, removed with the last.
	const wakeAll = () => {
		for (const entry of entries.values()) entry.conn.nudge();
	};
	const onVisible = () => {
		if (!env.hidden()) wakeAll();
	};
	let listening = false;
	const listenEnvironment = () => {
		if (listening) return;
		listening = true;
		env.online()?.addEventListener('online', wakeAll);
		env.visibility()?.addEventListener('visibilitychange', onVisible);
	};
	const unlistenEnvironment = () => {
		if (!listening || entries.size > 0) return;
		listening = false;
		env.online()?.removeEventListener('online', wakeAll);
		env.visibility()?.removeEventListener('visibilitychange', onVisible);
	};

	// One subscriber's throw must not starve the others of the event.
	const fanOut = (entry: Entry, fn: (h: Handlers) => void) => {
		for (const h of entry.subscribers.keys()) {
			try {
				fn(h);
			} catch (e) {
				console.error('[solder-sse] subscriber threw', e);
			}
		}
	};

	// Re-judge the link now and arm the next deadline; a changed verdict
	// fans out. Runs on every status report and on each deadline.
	const judge = (entry: Entry) => {
		clearTimeout(entry.deadline);
		entry.deadline = undefined;
		if (!entry.track) return;
		const now = env.now();
		const link = linkAt(entry.track, now, linkPolicy);
		if (link !== entry.link) {
			entry.link = link;
			if (link) fanOut(entry, (h) => h.onLink?.(link));
		}
		const at = nextDeadline(entry.track, now, linkPolicy);
		if (at != null) entry.deadline = setTimeout(() => judge(entry), Math.max(0, at - now));
	};

	const create = (url: string, names: readonly string[]): Entry => {
		const entry: Entry = {
			conn: undefined as unknown as Connection,
			subscribers: new Map(),
			names: new Map(),
			track: null,
			link: null,
			down: false
		};
		entry.conn = openConnection(url, names, policy, env, {
			status: (status) => {
				entry.track = trackStatus(entry.track, status, env.now());
				fanOut(entry, (h) => h.onStatus?.(status));
				if (status === 'retrying') entry.down = true;
				else if (status === 'live' && entry.down) {
					entry.down = false;
					// The cursor in force when the source opened is the one it
					// carried (no frame of the new source has arrived yet).
					const resumed = entry.conn.info().lastEventId != null;
					fanOut(entry, (h) => h.onReconnect?.(resumed));
				}
				judge(entry);
			},
			event: (frame) => fanOut(entry, (h) => h.onEvent?.(frame)),
			ping: () => fanOut(entry, (h) => h.onPing?.()),
			resync: (info) => fanOut(entry, (h) => h.onResync?.(info))
		});
		// The connection reports no status for its initial 'connecting' (it is
		// the state it starts in): seed the track from it and arm the grace.
		entry.track = trackStatus(null, entry.conn.info().status, env.now());
		judge(entry);
		entries.set(url, entry);
		listenEnvironment();
		return entry;
	};

	const retain = (entry: Entry, names: readonly string[]) => {
		for (const name of names) {
			const n = (entry.names.get(name) ?? 0) + 1;
			entry.names.set(name, n);
			if (n === 1) entry.conn.listen(name);
		}
	};
	const release = (entry: Entry, names: readonly string[]) => {
		for (const name of names) {
			const n = (entry.names.get(name) ?? 0) - 1;
			if (n > 0) entry.names.set(name, n);
			else {
				entry.names.delete(name);
				entry.conn.unlisten(name);
			}
		}
	};

	return {
		subscribe(url, handlers, options = {}) {
			// The profile's own names are wired by the connection itself.
			const names = [...new Set(options.events ?? [])].filter((n) => !isReserved(n));
			const entry = entries.get(url) ?? create(url, names);
			clearTimeout(entry.linger);
			entry.subscribers.set(handlers, names);
			retain(entry, names);
			// A late joiner must not sit on the default 'connecting' while the
			// shared source is already live — replay the current status (and
			// the link verdict, when there is one)…
			const status = entry.conn.info().status;
			handlers.onStatus?.(status);
			if (entry.link) handlers.onLink?.(entry.link);
			// …and a page (re)entering mid-backoff is the right moment to try
			// NOW instead of finishing a 30s wait.
			if (status === 'retrying') entry.conn.nudge();
			return () => {
				// Idempotent: a second call (or one after the linger already
				// closed this entry) must not re-arm a linger that would later
				// evict a LIVE successor entry from the registry.
				const owned = entry.subscribers.get(handlers);
				if (!owned) return;
				entry.subscribers.delete(handlers);
				release(entry, owned);
				if (entry.subscribers.size > 0) return;
				clearTimeout(entry.linger);
				entry.linger = setTimeout(() => {
					if (entry.subscribers.size === 0 && entries.get(url) === entry) {
						clearTimeout(entry.deadline);
						entry.conn.close();
						entries.delete(url);
						unlistenEnvironment();
					}
				}, lingerMs);
			};
		},
		nudge(url) {
			if (url != null) entries.get(url)?.conn.nudge();
			else wakeAll();
		},
		inspect(url) {
			const entry = entries.get(url);
			if (!entry) return null;
			return {
				...entry.conn.info(),
				link: entry.link,
				subscribers: entry.subscribers.size,
				events: [...entry.names.keys()]
			};
		},
		dispose() {
			for (const [url, entry] of entries) {
				clearTimeout(entry.linger);
				clearTimeout(entry.deadline);
				entry.conn.close();
				entries.delete(url);
			}
			unlistenEnvironment();
		}
	};
}
