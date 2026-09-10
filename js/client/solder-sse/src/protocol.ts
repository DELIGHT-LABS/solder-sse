// The wire profile the server side (crate `solder-sse`) speaks, and the
// small pure helpers around it.

/** Keep-alive event. Carries no id, so it never moves the resume cursor;
 * its presence tells the client the server supports the profile. Its body
 * MAY carry the server's interval in seconds — `{"every":15}` (v1.1) — from
 * which the client derives its dead-man window, and the lifetime after
 * which the server ends a healthy stream on purpose — `"max_age":30`
 * (v1.2, rotation) — which is information only. */
export const PING = 'ping';
/** Sent once after connect when the server could not replay from the
 * client's cursor. `data` is `{"reason":"expired"|"unknown","earliest_seq":N}`. */
export const RESYNC = 'resync';
/** Query parameter carrying the cursor when a client cannot set the
 * `Last-Event-ID` header (a fresh `new EventSource(url)`). */
export const RESUME_QUERY = 'last_event_id';

/** Append the resume cursor to a URL. */
export function withResume(url: string, query: string, lastEventId: string): string {
	const sep = url.includes('?') ? '&' : '?';
	return `${url}${sep}${encodeURIComponent(query)}=${encodeURIComponent(lastEventId)}`;
}

/** What the server said when it could not replay from the cursor. */
export interface ResyncInfo {
	reason: 'expired' | 'unknown' | (string & {});
	earliestSeq: number | null;
}

/** A `resync` body; a malformed one is still a resync of unknown reason. */
export function parseResync(data: string): ResyncInfo {
	try {
		const parsed = JSON.parse(data) as { reason?: unknown; earliest_seq?: unknown };
		return {
			reason: typeof parsed.reason === 'string' ? parsed.reason : 'unknown',
			earliestSeq: typeof parsed.earliest_seq === 'number' ? parsed.earliest_seq : null
		};
	} catch {
		return { reason: 'unknown', earliestSeq: null };
	}
}

/** What a `ping` body announces, in ms; null where it announces nothing
 * (profile v1 sends `{}`). */
export interface PingHints {
	/** The keep-alive interval (v1.1) — the dead-man window follows it. */
	everyMs: number | null;
	/** The rotation age (v1.2): the server ends a healthy stream on purpose
	 * after about this long. Information only — the reconnect policy already
	 * reopens a cut healthy stream at once. */
	maxAgeMs: number | null;
}

export function parsePing(data: string): PingHints {
	try {
		const parsed = JSON.parse(data) as { every?: unknown; max_age?: unknown };
		return { everyMs: secondsToMs(parsed.every), maxAgeMs: secondsToMs(parsed.max_age) };
	} catch {
		return { everyMs: null, maxAgeMs: null };
	}
}

const secondsToMs = (value: unknown): number | null =>
	typeof value === 'number' && value > 0 ? value * 1000 : null;

/** The dead-man window for a keep-alive interval: one missed ping plus a
 * margin for jitter and scheduling — the profile's `2 × every + 5s`. */
export function deadmanFor(everyMs: number): number {
	return 2 * everyMs + 5_000;
}

/** One delivered event. `json()` parses the body once for every subscriber
 * of the stream (the result — or the SyntaxError — is memoised). */
export interface Frame {
	readonly name: string;
	readonly data: string;
	/** The cursor in force when this frame arrived (the browser's own
	 * `lastEventId`, kept across frames that carry no id). */
	readonly lastEventId: string | null;
	json<T = unknown>(): T;
}

export function makeFrame(name: string, data: string, lastEventId: string | null): Frame {
	let parsed: { value: unknown } | { error: unknown } | undefined;
	return {
		name,
		data,
		lastEventId,
		json<T>(): T {
			if (!parsed) {
				try {
					parsed = { value: JSON.parse(data) };
				} catch (error) {
					parsed = { error };
				}
			}
			if ('error' in parsed) throw parsed.error;
			return parsed.value as T;
		}
	};
}
