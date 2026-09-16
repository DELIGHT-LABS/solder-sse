// The wire profile the server side (crate `solder-sse`) speaks, and the
// small pure helpers around it.

/** Keep-alive event. Carries no id, so it never moves the resume cursor;
 * its presence tells the client the server supports the profile. Its body
 * carries the server's interval in seconds — `{"every":15}` — from which
 * the client derives its dead-man window, and, when the server rotates
 * streams, the lifetime after which it ends a healthy one on purpose —
 * `"max_age":30` — which is information only. */
export const PING = 'ping';
/** Sent once after connect when the server could not replay from the
 * client's cursor. `data` is
 * `{"reason":"expired"|"unknown","earliest":"<cursor>"|null}`. */
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
	/** The oldest cursor the server still holds, when it named one — `null`
	 * as on the wire. A cursor is opaque: this is for a log line, not for
	 * arithmetic. */
	earliest: string | null;
}

/** A `resync` body; a malformed one is still a resync of unknown reason. */
export function parseResync(data: string): ResyncInfo {
	try {
		const parsed = JSON.parse(data) as { reason?: unknown; earliest?: unknown };
		return {
			reason: typeof parsed.reason === 'string' ? parsed.reason : 'unknown',
			earliest:
				typeof parsed.earliest === 'string' && parsed.earliest !== '' ? parsed.earliest : null
		};
	} catch {
		return { reason: 'unknown', earliest: null };
	}
}

/** What a `ping` body announces, in ms; undefined where it announces nothing. */
export interface PingHints {
	/** The keep-alive interval — the dead-man window follows it. */
	everyMs: number | undefined;
	/** The rotation age: the server ends a healthy stream on purpose after
	 * about this long. Information only — the reconnect policy already
	 * reopens a cut healthy stream at once. */
	maxAgeMs: number | undefined;
}

export function parsePing(data: string): PingHints {
	try {
		const parsed = JSON.parse(data) as { every?: unknown; max_age?: unknown };
		return { everyMs: secondsToMs(parsed.every), maxAgeMs: secondsToMs(parsed.max_age) };
	} catch {
		return { everyMs: undefined, maxAgeMs: undefined };
	}
}

const secondsToMs = (value: unknown): number | undefined =>
	typeof value === 'number' && value > 0 ? value * 1000 : undefined;

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
	 * `lastEventId`, kept across frames that carry no id); undefined before
	 * any. Opaque: the server issued it and only the server can read it. */
	readonly lastEventId: string | undefined;
	json<T = unknown>(): T;
}

export function makeFrame(name: string, data: string, lastEventId?: string): Frame {
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
