// The link state: what a SURFACE should say about the stream, derived once
// from the transport status a connection reports.
//
// The transport status is exact and busy — a healthy stream the far end
// cuts on a timer goes retrying → connecting → live inside a second — and a
// surface that repeats it verbatim flashes "reconnecting" every half minute
// while nothing is wrong. The link state is that status with judgement: a
// drop is not reported until it has lasted `graceMs`, it becomes `offline`
// after `offlineMs`, and until the first verdict there is nothing to say.
// Pure functions over timestamps, so the policy is tested as a table; the
// registry keeps one track per connection and re-evaluates on the
// deadlines these functions name.

import type { StreamStatus } from './connection.js';

export type LinkState = 'connecting' | 'live' | 'reconnecting' | 'offline';

export interface LinkPolicy {
	/** A non-live status shorter than this is not reported: the verdict
	 * before it holds (`live` through a brief reconnect, none before the
	 * first open). */
	graceMs: number;
	/** A non-live status longer than this is `offline`. */
	offlineMs: number;
}

export const DEFAULT_LINK: LinkPolicy = { graceMs: 2_000, offlineMs: 15_000 };

/** The transport status with when it began and whether the stream has ever
 * been live — everything a verdict needs. */
export interface LinkTrack {
	status: StreamStatus;
	since: number;
	everLive: boolean;
}

/** Fold a status report into the track. A repeated status keeps its
 * `since`: the clock measures how long the stream has been non-live, not
 * how long since the transport last spoke. */
export function trackStatus(
	prev: LinkTrack | undefined,
	status: StreamStatus,
	now: number
): LinkTrack {
	const everLive = (prev?.everLive ?? false) || status === 'live';
	if (prev && prev.status !== 'live' && status !== 'live') {
		return { status, since: prev.since, everLive };
	}
	return { status, since: now, everLive };
}

/** The verdict at `now`; undefined while there is none yet (a first
 * connect still within its grace). */
export function linkAt(track: LinkTrack, now: number, policy: LinkPolicy): LinkState | undefined {
	if (track.status === 'live') return 'live';
	const down = now - track.since;
	if (down >= policy.offlineMs) return 'offline';
	if (down < policy.graceMs) return track.everLive ? 'live' : undefined;
	return track.everLive ? 'reconnecting' : 'connecting';
}

/** When the verdict may change on its own (a grace or offline deadline
 * passing); undefined when only a status report can change it. */
export function nextDeadline(
	track: LinkTrack,
	now: number,
	policy: LinkPolicy
): number | undefined {
	if (track.status === 'live') return undefined;
	for (const at of [track.since + policy.graceMs, track.since + policy.offlineMs]) {
		if (at > now) return at;
	}
	return undefined;
}
