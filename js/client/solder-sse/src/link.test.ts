import { describe, expect, it } from 'vitest';
import { DEFAULT_LINK, linkAt, nextDeadline, trackStatus, type LinkTrack } from './link.js';

const P = DEFAULT_LINK;
const at = (track: LinkTrack, t: number) => linkAt(track, t, P);

describe('trackStatus', () => {
	it('starts a non-live span at the first non-live report and keeps it across retrying/connecting', () => {
		const a = trackStatus(undefined, 'connecting', 0);
		expect(a).toEqual({ status: 'connecting', since: 0, everLive: false });
		const b = trackStatus(a, 'retrying', 500);
		expect(b.since).toBe(0);
		const c = trackStatus(b, 'connecting', 900);
		expect(c.since).toBe(0);
	});

	it('remembers that the stream was live once', () => {
		const live = trackStatus(trackStatus(undefined, 'connecting', 0), 'live', 100);
		expect(live).toEqual({ status: 'live', since: 100, everLive: true });
		const drop = trackStatus(live, 'retrying', 5_000);
		expect(drop).toEqual({ status: 'retrying', since: 5_000, everLive: true });
	});
});

describe('linkAt', () => {
	it('says nothing during the first connect, then "connecting" past the grace', () => {
		const t = trackStatus(undefined, 'connecting', 0);
		expect(at(t, 0)).toBeUndefined();
		expect(at(t, P.graceMs - 1)).toBeUndefined();
		expect(at(t, P.graceMs)).toBe('connecting');
		expect(at(t, P.offlineMs)).toBe('offline');
	});

	it('holds "live" through a reconnect shorter than the grace — the scheduled cut', () => {
		const live = trackStatus(undefined, 'live', 0);
		const drop = trackStatus(live, 'retrying', 30_000);
		const reopening = trackStatus(drop, 'connecting', 30_400);
		expect(at(drop, 30_000)).toBe('live');
		expect(at(reopening, 30_900)).toBe('live');
		const back = trackStatus(reopening, 'live', 31_000);
		expect(at(back, 31_000)).toBe('live');
	});

	it('reports a drop that outlasts the grace, then offline', () => {
		const drop = trackStatus(trackStatus(undefined, 'live', 0), 'retrying', 30_000);
		expect(at(drop, 30_000 + P.graceMs)).toBe('reconnecting');
		expect(at(drop, 30_000 + P.offlineMs - 1)).toBe('reconnecting');
		expect(at(drop, 30_000 + P.offlineMs)).toBe('offline');
	});

	it('is live the moment the transport is', () => {
		const t = trackStatus(trackStatus(undefined, 'retrying', 0), 'live', 60_000);
		expect(at(t, 60_000)).toBe('live');
	});
});

describe('nextDeadline', () => {
	it('names the grace deadline, then the offline one, then nothing', () => {
		const t = trackStatus(undefined, 'retrying', 1_000);
		expect(nextDeadline(t, 1_000, P)).toBe(1_000 + P.graceMs);
		expect(nextDeadline(t, 1_000 + P.graceMs, P)).toBe(1_000 + P.offlineMs);
		expect(nextDeadline(t, 1_000 + P.offlineMs, P)).toBeUndefined();
	});

	it('has none while live', () => {
		expect(nextDeadline(trackStatus(undefined, 'live', 0), 0, P)).toBeUndefined();
	});
});
