import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { DEFAULT_DEADMAN_MS } from './connection.ts';
import { createSolder, type Handlers, type Solder } from './registry.ts';

// Minimal EventSource double — enough to drive named-event dispatch, the
// transport-status callbacks, and the give-up (CLOSED) path the watchdog
// takes over. Real reconnection stays the browser's job. Like the real
// one, `url` is the ABSOLUTE resolved URL. It carries none of the static
// `readyState` constants on purpose: the connection must read the state
// from the instance and know the values itself.
const CONNECTING = 0;
const OPEN = 1;
const CLOSED = 2;

class FakeEventSource {
	static instances: FakeEventSource[] = [];
	static throwNext = false;
	static last() {
		return this.instances[this.instances.length - 1];
	}

	readonly url: string;
	readyState = OPEN;
	closed = false;
	onopen: ((e: Event) => void) | null = null;
	readonly listeners = new Map<string, Set<(e: Event) => void>>();

	constructor(url: string | URL) {
		if (FakeEventSource.throwNext) {
			FakeEventSource.throwNext = false;
			throw new TypeError('bad url');
		}
		this.url = new URL(String(url), 'http://app.test').href;
		FakeEventSource.instances.push(this);
	}

	addEventListener(type: string, fn: (e: Event) => void) {
		if (!this.listeners.has(type)) this.listeners.set(type, new Set());
		this.listeners.get(type)!.add(fn);
	}

	removeEventListener(type: string, fn: (e: Event) => void) {
		this.listeners.get(type)?.delete(fn);
	}

	close() {
		this.closed = true;
	}

	dispatch(type: string, event: Event) {
		for (const fn of this.listeners.get(type) ?? []) fn(event);
		if (type === 'open') this.onopen?.(event);
	}

	open() {
		this.dispatch('open', new Event('open'));
	}

	/** A named server event carrying a JSON body, optionally with an id. */
	message(type: string, data: unknown, lastEventId?: string) {
		this.dispatch(
			type,
			new MessageEvent(type, { data: JSON.stringify(data), lastEventId: lastEventId ?? '' })
		);
	}

	/** EventSource gave up permanently (HTTP error response). */
	die() {
		this.readyState = CLOSED;
		this.dispatch('error', new Event('error'));
	}

	/** A transport drop the browser is retrying by itself. */
	drop() {
		this.readyState = CONNECTING;
		this.dispatch('error', new Event('error'));
	}

	/** The browser's own reconnect succeeded: the same source opens again. */
	reconnect() {
		this.readyState = OPEN;
		this.open();
	}
}

let solder: Solder;
const created: Solder[] = [];
const make = (options: Parameters<typeof createSolder>[0] = {}) => {
	// random → 1 makes the full-jitter backoff deterministic at its cap:
	// 1s, 2s, 4s … 30s.
	const s = createSolder({ random: () => 1, ...options });
	created.push(s);
	return s;
};
const EVENTS = ['tick'] as const;
const sub = (url: string, handlers: Handlers = {}) =>
	solder.subscribe(url, handlers, { events: EVENTS });
const abs = (path: string) => `http://app.test${path}`;

beforeEach(() => {
	FakeEventSource.instances = [];
	FakeEventSource.throwNext = false;
	vi.stubGlobal('EventSource', FakeEventSource);
	vi.useFakeTimers();
	solder = make();
});
afterEach(() => {
	// A registry keeps window listeners and sources until disposed; a leak
	// here would let one test's `online` wake another test's streams.
	for (const s of created.splice(0)) s.dispose();
	vi.unstubAllGlobals();
	vi.useRealTimers();
});

describe('subscribe', () => {
	it('delivers frames with name, raw data, cursor and a memoised json()', () => {
		const onEvent = vi.fn();
		const onStatus = vi.fn();
		sub('/a', { onEvent, onStatus });
		expect(onStatus).toHaveBeenCalledWith('connecting');
		FakeEventSource.last().open();
		expect(onStatus).toHaveBeenCalledWith('live');
		FakeEventSource.last().message('tick', { n: 1 }, '7');
		const frame = onEvent.mock.calls[0][0];
		expect(frame).toMatchObject({ name: 'tick', data: '{"n":1}', lastEventId: '7' });
		const parse = vi.spyOn(JSON, 'parse');
		expect(frame.json()).toEqual({ n: 1 });
		expect(frame.json()).toEqual({ n: 1 });
		expect(parse).toHaveBeenCalledTimes(1);
		parse.mockRestore();
	});

	it('json() throws the same SyntaxError every time for a malformed body', () => {
		const onEvent = vi.fn();
		sub('/bad', { onEvent });
		FakeEventSource.last().dispatch('tick', new MessageEvent('tick', { data: '{nope' }));
		const frame = onEvent.mock.calls[0][0];
		expect(() => frame.json()).toThrow(SyntaxError);
		expect(() => frame.json()).toThrow(SyntaxError);
	});

	it('subscribers share one source and parse once; a late joiner gets the status replayed', () => {
		const a = { onEvent: vi.fn(), onStatus: vi.fn() };
		sub('/sh1', a);
		FakeEventSource.last().open();
		const b = { onEvent: vi.fn(), onStatus: vi.fn() };
		sub('/sh1', b);
		expect(FakeEventSource.instances).toHaveLength(1);
		expect(b.onStatus).toHaveBeenCalledWith('live');
		FakeEventSource.last().message('tick', 1);
		expect(a.onEvent.mock.calls[0][0]).toBe(b.onEvent.mock.calls[0][0]);
	});

	it('event names are refcounted: wired for a late subscriber, unwired when its last owner leaves', () => {
		sub('/ev', {});
		const onEvent = vi.fn();
		const off = solder.subscribe('/ev', { onEvent }, { events: ['other'] });
		const src = FakeEventSource.last();
		src.message('other', 2);
		expect(onEvent).toHaveBeenCalledTimes(1);
		expect(solder.inspect('/ev')?.events).toContain('other');
		off();
		expect(src.listeners.get('other')?.size ?? 0).toBe(0);
		src.message('other', 3);
		expect(onEvent).toHaveBeenCalledTimes(1);
		// reserved names cannot be claimed by a subscriber
		solder.subscribe('/ev', {}, { events: ['ping', 'error'] });
		expect(solder.inspect('/ev')?.events).not.toContain('ping');
	});

	it('the unnamed `message` is always wired: listing it changes nothing, leaving does not unwire it', () => {
		const stays = vi.fn();
		sub('/msg', { onEvent: stays });
		const offListed = solder.subscribe('/msg', {}, { events: ['message'] });
		expect(solder.inspect('/msg')?.events).not.toContain('message');
		offListed();
		FakeEventSource.last().dispatch('message', new MessageEvent('message', { data: '"plain"' }));
		expect(stays).toHaveBeenCalledTimes(1);
		expect(stays.mock.calls[0][0]).toMatchObject({ name: 'message', data: '"plain"' });
	});

	it('the same handlers object subscribed twice is two subscriptions, released one at a time', () => {
		const onEvent = vi.fn();
		const handlers: Handlers = { onEvent };
		const off1 = sub('/twice', handlers);
		const off2 = sub('/twice', handlers);
		expect(solder.inspect('/twice')?.subscribers).toBe(2);
		FakeEventSource.last().message('tick', 1);
		expect(onEvent).toHaveBeenCalledTimes(2);
		off1();
		off1();
		expect(solder.inspect('/twice')?.subscribers).toBe(1);
		expect(solder.inspect('/twice')?.events).toContain('tick');
		FakeEventSource.last().message('tick', 2);
		expect(onEvent).toHaveBeenCalledTimes(3);
		off2();
		expect(solder.inspect('/twice')?.subscribers).toBe(0);
		expect(solder.inspect('/twice')?.events).not.toContain('tick');
	});

	it('one subscriber throwing does not starve the others', () => {
		const bad = vi.fn(() => {
			throw new Error('boom');
		});
		const good = vi.fn();
		const err = vi.spyOn(console, 'error').mockImplementation(() => {});
		sub('/thr', { onEvent: bad });
		sub('/thr', { onEvent: good });
		FakeEventSource.last().message('tick', 1);
		expect(good).toHaveBeenCalledTimes(1);
		err.mockRestore();
	});

	it('unsubscribe lingers, then closes the shared source', () => {
		const off = sub('/t5');
		expect(FakeEventSource.last().closed).toBe(false);
		off();
		expect(FakeEventSource.last().closed).toBe(false);
		vi.advanceTimersByTime(60_000);
		expect(FakeEventSource.last().closed).toBe(true);
		expect(solder.inspect('/t5')).toBeNull();
	});

	it('re-subscribing within the linger re-attaches to the SAME connection', () => {
		const off = sub('/sh2');
		off();
		vi.advanceTimersByTime(30_000);
		const onEvent = vi.fn();
		sub('/sh2', { onEvent });
		vi.advanceTimersByTime(60_000);
		expect(FakeEventSource.instances).toHaveLength(1);
		expect(FakeEventSource.last().closed).toBe(false);
		FakeEventSource.last().message('tick', 10);
		expect(onEvent).toHaveBeenCalledTimes(1);
	});

	it('a second unsubscribe is a no-op and cannot evict a live successor', () => {
		const off = sub('/sh8');
		off();
		vi.advanceTimersByTime(60_000);
		expect(FakeEventSource.last().closed).toBe(true);
		sub('/sh8');
		off();
		vi.advanceTimersByTime(120_000);
		expect(FakeEventSource.last().closed).toBe(false);
	});

	it('keys are isolated', () => {
		const a = vi.fn();
		const b = vi.fn();
		sub('/iso-a', { onEvent: a });
		sub('/iso-b', { onEvent: b });
		expect(FakeEventSource.instances).toHaveLength(2);
		FakeEventSource.instances[0].message('tick', 1);
		expect(a).toHaveBeenCalledTimes(1);
		expect(b).not.toHaveBeenCalled();
	});

	it('splits the named server "error" event from a transport error', () => {
		const onEvent = vi.fn();
		const onStatus = vi.fn();
		sub('/err', { onEvent, onStatus });
		onStatus.mockClear();
		FakeEventSource.last().message('error', { event_type: 'STREAM_ERROR' });
		expect(onEvent.mock.calls[0][0]).toMatchObject({ name: 'error' });
		expect(onStatus).not.toHaveBeenCalled();
		FakeEventSource.last().dispatch('error', new Event('error'));
		expect(onStatus).toHaveBeenCalledWith('retrying');
		expect(FakeEventSource.instances).toHaveLength(1);
	});

	it('installs ONE environment listener pair for any number of streams, and removes it with the last', () => {
		const add = vi.spyOn(window, 'addEventListener');
		const remove = vi.spyOn(window, 'removeEventListener');
		const offA = sub('/env-a');
		const offB = sub('/env-b');
		expect(add.mock.calls.filter(([t]) => t === 'online')).toHaveLength(1);
		offA();
		offB();
		vi.advanceTimersByTime(60_000);
		expect(remove.mock.calls.filter(([t]) => t === 'online')).toHaveLength(1);
		add.mockRestore();
		remove.mockRestore();
	});
});

describe('watchdog', () => {
	it('recreates a source that gave up, with exponential backoff', () => {
		const onEvent = vi.fn();
		sub('/t6', { onEvent });
		FakeEventSource.last().die();
		expect(FakeEventSource.instances).toHaveLength(1);
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.instances).toHaveLength(2);
		FakeEventSource.last().message('tick', 5);
		expect(onEvent).toHaveBeenCalledTimes(1);
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.instances).toHaveLength(2);
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.instances).toHaveLength(3);
		expect(solder.inspect('/t6')?.attempt).toBe(2);
	});

	it('backoff is full-jitter: random·cap, never below the floor', () => {
		solder = make({ random: () => 0.5 });
		sub('/jit');
		FakeEventSource.last().die();
		vi.advanceTimersByTime(499);
		expect(FakeEventSource.instances).toHaveLength(1);
		vi.advanceTimersByTime(1);
		expect(FakeEventSource.instances).toHaveLength(2);
		solder = make({ random: () => 0 });
		sub('/floor');
		FakeEventSource.last().die();
		vi.advanceTimersByTime(199);
		expect(FakeEventSource.instances).toHaveLength(3);
		vi.advanceTimersByTime(1);
		expect(FakeEventSource.instances).toHaveLength(4);
	});

	it('a successful open resets the backoff', () => {
		sub('/t7');
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		FakeEventSource.last().open();
		expect(solder.inspect('/t7')?.attempt).toBe(0);
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.instances).toHaveLength(3);
	});

	it('backoff stops growing at the 30s cap', () => {
		sub('/t10');
		for (const delay of [1_000, 2_000, 4_000, 8_000, 16_000]) {
			FakeEventSource.last().die();
			vi.advanceTimersByTime(delay);
		}
		expect(FakeEventSource.instances).toHaveLength(6);
		FakeEventSource.last().die();
		vi.advanceTimersByTime(29_999);
		expect(FakeEventSource.instances).toHaveLength(6);
		vi.advanceTimersByTime(1);
		expect(FakeEventSource.instances).toHaveLength(7);
	});

	it('a double error burst schedules exactly one revival', () => {
		sub('/t9');
		FakeEventSource.last().die();
		FakeEventSource.last().dispatch('error', new Event('error'));
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.instances).toHaveLength(2);
		vi.advanceTimersByTime(60_000);
		expect(FakeEventSource.instances).toHaveLength(2);
	});

	it('a throwing EventSource constructor is reported and retried, not fatal', () => {
		const err = vi.spyOn(console, 'error').mockImplementation(() => {});
		FakeEventSource.throwNext = true;
		const onStatus = vi.fn();
		sub('/throw', { onStatus });
		// The failed open is reported; the joining subscriber's nudge retries
		// at once (one immediate attempt, then the normal backoff).
		expect(onStatus).toHaveBeenCalledWith('retrying');
		expect(FakeEventSource.instances).toHaveLength(1);
		// A throw inside a scheduled revival is handled the same way: the
		// attempt counts and the next cap doubles.
		FakeEventSource.throwNext = true;
		FakeEventSource.last().die();
		vi.advanceTimersByTime(2_000); // attempt 1 → 2s cap: this open throws
		expect(FakeEventSource.instances).toHaveLength(1);
		vi.advanceTimersByTime(4_000); // attempt 2 → 4s cap: succeeds
		expect(FakeEventSource.instances).toHaveLength(2);
		err.mockRestore();
	});

	it('a connection stuck in CONNECTING is killed after the connect timeout and retried', () => {
		sub('/sh5');
		const first = FakeEventSource.last();
		first.readyState = CONNECTING;
		vi.advanceTimersByTime(20_000);
		expect(first.closed).toBe(true);
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.instances).toHaveLength(2);
		expect(FakeEventSource.last().closed).toBe(false);
	});

	it("a native reconnect stuck in CONNECTING is capped like the watchdog's own opens", () => {
		sub('/sh7');
		FakeEventSource.last().open();
		const src = FakeEventSource.last();
		src.drop();
		vi.advanceTimersByTime(20_000);
		expect(src.closed).toBe(true);
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.instances).toHaveLength(2);
	});

	it('a healthy stream the server cut reopens at once, ahead of the native retry', () => {
		const onStatus = vi.fn();
		sub('/t7b', { onStatus });
		const first = FakeEventSource.last();
		first.open();
		vi.advanceTimersByTime(30_000);
		first.drop();
		vi.advanceTimersByTime(250);
		expect(first.closed).toBe(true);
		expect(FakeEventSource.instances).toHaveLength(2);
		expect(onStatus.mock.calls.map(([s]) => s)).toEqual([
			'connecting',
			'live',
			'retrying',
			'connecting'
		]);
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.instances).toHaveLength(3);
	});

	it('a stream that dropped right after opening is left to the native retry (no tight loop)', () => {
		sub('/t7c');
		const first = FakeEventSource.last();
		first.open();
		vi.advanceTimersByTime(1_000);
		first.drop();
		vi.advanceTimersByTime(250);
		expect(first.closed).toBe(false);
		expect(FakeEventSource.instances).toHaveLength(1);
	});

	it('the watchdog keeps reviving through the linger, then the close stops it', () => {
		const off = sub('/t8');
		FakeEventSource.last().die();
		off();
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.instances).toHaveLength(2);
		vi.advanceTimersByTime(60_000);
		expect(FakeEventSource.instances).toHaveLength(2);
		expect(FakeEventSource.last().closed).toBe(true);
	});
});

describe('nudge', () => {
	it('a subscriber joining mid-backoff reconnects immediately', () => {
		sub('/sh3');
		FakeEventSource.last().die();
		sub('/sh3');
		expect(FakeEventSource.instances).toHaveLength(2);
		expect(FakeEventSource.last().closed).toBe(false);
	});

	it('coming back online collapses a pending backoff into an immediate retry', () => {
		sub('/sh4');
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		FakeEventSource.last().die();
		expect(FakeEventSource.instances).toHaveLength(2);
		window.dispatchEvent(new Event('online'));
		expect(FakeEventSource.instances).toHaveLength(3);
	});

	it('a visible page collapses a pending backoff; a live stream ignores nudges', () => {
		sub('/sh6');
		FakeEventSource.last().open();
		window.dispatchEvent(new Event('online'));
		document.dispatchEvent(new Event('visibilitychange'));
		solder.nudge();
		solder.nudge('/sh6');
		expect(FakeEventSource.instances).toHaveLength(1);
		FakeEventSource.last().die();
		document.dispatchEvent(new Event('visibilitychange'));
		expect(FakeEventSource.instances).toHaveLength(2);
	});
});

describe('resume', () => {
	it('remembers the last event id and sends it as a query on a watchdog reopen', () => {
		sub('/r1');
		FakeEventSource.last().open();
		FakeEventSource.last().message('tick', 1, '41');
		FakeEventSource.last().message('tick', 2, '42');
		expect(solder.inspect('/r1')?.lastEventId).toBe('42');
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.last().url).toBe(abs('/r1?last_event_id=42'));
		// …and on an eager reopen, with an existing query string too.
		sub('/r2?x=1');
		FakeEventSource.last().open();
		FakeEventSource.last().message('tick', 1, '9');
		vi.advanceTimersByTime(6_000);
		FakeEventSource.last().drop();
		vi.advanceTimersByTime(250);
		expect(FakeEventSource.last().url).toBe(abs('/r2?x=1&last_event_id=9'));
	});

	it('a first open carries no cursor; events without an id do not move it', () => {
		sub('/r3');
		expect(FakeEventSource.last().url).toBe(abs('/r3'));
		FakeEventSource.last().message('tick', 1, '5');
		FakeEventSource.last().message('tick', 2);
		expect(solder.inspect('/r3')?.lastEventId).toBe('5');
	});

	it('resync is delivered and clears the rejected cursor', () => {
		const onResync = vi.fn();
		sub('/r4', { onResync });
		FakeEventSource.last().message('tick', 1, '77');
		FakeEventSource.last().message('resync', { reason: 'expired', earliest: 'g-90' });
		expect(onResync).toHaveBeenCalledWith({ reason: 'expired', earliest: 'g-90' });
		expect(solder.inspect('/r4')?.lastEventId).toBeNull();
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.last().url).toBe(abs('/r4'));
	});

	it('after a resync on a source opened WITH a query, the next drop reopens at once with a clean URL', () => {
		sub('/r6', {});
		FakeEventSource.last().open();
		FakeEventSource.last().message('tick', 1, '2');
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		const resumed = FakeEventSource.last();
		expect(resumed.url).toBe(abs('/r6?last_event_id=2'));
		resumed.open();
		resumed.message('resync', { reason: 'expired', earliest: 'g-9' });
		vi.advanceTimersByTime(1_000);
		resumed.drop();
		expect(resumed.closed).toBe(true);
		expect(FakeEventSource.last().url).toBe(abs('/r6'));
	});

	it('a resync on a source opened WITHOUT a query leaves an early drop to the native retry', () => {
		// The app's URLs are relative in production; EventSource.url is
		// absolute — the two must not be compared to detect a stale query.
		sub('/r7', {});
		const first = FakeEventSource.last();
		first.open();
		first.message('resync', { reason: 'unknown', earliest: null });
		vi.advanceTimersByTime(1_000);
		first.drop();
		expect(first.closed).toBe(false);
		expect(FakeEventSource.instances).toHaveLength(1);
	});

	it('can be disabled', () => {
		solder = make({ resume: false });
		sub('/r5');
		FakeEventSource.last().message('tick', 1, '3');
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.last().url).toBe(abs('/r5'));
	});
});

describe('reconnect edge', () => {
	it('fires once per retrying → live edge — resumed when a cursor was carried, fresh otherwise, never on the first open or a status replay', () => {
		const onReconnect = vi.fn();
		sub('/rc1', { onReconnect });
		const first = FakeEventSource.last();
		first.open();
		expect(onReconnect).not.toHaveBeenCalled();
		// a late joiner gets the status replay, not a reconnect
		const late = vi.fn();
		sub('/rc1', { onReconnect: late });
		expect(late).not.toHaveBeenCalled();
		// no id seen yet: the eager reopen is fresh — the consumer's cue to poll
		vi.advanceTimersByTime(6_000);
		first.drop();
		vi.advanceTimersByTime(250);
		FakeEventSource.last().open();
		expect(onReconnect).toHaveBeenCalledTimes(1);
		expect(onReconnect).toHaveBeenLastCalledWith(false);
		expect(late).toHaveBeenCalledWith(false);
		// an id seen: the next reopen carries it — the server replays
		FakeEventSource.last().message('tick', 1, '7');
		vi.advanceTimersByTime(6_000);
		FakeEventSource.last().drop();
		vi.advanceTimersByTime(250);
		FakeEventSource.last().open();
		expect(onReconnect).toHaveBeenCalledTimes(2);
		expect(onReconnect).toHaveBeenLastCalledWith(true);
		// a resync dropped the cursor: the reopen after it is fresh again
		FakeEventSource.last().message('resync', { reason: 'unknown', earliest: null });
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		FakeEventSource.last().open();
		expect(onReconnect).toHaveBeenCalledTimes(3);
		expect(onReconnect).toHaveBeenLastCalledWith(false);
	});

	it('is fresh when resume is off, whatever cursor was seen — the open carried none', () => {
		solder = make({ resume: false });
		const onReconnect = vi.fn();
		sub('/rc3', { onReconnect });
		FakeEventSource.last().open();
		FakeEventSource.last().message('tick', 1, '7');
		expect(solder.inspect('/rc3')?.lastEventId).toBe('7');
		FakeEventSource.last().die();
		vi.advanceTimersByTime(1_000);
		expect(FakeEventSource.last().url).toBe(abs('/rc3'));
		FakeEventSource.last().open();
		expect(onReconnect).toHaveBeenLastCalledWith(false);
		expect(solder.inspect('/rc3')?.resumed).toBe(false);
	});

	it("is resumed on the browser's own reconnect while a cursor is in force — it sends the header itself", () => {
		const onReconnect = vi.fn();
		sub('/rc4', { onReconnect });
		const src = FakeEventSource.last();
		src.open();
		// no cursor yet: a native reconnect is fresh
		vi.advanceTimersByTime(1_000);
		src.drop();
		vi.advanceTimersByTime(2_000);
		src.reconnect();
		expect(onReconnect).toHaveBeenLastCalledWith(false);
		// a cursor seen: the browser sends it, even with resume off
		src.message('tick', 1, '7');
		vi.advanceTimersByTime(1_000);
		src.drop();
		src.reconnect();
		expect(onReconnect).toHaveBeenLastCalledWith(true);
		expect(solder.inspect('/rc4')?.resumed).toBe(true);
		// a resync cleared it (an empty id resets the browser's too): fresh
		src.message('resync', { reason: 'unknown', earliest: null });
		vi.advanceTimersByTime(1_000);
		src.drop();
		src.reconnect();
		expect(onReconnect).toHaveBeenLastCalledWith(false);
		expect(FakeEventSource.instances).toHaveLength(1);
	});

	it('keeps the rotation age a ping announces as information only', () => {
		sub('/rc2');
		FakeEventSource.last().open();
		FakeEventSource.last().message('ping', { every: 15, max_age: 30 });
		expect(solder.inspect('/rc2')?.maxAgeMs).toBe(30_000);
		expect(solder.inspect('/rc2')?.deadmanMs).toBe(DEFAULT_DEADMAN_MS);
		FakeEventSource.last().message('ping', { every: 15 });
		expect(solder.inspect('/rc2')?.maxAgeMs).toBeNull();
	});
});

describe('dead-man', () => {
	it('arms only once the server has sent a ping, then reopens after 35s of silence', () => {
		const onPing = vi.fn();
		sub('/d1', { onPing });
		FakeEventSource.last().open();
		vi.advanceTimersByTime(60_000);
		expect(FakeEventSource.instances).toHaveLength(1);
		FakeEventSource.last().message('ping', {});
		expect(onPing).toHaveBeenCalledTimes(1);
		expect(solder.inspect('/d1')).toMatchObject({ pingSeen: true, deadmanMs: DEFAULT_DEADMAN_MS });
		// the ticker checks every window/4: the verdict lands within one tick
		vi.advanceTimersByTime(35_000 - 1);
		expect(FakeEventSource.instances).toHaveLength(1);
		vi.advanceTimersByTime(35_000 / 4 + 1);
		expect(FakeEventSource.instances[0].closed).toBe(true);
		expect(FakeEventSource.instances).toHaveLength(2);
	});

	it('is armed per connection: a ping on one source proves nothing about the next', () => {
		sub('/d7');
		const first = FakeEventSource.last();
		first.open();
		first.message('ping', { every: 5, max_age: 30 });
		expect(solder.inspect('/d7')).toMatchObject({
			pingSeen: true,
			deadmanMs: 15_000,
			maxAgeMs: 30_000
		});
		first.die();
		vi.advanceTimersByTime(1_000);
		const second = FakeEventSource.last();
		second.open();
		// The new connection has shown no ping: no dead-man, and the window
		// and the age it announced are back to the defaults.
		expect(solder.inspect('/d7')).toMatchObject({
			pingSeen: false,
			deadmanMs: DEFAULT_DEADMAN_MS,
			maxAgeMs: null
		});
		vi.advanceTimersByTime(120_000);
		expect(FakeEventSource.instances).toHaveLength(2);
		// …until it does.
		second.message('ping', {});
		vi.advanceTimersByTime(35_000 + 35_000 / 4 + 1);
		expect(FakeEventSource.instances).toHaveLength(3);
		// The browser's own reconnect of one source is a new connection too.
		const third = FakeEventSource.last();
		third.open();
		third.message('ping', {});
		expect(solder.inspect('/d7')?.pingSeen).toBe(true);
		third.drop();
		third.reconnect();
		expect(solder.inspect('/d7')?.pingSeen).toBe(false);
	});

	it('any event resets the silence', () => {
		sub('/d2');
		FakeEventSource.last().open();
		FakeEventSource.last().message('ping', {});
		vi.advanceTimersByTime(30_000);
		FakeEventSource.last().message('tick', 1);
		vi.advanceTimersByTime(30_000);
		expect(FakeEventSource.instances).toHaveLength(1);
		vi.advanceTimersByTime(15_000);
		expect(FakeEventSource.instances).toHaveLength(2);
	});

	it('follows the interval a ping announces: {"every":5} → 15s', () => {
		sub('/d4');
		FakeEventSource.last().open();
		FakeEventSource.last().message('ping', { every: 5 });
		expect(solder.inspect('/d4')?.deadmanMs).toBe(15_000);
		vi.advanceTimersByTime(15_000 + 4_000);
		expect(FakeEventSource.instances).toHaveLength(2);
		// an explicit window is never overridden
		solder = make({ deadmanMs: 50_000 });
		sub('/d5');
		FakeEventSource.last().message('ping', { every: 5 });
		expect(solder.inspect('/d5')?.deadmanMs).toBe(50_000);
	});

	it('a hidden tab defers the verdict until it is visible again', () => {
		const state = { visibilityState: 'hidden' as 'hidden' | 'visible' };
		const listeners = new Set<() => void>();
		solder = make({
			visibility: {
				get visibilityState() {
					return state.visibilityState;
				},
				addEventListener: (_: 'visibilitychange', fn: () => void) => void listeners.add(fn),
				removeEventListener: (_: 'visibilitychange', fn: () => void) => void listeners.delete(fn)
			}
		});
		sub('/d3');
		FakeEventSource.last().open();
		FakeEventSource.last().message('ping', {});
		vi.advanceTimersByTime(45_000);
		expect(FakeEventSource.instances).toHaveLength(1);
		state.visibilityState = 'visible';
		for (const fn of listeners) fn();
		expect(FakeEventSource.instances).toHaveLength(2);
	});

	it('keeps watching through the linger, then stops with the connection', () => {
		const off = sub('/d6');
		FakeEventSource.last().open();
		FakeEventSource.last().message('ping', {});
		off();
		// Within the linger the stream is still maintained: the dead-man
		// reopens the silent source so a page returning finds a live one.
		vi.advanceTimersByTime(60_000);
		const afterLinger = FakeEventSource.instances.length;
		expect(afterLinger).toBeGreaterThanOrEqual(2);
		expect(FakeEventSource.last().closed).toBe(true);
		vi.advanceTimersByTime(120_000); // a live ticker would have reopened by now
		expect(FakeEventSource.instances).toHaveLength(afterLinger);
	});
});

describe('link state', () => {
	it('says nothing during a quick first connect, then live', () => {
		const onLink = vi.fn();
		sub('/l', { onLink });
		expect(onLink).not.toHaveBeenCalled();
		FakeEventSource.last().open();
		expect(onLink).toHaveBeenCalledTimes(1);
		expect(onLink).toHaveBeenLastCalledWith('live');
		expect(solder.inspect('/l')?.link).toBe('live');
	});

	it('reports a first connect that outlasts the grace, then offline', () => {
		const onLink = vi.fn();
		sub('/slow', { onLink });
		vi.advanceTimersByTime(1_999);
		expect(onLink).not.toHaveBeenCalled();
		vi.advanceTimersByTime(1);
		expect(onLink).toHaveBeenLastCalledWith('connecting');
		vi.advanceTimersByTime(13_000);
		expect(onLink).toHaveBeenLastCalledWith('offline');
	});

	it('holds live through a reconnect shorter than the grace — the scheduled cut is invisible', () => {
		const onLink = vi.fn();
		const onStatus = vi.fn();
		sub('/cut', { onLink, onStatus });
		FakeEventSource.last().open();
		onLink.mockClear();
		// The far end cuts a healthy stream; the watchdog reopens within a second.
		vi.advanceTimersByTime(30_000);
		FakeEventSource.last().drop();
		expect(onStatus).toHaveBeenLastCalledWith('retrying');
		vi.advanceTimersByTime(1_000);
		FakeEventSource.last().open();
		expect(onLink).not.toHaveBeenCalled();
		expect(solder.inspect('/cut')?.link).toBe('live');
	});

	it('reports a drop that outlasts the grace, and offline after the window, then recovers at once', () => {
		const onLink = vi.fn();
		sub('/down', { onLink });
		FakeEventSource.last().open();
		onLink.mockClear();
		FakeEventSource.last().die();
		vi.advanceTimersByTime(2_000);
		expect(onLink).toHaveBeenLastCalledWith('reconnecting');
		vi.advanceTimersByTime(13_000);
		expect(onLink).toHaveBeenLastCalledWith('offline');
		// The watchdog's next open succeeds.
		vi.advanceTimersByTime(30_000);
		FakeEventSource.last().open();
		expect(onLink).toHaveBeenLastCalledWith('live');
	});

	it('replays the verdict to a late joiner, and never a stale one', () => {
		sub('/late', {});
		const early = { onLink: vi.fn() };
		sub('/late', early);
		expect(early.onLink).not.toHaveBeenCalled();
		FakeEventSource.last().open();
		const late = { onLink: vi.fn() };
		sub('/late', late);
		expect(late.onLink).toHaveBeenCalledWith('live');
	});
});
