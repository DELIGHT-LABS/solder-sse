import { createRoot, createSignal, flush } from 'solid-js';
import {
	DEFAULT_DEADMAN_MS,
	makeFrame,
	type Handlers,
	type Solder,
	type StreamInfo
} from 'solder-sse';
import { describe, expect, it, vi } from 'vitest';
import { createStream } from './index.ts';

/** What the registry would say about a lingering stream holding a cursor. */
function lingering(lastEventId: string): StreamInfo {
	return {
		status: 'live',
		lastEventId,
		opens: 1,
		pingSeen: true,
		deadmanMs: DEFAULT_DEADMAN_MS,
		maxAgeMs: null,
		attempt: 0,
		resumed: false,
		link: 'live',
		subscribers: 0,
		events: []
	};
}

/** A registry double: hands the handlers back so the test plays the server. */
function fakeSolder(known: Record<string, StreamInfo> = {}) {
	const subs = new Map<string, Handlers>();
	const unsubscribed: string[] = [];
	const solder: Solder = {
		subscribe: (url, handlers) => {
			subs.set(url, handlers);
			return () => {
				unsubscribed.push(url);
				subs.delete(url);
			};
		},
		nudge: () => {},
		inspect: (url) => known[url] ?? null,
		dispose: () => subs.clear()
	};
	return { solder, subs, unsubscribed };
}

describe('createStream', () => {
	it('subscribes for as long as the owner lives and the URL is set, and follows a URL change', () => {
		const { solder, subs, unsubscribed } = fakeSolder();
		const [url, setUrl] = createSignal<string | null>('/a');
		const onSwitch = vi.fn();
		const onStatus = vi.fn();
		let stream!: ReturnType<typeof createStream>;
		let dispose!: () => void;
		createRoot((d) => {
			dispose = d;
			stream = createStream(solder, url, { onStatus }, { events: ['tick'], onSwitch });
		});
		// Solid 2.0 schedules effects; flush runs what is due.
		flush();
		expect([...subs.keys()]).toEqual(['/a']);
		expect(onSwitch).toHaveBeenCalledWith(null, '/a');
		expect(stream.status()).toBe('connecting');
		expect(stream.link()).toBeNull();

		// The registry's reports land in the signals — and in the caller's handlers.
		const a = subs.get('/a')!;
		a.onStatus?.('live');
		a.onLink?.('live');
		flush(); // Solid 2.0 queues signal writes
		expect(stream.status()).toBe('live');
		expect(stream.link()).toBe('live');
		expect(onStatus).toHaveBeenCalledWith('live');

		// A URL change: onSwitch first, the old subscription gone, the link reset.
		setUrl('/b');
		flush();
		expect(onSwitch).toHaveBeenLastCalledWith('/a', '/b');
		expect(unsubscribed).toEqual(['/a']);
		expect([...subs.keys()]).toEqual(['/b']);
		expect(stream.link()).toBeNull();

		// No URL: no subscription. Dispose: nothing left.
		setUrl(null);
		flush();
		expect(subs.size).toBe(0);
		dispose();
		flush();
		expect(subs.size).toBe(0);
	});

	it('tracks the cursor of the current stream: every frame moves it, a resync clears it, a URL change resets it', () => {
		const { solder, subs } = fakeSolder({ '/b': lingering('b-40') });
		const [url, setUrl] = createSignal<string | null>('/a');
		const onResync = vi.fn();
		let stream!: ReturnType<typeof createStream>;
		createRoot(() => {
			stream = createStream(solder, url, { onResync });
		});
		flush();
		expect(stream.lastEventId()).toBeNull();

		const a = subs.get('/a')!;
		a.onEvent?.(makeFrame('tick', '1', 'a-7'));
		flush();
		expect(stream.lastEventId()).toBe('a-7');
		// a frame without an id keeps the cursor in force — the frame says so
		a.onEvent?.(makeFrame('tick', '2', 'a-7'));
		flush();
		expect(stream.lastEventId()).toBe('a-7');
		a.onResync?.({ reason: 'unknown', earliest: null });
		flush();
		expect(stream.lastEventId()).toBeNull();
		expect(onResync).toHaveBeenCalledWith({ reason: 'unknown', earliest: null });
		a.onEvent?.(makeFrame('tick', '3', 'a-9'));
		flush();
		expect(stream.lastEventId()).toBe('a-9');

		// A hop to a URL whose source lingers in the registry starts from
		// ITS cursor, never from the previous stream's.
		setUrl('/b');
		flush();
		expect(stream.lastEventId()).toBe('b-40');
		setUrl('/c');
		flush();
		expect(stream.lastEventId()).toBeNull();
	});
});
