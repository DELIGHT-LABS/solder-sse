// solder-sse for SolidJS 2.0. An adapter does two things: bind a
// subscription to the owner's lifetime (and to a reactive URL), and expose
// the stream's status reactively. Everything else lives in solder-sse.
import { createEffect, createSignal, type Accessor } from 'solid-js';
import type { Handlers, LinkState, Solder, StreamStatus, SubscribeOptions } from 'solder-sse';

export interface Stream {
	/** The transport status — what the connection did. */
	status: Accessor<StreamStatus>;
	/** The link verdict — what a surface shows; undefined before the first. */
	link: Accessor<LinkState | undefined>;
	/** The cursor in force on the current stream: seeded from the registry
	 * when the URL is (re)entered, moved by every frame, cleared by a
	 * `resync` and by a URL change. Opaque — for display and tests. */
	lastEventId: Accessor<string | undefined>;
}

export interface StreamOptions extends SubscribeOptions {
	/** Runs when the URL changes (and once when it first becomes defined),
	 * BEFORE subscribing to the new one — the place to drop state derived
	 * from the previous stream so a screen never shows another's figures. */
	onSwitch?: (from: string | undefined, to: string | undefined) => void;
}

/** Subscribe for as long as the owner lives and `url()` is defined; a URL
 * change unsubscribes and subscribes again (the registry keeps the old
 * source lingering, so a hop back re-attaches without a reconnect). */
export function createStream(
	solder: Solder,
	url: Accessor<string | undefined>,
	handlers: Handlers,
	options: StreamOptions = {}
): Stream {
	const [status, setStatus] = createSignal<StreamStatus>('connecting');
	const [link, setLink] = createSignal<LinkState | undefined>();
	const [lastEventId, setLastEventId] = createSignal<string | undefined>();
	let current: string | undefined;
	// Solid 2.0 two-phase effect: the apply's return value is the cleanup,
	// run on dispose and before the next apply.
	createEffect(url, (next) => {
		if (next !== current) {
			options.onSwitch?.(current, next);
			current = next;
			setLink(undefined);
			// A cursor belongs to one stream. The new URL's may still be in
			// the registry (a lingering source a hop back re-attaches to).
			setLastEventId(next === undefined ? undefined : solder.inspect(next)?.lastEventId);
		}
		if (next === undefined) return;
		return solder.subscribe(
			next,
			{
				...handlers,
				onStatus: (s) => {
					setStatus(s);
					handlers.onStatus?.(s);
				},
				onLink: (l) => {
					setLink(l);
					handlers.onLink?.(l);
				},
				onEvent: (frame) => {
					setLastEventId(frame.lastEventId);
					handlers.onEvent?.(frame);
				},
				onResync: (info) => {
					setLastEventId(undefined);
					handlers.onResync?.(info);
				}
			},
			{ events: options.events }
		);
	});
	return { status, link, lastEventId };
}
