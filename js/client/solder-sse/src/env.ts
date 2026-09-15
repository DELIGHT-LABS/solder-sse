// Everything the core touches in its environment, behind minimal interfaces
// so it runs in a window, a worker (inject nothing for visibility/online),
// or a test (inject fakes). Globals are resolved at USE, not at creation:
// a test that stubs `EventSource` after importing the app still wins.

export interface VisibilityLike {
	readonly visibilityState: 'visible' | 'hidden' | (string & {});
	addEventListener(type: 'visibilitychange', listener: () => void): void;
	removeEventListener(type: 'visibilitychange', listener: () => void): void;
}

export interface OnlineLike {
	addEventListener(type: 'online', listener: () => void): void;
	removeEventListener(type: 'online', listener: () => void): void;
}

export interface Environment {
	EventSource?: typeof EventSource;
	/** `document`-like. `null` disables visibility handling (a worker). */
	visibility?: VisibilityLike | null;
	/** `window`-like. `null` disables the `online` accelerator. */
	online?: OnlineLike | null;
	now?: () => number;
	/** Uniform in [0, 1) — inject a constant for deterministic tests. */
	random?: () => number;
}

export interface ResolvedEnv {
	eventSource(): typeof EventSource | undefined;
	visibility(): VisibilityLike | null;
	online(): OnlineLike | null;
	hidden(): boolean;
	now(): number;
	random(): number;
}

export function resolveEnv(options: Environment): ResolvedEnv {
	const visibility = () =>
		options.visibility === undefined
			? typeof document === 'undefined'
				? null
				: (document as VisibilityLike)
			: options.visibility;
	return {
		eventSource: () => options.EventSource ?? globalThis.EventSource,
		visibility,
		online: () =>
			options.online === undefined
				? typeof window === 'undefined'
					? null
					: (window as OnlineLike)
				: options.online,
		hidden: () => visibility()?.visibilityState === 'hidden',
		now: options.now ?? (() => Date.now()),
		random: options.random ?? Math.random
	};
}
