# solder-sse-solid

SolidJS 2.0 adapter for `solder-sse`. An adapter does two things: bind a subscription to the
owner's lifetime (and to a reactive URL), and expose the stream's status reactively.

```ts
import { createStream } from 'solder-sse-solid';

const { status, lastEventId } = createStream(
	solder,
	() => `/topics/${topic()}/stream`,
	{
		onEvent: (frame) => {
			/* frame.name, frame.json() */
		}
	},
	{
		events: ['snapshot', 'tick'],
		// Runs before subscribing to a new URL: drop state derived from the old one.
		onSwitch: () => reset()
	}
);
```

The subscription lives while the owner does and the URL accessor is non-null; a URL change
unsubscribes and subscribes again (the registry keeps the old source lingering, so a hop back
re-attaches without a reconnect). Uses Solid 2.0's two-phase `createEffect`, whose apply
return value is the cleanup.
\n\nLicense: MIT or Apache-2.0, at your option.\n
