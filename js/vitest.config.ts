import { defineConfig } from 'vitest/config';

// The browser client is tested against a DOM (`window`, `document`,
// `MessageEvent`): jsdom, the environment its consumers run it in. The
// same goes for the packages it is tested with — `solid-js` resolves to
// its server build under Node's conditions, where effects never run.
export default defineConfig({
	resolve: { conditions: ['browser'] },
	test: { environment: 'jsdom', include: ['client/*/src/**/*.test.ts'] }
});
