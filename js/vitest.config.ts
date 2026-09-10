import { defineConfig } from 'vitest/config';

// The browser client is tested against a DOM (`window`, `document`,
// `MessageEvent`): jsdom, the same environment its consumers run it in.
export default defineConfig({
	test: { environment: 'jsdom', include: ['client/*/src/**/*.test.ts'] }
});
