import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

// The browser client is tested against a DOM (`window`, `document`,
// `MessageEvent`): jsdom, the same environment its consumers run it in.
//
// Inside this repository `solder-sse` is its source, not its `dist/`: the
// published package points only at built files, so the root tsconfig's
// `paths` (the type checker, the type-aware linter) and this alias (the
// tests) resolve it to `client/solder-sse/src` — nothing has to be built.
// Each package's `tsconfig.build.json` is for emitting `dist/` only.
export default defineConfig({
	resolve: {
		alias: {
			'solder-sse': fileURLToPath(new URL('./client/solder-sse/src/index.ts', import.meta.url))
		}
	},
	test: { environment: 'jsdom', include: ['client/*/src/**/*.test.ts'] }
});
