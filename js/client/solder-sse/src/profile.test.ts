import { readFileSync, readdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import { parsePing, PING, RESYNC, RESUME_QUERY } from './protocol.ts';

// `spec/` at the repository root — the same files every implementation, in
// every language, is checked against. From this module's directory when
// the runtime exposes it, else from the `js/` workspace vitest runs in.
const spec = `${import.meta.dirname ? join(import.meta.dirname, '..', '..', '..', '..', 'spec') : resolve(process.cwd(), '..', 'spec')}/`;
const profile = JSON.parse(readFileSync(`${spec}profile.json`, 'utf8')) as {
	events: { ping: string; resync: string };
	resume: { query: string };
	deadmanSeconds: number;
};

/** A minimal, spec-shaped reader for the golden vectors: one frame per
 * blank line, `field: value` per line, `data` lines joined. */
function frames(text: string) {
	return text
		.split('\n\n')
		.filter((f) => f.length > 0)
		.map((f) => {
			const out: Record<string, string> = {};
			for (const line of f.split('\n')) {
				const i = line.indexOf(':');
				const key = line.slice(0, i);
				const value = line.slice(i + 1).replace(/^ /, '');
				out[key] = key in out && key === 'data' ? `${out[key]}\n${value}` : value;
			}
			return out;
		});
}

describe('profile v1', () => {
	it("the package constants are the profile's", () => {
		expect(PING).toBe(profile.events.ping);
		expect(RESYNC).toBe(profile.events.resync);
		expect(RESUME_QUERY).toBe(profile.resume.query);
		expect(profile.deadmanSeconds * 1000).toBe(35_000);
	});

	it('the golden vectors read as the frames the server side encodes', () => {
		const read = (name: string) => frames(readFileSync(`${spec}vectors/${name}.sse`, 'utf8'));
		expect(read('connect')).toEqual([{ retry: '750' }, { event: 'ping', data: '{"every":15}' }]);
		expect(read('event')).toEqual([{ id: '48211', event: 'new_message', data: '{"seq":48211}' }]);
		expect(read('resync')).toEqual([
			{ id: '', event: 'resync', data: '{"earliest_seq":47900,"reason":"expired"}' }
		]);
		expect(read('multiline')).toEqual([{ id: '5', event: 'note', data: 'line one\nline two' }]);
		// v1.2: the ping may announce the rotation age; this package reads it
		// as information and keeps its dead-man on `every`.
		const pingMaxAge = read('ping-max-age');
		expect(pingMaxAge).toEqual([{ event: 'ping', data: '{"every":15,"max_age":30}' }]);
		expect(parsePing(pingMaxAge[0].data)).toEqual({ everyMs: 15_000, maxAgeMs: 30_000 });
		expect(parsePing('{"every":15}')).toEqual({ everyMs: 15_000, maxAgeMs: null });
		expect(readdirSync(`${spec}vectors`).sort()).toEqual([
			'connect.sse',
			'event.sse',
			'multiline.sse',
			'ping-max-age.sse',
			'resync.sse'
		]);
	});
});
