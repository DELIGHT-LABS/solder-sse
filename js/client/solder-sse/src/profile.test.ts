import { readFileSync, readdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import { parsePing, parseResync, PING, RESYNC, RESUME_QUERY, withResume } from './protocol.ts';

// `spec/` at the repository root — the same files every implementation, in
// every language, is checked against. From this module's directory when
// the runtime exposes it, else from the `js/` workspace vitest runs in.
const spec = `${import.meta.dirname ? join(import.meta.dirname, '..', '..', '..', '..', 'spec') : resolve(process.cwd(), '..', 'spec')}/`;
const profile = JSON.parse(readFileSync(`${spec}profile.json`, 'utf8')) as {
	version: number;
	events: { ping: string; resync: string };
	resume: { query: string };
	cursor: { alphabet: string; maxLength: number };
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
		expect(profile.version).toBe(1);
		expect(PING).toBe(profile.events.ping);
		expect(RESYNC).toBe(profile.events.resync);
		expect(RESUME_QUERY).toBe(profile.resume.query);
		expect(profile.deadmanSeconds * 1000).toBe(35_000);
	});

	it('the resume query carries a cursor as it is — the server does not decode it', () => {
		// Every character of the cursor alphabet is left alone by
		// encodeURIComponent, so what the server reads is the token it issued.
		const cursor = 'Az09-._~';
		expect(cursor).toMatch(new RegExp(`^[${profile.cursor.alphabet}]+$`));
		expect(withResume('/s', RESUME_QUERY, cursor)).toBe(`/s?last_event_id=${cursor}`);
		expect(withResume('/s?x=1', RESUME_QUERY, cursor)).toBe(`/s?x=1&last_event_id=${cursor}`);
		// Anything else is not a cursor; it is encoded so the URL stays valid
		// and the server answers `resync`.
		expect(withResume('/s', RESUME_QUERY, 'a b/c')).toBe('/s?last_event_id=a%20b%2Fc');
	});

	it('the golden vectors read as the frames the server side encodes', () => {
		const read = (name: string) => frames(readFileSync(`${spec}vectors/${name}.sse`, 'utf8'));
		expect(read('connect')).toEqual([{ retry: '750' }, { event: 'ping', data: '{"every":15}' }]);
		// The cursor is opaque: a composite token here, a plain number in
		// `multiline`; the client echoes either and reads neither.
		expect(read('event')).toEqual([
			{ id: '7f3a9c2e-48211', event: 'new_message', data: '{"n":48211}' }
		]);
		expect(read('multiline')).toEqual([{ id: '5', event: 'note', data: 'line one\nline two' }]);
		const cursor = new RegExp(`^[${profile.cursor.alphabet}]{1,${profile.cursor.maxLength}}$`);
		for (const id of ['7f3a9c2e-48211', '5']) expect(id).toMatch(cursor);
		expect('a b').not.toMatch(cursor);

		const [resync] = read('resync');
		expect(resync).toEqual({
			id: '',
			event: 'resync',
			data: '{"reason":"expired","earliest":"7f3a9c2e-47900"}'
		});
		expect(parseResync(resync.data)).toEqual({ reason: 'expired', earliest: '7f3a9c2e-47900' });
		const [unknown] = read('resync-unknown');
		expect(unknown).toEqual({
			id: '',
			event: 'resync',
			data: '{"reason":"unknown","earliest":null}'
		});
		expect(parseResync(unknown.data)).toEqual({ reason: 'unknown', earliest: null });
		expect(parseResync('nope')).toEqual({ reason: 'unknown', earliest: null });

		// The ping may announce the rotation age; this package reads it as
		// information and keeps its dead-man on `every`.
		const pingMaxAge = read('ping-max-age');
		expect(pingMaxAge).toEqual([{ event: 'ping', data: '{"every":15,"max_age":30}' }]);
		expect(parsePing(pingMaxAge[0].data)).toEqual({ everyMs: 15_000, maxAgeMs: 30_000 });
		expect(parsePing('{"every":15}')).toEqual({ everyMs: 15_000, maxAgeMs: null });
		expect(readdirSync(`${spec}vectors`).sort()).toEqual([
			'connect.sse',
			'event.sse',
			'multiline.sse',
			'ping-max-age.sse',
			'resync-unknown.sse',
			'resync.sse'
		]);
	});
});
