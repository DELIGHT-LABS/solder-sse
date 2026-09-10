# solder-sse

The resumable Server-Sent Events profile, on the wire. What a server and a client agree on, and
nothing either side does alone:

| Item | Where |
| --- | --- |
| A frame and its encoder — `id:`, `event:`, `data:`, `retry:`, comments, in a fixed field order; no payload can forge a field | `Event`, `Event::encode` |
| The cursor: an opaque token in the URL-unreserved alphabet, issued by a server's log and echoed by a client | `Cursor` |
| The profile's own frames and their payloads, encoded by the server and decoded by the client from one type each: the `ping` keep-alive (its interval and, when the server rotates streams, their age) and `resync` | `Event::ping`, `Ping`, `Event::resync`, `Resync`, `PING`, `RESYNC` |
| A specification-faithful parser: chunk boundaries do not matter, an empty `id:` resets the cursor, and a frame reports both the cursor in force and the `id:` line it carried itself | `Parser`, `Frame`, `Parsed` |
| Scheduling jitter without a random-number dependency: uniform draws and full-jitter backoff | `jitter::uniform`, `jitter::backoff_ms` |

The contract is `spec/profile-v1.md` at the repository root; `spec/vectors/*.sse` are golden frames
this crate encodes and decodes in `tests/vectors.rs`, and every other implementation checks the same
files. The server half is `solder-sse-server`, the client half `solder-sse-client`.

License: MIT or Apache-2.0, at your option.
