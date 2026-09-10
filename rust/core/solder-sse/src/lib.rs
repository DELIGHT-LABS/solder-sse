//! The resumable Server-Sent Events profile — the wire.
//!
//! What both halves of a stream agree on: the frames ([`Event`] and its
//! encoder), a specification-faithful parser ([`Parser`]) and the
//! scheduling jitter that keeps a fleet from moving in lockstep
//! ([`jitter`]). A server builds its responses from these
//! (`solder-sse-server`); a client reads them (`solder-sse-client`, or
//! the browser package). The contract itself is `spec/profile-v1.md` at
//! the repository root, with golden vectors every implementation is
//! checked against.
//!
//! The profile in one breath: every state change carries an `id:` — a
//! [`Cursor`], opaque to the client, issued by the server's log; a
//! reconnecting client presents it and the server replays what it missed
//! — or answers `resync` when it cannot; a named `ping` lets the client
//! run a dead-man timer; a jittered `retry:` and an optional rotation age
//! announced in the `ping` keep a fleet's reconnects spread and under the
//! server's control.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod cursor;
pub mod jitter;
pub mod parse;
pub mod protocol;

pub use cursor::{Cursor, InvalidCursor};
pub use parse::{Frame, Parsed, Parser};
pub use protocol::{Event, Ping, Resync, ResyncReason, PING, PING_EVERY_SECS, RESYNC};
