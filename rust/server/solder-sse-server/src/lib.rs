//! Resumable Server-Sent Events — the server half.
//!
//! A stream can be cut at any moment: load-balancer idle timeouts, gRPC
//! deadlines, mobile networks, a laptop lid. This crate serves the
//! profile in `solder-sse` so that a cut costs nothing:
//!
//! * every real event carries an `id:` — a [`Cursor`] the [`EventLog`]
//!   issued, opaque to the client,
//! * a reconnecting client sends `Last-Event-ID` (the browser does this by
//!   itself) or `?last_event_id=` (for clients that cannot set headers),
//! * the server replays everything after that cursor from the bounded log,
//!   then continues live — or, when it cannot, says so with a `resync`
//!   event so the client reloads a snapshot instead of silently missing
//!   data,
//! * a named `ping` keep-alive lets the client run a dead-man timer
//!   (comment keep-alives are invisible to `EventSource`),
//! * a `retry:` hint with server-side jitter spreads a fleet's reconnects,
//! * an optional `max_age` ends a healthy stream on purpose (rotation), so
//!   connections turn over under the server's control — jittered, at a
//!   frame boundary, announced in `ping` — rather than at a proxy's timeout.
//!
//! The crate speaks [`http`] and [`http_body`] only. The response it builds
//! is an `http::Response<SseBody>` that hyper serves directly; framework
//! adapters (axum, actix, …) are a newtype each — see `solder-sse-axum`.
//!
//! ```ignore
//! let resume = Resume::from_parts(&parts);
//! // resumable() subscribes first (the closure), then reads the log.
//! let resumed = resumable(&*log, "topic", resume, || broadcast::bridge(bus.subscribe()), 500).await;
//! let response = SseResponseBuilder::new()
//!     .retry_jitter(500..1000)
//!     .keep_alive(Duration::from_secs(15))
//!     .max_age(Duration::from_secs(20))
//!     .resync(resumed.resync)
//!     .build(resumed.stream.map(|r| r.map(|p| Event::named("tick").cursor(p.cursor).data(p.event))));
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod log;
pub mod response;
pub mod resume;
pub mod stream;

pub use log::{Cursor, EventLog, LogError, MemoryLog, Published, Replay};
pub use response::{unavailable, SseBody, SseResponse, SseResponseBuilder};
pub use resume::Resume;
pub use stream::{broadcast, resumable, Resumed, StreamError};

/// The wire this server speaks, re-exported so a handler needs one `use`.
pub use solder_sse::{Event, Resync, ResyncReason, PING, RESYNC};
