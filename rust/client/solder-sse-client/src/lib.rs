//! The client half of the resumable SSE profile, for Rust programs.
//!
//! A subscription is a background task that keeps one stream alive for as
//! long as the [`Subscription`] lives:
//!
//! * every reconnect sends `Last-Event-ID` (the last cursor seen), so a
//!   server speaking the profile replays the gap,
//! * a healthy stream the far end cut is reopened at once (jittered ≤250ms);
//!   a transport error or a non-200 goes through full-jitter exponential
//!   backoff (1s → 30s), and a `503` honours `Retry-After` as the floor,
//! * the server's `retry:` hint replaces the base delay after a clean end,
//! * once the server has sent `ping`, silence longer than the dead-man
//!   window reopens the stream (a half-open connection is detected),
//! * `resync` is surfaced so the caller reloads a snapshot,
//! * what the server announces in `ping` — its keep-alive interval and,
//!   when it rotates streams, their lifetime — is kept as
//!   [`Subscription::server_hints`].
//!
//! ```ignore
//! let mut sub = subscribe(reqwest::Client::new(), url, Options::default());
//! while let Some(msg) = sub.recv().await {
//!     match msg {
//!         Message::Event(frame) => { /* frame.name, frame.data, frame.id */ }
//!         Message::Resync(_) => reload_snapshot().await,
//!         Message::Status(_) | Message::Ping => {}
//!     }
//! }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use futures_util::StreamExt;
use http::header::{HeaderMap, HeaderValue, ACCEPT, RETRY_AFTER};
use solder_sse::parse::{Frame, Parsed, Parser};
use solder_sse::{Ping, PING, RESYNC};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use solder_sse::parse;
pub use solder_sse::{Resync, ResyncReason};

/// Full-jitter exponential backoff parameters.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    /// First cap (attempt 0 draws from `0..base`).
    pub base: Duration,
    /// Largest cap.
    pub max: Duration,
    /// Never wait less than this.
    pub floor: Duration,
}

/// Reopen at once after a stream that had been live this long ends.
#[derive(Debug, Clone, Copy)]
pub struct Eager {
    /// A stream live for at least this long was cut by the far end.
    pub healthy: Duration,
    /// Spread the reopen over this window.
    pub jitter: Duration,
}

/// The dead-man window: how long a silence after a `ping` means half-open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Deadman {
    /// Follow the interval the server's `ping` announces (`2 × every + 5s`);
    /// 35s until it announces one. The profile's recommendation.
    #[default]
    Auto,
    /// A fixed window, whatever the server announces.
    Fixed(Duration),
    /// No dead-man: a half-open connection is left to the transport.
    Off,
}

/// Client behaviour. `Default` is the profile's recommendation.
#[derive(Debug, Clone)]
pub struct Options {
    /// Extra request headers (auth, for example).
    pub headers: HeaderMap,
    /// Time allowed for the response head to arrive. The stream itself is
    /// unbounded: its health is the dead-man's business.
    pub connect_timeout: Duration,
    /// Silence after a `ping` has been seen that means half-open.
    pub deadman: Deadman,
    /// Backoff after transport errors and non-200 responses.
    pub backoff: Backoff,
    /// Immediate reopen after a healthy stream ends. `None` disables.
    pub eager: Option<Eager>,
    /// Delay after a clean end when the server sent no `retry:` hint.
    pub default_retry: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            headers: HeaderMap::new(),
            connect_timeout: Duration::from_secs(20),
            deadman: Deadman::Auto,
            backoff: Backoff {
                base: Duration::from_secs(1),
                max: Duration::from_secs(30),
                floor: Duration::from_millis(200),
            },
            eager: Some(Eager {
                healthy: Duration::from_secs(5),
                jitter: Duration::from_millis(250),
            }),
            default_retry: Duration::from_secs(3),
        }
    }
}

/// Why the client is waiting before its next attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cause {
    /// The server ended the response on purpose: a rotation (`max_age`),
    /// a `take`, a deadline. A proxy timeout or a network drop usually
    /// shows as [`Cause::Transport`] instead — the response is cut without
    /// its terminating chunk; a proxy that ends the response cleanly lands
    /// here too, which a rotation-cadence check tells apart.
    Ended,
    /// The request or the read failed.
    Transport(String),
    /// A non-200 response (the browser would give up here).
    Http(u16),
    /// No `ping` within the dead-man window.
    Deadman,
}

/// Transport status, mirrored from the browser client's vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    /// A request is in flight.
    Connecting,
    /// The stream is open.
    Live,
    /// Waiting `after` before the next attempt.
    Retrying {
        /// The wait.
        after: Duration,
        /// Why.
        cause: Cause,
    },
}

/// What a subscription yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Transport status changes.
    Status(Status),
    /// A data event (named or default), with the last event id in force.
    Event(Frame),
    /// A keep-alive.
    Ping,
    /// The server could not replay from the cursor; reload a snapshot. Says
    /// why and, when the server named it, the oldest cursor it still holds
    /// — information for a log line: a cursor is opaque to a client.
    Resync(Resync),
}

/// What the server announced in its `ping` on the current (or last)
/// connection. Informational: the reconnect policy does not change for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ServerHints {
    /// `{"every": n}` — the keep-alive interval.
    pub ping_every: Option<Duration>,
    /// `{"max_age": n}` — the lifetime after which the server ends a
    /// healthy stream on purpose (rotation); `None` when it announces none.
    pub max_age: Option<Duration>,
}

impl From<Ping> for ServerHints {
    /// A zero on the wire announces nothing.
    fn from(ping: Ping) -> Self {
        let secs = |n: u64| (n > 0).then(|| Duration::from_secs(n));
        Self {
            ping_every: secs(ping.every),
            max_age: ping.max_age.and_then(secs),
        }
    }
}

#[derive(Default)]
struct Shared {
    last_event_id: Mutex<Option<String>>,
    hints: Mutex<ServerHints>,
}

/// A live subscription. Dropping it stops the background task.
pub struct Subscription {
    rx: mpsc::Receiver<Message>,
    handle: JoinHandle<()>,
    shared: Arc<Shared>,
}

impl Subscription {
    /// The next message; `None` once the task has stopped.
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    /// The cursor the next reconnect would send.
    pub fn last_event_id(&self) -> Option<String> {
        self.shared.last_event_id.lock().expect("cursor").clone()
    }

    /// What the server's latest `ping` announced (empty until one arrives).
    pub fn server_hints(&self) -> ServerHints {
        *self.shared.hints.lock().expect("hints")
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Subscribe to `url` on the given reqwest client. Messages are buffered
/// (256); a slow consumer applies back-pressure to the reader.
///
/// Build the client without a request `timeout`: reqwest's covers the
/// whole body and would end every healthy stream at that age. The response
/// head is bounded by [`Options::connect_timeout`], the stream by the
/// dead-man.
pub fn subscribe(http: reqwest::Client, url: impl Into<String>, options: Options) -> Subscription {
    let (tx, rx) = mpsc::channel(256);
    let shared = Arc::new(Shared::default());
    let handle = tokio::spawn(run(http, url.into(), options, tx, shared.clone()));
    Subscription { rx, handle, shared }
}

async fn run(
    http: reqwest::Client,
    url: String,
    options: Options,
    tx: mpsc::Sender<Message>,
    shared: Arc<Shared>,
) {
    let mut attempt: u32 = 0;
    let mut retry_hint: Option<Duration> = None;
    loop {
        if tx.send(Message::Status(Status::Connecting)).await.is_err() {
            return;
        }
        let mut req = http
            .get(&url)
            .header(ACCEPT, HeaderValue::from_static("text/event-stream"));
        for (k, v) in options.headers.iter() {
            req = req.header(k, v);
        }
        if let Some(id) = shared.last_event_id.lock().expect("cursor").clone() {
            match HeaderValue::from_str(&id) {
                Ok(value) => req = req.header("last-event-id", value),
                // Not a value this profile could have issued: connect
                // without it rather than fail every request from here on.
                Err(_) => tracing::warn!(
                    id,
                    "solder-sse-client: last event id is not a header value; not resuming"
                ),
            }
        }

        // The timeout covers the request up to the response head only; a
        // reqwest-level timeout would cover the body too and cut every
        // healthy stream at that age.
        let sent = tokio::time::timeout(options.connect_timeout, req.send())
            .await
            .map_err(|_| format!("no response within {:?}", options.connect_timeout))
            .and_then(|sent| sent.map_err(|e| e.to_string()));
        let (cause, wait) = match sent {
            Err(e) => {
                let wait = backoff(&options, attempt);
                attempt += 1;
                (Cause::Transport(e), wait)
            }
            Ok(resp) if resp.status() != reqwest::StatusCode::OK => {
                let status = resp.status().as_u16();
                let retry_after = resp
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(Duration::from_secs);
                let mut wait = backoff(&options, attempt);
                attempt += 1;
                if let Some(floor) = retry_after {
                    wait = wait.max(floor);
                }
                (Cause::Http(status), wait)
            }
            Ok(resp) => {
                let opened = Instant::now();
                if tx.send(Message::Status(Status::Live)).await.is_err() {
                    return;
                }
                attempt = 0;
                let outcome = read(resp, &options, &tx, &shared, &mut retry_hint).await;
                match outcome {
                    Read::Closed => return,
                    Read::Deadman => (Cause::Deadman, Duration::ZERO),
                    Read::Failed(e) => {
                        let wait = backoff(&options, attempt);
                        attempt += 1;
                        (Cause::Transport(e), wait)
                    }
                    Read::Ended => {
                        let lived = opened.elapsed();
                        let wait = match options.eager {
                            Some(eager) if lived >= eager.healthy => Duration::from_millis(
                                solder_sse::jitter::uniform(0..eager.jitter.as_millis() as u64),
                            ),
                            _ => retry_hint.unwrap_or(options.default_retry),
                        };
                        (Cause::Ended, wait)
                    }
                }
            }
        };
        tracing::debug!(url = %url, ?cause, ?wait, "solder-sse-client: reconnecting");
        if tx
            .send(Message::Status(Status::Retrying { after: wait, cause }))
            .await
            .is_err()
        {
            return;
        }
        tokio::time::sleep(wait).await;
    }
}

enum Read {
    /// The consumer went away.
    Closed,
    Ended,
    Failed(String),
    Deadman,
}

async fn read(
    resp: reqwest::Response,
    options: &Options,
    tx: &mpsc::Sender<Message>,
    shared: &Shared,
    retry_hint: &mut Option<Duration>,
) -> Read {
    // The cursor persists across connections: a frame without an id on the
    // new stream must not erase it (only an empty `id:` — resync — does).
    let mut parser =
        Parser::new().with_last_event_id(shared.last_event_id.lock().expect("cursor").clone());
    let mut body = resp.bytes_stream();
    let mut ping_seen = false;
    let (auto_deadman, mut deadman_window) = match options.deadman {
        Deadman::Auto => (true, Some(DEFAULT_DEADMAN)),
        Deadman::Fixed(window) => (false, Some(window)),
        Deadman::Off => (false, None),
    };
    loop {
        let timeout_duration = if ping_seen { deadman_window } else { None };
        let next = match timeout_duration {
            Some(window) => match tokio::time::timeout(window, body.next()).await {
                Ok(next) => next,
                Err(_) => return Read::Deadman,
            },
            None => body.next().await,
        };
        let chunk = match next {
            None => return Read::Ended,
            Some(Err(e)) => return Read::Failed(e.to_string()),
            Some(Ok(chunk)) => chunk,
        };
        for parsed in parser.feed(&chunk) {
            let message = match parsed {
                Parsed::Retry(d) => {
                    *retry_hint = Some(d);
                    continue;
                }
                Parsed::Event(frame) => {
                    *shared.last_event_id.lock().expect("cursor") =
                        parser.last_event_id().map(str::to_owned);
                    match frame.name.as_deref() {
                        Some(PING) => {
                            ping_seen = true;
                            // A payload the wire type cannot read announces nothing.
                            let hints = serde_json::from_str::<Ping>(&frame.data)
                                .map_or_else(|_| ServerHints::default(), ServerHints::from);
                            *shared.hints.lock().expect("hints") = hints;
                            if auto_deadman {
                                if let Some(every) = hints.ping_every {
                                    deadman_window = Some(deadman_for(every));
                                }
                            }
                            Message::Ping
                        }
                        Some(RESYNC) => Message::Resync(
                            serde_json::from_str(&frame.data).unwrap_or_else(|_| Resync::unknown()),
                        ),
                        _ => Message::Event(frame),
                    }
                }
            };
            if tx.send(message).await.is_err() {
                return Read::Closed;
            }
        }
    }
}

/// The dead-man window until the `ping` announces an interval (profile v1: 35s).
const DEFAULT_DEADMAN: Duration = Duration::from_secs(35);

/// The dead-man window for a keep-alive interval: `2 × every + 5s`.
fn deadman_for(every: Duration) -> Duration {
    Duration::from_secs(2 * every.as_secs() + 5)
}

fn backoff(options: &Options, attempt: u32) -> Duration {
    Duration::from_millis(solder_sse::jitter::backoff_ms(
        attempt,
        options.backoff.base.as_millis() as u64,
        options.backoff.max.as_millis() as u64,
        options.backoff.floor.as_millis() as u64,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ping_becomes_hints_and_a_zero_announces_nothing() {
        let hints = |every, max_age| ServerHints::from(Ping { every, max_age });
        assert_eq!(
            hints(15, Some(30)),
            ServerHints {
                ping_every: Some(Duration::from_secs(15)),
                max_age: Some(Duration::from_secs(30)),
            }
        );
        assert_eq!(hints(15, None).max_age, None);
        assert_eq!(hints(15, Some(0)).max_age, None);
        assert_eq!(hints(0, None), ServerHints::default());
        assert_eq!(
            deadman_for(Duration::from_secs(15)),
            Duration::from_secs(35)
        );
        assert_eq!(
            deadman_for(Duration::from_secs(10)),
            Duration::from_secs(25)
        );
    }

    #[test]
    fn defaults_match_the_profile() {
        let o = Options::default();
        assert_eq!(o.deadman, Deadman::Auto);
        assert_eq!(o.backoff.max, Duration::from_secs(30));
    }
}
