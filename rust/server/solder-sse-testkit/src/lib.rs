//! A server that ends streams every way an intermediary can, so a client —
//! in any language — can prove the profile against it.
//!
//! [`Chaos`] serves one topic. A producer publishes numbered events; each
//! connection ends the way its [`Cut`] says: cleanly after `n` events (a
//! `take`), by a reset with no terminating chunk (a proxy timeout, a
//! dropped network), by rotation (`max_age`), or never. A "restart" swaps
//! the log for a fresh one — a new generation, so every cursor the old
//! log issued is unknown: one `resync`, and none of the new cursors can be
//! mistaken for an old one. `GET /down` is the `503 + Retry-After` answer.
//!
//! Used by `solder-sse-client`'s end-to-end tests, and as a binary for a
//! browser or a foreign client (`solder-sse-testkit --cut rotate:8000`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::StreamExt;
use solder_sse::{Cursor, Event};
use solder_sse_axum::{unavailable, Resume, Sse};
use solder_sse_server::{
    broadcast, resumable, EventLog, MemoryLog, Published, SseResponseBuilder, StreamError,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast as tokio_broadcast, Notify, RwLock};
use tokio::task::JoinHandle;

/// How each connection ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cut {
    /// Never on purpose: the stream lives until the client leaves.
    Never,
    /// A clean end after this many events (a `take`).
    After(usize),
    /// A reset after this many events — no terminating chunk, the way a
    /// proxy timeout or a dropped network looks from the client.
    Abort(usize),
    /// Rotation: every response ends after exactly this lifetime.
    Rotate(Duration),
}

/// What the server saw, for a test to assert on.
#[derive(Debug, Default)]
pub struct Stats {
    /// Stream connections opened.
    pub connections: AtomicUsize,
    /// Of those, the ones that carried a cursor.
    pub resumed_requests: AtomicUsize,
}

/// The server: one topic, one way of ending connections.
#[derive(Clone)]
pub struct Chaos {
    /// Swapped for an empty one by [`Chaos::restart`].
    log: Arc<RwLock<Arc<MemoryLog<u64>>>>,
    bus: tokio_broadcast::Sender<Published<u64>>,
    cut: Cut,
    retry: Duration,
    keep_alive: Duration,
    restart_on_connection: usize,
    /// Notified when the server restarts.
    pub restarted: Arc<Notify>,
    /// Counters for assertions.
    pub stats: Arc<Stats>,
}

const TOPIC: &str = "t";
const REPLAY_LIMIT: usize = 500;

impl Chaos {
    /// A server that ends connections as `cut` says. Defaults: a `retry:`
    /// hint of 50ms and a `ping` every 200ms, so a test crosses many
    /// reconnects in a second; the log keeps 10 000 events for 60s.
    pub fn new(cut: Cut) -> Self {
        let (bus, _) = tokio_broadcast::channel(1024);
        Self {
            log: Arc::new(RwLock::new(Arc::new(Self::fresh_log()))),
            bus,
            cut,
            retry: Duration::from_millis(50),
            keep_alive: Duration::from_millis(200),
            restart_on_connection: 0,
            restarted: Arc::new(Notify::new()),
            stats: Arc::new(Stats::default()),
        }
    }

    fn fresh_log() -> MemoryLog<u64> {
        MemoryLog::new(10_000, Duration::from_secs(60))
    }

    /// Restart the server (swap the log) just before serving the `n`th
    /// connection; 0 never.
    pub fn restart_on_connection(mut self, n: usize) -> Self {
        self.restart_on_connection = n;
        self
    }

    /// The `retry:` hint every response opens with.
    pub fn with_retry(mut self, retry: Duration) -> Self {
        self.retry = retry;
        self
    }

    /// The keep-alive interval.
    pub fn with_keep_alive(mut self, every: Duration) -> Self {
        self.keep_alive = every;
        self
    }

    /// Publish one event; returns its cursor. The log and the bus are
    /// written under the log's read lock, in that order, so a subscriber
    /// never sees an event the log does not have.
    pub async fn publish(&self, event: u64) -> Cursor {
        let log = self.log.read().await;
        let cursor = log.append(TOPIC, event).await.expect("memory log");
        let _ = self.bus.send(Published {
            cursor: Some(cursor.clone()),
            event,
        });
        cursor
    }

    /// A process restart in miniature: every cursor the old log issued is
    /// unknown to the new, empty one, whose own cursors are a new
    /// generation.
    pub async fn restart(&self) {
        *self.log.write().await = Arc::new(Self::fresh_log());
        self.restarted.notify_one();
    }

    /// The routes: `GET /stream`, `GET /down`, `POST /publish` (the event
    /// as decimal text; answers its cursor), `POST /restart`.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/stream", get(stream))
            .route(
                "/down",
                get(|| async { unavailable(Duration::from_secs(1)) }),
            )
            .route("/publish", post(publish))
            .route("/restart", post(restart))
            .with_state(self.clone())
    }

    /// Serve on an ephemeral loopback port.
    pub async fn serve(self) -> Served {
        self.serve_on(SocketAddr::from(([127, 0, 0, 1], 0))).await
    }

    /// Serve on `addr`.
    pub async fn serve_on(self, addr: SocketAddr) -> Served {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("bind the testkit listener");
        let addr = listener.local_addr().expect("local addr");
        let router = self.router();
        let task = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                eprintln!("solder-sse-testkit: server ended: {e}");
            }
        });
        Served {
            base_url: format!("http://{addr}"),
            task,
        }
    }
}

/// A running server. Dropping it stops the server.
pub struct Served {
    /// `http://127.0.0.1:<port>`.
    pub base_url: String,
    task: JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `event: tick`, the cursor as `id:`, the number as `data:`.
fn frame(item: Result<Published<u64>, StreamError>) -> Event {
    match item {
        Ok(p) => Event::named("tick")
            .cursor(p.cursor)
            .data(p.event.to_string()),
        Err(e) => Event::named("error").data(e.to_string()),
    }
}

async fn stream(State(chaos): State<Chaos>, Resume(resume): Resume) -> Response {
    let n = chaos.stats.connections.fetch_add(1, Ordering::SeqCst) + 1;
    if n == chaos.restart_on_connection {
        chaos.restart().await;
    }
    if matches!(resume, solder_sse_server::Resume::Since(_)) {
        chaos.stats.resumed_requests.fetch_add(1, Ordering::SeqCst);
    }
    let log = chaos.log.read().await.clone();
    let r = resumable(
        &*log,
        TOPIC,
        resume,
        || broadcast::bridge(chaos.bus.subscribe()),
        REPLAY_LIMIT,
    )
    .await;
    let builder = SseResponseBuilder::new()
        .retry(chaos.retry)
        .keep_alive(chaos.keep_alive)
        .resync(r.resync);
    match chaos.cut {
        Cut::Never => {
            let events = r.stream.map(|item| Ok::<_, std::io::Error>(frame(item)));
            Sse(builder.build(events)).into_response()
        }
        Cut::After(after) => {
            let events = r
                .stream
                .take(after)
                .map(|item| Ok::<_, std::io::Error>(frame(item)));
            Sse(builder.build(events)).into_response()
        }
        Cut::Abort(after) => {
            // After `after` events the body yields an error: hyper drops the
            // connection without the terminating chunk.
            let events = r.stream.enumerate().map(move |(i, item)| {
                if i < after {
                    Ok(frame(item))
                } else {
                    Err(std::io::Error::other("cut by the intermediary"))
                }
            });
            Sse(builder.build(events)).into_response()
        }
        Cut::Rotate(age) => {
            let ms = age.as_millis() as u64;
            let events = r.stream.map(|item| Ok::<_, std::io::Error>(frame(item)));
            Sse(builder.max_age_jitter(ms..ms + 1).build(events)).into_response()
        }
    }
}

async fn publish(State(chaos): State<Chaos>, body: Bytes) -> Response {
    match std::str::from_utf8(&body)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
    {
        Some(event) => chaos.publish(event).await.to_string().into_response(),
        None => (StatusCode::BAD_REQUEST, "the event, as decimal text").into_response(),
    }
}

async fn restart(State(chaos): State<Chaos>) -> StatusCode {
    chaos.restart().await;
    StatusCode::NO_CONTENT
}
