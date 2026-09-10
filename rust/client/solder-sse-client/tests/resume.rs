//! The existence proof, in Rust alone: a server that ends every stream
//! after a few events, rotates it on a timer, or resets the connection
//! outright — a proxy timeout in miniature — and a producer that never
//! pauses; the client must still see every sequence exactly once, in
//! order, via `Last-Event-ID`. A server restart costs one `resync`, never a
//! silent gap.
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::StreamExt;
use solder_sse::{broadcast, resumable, Event, EventLog, MemoryLog, Published, SseResponseBuilder};
use solder_sse_axum::{unavailable, Resume, Sse};
use solder_sse_client::{subscribe, Backoff, Cause, Deadman, Eager, Message, Options, Status};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast as tokio_broadcast, Notify, RwLock};

/// How the server ends each connection.
#[derive(Clone, Copy)]
enum Cut {
    /// End the response cleanly after this many events (a `take`).
    After(usize),
    /// Reset the connection after this many events — no terminating chunk,
    /// the way a proxy timeout or a network drop looks from the client.
    Abort(usize),
    /// Rotate: end every response after this lifetime (`max_age`).
    Rotate(Duration),
}

#[derive(Clone)]
struct App {
    /// Swapped for an empty one by [`App::restart`] — a process restart in
    /// miniature: every cursor the old log issued is unknown to the new.
    log: Arc<RwLock<Arc<MemoryLog<u64>>>>,
    bus: tokio_broadcast::Sender<Published<u64>>,
    cut: Cut,
    /// Restart the server (swap the log) just before serving this
    /// connection; 0 never.
    restart_on_connection: usize,
    restarted: Arc<Notify>,
    connections: Arc<AtomicUsize>,
    resumed_requests: Arc<AtomicUsize>,
}

impl App {
    fn new(cut: Cut) -> Self {
        let (bus, _) = tokio_broadcast::channel(1024);
        Self {
            log: Arc::new(RwLock::new(Arc::new(MemoryLog::new(
                10_000,
                Duration::from_secs(60),
            )))),
            bus,
            cut,
            restart_on_connection: 0,
            restarted: Arc::new(Notify::new()),
            connections: Arc::new(AtomicUsize::new(0)),
            resumed_requests: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn publish(&self, event: u64) {
        let log = self.log.read().await.clone();
        let seq = log.append("t", event).await.unwrap();
        let _ = self.bus.send(Published { seq, event });
    }

    async fn restart(&self) {
        *self.log.write().await = Arc::new(MemoryLog::new(10_000, Duration::from_secs(60)));
        self.restarted.notify_one();
    }
}

async fn stream(State(app): State<App>, Resume(resume): Resume) -> Response {
    let n = app.connections.fetch_add(1, Ordering::SeqCst) + 1;
    if n == app.restart_on_connection {
        app.restart().await;
    }
    if matches!(resume, solder_sse::Resume::Since(_)) {
        app.resumed_requests.fetch_add(1, Ordering::SeqCst);
    }
    let log = app.log.read().await.clone();
    let r = resumable(
        &*log,
        "t",
        resume,
        || broadcast::bridge(app.bus.subscribe()),
        500,
    )
    .await;
    let frame = |item: Result<Published<u64>, solder_sse::StreamError>| match item {
        Ok(p) => Event::named("n").seq(p.seq).data(p.event.to_string()),
        Err(e) => Event::named("error").data(e.to_string()),
    };
    let builder = SseResponseBuilder::new()
        .retry(Duration::from_millis(50))
        .keep_alive(Duration::from_millis(200))
        .resync(r.resync);
    match app.cut {
        Cut::After(after) => {
            let events = r
                .stream
                .take(after)
                .map(move |item| Ok::<_, std::io::Error>(frame(item)));
            Sse(builder.build(events)).into_response()
        }
        Cut::Abort(after) => {
            // After `after` events the body yields an error: hyper drops the
            // connection without the terminating chunk.
            let events = r.stream.enumerate().map(move |(i, item)| {
                if i < after {
                    Ok(frame(item))
                } else {
                    Err(std::io::Error::other("cut by the proxy"))
                }
            });
            Sse(builder.build(events)).into_response()
        }
        Cut::Rotate(age) => {
            let ms = age.as_millis() as u64;
            let events = r
                .stream
                .map(move |item| Ok::<_, std::io::Error>(frame(item)));
            Sse(builder.max_age_jitter(ms..ms + 1).build(events)).into_response()
        }
    }
}

async fn serve(app: App) -> String {
    let router = Router::new()
        .route("/stream", get(stream))
        .route(
            "/down",
            get(|| async { unavailable(Duration::from_secs(1)) }),
        )
        .with_state(app);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}

/// Reopen at once after any end, and back off in milliseconds, so a test
/// crosses many reconnects in a second.
fn fast() -> Options {
    Options {
        eager: Some(Eager {
            healthy: Duration::ZERO,
            jitter: Duration::from_millis(5),
        }),
        backoff: Backoff {
            base: Duration::from_millis(10),
            max: Duration::from_millis(50),
            floor: Duration::from_millis(5),
        },
        ..Options::default()
    }
}

/// Wait until the subscription is live: a first connection has no cursor
/// and gets live events only (a real server sends its connect snapshot
/// there), so a producer must not start before it.
async fn wait_live(sub: &mut solder_sse_client::Subscription) {
    loop {
        match sub.recv().await.unwrap() {
            Message::Status(Status::Live) => break,
            Message::Status(Status::Retrying { cause, .. }) => {
                panic!("first open failed: {cause:?}")
            }
            _ => {}
        }
    }
}

/// `count` events, one every 10ms, regardless of who is connected.
fn produce(app: App, from: u64, count: u64) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        for i in from..from + count {
            app.publish(i).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
}

/// What the client saw: the sequences in order, the reconnect causes and
/// the resyncs, collected until `count` events have arrived.
#[derive(Default)]
struct Seen {
    seqs: Vec<u64>,
    causes: Vec<Cause>,
    resyncs: usize,
}

async fn collect(sub: &mut solder_sse_client::Subscription, count: usize) -> Seen {
    let mut seen = Seen::default();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while seen.seqs.len() < count {
        let msg = tokio::time::timeout_at(deadline, sub.recv())
            .await
            .unwrap_or_else(|_| panic!("all {count} events within 20s; saw {:?}", seen.seqs))
            .expect("subscription alive");
        match msg {
            Message::Event(f) => seen.seqs.push(f.id.unwrap().parse::<u64>().unwrap()),
            Message::Status(Status::Retrying { cause, .. }) => seen.causes.push(cause),
            Message::Resync { .. } => seen.resyncs += 1,
            _ => {}
        }
    }
    seen
}

#[tokio::test]
async fn every_sequence_exactly_once_across_cuts() {
    let app = App::new(Cut::After(5));
    let base = serve(app.clone()).await;
    let mut sub = subscribe(reqwest::Client::new(), format!("{base}/stream"), fast());
    wait_live(&mut sub).await;

    let producer = produce(app.clone(), 1, 60);
    let seen = collect(&mut sub, 60).await;
    producer.await.unwrap();
    assert_eq!(
        seen.seqs,
        (1..=60).collect::<Vec<_>>(),
        "contiguous, no repeats"
    );
    assert_eq!(seen.resyncs, 0);
    assert!(
        seen.causes.iter().all(|c| *c == Cause::Ended),
        "a server-side end is not a transport error: {:?}",
        seen.causes
    );
    assert!(
        app.resumed_requests.load(Ordering::SeqCst) >= 10,
        "every reconnect after the first carried Last-Event-ID"
    );
    assert_eq!(sub.last_event_id().as_deref(), Some("60"));
}

#[tokio::test]
async fn a_rotated_stream_ends_cleanly_announces_its_age_and_resumes() {
    // Every response lives 1s; the producer runs ~1.5s, so at least one
    // rotation falls inside the run.
    let app = App::new(Cut::Rotate(Duration::from_secs(1)));
    let base = serve(app.clone()).await;
    let mut sub = subscribe(reqwest::Client::new(), format!("{base}/stream"), fast());
    wait_live(&mut sub).await;

    let producer = produce(app.clone(), 1, 150);
    let seen = collect(&mut sub, 150).await;
    producer.await.unwrap();
    assert_eq!(seen.seqs, (1..=150).collect::<Vec<_>>());
    assert_eq!(seen.resyncs, 0, "one lifetime is well inside retention");
    assert!(
        !seen.causes.is_empty() && seen.causes.iter().all(|c| *c == Cause::Ended),
        "a rotation is an ordinary end: {:?}",
        seen.causes
    );
    assert_eq!(
        sub.server_hints().max_age,
        Some(Duration::from_secs(1)),
        "the ping announced the nominal age"
    );
}

#[tokio::test]
async fn an_aborted_connection_is_a_transport_error_and_still_resumes() {
    // The body errors after 5 events: no terminating chunk, the way a
    // load-balancer timeout or a dropped network looks from the client.
    let app = App::new(Cut::Abort(5));
    let base = serve(app.clone()).await;
    let mut sub = subscribe(reqwest::Client::new(), format!("{base}/stream"), fast());
    wait_live(&mut sub).await;

    let producer = produce(app.clone(), 1, 40);
    let seen = collect(&mut sub, 40).await;
    producer.await.unwrap();
    assert_eq!(
        seen.seqs,
        (1..=40).collect::<Vec<_>>(),
        "replay closed every gap"
    );
    assert_eq!(seen.resyncs, 0);
    assert!(
        !seen.causes.is_empty() && seen.causes.iter().all(|c| matches!(c, Cause::Transport(_))),
        "an abrupt close is a transport error, never a clean end: {:?}",
        seen.causes
    );
    assert!(app.resumed_requests.load(Ordering::SeqCst) >= 5);
}

#[tokio::test]
async fn a_server_restart_is_one_resync_and_the_new_sequences_follow() {
    // Connections 1–4 carry 1..=20 (five each); the 5th request arrives
    // with cursor 20 at a server whose log is new and empty → `resync
    // (unknown)`, cursor dropped, then the new log's sequences from 1.
    let mut app = App::new(Cut::After(5));
    app.restart_on_connection = 5;
    let base = serve(app.clone()).await;
    let mut sub = subscribe(reqwest::Client::new(), format!("{base}/stream"), fast());
    wait_live(&mut sub).await;

    produce(app.clone(), 1, 20).await.unwrap();
    let before = collect(&mut sub, 20).await;
    assert_eq!(before.seqs, (1..=20).collect::<Vec<_>>());
    assert_eq!(before.resyncs, 0);

    // The 5th connection restarts the server; produce into the new log
    // only once that has happened.
    tokio::time::timeout(Duration::from_secs(5), app.restarted.notified())
        .await
        .expect("the reconnect after event 20 restarted the server");
    let producer = produce(app.clone(), 100, 5);
    let after = collect(&mut sub, 5).await;
    producer.await.unwrap();
    assert_eq!(after.resyncs, 1, "exactly one resync for the restart");
    assert_eq!(
        after.seqs,
        (1..=5).collect::<Vec<_>>(),
        "the new log numbers from one again"
    );
    assert_eq!(sub.last_event_id().as_deref(), Some("5"));
}

#[tokio::test]
async fn resync_is_surfaced_and_the_cursor_is_cleared() {
    let app = App::new(Cut::After(1));
    let base = serve(app).await;
    let http = reqwest::Client::new();
    // A cursor the server never issued.
    let mut headers = http::HeaderMap::new();
    headers.insert("last-event-id", "999999".parse().unwrap());
    let mut sub = subscribe(
        http,
        format!("{base}/stream"),
        Options {
            headers,
            ..Options::default()
        },
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let msg = tokio::time::timeout_at(deadline, sub.recv())
            .await
            .expect("resync within 5s")
            .unwrap();
        if let Message::Resync {
            reason,
            earliest_seq,
        } = msg
        {
            assert_eq!(reason, "unknown");
            assert_eq!(earliest_seq, 0);
            break;
        }
    }
    assert_eq!(sub.last_event_id(), None, "the rejected cursor is dropped");
}

#[tokio::test]
async fn a_503_waits_at_least_retry_after_and_a_ping_arms_the_deadman() {
    let app = App::new(Cut::After(100));
    let base = serve(app).await;
    let mut sub = subscribe(
        reqwest::Client::new(),
        format!("{base}/down"),
        Options::default(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let msg = tokio::time::timeout_at(deadline, sub.recv())
            .await
            .unwrap()
            .unwrap();
        if let Message::Status(Status::Retrying { after, cause }) = msg {
            assert_eq!(cause, Cause::Http(503));
            assert!(
                after >= Duration::from_secs(1),
                "Retry-After is the floor: {after:?}"
            );
            break;
        }
    }
    drop(sub);

    // A quiet stream that pings every 200ms: the dead-man (300ms) must NOT
    // fire while pings arrive…
    let mut sub = subscribe(
        reqwest::Client::new(),
        format!("{base}/stream"),
        Options {
            deadman: Deadman::Fixed(Duration::from_millis(300)),
            ..Options::default()
        },
    );
    let mut pings = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while pings < 3 {
        match tokio::time::timeout_at(deadline, sub.recv())
            .await
            .unwrap()
            .unwrap()
        {
            Message::Ping => pings += 1,
            Message::Status(Status::Retrying { cause, .. }) => {
                panic!("reconnected while pings were flowing: {cause:?}")
            }
            _ => {}
        }
    }
    // …and a sub-second keep-alive announces the profile default, no age.
    assert_eq!(
        sub.server_hints(),
        solder_sse_client::ServerHints {
            ping_every: Some(Duration::from_secs(15)),
            max_age: None
        }
    );
}
