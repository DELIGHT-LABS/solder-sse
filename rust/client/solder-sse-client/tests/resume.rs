//! The existence proof, in Rust alone, against `solder-sse-testkit`: a
//! server that ends every stream after a few events, rotates it on a
//! timer, or resets the connection outright — a proxy timeout in
//! miniature — and a producer that never pauses; the client must still see
//! every event exactly once, in order, via `Last-Event-ID`. A server
//! restart costs one `resync`, never a silent gap.
use solder_sse::Cursor;
use solder_sse_client::{
    subscribe, Backoff, Cause, Deadman, Eager, Message, Options, Resync, Status, Subscription,
};
use solder_sse_testkit::{Chaos, Cut};
use std::sync::atomic::Ordering;
use std::time::Duration;

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
/// and gets live events only, so a producer must not start before it.
async fn wait_live(sub: &mut Subscription) {
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

/// `count` events numbered from `from`, one every 10ms, regardless of who
/// is connected; yields their cursors.
fn produce(chaos: Chaos, from: u64, count: u64) -> tokio::task::JoinHandle<Vec<Cursor>> {
    tokio::spawn(async move {
        let mut cursors = Vec::new();
        for i in from..from + count {
            cursors.push(chaos.publish(i).await);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cursors
    })
}

/// What the client saw: the event numbers in order, the reconnect causes
/// and the resyncs, collected until `count` events have arrived.
#[derive(Default)]
struct Seen {
    events: Vec<u64>,
    causes: Vec<Cause>,
    resyncs: usize,
}

async fn collect(sub: &mut Subscription, count: usize) -> Seen {
    let mut seen = Seen::default();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while seen.events.len() < count {
        let msg = tokio::time::timeout_at(deadline, sub.recv())
            .await
            .unwrap_or_else(|_| panic!("all {count} events within 20s; saw {:?}", seen.events))
            .expect("subscription alive");
        match msg {
            Message::Event(f) => seen.events.push(f.data.parse::<u64>().unwrap()),
            Message::Status(Status::Retrying { cause, .. }) => seen.causes.push(cause),
            Message::Resync(_) => seen.resyncs += 1,
            _ => {}
        }
    }
    seen
}

#[tokio::test]
async fn every_event_exactly_once_across_cuts() {
    let chaos = Chaos::new(Cut::After(5));
    let served = chaos.clone().serve().await;
    let mut sub = subscribe(
        reqwest::Client::new(),
        format!("{}/stream", served.base_url),
        fast(),
    );
    wait_live(&mut sub).await;

    let producer = produce(chaos.clone(), 1, 60);
    let seen = collect(&mut sub, 60).await;
    let cursors = producer.await.unwrap();
    assert_eq!(
        seen.events,
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
        chaos.stats.resumed_requests.load(Ordering::SeqCst) >= 10,
        "every reconnect after the first carried Last-Event-ID"
    );
    assert_eq!(
        sub.last_event_id().as_deref(),
        Some(cursors.last().unwrap().as_str()),
        "the cursor in force is the last event's, as the server issued it"
    );
}

#[tokio::test]
async fn a_rotated_stream_ends_cleanly_announces_its_age_and_resumes() {
    // Every response lives 1s; the producer runs ~1.5s, so at least one
    // rotation falls inside the run.
    let chaos = Chaos::new(Cut::Rotate(Duration::from_secs(1)));
    let served = chaos.clone().serve().await;
    let mut sub = subscribe(
        reqwest::Client::new(),
        format!("{}/stream", served.base_url),
        fast(),
    );
    wait_live(&mut sub).await;

    let producer = produce(chaos.clone(), 1, 150);
    let seen = collect(&mut sub, 150).await;
    producer.await.unwrap();
    assert_eq!(seen.events, (1..=150).collect::<Vec<_>>());
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
    let chaos = Chaos::new(Cut::Abort(5));
    let served = chaos.clone().serve().await;
    let mut sub = subscribe(
        reqwest::Client::new(),
        format!("{}/stream", served.base_url),
        fast(),
    );
    wait_live(&mut sub).await;

    let producer = produce(chaos.clone(), 1, 40);
    let seen = collect(&mut sub, 40).await;
    producer.await.unwrap();
    assert_eq!(
        seen.events,
        (1..=40).collect::<Vec<_>>(),
        "replay closed every gap"
    );
    assert_eq!(seen.resyncs, 0);
    assert!(
        !seen.causes.is_empty() && seen.causes.iter().all(|c| matches!(c, Cause::Transport(_))),
        "an abrupt close is a transport error, never a clean end: {:?}",
        seen.causes
    );
    assert!(chaos.stats.resumed_requests.load(Ordering::SeqCst) >= 5);
}

#[tokio::test]
async fn a_server_restart_is_one_resync_and_nothing_is_mistaken_for_old() {
    // Connections 1–4 carry events 1..=20 (five each); the 5th request
    // arrives with the 20th cursor at a server whose log is new →
    // `resync (unknown)`, cursor dropped, then the new log's events — under
    // cursors of a new generation, none of them one the client has seen.
    let chaos = Chaos::new(Cut::After(5)).restart_on_connection(5);
    let served = chaos.clone().serve().await;
    let mut sub = subscribe(
        reqwest::Client::new(),
        format!("{}/stream", served.base_url),
        fast(),
    );
    wait_live(&mut sub).await;

    let old = produce(chaos.clone(), 1, 20).await.unwrap();
    let before = collect(&mut sub, 20).await;
    assert_eq!(before.events, (1..=20).collect::<Vec<_>>());
    assert_eq!(before.resyncs, 0);

    // The 5th connection restarts the server; produce into the new log
    // only once that has happened.
    tokio::time::timeout(Duration::from_secs(5), chaos.restarted.notified())
        .await
        .expect("the reconnect after event 20 restarted the server");
    let producer = produce(chaos.clone(), 100, 5);
    let after = collect(&mut sub, 5).await;
    let new = producer.await.unwrap();
    assert_eq!(after.resyncs, 1, "exactly one resync for the restart");
    assert_eq!(after.events, (100..=104).collect::<Vec<_>>());
    assert!(
        new.iter().all(|c| !old.contains(c)),
        "a new generation shares no cursor with the old: {new:?}"
    );
    assert_eq!(
        sub.last_event_id().as_deref(),
        Some(new.last().unwrap().as_str())
    );
}

#[tokio::test]
async fn resync_is_surfaced_and_the_cursor_is_cleared() {
    let chaos = Chaos::new(Cut::After(1));
    let served = chaos.serve().await;
    let http = reqwest::Client::new();
    // A cursor the server never issued.
    let mut headers = http::HeaderMap::new();
    headers.insert("last-event-id", "deadbeef-999999".parse().unwrap());
    let mut sub = subscribe(
        http,
        format!("{}/stream", served.base_url),
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
        if let Message::Resync(resync) = msg {
            assert_eq!(resync, Resync::unknown());
            break;
        }
    }
    assert_eq!(sub.last_event_id(), None, "the rejected cursor is dropped");
}

#[tokio::test]
async fn a_503_waits_at_least_retry_after_and_a_ping_arms_the_deadman() {
    let served = Chaos::new(Cut::After(100)).serve().await;
    let mut sub = subscribe(
        reqwest::Client::new(),
        format!("{}/down", served.base_url),
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
        format!("{}/stream", served.base_url),
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

/// Over HTTP/2 a rotation ends a stream, not the connection. The testkit's
/// server accepts HTTP/2 by prior knowledge (there is no TLS in a test),
/// which is what ALPN negotiates in production.
#[cfg(feature = "http2")]
#[tokio::test]
async fn http2_streams_rotate_and_resume_like_any_other() {
    let chaos = Chaos::new(Cut::Rotate(Duration::from_millis(300)));
    let served = chaos.clone().serve().await;
    let http = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let head = http
        .get(format!("{}/down", served.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(head.version(), http::Version::HTTP_2);

    let mut sub = subscribe(http, format!("{}/stream", served.base_url), fast());
    wait_live(&mut sub).await;
    let producer = produce(chaos.clone(), 1, 100);
    let seen = collect(&mut sub, 100).await;
    producer.await.unwrap();
    assert_eq!(seen.events, (1..=100).collect::<Vec<_>>());
    assert_eq!(seen.resyncs, 0);
    assert!(
        !seen.causes.is_empty() && seen.causes.iter().all(|c| *c == Cause::Ended),
        "an HTTP/2 END_STREAM is a clean end: {:?}",
        seen.causes
    );
}

#[tokio::test]
async fn the_connect_timeout_bounds_the_response_head_not_the_stream() {
    // A healthy, quiet stream must outlive the connect timeout by any
    // margin: the timeout covers the request until the head arrives, and
    // the pings (every 20ms) keep the body's dead-man happy.
    let served = Chaos::new(Cut::Never)
        .with_keep_alive(Duration::from_millis(20))
        .serve()
        .await;
    let mut sub = subscribe(
        reqwest::Client::new(),
        format!("{}/stream", served.base_url),
        Options {
            connect_timeout: Duration::from_millis(300),
            ..Options::default()
        },
    );
    wait_live(&mut sub).await;
    let until = tokio::time::Instant::now() + Duration::from_millis(1_200);
    let mut pings = 0;
    while tokio::time::Instant::now() < until {
        match tokio::time::timeout_at(until, sub.recv()).await {
            Ok(Some(Message::Ping)) => pings += 1,
            Ok(Some(Message::Status(Status::Retrying { cause, .. }))) => {
                panic!("the stream was cut at the connect timeout: {cause:?}")
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!("subscription ended"),
            Err(_) => break,
        }
    }
    assert!(
        pings >= 10,
        "pings kept flowing well past the 300ms connect timeout: {pings}"
    );
}
