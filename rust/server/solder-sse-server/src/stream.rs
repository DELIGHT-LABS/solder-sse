//! Replay ∪ live, with the gap closed.

use crate::log::{Cursor, EventLog, LogError, Published, Replay};
use crate::resume::Resume;
use futures_core::Stream;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use solder_sse::Resync;
use std::collections::HashSet;

/// Why the live stream stopped delivering.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
    /// A slow consumer fell behind the fan-out buffer and lost `n` events.
    /// The right answer is to end the response: the client reconnects with
    /// its cursor and the log replays what the buffer dropped.
    #[error("consumer lagged by {0} events")]
    Lagged(u64),
    /// The log failed.
    #[error(transparent)]
    Log(#[from] LogError),
}

/// The result of [`resumable`]: what to tell the client and what to send.
pub struct Resumed<S> {
    /// `Some` when the cursor could not be served — emit a `resync` frame
    /// (and the connect snapshot) before the stream.
    pub resync: Option<Resync>,
    /// True when the connect snapshot should be sent: a first connection or
    /// a failed resume. A served replay carries the state transitions
    /// already, so no snapshot is needed.
    pub snapshot: bool,
    /// Replayed events followed by live ones, without overlap.
    pub stream: S,
}

/// Build the event stream for one connection.
///
/// Order matters and the API enforces it: `subscribe` is called FIRST (join
/// the live fan-out), the log is read second, and a live event whose cursor
/// the replay already delivered — or that the client's own cursor names —
/// is dropped, so nothing published between the two steps is lost or
/// duplicated. A caller cannot get this wrong by creating the subscription
/// late, because it does not create it at all.
///
/// The overlap check is a set of the replayed cursors, kept for the life
/// of the connection: at most `limit` tokens, and the only thing that
/// works for cursors the server cannot order. A live event without a
/// cursor (the log write failed; see [`EventLog::mark_gap`]) always passes.
pub async fn resumable<L, S>(
    log: &L,
    topic: &str,
    resume: Resume,
    subscribe: impl FnOnce() -> S,
    limit: usize,
) -> Resumed<BoxStream<'static, Result<Published<L::Event>, StreamError>>>
where
    L: EventLog,
    S: Stream<Item = Result<Published<L::Event>, StreamError>> + Send + 'static,
{
    // The stream is boxed so its type does not carry the subscribe closure:
    // a closure may borrow its context (the borrow ends when it returns),
    // and the resulting stream is still `'static` for a response body.
    let live = subscribe();
    let (replay, resync, snapshot) = match &resume {
        Resume::None => (Vec::new(), None, true),
        Resume::Invalid => (Vec::new(), Some(Resync::unknown()), true),
        Resume::Since(cursor) => match log.since(topic, cursor, limit).await {
            Ok(Replay::Events(events)) => (events, None, false),
            Ok(Replay::Expired { earliest }) => (Vec::new(), Some(Resync::expired(earliest)), true),
            // A failing log must not take the live path down with it: serve
            // a snapshot and say the cursor could not be honoured.
            Ok(Replay::Unknown) | Err(_) => (Vec::new(), Some(Resync::unknown()), true),
        },
    };
    // What the client already has: its own cursor's event and the replay.
    let mut seen: HashSet<Cursor> = replay.iter().filter_map(|p| p.cursor.clone()).collect();
    if let Resume::Since(cursor) = resume {
        seen.insert(cursor);
    }
    let stream = futures_util::stream::iter(replay.into_iter().map(Ok))
        .chain(live.filter(move |item| {
            let keep = match item {
                Ok(p) => p.cursor.as_ref().is_none_or(|c| !seen.contains(c)),
                Err(_) => true,
            };
            std::future::ready(keep)
        }))
        .boxed();
    Resumed {
        resync,
        snapshot,
        stream,
    }
}

/// Adapters from `tokio::sync::broadcast` — the usual in-process fan-out.
pub mod broadcast {
    use super::*;
    use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
    use tokio_stream::wrappers::BroadcastStream;

    /// A live stream from a broadcast receiver. `Lagged` becomes
    /// [`StreamError::Lagged`] instead of being dropped on the floor.
    pub fn bridge<E: Clone + Send + 'static>(
        rx: tokio::sync::broadcast::Receiver<Published<E>>,
    ) -> impl Stream<Item = Result<Published<E>, StreamError>> {
        BroadcastStream::new(rx)
            .map(|r| r.map_err(|BroadcastStreamRecvError::Lagged(n)| StreamError::Lagged(n)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::MemoryLog;
    use solder_sse::ResyncReason;
    use std::time::Duration;

    /// A log with `n` events on topic `a`, and their cursors.
    async fn log_with(n: usize, capacity: usize) -> (MemoryLog<&'static str>, Vec<Cursor>) {
        let log = MemoryLog::new(capacity, Duration::from_secs(60));
        let mut cursors = Vec::new();
        for _ in 0..n {
            cursors.push(log.append("a", "logged").await.unwrap());
        }
        (log, cursors)
    }

    fn live(
        items: Vec<Option<Cursor>>,
    ) -> impl Stream<Item = Result<Published<&'static str>, StreamError>> {
        futures_util::stream::iter(
            items
                .into_iter()
                .map(|cursor| {
                    Ok(Published {
                        cursor,
                        event: "live",
                    })
                })
                .collect::<Vec<_>>(),
        )
    }

    async fn cursors<S: Stream<Item = Result<Published<&'static str>, StreamError>>>(
        s: S,
    ) -> Vec<Option<Cursor>> {
        s.map(|r| r.unwrap().cursor).collect().await
    }

    fn some(cs: &[Cursor]) -> Vec<Option<Cursor>> {
        cs.iter().cloned().map(Some).collect()
    }

    #[tokio::test]
    async fn replay_then_live_without_overlap() {
        let (log, c) = log_with(4, 100).await;
        let fresh = Cursor::parse("live-only").unwrap();
        // The live subscription was created before the read and saw 3, 4
        // and a newer event; the client holds 2.
        let r = resumable(
            &log,
            "a",
            Resume::Since(c[1].clone()),
            || {
                live(vec![
                    Some(c[2].clone()),
                    Some(c[3].clone()),
                    Some(fresh.clone()),
                ])
            },
            100,
        )
        .await;
        assert!(r.resync.is_none());
        assert!(!r.snapshot);
        assert_eq!(
            cursors(r.stream).await,
            some(&[c[2].clone(), c[3].clone(), fresh])
        );
    }

    #[tokio::test]
    async fn the_clients_own_event_arriving_late_on_the_bus_is_dropped() {
        // The publisher appends, the client sees the event on its previous
        // connection and reconnects, and only then does the publisher's
        // broadcast reach the new subscription: an exact, empty replay
        // must not let it through a second time.
        let (log, c) = log_with(2, 100).await;
        let r = resumable(
            &log,
            "a",
            Resume::Since(c[1].clone()),
            || live(vec![Some(c[1].clone())]),
            100,
        )
        .await;
        assert!(r.resync.is_none());
        assert_eq!(cursors(r.stream).await, vec![]);
    }

    #[tokio::test]
    async fn an_unsequenced_live_event_always_passes() {
        let (log, c) = log_with(2, 100).await;
        let r = resumable(
            &log,
            "a",
            Resume::Since(c[0].clone()),
            || live(vec![Some(c[1].clone()), None, Some(c[1].clone())]),
            100,
        )
        .await;
        assert_eq!(cursors(r.stream).await, vec![Some(c[1].clone()), None]);
    }

    #[tokio::test]
    async fn first_connection_gets_snapshot_and_all_live() {
        let (log, c) = log_with(2, 100).await;
        let r = resumable(&log, "a", Resume::None, || live(some(&c)), 100).await;
        assert!(r.snapshot && r.resync.is_none());
        assert_eq!(cursors(r.stream).await, some(&c));
    }

    #[tokio::test]
    async fn expired_unknown_and_invalid_cursors_resync_with_snapshot_and_full_live() {
        // capacity 1: the ring holds only the third event, so the first
        // cursor has lost the second
        let (log, c) = log_with(3, 1).await;
        let after = Cursor::parse("after").unwrap();
        let r = resumable(
            &log,
            "a",
            Resume::Since(c[0].clone()),
            || live(vec![Some(after.clone())]),
            100,
        )
        .await;
        assert_eq!(r.resync, Some(Resync::expired(Some(c[2].clone()))));
        assert!(r.snapshot);
        assert_eq!(cursors(r.stream).await, vec![Some(after.clone())]);

        let foreign = Cursor::parse("another-log-99").unwrap();
        let r = resumable(&log, "a", Resume::Since(foreign), || live(vec![]), 100).await;
        assert_eq!(r.resync, Some(Resync::unknown()));
        assert_eq!(r.resync.unwrap().reason, ResyncReason::Unknown);
        let r = resumable(
            &log,
            "a",
            Resume::Invalid,
            || live(vec![Some(after.clone())]),
            100,
        )
        .await;
        assert_eq!(r.resync, Some(Resync::unknown()));
        assert!(r.snapshot);
        assert_eq!(cursors(r.stream).await, vec![Some(after)]);
    }

    #[tokio::test]
    async fn lagged_is_surfaced_not_swallowed() {
        let (tx, rx) = tokio::sync::broadcast::channel::<Published<u8>>(2);
        let mut s = Box::pin(broadcast::bridge(rx));
        for n in 1..=4 {
            tx.send(Published {
                cursor: Cursor::parse(&n.to_string()).ok(),
                event: 0,
            })
            .unwrap();
        }
        assert!(matches!(s.next().await, Some(Err(StreamError::Lagged(2)))));
        assert_eq!(s.next().await.unwrap().unwrap().cursor.unwrap(), "3");
    }
}
