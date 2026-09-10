//! Replay ∪ live, with the gap closed.

use crate::log::{EventLog, LogError, Published, Replay};
use crate::protocol::ResyncReason;
use crate::resume::Resume;
use futures_core::Stream;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;

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
    pub resync: Option<(ResyncReason, u64)>,
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
/// the live fan-out), the log is read second, and only live events with
/// `seq` above the last replayed one pass — so nothing published between
/// the two steps is lost or duplicated. A caller cannot get this wrong by
/// creating the subscription late, because it does not create it at all.
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
    let (replay, resync, snapshot, floor) = match resume {
        Resume::None => (Vec::new(), None, true, 0),
        Resume::Malformed => (Vec::new(), Some((ResyncReason::Unknown, 0)), true, 0),
        Resume::Since(cursor) => match log.since(topic, cursor, limit).await {
            Ok(Replay::Events(events)) => {
                let floor = events.last().map_or(cursor, |p| p.seq);
                (events, None, false, floor)
            }
            Ok(Replay::Expired { earliest }) => {
                (Vec::new(), Some((ResyncReason::Expired, earliest)), true, 0)
            }
            Ok(Replay::Unknown) => (Vec::new(), Some((ResyncReason::Unknown, 0)), true, 0),
            // A failing log must not take the live path down with it: serve
            // a snapshot and say the cursor could not be honoured.
            Err(_) => (Vec::new(), Some((ResyncReason::Unknown, 0)), true, 0),
        },
    };
    // Live events below the replayed floor are the overlap and are dropped;
    // an UNSEQUENCED live event (`seq == 0`: the log write failed and the
    // publisher broadcast anyway) always passes — it carries no `id:`, so it
    // cannot disturb the cursor, and the log's gap mark makes the next
    // reconnect resync.
    let stream = futures_util::stream::iter(replay.into_iter().map(Ok))
        .chain(live.filter(move |item| {
            let keep = match item {
                Ok(p) => p.seq == 0 || p.seq > floor,
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
    use std::time::Duration;

    fn live(seqs: &[u64]) -> impl Stream<Item = Result<Published<&'static str>, StreamError>> {
        futures_util::stream::iter(
            seqs.iter()
                .map(|&seq| Ok(Published { seq, event: "live" }))
                .collect::<Vec<_>>(),
        )
    }

    async fn seqs<S: Stream<Item = Result<Published<&'static str>, StreamError>>>(
        s: S,
    ) -> Vec<u64> {
        s.map(|r| r.unwrap().seq).collect().await
    }

    #[tokio::test]
    async fn replay_then_live_without_overlap() {
        let log = MemoryLog::new(100, Duration::from_secs(60));
        for e in ["1", "2", "3", "4"] {
            log.append("a", e).await.unwrap();
        }
        // live subscription was created before the read and saw 3, 4, 5
        let r = resumable(&log, "a", Resume::Since(2), || live(&[3, 4, 5]), 100).await;
        assert!(r.resync.is_none());
        assert!(!r.snapshot);
        assert_eq!(seqs(r.stream).await, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn an_unsequenced_live_event_passes_the_replay_floor() {
        let log = MemoryLog::new(100, Duration::from_secs(60));
        for e in ["1", "2"] {
            log.append("a", e).await.unwrap();
        }
        let r = resumable(&log, "a", Resume::Since(1), || live(&[2, 0, 3]), 100).await;
        assert_eq!(seqs(r.stream).await, vec![2, 0, 3]);
    }

    #[tokio::test]
    async fn first_connection_gets_snapshot_and_all_live() {
        let log = MemoryLog::<&str>::new(100, Duration::from_secs(60));
        let r = resumable(&log, "a", Resume::None, || live(&[1, 2]), 100).await;
        assert!(r.snapshot && r.resync.is_none());
        assert_eq!(seqs(r.stream).await, vec![1, 2]);
    }

    #[tokio::test]
    async fn expired_and_unknown_cursors_resync_with_snapshot_and_full_live() {
        // capacity 1: the ring holds only seq 3, so a cursor of 1 has lost 2
        let log = MemoryLog::new(1, Duration::from_secs(60));
        for e in ["1", "2", "3"] {
            log.append("a", e).await.unwrap();
        }
        let r = resumable(&log, "a", Resume::Since(1), || live(&[4]), 100).await;
        assert_eq!(r.resync, Some((ResyncReason::Expired, 3)));
        assert!(r.snapshot);
        assert_eq!(seqs(r.stream).await, vec![4]);

        let r = resumable(&log, "a", Resume::Since(99), || live(&[4]), 100).await;
        assert_eq!(r.resync, Some((ResyncReason::Unknown, 0)));
        let r = resumable(&log, "a", Resume::Malformed, || live(&[4]), 100).await;
        assert_eq!(r.resync, Some((ResyncReason::Unknown, 0)));
        assert_eq!(seqs(r.stream).await, vec![4]);
    }

    #[tokio::test]
    async fn lagged_is_surfaced_not_swallowed() {
        let (tx, rx) = tokio::sync::broadcast::channel::<Published<u8>>(2);
        let mut s = Box::pin(broadcast::bridge(rx));
        for seq in 1..=4 {
            tx.send(Published { seq, event: 0 }).unwrap();
        }
        assert!(matches!(s.next().await, Some(Err(StreamError::Lagged(2)))));
        assert_eq!(s.next().await.unwrap().unwrap().seq, 3);
    }
}
