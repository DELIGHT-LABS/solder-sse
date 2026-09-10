//! The replay buffer behind the resume contract.
//!
//! [`EventLog`] is the only storage seam: the crate ships [`MemoryLog`],
//! a per-topic ring that suits a single-process server (a restart makes
//! old cursors `Unknown`, which the client answers with a snapshot — no
//! loss). A shared store (SQL, Redis Streams) implements the same trait
//! when the server scales out.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;
// tokio's Instant equals std's outside tests and honours paused time inside them.
use tokio::time::Instant;

/// An event with the sequence number the log assigned to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published<E> {
    /// Monotonic across the whole log (not per topic), starting at 1.
    pub seq: u64,
    /// The payload.
    pub event: E,
}

/// The answer to "everything after `seq`".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Replay<E> {
    /// Events with `seq > cursor`, ascending. May be empty.
    Events(Vec<Published<E>>),
    /// The cursor is older than the retained window (or the gap is larger
    /// than the replay limit). `earliest` is the oldest retained sequence.
    Expired {
        /// Oldest sequence the log can still replay.
        earliest: u64,
    },
    /// The cursor was never issued by this log.
    Unknown,
}

/// A storage failure. Memory never fails; a backend maps its own errors.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// The storage backend failed.
    #[error("event log backend: {0}")]
    Backend(String),
}

/// A bounded, sequence-numbered event log.
///
/// `topic` partitions events (one screen, one channel …) while `seq` stays
/// global, so a client of a quiet topic can present a cursor issued for a
/// busier one and still get an exact, empty replay.
pub trait EventLog: Send + Sync {
    /// The payload type.
    type Event: Clone + Send + 'static;

    /// Append and return the assigned sequence.
    fn append(
        &self,
        topic: &str,
        event: Self::Event,
    ) -> impl Future<Output = Result<u64, LogError>> + Send;

    /// Everything after `seq` on `topic`, at most `limit` events.
    fn since(
        &self,
        topic: &str,
        seq: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Replay<Self::Event>, LogError>> + Send;

    /// Drop events older than `older_than`; returns how many.
    fn trim(&self, older_than: Duration) -> impl Future<Output = Result<u64, LogError>> + Send;

    /// Declare that events up to now may be missing from the log (a write
    /// failed after the state changed): every cursor issued before this
    /// point answers `Expired`, so a client reconnecting across the gap
    /// reloads a snapshot instead of silently missing what was lost.
    fn mark_gap(&self) -> impl Future<Output = Result<(), LogError>> + Send;
}

/// The default log: per-topic rings in memory, bounded by count and age.
pub struct MemoryLog<E> {
    inner: Mutex<Topics<E>>,
    next_seq: AtomicU64,
    /// Cursors below this are unreplayable whatever the rings hold
    /// ([`EventLog::mark_gap`]).
    gap_floor: AtomicU64,
    capacity: usize,
    ttl: Duration,
}

struct Topics<E> {
    topics: HashMap<String, Topic<E>>,
}

struct Topic<E> {
    ring: VecDeque<Entry<E>>,
    /// Highest sequence ever dropped from this ring — a cursor below it
    /// has lost events.
    trimmed_to: u64,
}

struct Entry<E> {
    seq: u64,
    at: Instant,
    event: E,
}

impl<E> MemoryLog<E> {
    /// A log keeping at most `capacity` events per topic for at most `ttl`.
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(Topics {
                topics: HashMap::new(),
            }),
            next_seq: AtomicU64::new(1),
            gap_floor: AtomicU64::new(0),
            capacity: capacity.max(1),
            ttl,
        }
    }

    /// The last sequence assigned (0 before the first append).
    pub fn last_seq(&self) -> u64 {
        self.next_seq.load(Ordering::SeqCst) - 1
    }
}

impl<E> Topic<E> {
    fn trim_to(&mut self, capacity: usize, ttl: Duration, now: Instant) {
        while self.ring.len() > capacity
            || self
                .ring
                .front()
                .is_some_and(|e| now.duration_since(e.at) > ttl)
        {
            if let Some(dropped) = self.ring.pop_front() {
                self.trimmed_to = self.trimmed_to.max(dropped.seq);
            }
        }
    }
}

impl<E: Clone + Send + 'static> EventLog for MemoryLog<E> {
    type Event = E;

    async fn append(&self, topic: &str, event: E) -> Result<u64, LogError> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let now = Instant::now();
        let mut inner = self.inner.lock().expect("memory log poisoned");
        let t = inner
            .topics
            .entry(topic.to_owned())
            .or_insert_with(|| Topic {
                ring: VecDeque::new(),
                trimmed_to: 0,
            });
        t.ring.push_back(Entry {
            seq,
            at: now,
            event,
        });
        t.trim_to(self.capacity, self.ttl, now);
        Ok(seq)
    }

    async fn since(&self, topic: &str, seq: u64, limit: usize) -> Result<Replay<E>, LogError> {
        let last = self.last_seq();
        if seq > last {
            return Ok(Replay::Unknown);
        }
        let gap_floor = self.gap_floor.load(Ordering::SeqCst);
        if seq < gap_floor {
            return Ok(Replay::Expired {
                earliest: gap_floor + 1,
            });
        }
        let now = Instant::now();
        let mut inner = self.inner.lock().expect("memory log poisoned");
        let Some(t) = inner.topics.get_mut(topic) else {
            // Nothing was ever published on this topic: an exact, empty replay.
            return Ok(Replay::Events(Vec::new()));
        };
        t.trim_to(self.capacity, self.ttl, now);
        if seq < t.trimmed_to {
            let earliest = t.ring.front().map_or(t.trimmed_to + 1, |e| e.seq);
            return Ok(Replay::Expired { earliest });
        }
        let out: Vec<Published<E>> = t
            .ring
            .iter()
            .filter(|e| e.seq > seq)
            .take(limit + 1)
            .map(|e| Published {
                seq: e.seq,
                event: e.event.clone(),
            })
            .collect();
        if out.len() > limit {
            let earliest = out.first().map_or(seq + 1, |p| p.seq);
            return Ok(Replay::Expired { earliest });
        }
        Ok(Replay::Events(out))
    }

    async fn trim(&self, older_than: Duration) -> Result<u64, LogError> {
        let now = Instant::now();
        let mut inner = self.inner.lock().expect("memory log poisoned");
        let mut dropped = 0u64;
        for t in inner.topics.values_mut() {
            let before = t.ring.len();
            t.trim_to(self.capacity, older_than, now);
            dropped += (before - t.ring.len()) as u64;
        }
        Ok(dropped)
    }

    async fn mark_gap(&self) -> Result<(), LogError> {
        self.gap_floor.fetch_max(self.last_seq(), Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log() -> MemoryLog<&'static str> {
        MemoryLog::new(4, Duration::from_secs(60))
    }

    #[tokio::test]
    async fn seq_is_global_and_starts_at_one() {
        let l = log();
        assert_eq!(l.append("a", "a1").await.unwrap(), 1);
        assert_eq!(l.append("b", "b1").await.unwrap(), 2);
        assert_eq!(l.append("a", "a2").await.unwrap(), 3);
        assert_eq!(l.last_seq(), 3);
    }

    #[tokio::test]
    async fn replays_strictly_after_the_cursor_in_order() {
        let l = log();
        for e in ["a1", "a2", "a3"] {
            l.append("a", e).await.unwrap();
        }
        let Replay::Events(v) = l.since("a", 1, 100).await.unwrap() else {
            panic!("expected events")
        };
        assert_eq!(v.iter().map(|p| p.seq).collect::<Vec<_>>(), vec![2, 3]);
        assert_eq!(v[0].event, "a2");
        // cursor == last → exact empty replay
        assert_eq!(l.since("a", 3, 100).await.unwrap(), Replay::Events(vec![]));
    }

    #[tokio::test]
    async fn a_quiet_topic_answers_a_foreign_cursor_with_an_empty_replay() {
        let l = log();
        l.append("busy", "x").await.unwrap();
        l.append("busy", "y").await.unwrap();
        assert_eq!(
            l.since("quiet", 2, 100).await.unwrap(),
            Replay::Events(vec![])
        );
        assert_eq!(
            l.since("quiet", 1, 100).await.unwrap(),
            Replay::Events(vec![])
        );
    }

    #[tokio::test]
    async fn a_cursor_past_the_end_is_unknown() {
        let l = log();
        l.append("a", "a1").await.unwrap();
        assert_eq!(l.since("a", 2, 100).await.unwrap(), Replay::Unknown);
        assert_eq!(l.since("a", u64::MAX, 100).await.unwrap(), Replay::Unknown);
    }

    #[tokio::test]
    async fn capacity_trims_the_oldest_and_a_cursor_below_the_trim_is_expired() {
        let l = log(); // capacity 4
        for e in ["1", "2", "3", "4", "5", "6"] {
            l.append("a", e).await.unwrap();
        }
        // ring holds 3..=6; seq 1 and 2 were dropped
        assert_eq!(
            l.since("a", 1, 100).await.unwrap(),
            Replay::Expired { earliest: 3 }
        );
        // cursor 2: nothing dropped ABOVE it, so the replay is exact
        let Replay::Events(v) = l.since("a", 2, 100).await.unwrap() else {
            panic!()
        };
        assert_eq!(
            v.iter().map(|p| p.seq).collect::<Vec<_>>(),
            vec![3, 4, 5, 6]
        );
    }

    #[tokio::test]
    async fn a_gap_wider_than_the_limit_is_expired() {
        let l = MemoryLog::new(100, Duration::from_secs(60));
        for i in 0..10 {
            l.append("a", if i % 2 == 0 { "e" } else { "o" })
                .await
                .unwrap();
        }
        assert_eq!(
            l.since("a", 2, 3).await.unwrap(),
            Replay::Expired { earliest: 3 }
        );
        assert!(matches!(l.since("a", 7, 3).await.unwrap(), Replay::Events(v) if v.len() == 3));
    }

    #[tokio::test]
    async fn a_gap_expires_every_earlier_cursor_but_not_later_ones() {
        let l = log();
        for e in ["1", "2", "3"] {
            l.append("a", e).await.unwrap();
        }
        l.mark_gap().await.unwrap(); // something after seq 3 may be missing
        assert_eq!(
            l.since("a", 2, 100).await.unwrap(),
            Replay::Expired { earliest: 4 }
        );
        // the cursor AT the gap is fine: nothing before it was lost
        assert_eq!(l.since("a", 3, 100).await.unwrap(), Replay::Events(vec![]));
        l.append("a", "4").await.unwrap();
        assert!(matches!(l.since("a", 3, 100).await.unwrap(), Replay::Events(v) if v.len() == 1));
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_trims_by_age() {
        let l = MemoryLog::new(100, Duration::from_secs(10));
        l.append("a", "old").await.unwrap();
        tokio::time::advance(Duration::from_secs(11)).await;
        l.append("a", "new").await.unwrap();
        assert_eq!(
            l.since("a", 0, 100).await.unwrap(),
            Replay::Expired { earliest: 2 }
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(l.trim(Duration::from_secs(0)).await.unwrap(), 1);
    }
}
