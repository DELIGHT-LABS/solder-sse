//! The replay buffer behind the resume contract.
//!
//! [`EventLog`] is the only storage seam: the crate ships [`MemoryLog`],
//! a per-topic ring that suits a single-process server. A shared store
//! (SQL, Redis Streams) implements the same trait when the server scales
//! out; its cursors are its own — the trait never reads one.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;
// tokio's Instant equals std's outside tests and honours paused time inside them.
use tokio::time::Instant;

pub use solder_sse::Cursor;

/// An event as it travels from the log to a response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published<E> {
    /// The cursor the log issued for it — every replayed event carries one.
    /// `None` is a live event the log could not take (the write failed
    /// after the state changed): it goes out without an `id:`, so it
    /// cannot move the client's cursor, and [`EventLog::mark_gap`] makes
    /// the next reconnect resync.
    pub cursor: Option<Cursor>,
    /// The payload.
    pub event: E,
}

/// The answer to "everything after this cursor".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Replay<E> {
    /// The events that followed the cursor, in order. May be empty: the
    /// client is up to date.
    Events(Vec<Published<E>>),
    /// The log can no longer replay from the cursor: it is older than the
    /// retained window, the gap is wider than the replay limit, or a gap
    /// was marked since. `earliest` is the oldest event still retained on
    /// the topic, when there is one.
    Expired {
        /// The oldest cursor the log could still replay from.
        earliest: Option<Cursor>,
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

/// A bounded event log that issues cursors.
///
/// `topic` partitions events (one screen, one channel …); a cursor is
/// issued by the log as a whole, so a client of a quiet topic can present
/// one issued for a busier topic and still get an exact, empty replay. What
/// a cursor looks like is the log's business — see [`Cursor`] for the wire
/// alphabet every log must stay within.
pub trait EventLog: Send + Sync {
    /// The payload type.
    type Event: Clone + Send + 'static;

    /// Append and return the cursor issued for the event.
    fn append(
        &self,
        topic: &str,
        event: Self::Event,
    ) -> impl Future<Output = Result<Cursor, LogError>> + Send;

    /// Everything after `cursor` on `topic`, at most `limit` events.
    fn since(
        &self,
        topic: &str,
        cursor: &Cursor,
        limit: usize,
    ) -> impl Future<Output = Result<Replay<Self::Event>, LogError>> + Send;

    /// Declare that events up to now may be missing from the log (a write
    /// failed after the state changed): every cursor issued so far answers
    /// `Expired`, so a client reconnecting across the gap reloads a
    /// snapshot instead of silently missing what was lost.
    fn mark_gap(&self) -> impl Future<Output = Result<(), LogError>> + Send;
}

/// The default log: per-topic rings in memory, bounded by count and age.
///
/// Its cursors are `<generation>-<sequence>`: a generation drawn at random
/// when the log is created, and a sequence counted across the whole log
/// from 1. A cursor from another generation — an earlier process, another
/// replica — is `Unknown`, which the client answers with a snapshot; the
/// numbering never collides across restarts.
pub struct MemoryLog<E> {
    inner: Mutex<Inner<E>>,
    generation: String,
    capacity: usize,
    ttl: Duration,
}

struct Inner<E> {
    topics: HashMap<String, Topic<E>>,
    /// The last sequence issued (0 before the first append). Allocated
    /// under this lock, so a ring's order is the sequence order.
    last_seq: u64,
    /// Sequences at or below this cannot be replayed from, whatever the
    /// rings hold ([`EventLog::mark_gap`]).
    gap_floor: u64,
}

struct Topic<E> {
    ring: VecDeque<Entry<E>>,
    /// Highest sequence ever dropped from this ring — a cursor below it
    /// has lost events.
    trimmed_to: u64,
}

impl<E> Topic<E> {
    fn new() -> Self {
        Self {
            ring: VecDeque::new(),
            trimmed_to: 0,
        }
    }

    /// Drop what is over `capacity` or older than `ttl`; a cursor below
    /// what was dropped has lost events.
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

struct Entry<E> {
    seq: u64,
    /// Built once here; a replay and the fan-out clone the reference.
    cursor: Cursor,
    at: Instant,
    event: E,
}

impl<E> MemoryLog<E> {
    /// A log keeping at most `capacity` events per topic for at most `ttl`.
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner {
                topics: HashMap::new(),
                last_seq: 0,
                gap_floor: 0,
            }),
            // 32 random bits, hex: distinct across restarts and replicas
            // for any practical purpose, short on every frame.
            generation: format!("{:08x}", solder_sse::jitter::uniform(0..1 << 32)),
            capacity: capacity.max(1),
            ttl,
        }
    }

    fn cursor(&self, seq: u64) -> Cursor {
        Cursor::try_from(format!("{}-{seq}", self.generation))
            .expect("hex, a dash and digits are within the cursor alphabet")
    }

    /// The sequence behind a cursor of this generation; `None` for any
    /// other token.
    fn seq_of(&self, cursor: &Cursor) -> Option<u64> {
        let rest = cursor.as_str().strip_prefix(self.generation.as_str())?;
        let digits = rest.strip_prefix('-')?;
        // Only digits: `u64::from_str` would also take a `+`.
        if digits.bytes().all(|b| b.is_ascii_digit()) {
            digits.parse().ok()
        } else {
            None
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<E>> {
        self.inner.lock().expect("memory log poisoned")
    }
}

impl<E: Clone + Send + 'static> EventLog for MemoryLog<E> {
    type Event = E;

    async fn append(&self, topic: &str, event: E) -> Result<Cursor, LogError> {
        let now = Instant::now();
        let mut inner = self.lock();
        inner.last_seq += 1;
        let seq = inner.last_seq;
        let cursor = self.cursor(seq);
        // The topic's name is allocated for its first event only; the
        // common path is a lookup by `&str` under the lock.
        if !inner.topics.contains_key(topic) {
            inner.topics.insert(topic.to_owned(), Topic::new());
        }
        let t = inner
            .topics
            .get_mut(topic)
            .expect("present or just inserted");
        t.ring.push_back(Entry {
            seq,
            cursor: cursor.clone(),
            at: now,
            event,
        });
        t.trim_to(self.capacity, self.ttl, now);
        Ok(cursor)
    }

    async fn since(
        &self,
        topic: &str,
        cursor: &Cursor,
        limit: usize,
    ) -> Result<Replay<E>, LogError> {
        let Some(seq) = self.seq_of(cursor) else {
            return Ok(Replay::Unknown);
        };
        let now = Instant::now();
        let mut inner = self.lock();
        if seq == 0 || seq > inner.last_seq {
            return Ok(Replay::Unknown);
        }
        let gap_floor = inner.gap_floor;
        let Some(t) = inner.topics.get_mut(topic) else {
            // Nothing was ever published on this topic: an exact, empty
            // replay — unless a gap swallowed the cursor's own successors.
            return Ok(if seq <= gap_floor {
                Replay::Expired { earliest: None }
            } else {
                Replay::Events(Vec::new())
            });
        };
        t.trim_to(self.capacity, self.ttl, now);
        // A gap at or after the cursor, or a trim above it, means events
        // after it are gone: nothing the ring holds can close that.
        if seq <= gap_floor || seq < t.trimmed_to {
            // The oldest event a client could still replay from: the first
            // one the ring holds past the gap.
            let earliest = t
                .ring
                .iter()
                .find(|e| e.seq > gap_floor)
                .map(|e| e.cursor.clone());
            return Ok(Replay::Expired { earliest });
        }
        let out: Vec<Published<E>> = t
            .ring
            .iter()
            .filter(|e| e.seq > seq)
            .take(limit + 1)
            .map(|e| Published {
                cursor: Some(e.cursor.clone()),
                event: e.event.clone(),
            })
            .collect();
        if out.len() > limit {
            let earliest = out.into_iter().next().and_then(|p| p.cursor);
            return Ok(Replay::Expired { earliest });
        }
        Ok(Replay::Events(out))
    }

    async fn mark_gap(&self) -> Result<(), LogError> {
        let mut inner = self.lock();
        inner.gap_floor = inner.last_seq;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log() -> MemoryLog<&'static str> {
        MemoryLog::new(4, Duration::from_secs(60))
    }

    /// The sequences of a replay, for assertions on order and boundaries.
    fn seqs<E>(log: &MemoryLog<E>, replay: Replay<E>) -> Vec<u64> {
        let Replay::Events(v) = replay else {
            panic!("expected events, got {}", describe(&replay))
        };
        v.iter()
            .map(|p| log.seq_of(p.cursor.as_ref().unwrap()).unwrap())
            .collect()
    }

    fn describe<E>(r: &Replay<E>) -> &'static str {
        match r {
            Replay::Events(_) => "events",
            Replay::Expired { .. } => "expired",
            Replay::Unknown => "unknown",
        }
    }

    #[tokio::test]
    async fn cursors_are_the_generation_and_a_global_sequence_from_one() {
        let l = log();
        let a1 = l.append("a", "a1").await.unwrap();
        let b1 = l.append("b", "b1").await.unwrap();
        let a2 = l.append("a", "a2").await.unwrap();
        let g = &l.generation;
        assert_eq!(g.len(), 8);
        assert_eq!(a1, format!("{g}-1").as_str());
        assert_eq!(b1, format!("{g}-2").as_str());
        assert_eq!(a2, format!("{g}-3").as_str());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_keep_every_ring_in_sequence_order() {
        let l = std::sync::Arc::new(MemoryLog::new(10_000, Duration::from_secs(60)));
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let l = l.clone();
                tokio::spawn(async move {
                    for _ in 0..200 {
                        l.append(if i % 2 == 0 { "a" } else { "b" }, i)
                            .await
                            .unwrap();
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        let inner = l.lock();
        for t in inner.topics.values() {
            assert!(
                t.ring.iter().map(|e| e.seq).is_sorted(),
                "a ring out of order would replay out of order"
            );
        }
        assert_eq!(inner.last_seq, 1600);
    }

    #[tokio::test]
    async fn replays_strictly_after_the_cursor_in_order() {
        let l = log();
        let mut cursors = Vec::new();
        for e in ["a1", "a2", "a3"] {
            cursors.push(l.append("a", e).await.unwrap());
        }
        let Replay::Events(v) = l.since("a", &cursors[0], 100).await.unwrap() else {
            panic!("expected events")
        };
        assert_eq!(
            v.iter()
                .map(|p| p.cursor.clone().unwrap())
                .collect::<Vec<_>>(),
            cursors[1..]
        );
        assert_eq!(v[0].event, "a2");
        // cursor == last → exact empty replay
        assert_eq!(
            l.since("a", &cursors[2], 100).await.unwrap(),
            Replay::Events(vec![])
        );
    }

    #[tokio::test]
    async fn a_quiet_topic_answers_a_foreign_cursor_with_an_empty_replay() {
        let l = log();
        let x = l.append("busy", "x").await.unwrap();
        let y = l.append("busy", "y").await.unwrap();
        for c in [&x, &y] {
            assert_eq!(
                l.since("quiet", c, 100).await.unwrap(),
                Replay::Events(vec![])
            );
        }
    }

    #[tokio::test]
    async fn a_cursor_this_log_never_issued_is_unknown() {
        let l = log();
        let c1 = l.append("a", "a1").await.unwrap();
        // past the end, zero, another generation, not this log's shape
        for token in [
            format!("{}-2", l.generation),
            format!("{}-0", l.generation),
            format!("{}-1", MemoryLog::<u8>::new(1, Duration::ZERO).generation),
            "1".to_owned(),
            format!("{}-1x", l.generation),
            format!("{}-", l.generation),
            l.generation.clone(),
        ] {
            let c = Cursor::parse(&token).unwrap();
            assert_eq!(
                l.since("a", &c, 100).await.unwrap(),
                Replay::Unknown,
                "{token}"
            );
        }
        // …and a fresh log knows none of the old one's cursors: a restart
        // costs one resync, never a silent gap or a collision.
        let fresh = MemoryLog::<&str>::new(4, Duration::from_secs(60));
        assert_eq!(fresh.since("a", &c1, 100).await.unwrap(), Replay::Unknown);
    }

    #[tokio::test]
    async fn capacity_trims_the_oldest_and_a_cursor_below_the_trim_is_expired() {
        let l = log(); // capacity 4
        let mut cursors = Vec::new();
        for e in ["1", "2", "3", "4", "5", "6"] {
            cursors.push(l.append("a", e).await.unwrap());
        }
        // ring holds 3..=6; seq 1 and 2 were dropped
        assert_eq!(
            l.since("a", &cursors[0], 100).await.unwrap(),
            Replay::Expired {
                earliest: Some(cursors[2].clone())
            }
        );
        // cursor 2: nothing dropped ABOVE it, so the replay is exact
        let r = l.since("a", &cursors[1], 100).await.unwrap();
        assert_eq!(seqs(&l, r), vec![3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn a_cursor_from_another_topic_below_this_topics_trim_is_expired() {
        let l = log(); // capacity 4
        let b1 = l.append("b", "b1").await.unwrap(); // seq 1
        let mut a = Vec::new();
        for e in ["1", "2", "3", "4", "5"] {
            a.push(l.append("a", e).await.unwrap()); // seq 2..=6; ring a holds 3..=6
        }
        // Seq 2 on `a` was dropped, and it came after `b1`: lost.
        assert_eq!(
            l.since("a", &b1, 100).await.unwrap(),
            Replay::Expired {
                earliest: Some(a[1].clone())
            }
        );
        // On its own topic the same cursor is exact and up to date.
        assert_eq!(
            l.since("b", &b1, 100).await.unwrap(),
            Replay::Events(vec![])
        );
    }

    #[tokio::test]
    async fn a_gap_wider_than_the_limit_is_expired() {
        let l = MemoryLog::new(100, Duration::from_secs(60));
        let mut cursors = Vec::new();
        for i in 0..10 {
            cursors.push(
                l.append("a", if i % 2 == 0 { "e" } else { "o" })
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(
            l.since("a", &cursors[1], 3).await.unwrap(),
            Replay::Expired {
                earliest: Some(cursors[2].clone())
            }
        );
        let r = l.since("a", &cursors[6], 3).await.unwrap();
        assert_eq!(seqs(&l, r), vec![8, 9, 10]);
    }

    #[tokio::test]
    async fn a_gap_expires_every_cursor_issued_before_it_including_the_last() {
        let l = log();
        let mut cursors = Vec::new();
        for e in ["1", "2", "3"] {
            cursors.push(l.append("a", e).await.unwrap());
        }
        l.mark_gap().await.unwrap(); // an event after seq 3 may be missing
        assert_eq!(
            l.since("a", &cursors[1], 100).await.unwrap(),
            Replay::Expired { earliest: None }
        );
        // The cursor AT the gap is expired too: what was lost came after it.
        assert_eq!(
            l.since("a", &cursors[2], 100).await.unwrap(),
            Replay::Expired { earliest: None }
        );
        // A topic that never saw an event is not exempt.
        assert_eq!(
            l.since("quiet", &cursors[2], 100).await.unwrap(),
            Replay::Expired { earliest: None }
        );
        let c4 = l.append("a", "4").await.unwrap();
        assert_eq!(
            l.since("a", &cursors[2], 100).await.unwrap(),
            Replay::Expired {
                earliest: Some(c4.clone())
            }
        );
        // A cursor issued after the gap replays normally.
        assert_eq!(
            l.since("a", &c4, 100).await.unwrap(),
            Replay::Events(vec![])
        );
        assert_eq!(
            l.since("quiet", &c4, 100).await.unwrap(),
            Replay::Events(vec![])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_trims_by_age_on_every_read_and_write() {
        let l = MemoryLog::new(100, Duration::from_secs(10));
        let old = l.append("a", "old").await.unwrap();
        tokio::time::advance(Duration::from_secs(11)).await;
        let new = l.append("a", "new").await.unwrap();
        // `old` was dropped by age, but a client holding its cursor lost
        // nothing: everything after it is still here.
        let r = l.since("a", &old, 100).await.unwrap();
        assert_eq!(seqs(&l, r), vec![2]);
        // Now `new` ages out too — trimmed by the read itself, no sweeper.
        tokio::time::advance(Duration::from_secs(11)).await;
        assert_eq!(
            l.since("a", &old, 100).await.unwrap(),
            Replay::Expired { earliest: None }
        );
        // The last cursor issued still resumes exactly: nothing came after it.
        assert_eq!(
            l.since("a", &new, 100).await.unwrap(),
            Replay::Events(vec![])
        );
    }
}
