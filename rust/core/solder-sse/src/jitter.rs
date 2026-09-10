//! Scheduling jitter without a random-number dependency.

use std::hash::{BuildHasher, Hasher};
use std::ops::Range;

/// Uniform sample from `range` (empty or single-value ranges return
/// `range.start`), seeded from the std hasher's per-process randomness —
/// enough for spreading reconnects and retry hints, one dependency fewer.
pub fn uniform(range: Range<u64>) -> u64 {
    let span = range.end.saturating_sub(range.start);
    if span <= 1 {
        return range.start;
    }
    let r = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    range.start + r % span
}

/// Full jitter over an exponential cap: `random(0, min(max, base·2^attempt))`,
/// never below `floor`. Milliseconds in, milliseconds out.
pub fn backoff_ms(attempt: u32, base_ms: u64, max_ms: u64, floor_ms: u64) -> u64 {
    let cap = base_ms.saturating_mul(1u64 << attempt.min(30)).min(max_ms);
    uniform(0..cap.max(1)).max(floor_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stays_in_range() {
        for _ in 0..1000 {
            assert!((500..1000).contains(&uniform(500..1000)));
        }
        assert_eq!(uniform(7..8), 7);
        assert_eq!(uniform(7..7), 7);
    }

    #[test]
    fn backoff_is_capped_and_floored() {
        for attempt in 0..40 {
            let d = backoff_ms(attempt, 1_000, 30_000, 200);
            assert!((200..=30_000).contains(&d), "attempt {attempt}: {d}");
        }
        assert!(backoff_ms(0, 1_000, 30_000, 200) <= 1_000);
    }
}
