//! Where a reconnecting client wants to resume from.

use http::request::Parts;
use http::HeaderMap;

/// Header the browser's own reconnect sends automatically.
pub const HEADER: &str = "last-event-id";
/// Query parameter for clients that cannot set request headers — a fresh
/// `new EventSource(url)` after a watchdog reopen, for example.
pub const QUERY: &str = "last_event_id";

/// The client's resume request, parsed from the header or the query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resume {
    /// No cursor: a first connection. The server sends its connect snapshot.
    None,
    /// Replay everything with `seq > n`.
    Since(u64),
    /// A cursor that is not a sequence number. Treated as unknown: the
    /// server answers with `resync(unknown)` and a snapshot.
    Malformed,
}

impl Resume {
    /// Parse from a request. The header wins over the query when both are
    /// present; an empty value counts as absent.
    pub fn parse(headers: &HeaderMap, query: Option<&str>) -> Self {
        let from_header = headers
            .get(HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let raw = from_header.or_else(|| query_value(query?, QUERY));
        match raw {
            None => Resume::None,
            Some(s) => match s.parse::<u64>() {
                Ok(0) => Resume::None,
                Ok(n) => Resume::Since(n),
                Err(_) => Resume::Malformed,
            },
        }
    }

    /// Parse from the request head (`http::request::Parts`).
    pub fn from_parts(parts: &Parts) -> Self {
        Self::parse(&parts.headers, parts.uri.query())
    }

    /// The cursor on an internal wire that has no room for an enum: `0` for
    /// none, `u64::MAX` for malformed (which any log reports as unknown).
    pub fn since(self) -> u64 {
        match self {
            Resume::None => 0,
            Resume::Since(n) => n,
            Resume::Malformed => u64::MAX,
        }
    }

    /// Inverse of [`Resume::since`].
    pub fn from_since(n: u64) -> Self {
        match n {
            0 => Resume::None,
            u64::MAX => Resume::Malformed,
            n => Resume::Since(n),
        }
    }
}

/// The first `key=value` pair in a query string, without percent-decoding
/// (a sequence number has nothing to decode; anything else is malformed).
fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (k == key).then_some(v.trim()).filter(|v| !v.is_empty())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn headers(v: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(v) = v {
            h.insert(HEADER, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    #[test]
    fn header_then_query_then_none() {
        assert_eq!(Resume::parse(&headers(Some("42")), None), Resume::Since(42));
        assert_eq!(
            Resume::parse(&headers(None), Some("a=1&last_event_id=7&b=2")),
            Resume::Since(7)
        );
        assert_eq!(Resume::parse(&headers(None), Some("a=1")), Resume::None);
        assert_eq!(Resume::parse(&headers(None), None), Resume::None);
    }

    #[test]
    fn header_wins_over_query() {
        assert_eq!(
            Resume::parse(&headers(Some("9")), Some("last_event_id=1")),
            Resume::Since(9)
        );
    }

    #[test]
    fn zero_and_empty_are_none_and_garbage_is_malformed() {
        assert_eq!(Resume::parse(&headers(Some("0")), None), Resume::None);
        assert_eq!(
            Resume::parse(&headers(Some("  ")), Some("last_event_id=3")),
            Resume::Since(3)
        );
        assert_eq!(
            Resume::parse(&headers(Some("abc")), None),
            Resume::Malformed
        );
        assert_eq!(
            Resume::parse(&headers(None), Some("last_event_id=-1")),
            Resume::Malformed
        );
    }

    #[test]
    fn wire_round_trip() {
        for r in [Resume::None, Resume::Since(5), Resume::Malformed] {
            assert_eq!(Resume::from_since(r.since()), r);
        }
    }

    #[test]
    fn from_parts_reads_uri_query() {
        let req = http::Request::builder()
            .uri("/v1/screens/a/stream?last_event_id=11")
            .body(())
            .unwrap();
        let (parts, _) = req.into_parts();
        assert_eq!(Resume::from_parts(&parts), Resume::Since(11));
    }
}
