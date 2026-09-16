//! Where a reconnecting client wants to resume from.

use http::request::Parts;
use http::HeaderMap;
use solder_sse::Cursor;

/// Header the browser's own reconnect sends automatically.
pub const HEADER: &str = "last-event-id";
/// Query parameter for clients that cannot set request headers — a fresh
/// `new EventSource(url)` after a watchdog reopen, for example.
pub const QUERY: &str = "last_event_id";

/// The client's resume request, parsed from the header or the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resume {
    /// No cursor: a first connection. The server sends its connect snapshot.
    None,
    /// Replay everything after this cursor.
    Since(Cursor),
    /// A value that is not a cursor at all (outside the token alphabet, or
    /// too long). Answered like a cursor the log never issued: `resync
    /// (unknown)` and a snapshot, so the client drops it.
    Invalid,
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
            Some(s) => Cursor::parse(s).map_or(Resume::Invalid, Resume::Since),
        }
    }

    /// Parse from the request head (`http::request::Parts`).
    pub fn from_parts(parts: &Parts) -> Self {
        Self::parse(&parts.headers, parts.uri.query())
    }

    /// The cursor, when there is one.
    pub fn cursor(&self) -> Option<&Cursor> {
        match self {
            Resume::Since(c) => Some(c),
            Resume::None | Resume::Invalid => None,
        }
    }
}

/// The first `key=value` pair in a query string, without percent-decoding:
/// a cursor's alphabet has nothing to decode, and anything else is not a
/// cursor.
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

    fn since(token: &str) -> Resume {
        Resume::Since(Cursor::parse(token).unwrap())
    }

    #[test]
    fn header_then_query_then_none() {
        assert_eq!(Resume::parse(&headers(Some("g-42")), None), since("g-42"));
        assert_eq!(
            Resume::parse(&headers(None), Some("a=1&last_event_id=g-7&b=2")),
            since("g-7")
        );
        assert_eq!(Resume::parse(&headers(None), Some("a=1")), Resume::None);
        assert_eq!(Resume::parse(&headers(None), None), Resume::None);
    }

    #[test]
    fn header_wins_over_query() {
        assert_eq!(
            Resume::parse(&headers(Some("9")), Some("last_event_id=1")),
            since("9")
        );
    }

    #[test]
    fn empty_is_none_and_a_non_token_is_invalid() {
        assert_eq!(Resume::parse(&headers(Some("  ")), None), Resume::None);
        assert_eq!(
            Resume::parse(&headers(Some("  ")), Some("last_event_id=3")),
            since("3")
        );
        assert_eq!(Resume::parse(&headers(Some("a b")), None), Resume::Invalid);
        assert_eq!(
            Resume::parse(&headers(None), Some("last_event_id=%2F")),
            Resume::Invalid
        );
        assert_eq!(
            Resume::parse(&headers(Some(&"x".repeat(129))), None),
            Resume::Invalid
        );
        assert_eq!(since("g-1").cursor().map(Cursor::as_str), Some("g-1"));
        assert_eq!(Resume::Invalid.cursor(), None);
    }

    #[test]
    fn from_parts_reads_uri_query() {
        let req = http::Request::builder()
            .uri("/v1/screens/a/stream?last_event_id=g-11")
            .body(())
            .unwrap();
        let (parts, _) = req.into_parts();
        assert_eq!(Resume::from_parts(&parts), since("g-11"));
    }
}
