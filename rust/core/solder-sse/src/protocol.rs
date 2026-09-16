//! The wire format: one [`Event`] is one SSE frame.

use crate::cursor::Cursor;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Keep-alive event name. Carries no `id`, so it never moves the client's
/// resume cursor.
pub const PING: &str = "ping";
/// Sent once, right after connect, when the server could not replay from
/// the client's `Last-Event-ID`. The client reloads a snapshot.
pub const RESYNC: &str = "resync";
/// The keep-alive interval the profile recommends and [`Ping::default`]
/// announces, seconds.
pub const PING_EVERY_SECS: u64 = 15;

/// Why a resume could not be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResyncReason {
    /// The id is older than what the log retains.
    Expired,
    /// The id is not one this server issued — a restart, a different
    /// deployment, or a value that is not a cursor at all.
    Unknown,
}

impl ResyncReason {
    /// The wire spelling (`expired` | `unknown`).
    pub fn as_str(self) -> &'static str {
        match self {
            ResyncReason::Expired => "expired",
            ResyncReason::Unknown => "unknown",
        }
    }
}

/// What a `resync` frame says (`{"reason":"expired","earliest":"<cursor>"}`):
/// why the resume failed and, when the server knows it, the oldest cursor
/// it could still have replayed from. The server encodes it and the client
/// decodes it — one type, one wire shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resync {
    /// Why.
    pub reason: ResyncReason,
    /// The oldest cursor still retained on this stream; `None` (`null` on
    /// the wire) when the server has none or does not know. Information
    /// for a log line — a client cannot do anything with it but show it.
    #[serde(default)]
    pub earliest: Option<Cursor>,
}

impl Resync {
    /// The cursor is older than what the server retains.
    pub fn expired(earliest: Option<Cursor>) -> Self {
        Self {
            reason: ResyncReason::Expired,
            earliest,
        }
    }

    /// The cursor was never issued by this server.
    pub fn unknown() -> Self {
        Self {
            reason: ResyncReason::Unknown,
            earliest: None,
        }
    }
}

/// What the `ping` frame says (`{"every":15}`, or `{"every":15,"max_age":30}`
/// when the server rotates streams — profile §8): the keep-alive interval
/// and the lifetime after which a healthy stream ends on purpose. Whole
/// seconds; zero or absent announces nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ping {
    /// The keep-alive interval, seconds.
    #[serde(default)]
    pub every: u64,
    /// The rotation lifetime, seconds; absent when the server does not rotate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<u64>,
}

impl Default for Ping {
    /// The profile's recommendation: every 15 s, no rotation.
    fn default() -> Self {
        Self {
            every: PING_EVERY_SECS,
            max_age: None,
        }
    }
}

/// One SSE frame. Build it with the chainable setters and encode with
/// [`Event::encode`]; every field is optional so the same type expresses a
/// data event, a bare `retry:` hint, or a comment.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Event {
    /// `id:` — the resume cursor ([`Cursor`]); the profile sets it only on
    /// events that change state.
    pub id: Option<String>,
    /// `event:` — the name a client listens to.
    pub name: Option<String>,
    /// `data:` — one `data:` line per line of text.
    pub data: Option<String>,
    /// `retry:` — reconnection delay hint for the browser's own retry.
    pub retry: Option<Duration>,
    /// `: …` — a comment line, invisible to `EventSource`.
    pub comment: Option<String>,
}

impl Event {
    /// An empty frame; set fields with the chainable setters.
    pub fn new() -> Self {
        Self::default()
    }

    /// A frame with an `event:` name.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }

    /// A comment-only frame (`: text`).
    pub fn comment(text: impl Into<String>) -> Self {
        Self {
            comment: Some(text.into()),
            ..Self::default()
        }
    }

    /// The `resync` frame for a failed resume: [`Resync`] as JSON.
    pub fn resync(resync: &Resync) -> Self {
        // An EMPTY `id:` resets the browser's last event id (WHATWG), so its
        // own next retry does not resend the cursor this frame rejected.
        Self::named(RESYNC).id("").data(profile_json(resync))
    }

    /// The keep-alive frame: `event: ping` carrying what the server
    /// announces, [`Ping`] as JSON. Never carries an `id:`.
    pub fn ping(ping: &Ping) -> Self {
        Self::named(PING).data(profile_json(ping))
    }

    /// Set `id:` from any displayable value — a [`Cursor`], or the empty
    /// string that resets the client's cursor.
    pub fn id(mut self, id: impl ToString) -> Self {
        self.id = Some(id.to_string());
        self
    }

    /// Set `id:` from the log's cursor; `None` leaves the frame without
    /// one, so it does not move the client's cursor.
    pub fn cursor(mut self, cursor: Option<Cursor>) -> Self {
        self.id = cursor.map(String::from);
        self
    }

    /// Set `event:`.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set `data:` from text. Every line break — LF, CR LF or a bare CR —
    /// becomes a new `data:` line, the way the parser reads them.
    pub fn data(mut self, data: impl Into<String>) -> Self {
        self.data = Some(data.into());
        self
    }

    /// Set `data:` from a JSON-serialisable value.
    pub fn json<T: Serialize + ?Sized>(self, value: &T) -> Result<Self, serde_json::Error> {
        Ok(self.data(serde_json::to_string(value)?))
    }

    /// Set `retry:` (milliseconds on the wire).
    pub fn retry(mut self, delay: Duration) -> Self {
        self.retry = Some(delay);
        self
    }

    /// Encode as wire bytes, terminated by the blank line.
    ///
    /// Field order is `retry`, `id`, `event`, `data`, comment — the
    /// specification does not require an order, but a fixed one keeps the
    /// golden vectors stable across implementations.
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(64 + self.data.as_ref().map_or(0, String::len));
        if let Some(retry) = self.retry {
            out.put_slice(b"retry: ");
            out.put_slice(retry.as_millis().to_string().as_bytes());
            out.put_u8(b'\n');
        }
        if let Some(id) = &self.id {
            out.put_slice(b"id: ");
            out.put_slice(strip_line_breaks(id).as_bytes());
            out.put_u8(b'\n');
        }
        if let Some(name) = &self.name {
            out.put_slice(b"event: ");
            out.put_slice(strip_line_breaks(name).as_bytes());
            out.put_u8(b'\n');
        }
        if let Some(data) = &self.data {
            for line in lines(data) {
                out.put_slice(b"data: ");
                out.put_slice(line.as_bytes());
                out.put_u8(b'\n');
            }
        }
        if let Some(comment) = &self.comment {
            out.put_slice(b": ");
            out.put_slice(strip_line_breaks(comment).as_bytes());
            out.put_u8(b'\n');
        }
        out.put_u8(b'\n');
        out.freeze()
    }
}

/// The profile's own payloads cannot fail to serialise: plain fields, no maps.
fn profile_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("a profile payload serialises")
}

/// The lines of `data`, ended by LF, CR LF or a bare CR — the three line
/// breaks the specification's parser recognises. Each becomes its own
/// `data:` line; a bare CR left inside one would end the line early on the
/// client and let the rest of the payload pose as another field.
fn lines(data: &str) -> impl Iterator<Item = &str> {
    let mut rest = Some(data);
    std::iter::from_fn(move || {
        let s = rest?;
        match s.find(['\n', '\r']) {
            None => {
                rest = None;
                Some(s)
            }
            Some(i) => {
                let next = if s[i..].starts_with("\r\n") {
                    i + 2
                } else {
                    i + 1
                };
                rest = Some(&s[next..]);
                Some(&s[..i])
            }
        }
    })
}

/// `id:` and `event:` are single-line fields; a line break inside one would
/// forge a second field. Fold it to a space rather than trusting the caller.
fn strip_line_breaks(s: &str) -> std::borrow::Cow<'_, str> {
    if s.contains(['\n', '\r', '\0']) {
        std::borrow::Cow::Owned(s.replace(['\n', '\r', '\0'], " "))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_a_full_frame_in_fixed_field_order() {
        let cursor = Cursor::parse("7f3a9c2e-48211").unwrap();
        let ev = Event::named("new_message")
            .cursor(Some(cursor))
            .data("{\"n\":48211}")
            .retry(Duration::from_millis(750));
        assert_eq!(
            ev.encode(),
            "retry: 750\nid: 7f3a9c2e-48211\nevent: new_message\ndata: {\"n\":48211}\n\n"
        );
        assert!(Event::named("x").cursor(None).id.is_none());
    }

    #[test]
    fn every_line_break_starts_a_new_data_line() {
        assert_eq!(
            Event::new().data("a\r\nb\nc\rd").encode(),
            "data: a\ndata: b\ndata: c\ndata: d\n\n"
        );
        // A trailing break keeps its (empty) last line, so it round-trips.
        assert_eq!(Event::new().data("a\n").encode(), "data: a\ndata: \n\n");
        assert_eq!(Event::new().data("").encode(), "data: \n\n");
    }

    #[test]
    fn a_bare_cr_in_data_cannot_forge_a_field() {
        // Read by a spec parser, "a\rid: 9" would end the data line at the
        // CR and take `id: 9` as a field — moving the client's cursor.
        let ev = Event::named("t").data("a\rid: 9");
        assert_eq!(ev.encode(), "event: t\ndata: a\ndata: id: 9\n\n");
        let mut p = crate::Parser::new();
        let out = p.feed(&ev.encode());
        assert!(matches!(&out[0], crate::Parsed::Event(f) if f.data == "a\nid: 9"));
        assert_eq!(p.last_event_id(), None);
    }

    #[test]
    fn ping_and_resync_frames_are_the_profile_constants() {
        assert_eq!(
            Event::ping(&Ping::default()).encode(),
            "event: ping\ndata: {\"every\":15}\n\n"
        );
        assert_eq!(
            Event::ping(&Ping {
                every: 30,
                max_age: None
            })
            .encode(),
            "event: ping\ndata: {\"every\":30}\n\n"
        );
        assert_eq!(
            Event::ping(&Ping {
                every: 15,
                max_age: Some(30)
            })
            .encode(),
            "event: ping\ndata: {\"every\":15,\"max_age\":30}\n\n"
        );
        let earliest = Cursor::parse("7f3a9c2e-47900").unwrap();
        assert_eq!(
            Event::resync(&Resync::expired(Some(earliest))).encode(),
            "id: \nevent: resync\ndata: {\"reason\":\"expired\",\"earliest\":\"7f3a9c2e-47900\"}\n\n"
        );
        assert_eq!(
            Event::resync(&Resync::unknown()).encode(),
            "id: \nevent: resync\ndata: {\"reason\":\"unknown\",\"earliest\":null}\n\n"
        );
        assert!(Event::ping(&Ping::default()).id.is_none());
    }

    #[test]
    fn the_profile_payloads_read_back_as_written() {
        let ping = Ping {
            every: 15,
            max_age: Some(30),
        };
        let read = |s: &str| serde_json::from_str::<Ping>(s).unwrap();
        assert_eq!(read(r#"{"every":15,"max_age":30}"#), ping);
        assert_eq!(read(r#"{"max_age": 30, "every": 15}"#), ping);
        assert_eq!(
            read("{}"),
            Ping {
                every: 0,
                max_age: None
            }
        );
        let earliest = Cursor::parse("7f3a9c2e-47900").unwrap();
        let read = |s: &str| serde_json::from_str::<Resync>(s);
        assert_eq!(
            read(r#"{"reason":"expired","earliest":"7f3a9c2e-47900"}"#).unwrap(),
            Resync::expired(Some(earliest))
        );
        assert_eq!(
            read(r#"{"earliest":null,"reason":"unknown"}"#).unwrap(),
            Resync::unknown()
        );
        assert_eq!(read(r#"{"reason":"unknown"}"#).unwrap(), Resync::unknown());
        assert!(read(r#"{"reason":"later"}"#).is_err());
        assert!(read(r#"{"reason":"expired","earliest":"a b"}"#).is_err());
    }

    #[test]
    fn a_line_break_cannot_forge_a_field() {
        let ev = Event::named("x\nevent: y").id("1\r\nid: 2");
        assert_eq!(ev.encode(), "id: 1  id: 2\nevent: x event: y\n\n");
    }

    #[test]
    fn comment_only_frame() {
        assert_eq!(Event::comment("keep").encode(), ": keep\n\n");
    }
}
