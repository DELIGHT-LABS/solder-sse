//! The wire format: one [`Event`] is one SSE frame.

use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use std::time::Duration;

/// Keep-alive event name. Carries no `id`, so it never moves the client's
/// resume cursor.
pub const PING: &str = "ping";
/// Sent once, right after connect, when the server could not replay from
/// the client's `Last-Event-ID`. The client reloads a snapshot.
pub const RESYNC: &str = "resync";
/// The keep-alive interval the profile recommends and [`Event::ping`]
/// announces, seconds.
pub const PING_EVERY_SECS: u64 = 15;

/// Why a resume could not be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ResyncReason {
    /// The id is older than what the log retains.
    Expired,
    /// The id is not one this server issued — a restart, a different
    /// deployment, or a malformed value.
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

/// One SSE frame. Build it with the chainable setters and encode with
/// [`Event::encode`]; every field is optional so the same type expresses a
/// data event, a bare `retry:` hint, or a comment.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Event {
    /// `id:` — the resume cursor. Must not contain a newline or NUL.
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

    /// The `resync` frame for a failed resume. `earliest_seq` is the oldest
    /// sequence the log still holds (0 when unknown).
    pub fn resync(reason: ResyncReason, earliest_seq: u64) -> Self {
        // An EMPTY `id:` resets the browser's last event id (WHATWG), so its
        // own next retry does not resend the cursor this frame rejected.
        Self::named(RESYNC).id("").data(
            serde_json::json!({ "reason": reason.as_str(), "earliest_seq": earliest_seq })
                .to_string(),
        )
    }

    /// The keep-alive frame: `event: ping` carrying the interval in seconds (`{"every":15}`).
    pub fn ping() -> Self {
        Self::ping_every(PING_EVERY_SECS)
    }

    /// The keep-alive frame carrying a specific interval in seconds: `{"every":<secs>}`.
    pub fn ping_every(every_secs: u64) -> Self {
        Self::ping_with(every_secs, None)
    }

    /// The keep-alive frame announcing the interval and, when the server
    /// rotates streams (profile §10), the lifetime after which a healthy
    /// stream ends on purpose: `{"every":15,"max_age":30}`. Both in whole
    /// seconds; `None` announces no rotation.
    pub fn ping_with(every_secs: u64, max_age_secs: Option<u64>) -> Self {
        let data = match max_age_secs {
            Some(max_age) => format!(r#"{{"every":{every_secs},"max_age":{max_age}}}"#),
            None => format!(r#"{{"every":{every_secs}}}"#),
        };
        Self::named(PING).data(data)
    }

    /// Set `id:` from any displayable value.
    pub fn id(mut self, id: impl ToString) -> Self {
        self.id = Some(id.to_string());
        self
    }

    /// Set `id:` from a sequence number — the profile's cursor.
    pub fn seq(self, seq: u64) -> Self {
        self.id(seq)
    }

    /// Set `event:`.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set `data:` from text. Multi-line text becomes several `data:` lines.
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

    /// True when the frame carries an `id:` — i.e. it moves the cursor.
    pub fn has_id(&self) -> bool {
        self.id.is_some()
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
            for line in data.split('\n') {
                out.put_slice(b"data: ");
                out.put_slice(line.trim_end_matches('\r').as_bytes());
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
        let ev = Event::named("new_message")
            .seq(48211)
            .data("{\"seq\":48211}")
            .retry(Duration::from_millis(750));
        assert_eq!(
            ev.encode(),
            "retry: 750\nid: 48211\nevent: new_message\ndata: {\"seq\":48211}\n\n"
        );
    }

    #[test]
    fn splits_multi_line_data_and_strips_cr() {
        let ev = Event::new().data("a\r\nb\nc");
        assert_eq!(ev.encode(), "data: a\ndata: b\ndata: c\n\n");
    }

    #[test]
    fn ping_and_resync_frames_are_the_profile_constants() {
        assert_eq!(
            Event::ping().encode(),
            "event: ping\ndata: {\"every\":15}\n\n"
        );
        assert_eq!(
            Event::ping_every(30).encode(),
            "event: ping\ndata: {\"every\":30}\n\n"
        );
        assert_eq!(
            Event::ping_with(15, Some(30)).encode(),
            "event: ping\ndata: {\"every\":15,\"max_age\":30}\n\n"
        );
        assert_eq!(Event::ping_with(15, None).encode(), Event::ping().encode());
        assert_eq!(
            Event::resync(ResyncReason::Expired, 47900).encode(),
            "id: \nevent: resync\ndata: {\"earliest_seq\":47900,\"reason\":\"expired\"}\n\n"
        );
        assert!(!Event::ping().has_id());
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
