//! A WHATWG-faithful SSE parser for clients and tests.
//!
//! Feed bytes as they arrive; get back dispatched frames and `retry:`
//! hints. The parser keeps the last event id exactly as the specification
//! does: an `id:` line sets it (even on a frame with no data), an EMPTY
//! `id:` resets it, and a NUL in the value is ignored.

use std::time::Duration;

/// One dispatched event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// `event:` name; `None` for the default `message` event.
    pub name: Option<String>,
    /// Joined `data:` lines (the trailing newline removed).
    pub data: String,
    /// The last event id in force when this frame was dispatched.
    pub id: Option<String>,
}

/// What a chunk of bytes produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parsed {
    /// A frame with data.
    Event(Frame),
    /// A `retry:` hint (milliseconds on the wire).
    Retry(Duration),
}

/// Incremental parser. One per connection.
#[derive(Debug, Default)]
pub struct Parser {
    line: Vec<u8>,
    saw_cr: bool,
    at_start: bool,
    data: String,
    event: Option<String>,
    pending_id: Option<String>,
    last_event_id: Option<String>,
}

impl Parser {
    /// A parser positioned at the start of a stream (a leading BOM is skipped).
    pub fn new() -> Self {
        Self {
            at_start: true,
            ..Self::default()
        }
    }

    /// A parser for a RECONNECTION: the last event id persists across
    /// connections (the browser keeps it too), so frames without an `id:`
    /// on the new stream still report the cursor in force.
    pub fn with_last_event_id(mut self, id: Option<String>) -> Self {
        self.last_event_id = id.filter(|s| !s.is_empty());
        self
    }

    /// The last event id string, `None` when unset or reset.
    pub fn last_event_id(&self) -> Option<&str> {
        self.last_event_id.as_deref()
    }

    /// Feed bytes; returns everything they completed.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<Parsed> {
        let mut out = Vec::new();
        for &b in bytes {
            if self.saw_cr {
                self.saw_cr = false;
                if b == b'\n' {
                    continue; // the \n of a \r\n
                }
            }
            match b {
                b'\r' => {
                    self.saw_cr = true;
                    self.end_line(&mut out);
                }
                b'\n' => self.end_line(&mut out),
                _ => self.line.push(b),
            }
        }
        out
    }

    fn end_line(&mut self, out: &mut Vec<Parsed>) {
        let mut line = std::mem::take(&mut self.line);
        if self.at_start {
            self.at_start = false;
            if line.starts_with(&[0xEF, 0xBB, 0xBF]) {
                line.drain(..3);
            }
        }
        let line = String::from_utf8_lossy(&line).into_owned();
        if line.is_empty() {
            self.dispatch(out);
            return;
        }
        if line.starts_with(':') {
            return; // comment
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line.as_str(), ""),
        };
        match field {
            "event" => self.event = Some(value.to_owned()),
            "data" => {
                self.data.push_str(value);
                self.data.push('\n');
            }
            "id" => {
                if !value.contains('\0') {
                    self.pending_id = Some(value.to_owned());
                }
            }
            // Only ASCII digits count (`u64::from_str` would also take a `+`).
            "retry" if !value.is_empty() && value.bytes().all(|c| c.is_ascii_digit()) => {
                if let Ok(ms) = value.parse::<u64>() {
                    out.push(Parsed::Retry(Duration::from_millis(ms)));
                }
            }
            _ => {} // unknown field: ignored
        }
    }

    fn dispatch(&mut self, out: &mut Vec<Parsed>) {
        if let Some(id) = self.pending_id.take() {
            self.last_event_id = if id.is_empty() { None } else { Some(id) };
        }
        let event = self.event.take();
        if self.data.is_empty() {
            return;
        }
        let mut data = std::mem::take(&mut self.data);
        data.pop(); // the trailing newline
        out.push(Parsed::Event(Frame {
            name: event,
            data,
            id: self.last_event_id.clone(),
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events(p: &mut Parser, s: &str) -> Vec<Parsed> {
        p.feed(s.as_bytes())
    }

    #[test]
    fn dispatches_named_and_default_events_with_joined_data() {
        let mut p = Parser::new();
        let out = events(&mut p, "event: tick\ndata: a\ndata: b\n\ndata: plain\n\n");
        assert_eq!(
            out,
            vec![
                Parsed::Event(Frame {
                    name: Some("tick".into()),
                    data: "a\nb".into(),
                    id: None
                }),
                Parsed::Event(Frame {
                    name: None,
                    data: "plain".into(),
                    id: None
                }),
            ]
        );
    }

    #[test]
    fn id_persists_across_frames_and_an_empty_id_resets_it() {
        let mut p = Parser::new();
        events(&mut p, "id: 7\nevent: a\ndata: x\n\n");
        assert_eq!(p.last_event_id(), Some("7"));
        let out = events(&mut p, "event: b\ndata: y\n\n");
        assert!(matches!(&out[0], Parsed::Event(f) if f.id.as_deref() == Some("7")));
        events(&mut p, "id: \nevent: resync\ndata: {}\n\n");
        assert_eq!(p.last_event_id(), None);
        // an id-only frame still moves the cursor; a NUL is ignored
        events(&mut p, "id: 9\n\n");
        assert_eq!(p.last_event_id(), Some("9"));
        events(&mut p, "id: 1\u{0}0\n\n");
        assert_eq!(p.last_event_id(), Some("9"));
    }

    #[test]
    fn a_seeded_cursor_survives_frames_without_an_id() {
        let mut p = Parser::new().with_last_event_id(Some("60".into()));
        let out = p.feed(b"event: ping\ndata: {}\n\n");
        assert!(matches!(&out[0], Parsed::Event(f) if f.id.as_deref() == Some("60")));
        assert_eq!(p.last_event_id(), Some("60"));
    }

    #[test]
    fn retry_only_frames_and_comments_dispatch_nothing_but_the_hint() {
        let mut p = Parser::new();
        let out = events(&mut p, "retry: 750\n\n: keep\n\nretry: x\n\n");
        assert_eq!(out, vec![Parsed::Retry(Duration::from_millis(750))]);
    }

    #[test]
    fn handles_crlf_and_cr_line_endings_split_across_chunks() {
        let mut p = Parser::new();
        let mut out = p.feed(b"event: a\r");
        out.extend(p.feed(b"\ndata: 1\r\n\r"));
        out.extend(p.feed(b"\n"));
        assert_eq!(
            out,
            vec![Parsed::Event(Frame {
                name: Some("a".into()),
                data: "1".into(),
                id: None
            })]
        );
    }

    #[test]
    fn skips_a_leading_bom_and_strips_one_space_after_the_colon() {
        let mut p = Parser::new();
        let out = p.feed(b"\xEF\xBB\xBFdata:  two spaces\n\n");
        assert!(matches!(&out[0], Parsed::Event(f) if f.data == " two spaces"));
    }
}
