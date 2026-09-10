//! The golden vectors of `spec/vectors/` — the encoder must produce them and
//! the parser must read them back. The browser package keeps the same files.
use solder_sse::parse::{Frame, Parsed, Parser};
use solder_sse::{Cursor, Event, Ping, Resync};
use std::time::Duration;

/// `spec/vectors/` at the repository root — the same files every
/// implementation, in every language, is checked against.
fn vector(name: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/../../../spec/vectors/{name}.sse",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

fn parse(bytes: &[u8]) -> Vec<Parsed> {
    let mut p = Parser::new();
    let mut out = Vec::new();
    // Feed one byte at a time: chunk boundaries must not matter.
    for b in bytes {
        out.extend(p.feed(std::slice::from_ref(b)));
    }
    out
}

fn cursor(token: &str) -> Cursor {
    Cursor::parse(token).unwrap()
}

#[test]
fn connect_is_retry_then_ping() {
    let bytes = vector("connect");
    let mut encoded = Event::new()
        .retry(Duration::from_millis(750))
        .encode()
        .to_vec();
    encoded.extend_from_slice(&Event::ping(&Ping::default()).encode());
    assert_eq!(encoded, bytes);
    assert_eq!(
        parse(&bytes),
        vec![
            Parsed::Retry(Duration::from_millis(750)),
            Parsed::Event(Frame {
                name: Some("ping".into()),
                data: r#"{"every":15}"#.into(),
                id: None,
                own_id: None
            })
        ]
    );
}

#[test]
fn ping_may_announce_the_rotation_age() {
    let bytes = vector("ping-max-age");
    let ping = Ping {
        every: 15,
        max_age: Some(30),
    };
    assert_eq!(Event::ping(&ping).encode(), bytes);
    assert_eq!(
        parse(&bytes),
        vec![Parsed::Event(Frame {
            name: Some("ping".into()),
            data: r#"{"every":15,"max_age":30}"#.into(),
            id: None,
            own_id: None
        })]
    );
}

#[test]
fn event_carries_its_cursor_as_id() {
    // The cursor is opaque: a log's `<generation>-<sequence>` here, a plain
    // number in `multiline` — a client echoes either without reading it.
    let bytes = vector("event");
    assert_eq!(
        Event::named("new_message")
            .cursor(Some(cursor("7f3a9c2e-48211")))
            .data(r#"{"n":48211}"#)
            .encode(),
        bytes
    );
    assert_eq!(
        parse(&bytes),
        vec![Parsed::Event(Frame {
            name: Some("new_message".into()),
            data: r#"{"n":48211}"#.into(),
            id: Some("7f3a9c2e-48211".into()),
            own_id: Some("7f3a9c2e-48211".into())
        })]
    );
}

#[test]
fn resync_resets_the_cursor_and_may_name_the_earliest() {
    let bytes = vector("resync");
    assert_eq!(
        Event::resync(&Resync::expired(Some(cursor("7f3a9c2e-47900")))).encode(),
        bytes
    );
    let mut p = Parser::new();
    p.feed(b"id: 7f3a9c2e-1\n\n");
    let out = p.feed(&bytes);
    assert_eq!(p.last_event_id(), None);
    assert!(matches!(
        &out[0],
        Parsed::Event(f)
            if f.name.as_deref() == Some("resync") && f.id.is_none() && f.own_id.as_deref() == Some("")
    ));

    let bytes = vector("resync-unknown");
    assert_eq!(Event::resync(&Resync::unknown()).encode(), bytes);
    assert!(
        matches!(&parse(&bytes)[0], Parsed::Event(f) if f.data == r#"{"reason":"unknown","earliest":null}"#)
    );
}

#[test]
fn multi_line_data_round_trips() {
    let bytes = vector("multiline");
    assert_eq!(
        Event::named("note")
            .cursor(Some(cursor("5")))
            .data("line one\nline two")
            .encode(),
        bytes
    );
    assert!(
        matches!(&parse(&bytes)[0], Parsed::Event(f) if f.data == "line one\nline two" && f.id.as_deref() == Some("5"))
    );
}
