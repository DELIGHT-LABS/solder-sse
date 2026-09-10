//! The resume cursor: an opaque token the server issues on `id:` and a
//! client hands back on reconnect.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// The longest token the profile allows, in bytes.
pub const MAX_LEN: usize = 128;

/// A resume cursor — the `id:` of an event, and the `Last-Event-ID` (or
/// `?last_event_id=`) a client presents on reconnect.
///
/// Opaque to the client: only the log that issued it can order it or say
/// whether it can still replay from it. On the wire it is 1 to 128 bytes
/// of the URL-unreserved alphabet (`A–Z a–z 0–9 - . _ ~`), so it travels
/// in a header, a query string and a JSON string without any encoding. A
/// log builds its tokens however it likes within that alphabet: a sequence
/// number, `<generation>-<sequence>`, a stream id, a hex digest.
///
/// Cloning is a reference count: a cursor is copied into every replayed
/// event, every fan-out receiver and the overlap set of every connection,
/// and none of those should allocate.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Cursor(Arc<str>);

/// Why a string is not a cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidCursor {
    /// The token is empty.
    Empty,
    /// The token is longer than [`MAX_LEN`] bytes.
    TooLong(usize),
    /// The token contains a character outside the unreserved alphabet.
    Character(char),
}

impl fmt::Display for InvalidCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidCursor::Empty => write!(f, "cursor is empty"),
            InvalidCursor::TooLong(n) => write!(f, "cursor is {n} bytes; at most {MAX_LEN}"),
            InvalidCursor::Character(c) => {
                write!(f, "cursor contains {c:?}; only A-Z a-z 0-9 - . _ ~")
            }
        }
    }
}

impl std::error::Error for InvalidCursor {}

impl Cursor {
    /// Validate a token.
    pub fn parse(token: &str) -> Result<Self, InvalidCursor> {
        validate(token)?;
        Ok(Self(Arc::from(token)))
    }

    /// The token.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate(token: &str) -> Result<(), InvalidCursor> {
    if token.is_empty() {
        return Err(InvalidCursor::Empty);
    }
    if token.len() > MAX_LEN {
        return Err(InvalidCursor::TooLong(token.len()));
    }
    match token.chars().find(|c| !is_unreserved(*c)) {
        Some(c) => Err(InvalidCursor::Character(c)),
        None => Ok(()),
    }
}

fn is_unreserved(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~')
}

impl fmt::Debug for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cursor({})", self.0)
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for Cursor {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl FromStr for Cursor {
    type Err = InvalidCursor;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<String> for Cursor {
    type Error = InvalidCursor;
    /// One copy, into the shared allocation; the `String` is not kept.
    fn try_from(s: String) -> Result<Self, Self::Error> {
        validate(&s)?;
        Ok(Self(Arc::from(s)))
    }
}

impl From<Cursor> for String {
    fn from(c: Cursor) -> Self {
        c.0.to_string()
    }
}

impl From<&Cursor> for String {
    fn from(c: &Cursor) -> Self {
        c.0.to_string()
    }
}

impl PartialEq<str> for Cursor {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for Cursor {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

impl serde::Serialize for Cursor {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for Cursor {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        Cursor::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_unreserved_alphabet_only() {
        for ok in ["1", "48211", "7f3a9c2e-48211", "a.b_c~d-E", "0"] {
            assert_eq!(Cursor::parse(ok).unwrap().as_str(), ok);
        }
        assert_eq!(Cursor::parse(""), Err(InvalidCursor::Empty));
        assert_eq!(Cursor::parse("a b"), Err(InvalidCursor::Character(' ')));
        assert_eq!(Cursor::parse("a&b=c"), Err(InvalidCursor::Character('&')));
        assert_eq!(Cursor::parse("1\n"), Err(InvalidCursor::Character('\n')));
        assert_eq!(Cursor::parse("é"), Err(InvalidCursor::Character('é')));
        let long = "x".repeat(MAX_LEN);
        assert!(Cursor::parse(&long).is_ok());
        assert_eq!(
            Cursor::parse(&format!("{long}x")),
            Err(InvalidCursor::TooLong(MAX_LEN + 1))
        );
    }

    #[test]
    fn clones_share_one_allocation() {
        let c = Cursor::parse("g-7").unwrap();
        let d = c.clone();
        assert!(Arc::ptr_eq(&c.0, &d.0));
    }

    #[test]
    fn converts_and_compares_as_its_token() {
        let c: Cursor = "g-7".parse().unwrap();
        assert_eq!(c, "g-7");
        assert_eq!(c.to_string(), "g-7");
        assert_eq!(String::from(&c), "g-7");
        assert_eq!(format!("{c:?}"), "Cursor(g-7)");
        assert_eq!(serde_json::to_string(&c).unwrap(), "\"g-7\"");
        assert_eq!(serde_json::from_str::<Cursor>("\"g-7\"").unwrap(), c);
        assert!(serde_json::from_str::<Cursor>("\"g 7\"").is_err());
        assert_eq!(Cursor::try_from(String::from("g-7")).unwrap(), c);
    }
}
