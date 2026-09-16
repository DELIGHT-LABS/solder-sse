//! The HTTP response: an `http_body::Body` that frames events, keeps the
//! connection alive with a named `ping` and, when asked, ends on its own
//! after a lifetime so that connections rotate (profile §8).

use bytes::Bytes;
use futures_core::Stream;
use http::header::{HeaderValue, CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER};
use http::{Response, StatusCode};
use http_body::Frame;
use http_body_util::Full;
use pin_project_lite::pin_project;
use solder_sse::{Event, Ping, Resync};
use std::collections::VecDeque;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Sleep};

/// The `Content-Type` every SSE response carries.
pub const CONTENT_TYPE_VALUE: &str = "text/event-stream; charset=utf-8";

pin_project! {
    /// The response body. Frames come from the event stream; while it is
    /// quiet, a keep-alive frame goes out every interval; past the
    /// rotation deadline — armed when the response is built — the body
    /// ends at the next frame boundary.
    pub struct SseBody<S> {
        #[pin]
        events: S,
        head: VecDeque<Bytes>,
        #[pin]
        keep_alive: Option<KeepAlive>,
        #[pin]
        deadline: Option<Sleep>,
        done: bool,
    }
}

pin_project! {
    struct KeepAlive {
        #[pin]
        sleep: Sleep,
        every: Duration,
        frame: Bytes,
    }
}

impl<S, E> http_body::Body for SseBody<S>
where
    S: Stream<Item = Result<Event, E>>,
{
    type Data = Bytes;
    type Error = E;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, E>>> {
        let mut this = self.project();
        if let Some(bytes) = this.head.pop_front() {
            return Poll::Ready(Some(Ok(Frame::data(bytes))));
        }
        if *this.done {
            return Poll::Ready(None);
        }
        // Rotation: past the deadline the response ends here — between
        // frames, after every head frame, never inside one. A client
        // speaking the profile reopens at once and resumes from its cursor,
        // so an event left behind is replayed on the next connection.
        if let Some(deadline) = this.deadline.as_mut().as_pin_mut() {
            if deadline.poll(cx).is_ready() {
                *this.done = true;
                return Poll::Ready(None);
            }
        }
        match this.events.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => {
                if let Some(ka) = this.keep_alive.as_mut().as_pin_mut() {
                    let ka = ka.project();
                    let every = *ka.every;
                    ka.sleep.reset(Instant::now() + every);
                }
                Poll::Ready(Some(Ok(Frame::data(event.encode()))))
            }
            Poll::Ready(Some(Err(e))) => {
                *this.done = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                *this.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                let Some(ka) = this.keep_alive.as_mut().as_pin_mut() else {
                    return Poll::Pending;
                };
                let mut ka = ka.project();
                ready!(ka.sleep.as_mut().poll(cx));
                let every = *ka.every;
                ka.sleep.reset(Instant::now() + every);
                Poll::Ready(Some(Ok(Frame::data(ka.frame.clone()))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done && self.head.is_empty()
    }
}

/// A built response: headers plus the [`SseBody`]. Turn it into
/// `http::Response` with [`SseResponse::into_http`]; framework adapters wrap
/// that in their own response type.
pub struct SseResponse<S> {
    body: SseBody<S>,
}

impl<S> SseResponse<S> {
    /// The `http::Response` with the SSE headers set: `Content-Type`,
    /// `Cache-Control: no-cache, no-transform` (no intermediary may buffer
    /// or recompress the stream) and `X-Accel-Buffering: no` (nginx).
    pub fn into_http(self) -> Response<SseBody<S>> {
        let mut resp = Response::new(self.body);
        let h = resp.headers_mut();
        h.insert(CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_VALUE));
        h.insert(
            CACHE_CONTROL,
            HeaderValue::from_static("no-cache, no-transform"),
        );
        h.insert("x-accel-buffering", HeaderValue::from_static("no"));
        resp
    }
}

/// Rotation: the lifetime window a connection draws from and what the
/// `ping` announces for it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MaxAge {
    /// Milliseconds, drawn uniformly per connection.
    millis: Range<u64>,
    /// The nominal lifetime in whole seconds; 0 announces nothing.
    announced_secs: u64,
}

/// Configures the frames that open every connection, the keep-alive and
/// the rotation.
#[derive(Debug, Clone)]
pub struct SseResponseBuilder {
    retry: Option<Range<u64>>,
    /// The keep-alive interval; `None` sends no `ping`.
    keep_alive: Option<Duration>,
    max_age: Option<MaxAge>,
    head: Vec<Event>,
}

impl Default for SseResponseBuilder {
    fn default() -> Self {
        Self {
            retry: Some(500..1000),
            keep_alive: Some(Duration::from_secs(solder_sse::PING_EVERY_SECS)),
            max_age: None,
            head: Vec::new(),
        }
    }
}

impl SseResponseBuilder {
    /// The defaults: `retry:` jittered in 500..1000ms, `ping` every 15s,
    /// no rotation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Rotation (profile §8): end the response after `age`, jittered ±10%
    /// per connection so a fleet does not reconnect in lockstep — what gRPC
    /// does for its max connection age. The end is an ordinary end of the
    /// response at a frame boundary, after every head frame; a client
    /// speaking the profile reopens at once and resumes from its cursor,
    /// so a rotation costs nothing. Keep the age below any intermediary's
    /// response timeout, so the server — not the proxy — ends the stream.
    /// The default `ping` announces the nominal age in whole seconds
    /// (`{"every":15,"max_age":30}`); under a second nothing is announced.
    /// `Duration::ZERO` means no rotation — a configuration's `0` can be
    /// passed through unchanged.
    pub fn max_age(mut self, age: Duration) -> Self {
        if age.is_zero() {
            self.max_age = None;
            return self;
        }
        let ms = age.as_millis() as u64;
        let spread = ms / 10;
        self.max_age_window(ms.saturating_sub(spread)..ms + spread + 1, age.as_secs())
    }

    /// Rotation with an explicit lifetime window in milliseconds, drawn
    /// uniformly per connection (`ms..ms + 1` is exact). The `ping`
    /// announces the window's midpoint in whole seconds.
    pub fn max_age_jitter(self, millis: Range<u64>) -> Self {
        let mid = millis.start + millis.end.saturating_sub(millis.start) / 2;
        self.max_age_window(millis, mid / 1000)
    }

    fn max_age_window(mut self, millis: Range<u64>, announced_secs: u64) -> Self {
        self.max_age = Some(MaxAge {
            millis,
            announced_secs,
        });
        self
    }

    /// A `retry:` hint drawn uniformly from `millis` per connection, so a
    /// fleet's browsers do not reconnect in lockstep. Default `500..1000`.
    pub fn retry_jitter(mut self, millis: Range<u64>) -> Self {
        self.retry = Some(millis);
        self
    }

    /// A fixed `retry:` hint.
    pub fn retry(mut self, delay: Duration) -> Self {
        let ms = delay.as_millis() as u64;
        self.retry = Some(ms..ms + 1);
        self
    }

    /// No `retry:` hint (the browser default applies: 3s Chromium, 5s Firefox).
    pub fn no_retry(mut self) -> Self {
        self.retry = None;
        self
    }

    /// Send [`Event::ping`] whenever the stream has been quiet for `every`.
    /// Default 15s — under every common proxy idle timeout. The frame is
    /// the profile's: it announces the interval (whole seconds; under a
    /// second it announces the default) and the rotation age, so a client
    /// can arm its dead-man timer — a comment keep-alive would be
    /// invisible to `EventSource` and announce nothing.
    pub fn keep_alive(mut self, every: Duration) -> Self {
        self.keep_alive = Some(every);
        self
    }

    /// No keep-alive.
    pub fn no_keep_alive(mut self) -> Self {
        self.keep_alive = None;
        self
    }

    /// Announce a failed resume: [`crate::Resumed::resync`]. `None` adds
    /// nothing.
    pub fn resync(mut self, resync: Option<Resync>) -> Self {
        if let Some(resync) = resync {
            self.head.push(Event::resync(&resync));
        }
        self
    }

    /// Any frame to send before the stream (a connect snapshot, for example).
    pub fn head(mut self, event: Event) -> Self {
        self.head.push(event);
        self
    }

    /// Build with the event stream. The first frame on the wire is the
    /// `retry:` hint, then `resync` and other head frames in the order they
    /// were added, then the stream.
    pub fn build<S, E>(self, events: S) -> SseResponse<S>
    where
        S: Stream<Item = Result<Event, E>>,
    {
        let mut head = VecDeque::with_capacity(self.head.len() + 2);
        if let Some(range) = self.retry {
            head.push_back(
                Event::new()
                    .retry(Duration::from_millis(solder_sse::jitter::uniform(range)))
                    .encode(),
            );
        }
        let announced_max_age = self
            .max_age
            .as_ref()
            .map(|m| m.announced_secs)
            .filter(|&secs| secs > 0);
        let keep_alive = self.keep_alive.map(|every| {
            // The announced interval is whole seconds: a sub-second
            // keep-alive (tests, tight links) announces the default and the
            // client keeps its default window.
            let secs = every.as_secs();
            let ping = Ping {
                every: if secs > 0 {
                    secs
                } else {
                    solder_sse::PING_EVERY_SECS
                },
                max_age: announced_max_age,
            };
            (every, Event::ping(&ping).encode())
        });
        // The keep-alive frame goes out once at connect, ahead of the first
        // interval: a client learns at once that this server sends pings and
        // can arm its dead-man timer — a connection cut before the first
        // interval would otherwise sit half-open with nothing to detect.
        if let Some((_, frame)) = &keep_alive {
            head.push_back(frame.clone());
        }
        head.extend(self.head.iter().map(Event::encode));
        let keep_alive = keep_alive.map(|(every, frame)| KeepAlive {
            sleep: tokio::time::sleep(every),
            every,
            frame,
        });
        let deadline = self.max_age.map(|m| {
            tokio::time::sleep(Duration::from_millis(solder_sse::jitter::uniform(m.millis)))
        });
        SseResponse {
            body: SseBody {
                events,
                head,
                keep_alive,
                deadline,
                done: false,
            },
        }
    }
}

/// The response for "the stream cannot be opened right now" (the upstream
/// is down, for example): 503 with `Retry-After`. A non-200 makes the
/// browser stop its own retry loop, so the client's watchdog takes over
/// with backoff instead of hammering every few seconds.
pub fn unavailable(retry_after: Duration) -> Response<Full<Bytes>> {
    let mut resp = Response::new(Full::new(Bytes::from_static(b"stream unavailable")));
    *resp.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    let secs = retry_after.as_secs().max(1).to_string();
    resp.headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from_str(&secs).expect("digits"));
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use http_body_util::BodyExt;
    use std::convert::Infallible;
    use std::sync::Arc;
    use tokio::sync::Notify;

    async fn frames<S>(body: SseBody<S>) -> String
    where
        S: Stream<Item = Result<Event, Infallible>>,
    {
        let bytes = Box::pin(body).collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn retry_then_head_then_events() {
        let events = stream::iter(vec![Ok::<_, Infallible>(Event::named("a").id(1).data("x"))]);
        let resp = SseResponseBuilder::new()
            .retry(Duration::from_millis(750))
            .no_keep_alive()
            .resync(Some(Resync::unknown()))
            .head(Event::comment("snapshot"))
            .build(events)
            .into_http();
        assert_eq!(resp.headers()[CONTENT_TYPE], CONTENT_TYPE_VALUE);
        assert_eq!(resp.headers()[CACHE_CONTROL], "no-cache, no-transform");
        assert_eq!(resp.headers()["x-accel-buffering"], "no");
        assert_eq!(
            frames(resp.into_body()).await,
            "retry: 750\n\nid: \nevent: resync\ndata: {\"reason\":\"unknown\",\"earliest\":null}\n\n: snapshot\n\nid: 1\nevent: a\ndata: x\n\n"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pings_while_quiet_and_resets_on_traffic() {
        let gate = Arc::new(Notify::new());
        let g = gate.clone();
        let events = stream::once(async move {
            g.notified().await;
            Ok::<_, Infallible>(Event::named("a").id(1))
        });
        let mut body = Box::pin(
            SseResponseBuilder::new()
                .no_retry()
                .keep_alive(Duration::from_secs(15))
                .build(events)
                .into_http()
                .into_body(),
        );
        // one ping at connect, then two more over 31s of silence
        let first = tokio::time::timeout(Duration::from_secs(31), async {
            let a = body.frame().await.unwrap().unwrap().into_data().unwrap();
            let b = body.frame().await.unwrap().unwrap().into_data().unwrap();
            let c = body.frame().await.unwrap().unwrap().into_data().unwrap();
            (a, b, c)
        })
        .await
        .expect("three pings within 31s");
        assert_eq!(first.0, "event: ping\ndata: {\"every\":15}\n\n");
        assert_eq!(first.1, "event: ping\ndata: {\"every\":15}\n\n");
        assert_eq!(first.2, "event: ping\ndata: {\"every\":15}\n\n");
        // an event resets the timer: nothing for the next 14s
        gate.notify_one();
        let ev = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(ev, "id: 1\nevent: a\n\n");
        assert!(body.frame().await.is_none()); // stream ended → body ends
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_ends_the_body_at_its_deadline_after_pinging_until_then() {
        let events = stream::pending::<Result<Event, Infallible>>();
        let mut body = Box::pin(
            SseResponseBuilder::new()
                .no_retry()
                .keep_alive(Duration::from_millis(400))
                .max_age_jitter(1_000..1_001)
                .build(events)
                .into_http()
                .into_body(),
        );
        let started = tokio::time::Instant::now();
        // the connect ping, then one per 400ms of silence: 0, 400, 800
        for _ in 0..3 {
            let f = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert!(f.starts_with(b"event: ping"), "{f:?}");
        }
        assert!(body.frame().await.is_none(), "the body ends on its own");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_secs(1) && elapsed < Duration::from_millis(1_200),
            "ended at the deadline: {elapsed:?}"
        );
        assert!(http_body::Body::is_end_stream(&*body));
    }

    #[tokio::test(start_paused = true)]
    async fn head_frames_are_delivered_even_past_the_deadline() {
        // A connect snapshot must always reach the client; only live events
        // are left for the next connection (the log replays them).
        let events = stream::iter(vec![Ok::<_, Infallible>(Event::named("a").id(1))]);
        let mut body = Box::pin(
            SseResponseBuilder::new()
                .no_retry()
                .no_keep_alive()
                .resync(Some(Resync::unknown()))
                .head(Event::comment("snapshot"))
                .max_age_jitter(0..1)
                .build(events)
                .into_http()
                .into_body(),
        );
        let a = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert!(a.starts_with(b"id: \nevent: resync"), "{a:?}");
        let b = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(b, ": snapshot\n\n");
        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn the_ping_announces_the_nominal_max_age_in_whole_seconds() {
        async fn first(b: SseResponseBuilder) -> Bytes {
            let mut body = Box::pin(
                b.no_retry()
                    .build(stream::empty::<Result<Event, Infallible>>())
                    .into_http()
                    .into_body(),
            );
            body.frame().await.unwrap().unwrap().into_data().unwrap()
        }
        assert_eq!(
            first(SseResponseBuilder::new().max_age(Duration::from_secs(30))).await,
            "event: ping\ndata: {\"every\":15,\"max_age\":30}\n\n"
        );
        assert_eq!(
            first(SseResponseBuilder::new().max_age_jitter(20_000..40_001)).await,
            "event: ping\ndata: {\"every\":15,\"max_age\":30}\n\n"
        );
        // under a second there is nothing worth announcing
        assert_eq!(
            first(SseResponseBuilder::new().max_age_jitter(500..600)).await,
            "event: ping\ndata: {\"every\":15}\n\n"
        );
    }

    #[test]
    fn max_age_draws_from_a_ten_percent_window() {
        let b = SseResponseBuilder::new().max_age(Duration::from_secs(30));
        assert_eq!(
            b.max_age,
            Some(MaxAge {
                millis: 27_000..33_001,
                announced_secs: 30
            })
        );
        let b = SseResponseBuilder::new().max_age_jitter(300..301);
        assert_eq!(b.max_age.unwrap().announced_secs, 0);
        // a configuration's 0 passes through as "never", and unsets
        let b = SseResponseBuilder::new().max_age(Duration::ZERO);
        assert_eq!(b.max_age, None);
        let b = SseResponseBuilder::new()
            .max_age(Duration::from_secs(30))
            .max_age(Duration::ZERO);
        assert_eq!(b.max_age, None);
    }

    #[tokio::test]
    async fn unavailable_is_503_with_retry_after() {
        let resp = unavailable(Duration::from_secs(2));
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers()[RETRY_AFTER], "2");
    }
}
