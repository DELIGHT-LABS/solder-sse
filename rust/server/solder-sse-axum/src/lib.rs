//! axum adapter for [`solder_sse_server`].
//!
//! Two newtypes, because Rust's orphan rule forbids implementing axum's
//! traits for `solder_sse`'s types from a third crate:
//!
//! * [`Resume`] — an extractor; `Deref`s to [`solder_sse_server::Resume`].
//! * [`Sse`] — wraps a built [`solder_sse_server::SseResponse`] as `IntoResponse`.
//!
//! ```ignore
//! async fn stream(Resume(resume): Resume, State(app): State<App>) -> Response {
//!     let r = resumable(&*app.log, "topic", resume, || broadcast::bridge(app.bus.subscribe()), 500).await;
//!     Sse(SseResponseBuilder::new().resync(r.resync).build(r.stream.map(to_event))).into_response()
//! }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use axum_core::extract::FromRequestParts;
use axum_core::response::{IntoResponse, Response};
use futures_core::Stream;
use http::request::Parts;
use solder_sse::Event;
use std::convert::Infallible;
use std::ops::Deref;
use std::time::Duration;

/// Extractor: the client's resume cursor from `Last-Event-ID` or
/// `?last_event_id=`. Never rejects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resume(pub solder_sse_server::Resume);

impl Deref for Resume {
    type Target = solder_sse_server::Resume;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Resume> for solder_sse_server::Resume {
    fn from(r: Resume) -> Self {
        r.0
    }
}

impl<S: Send + Sync> FromRequestParts<S> for Resume {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Infallible> {
        Ok(Resume(solder_sse_server::Resume::from_parts(parts)))
    }
}

/// A resumable SSE response as an axum response.
pub struct Sse<S>(pub solder_sse_server::SseResponse<S>);

impl<S, E> IntoResponse for Sse<S>
where
    S: Stream<Item = Result<Event, E>> + Send + 'static,
    E: Into<axum_core::BoxError> + 'static,
{
    fn into_response(self) -> Response {
        self.0.into_http().map(axum_core::body::Body::new)
    }
}

/// The 503 + `Retry-After` response for "cannot open the stream now".
pub fn unavailable(retry_after: Duration) -> Response {
    solder_sse_server::unavailable(retry_after).map(axum_core::body::Body::new)
}

/// Re-exports so an adapter user needs one `use`.
pub use solder_sse;
pub use solder_sse_server;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::Router;
    use futures_util::stream;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn handler(Resume(resume): Resume) -> Response {
        let events = stream::iter(vec![Ok::<_, Infallible>(Event::named("cursor").data(
            match &resume {
                solder_sse_server::Resume::Since(c) => c.as_str().to_owned(),
                solder_sse_server::Resume::None => "none".to_owned(),
                solder_sse_server::Resume::Invalid => "invalid".to_owned(),
            },
        ))]);
        Sse(solder_sse_server::SseResponseBuilder::new()
            .no_retry()
            .no_keep_alive()
            .build(events))
        .into_response()
    }

    #[tokio::test]
    async fn extractor_reads_header_and_query_and_response_is_event_stream() {
        let app = Router::new().route("/s", get(handler));
        let res = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/s?last_event_id=g-5")
                    .header("last-event-id", "g-9")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers()["content-type"],
            solder_sse_server::response::CONTENT_TYPE_VALUE
        );
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, "event: cursor\ndata: g-9\n\n");

        let res = app
            .oneshot(
                http::Request::builder()
                    .uri("/s?last_event_id=g-5")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, "event: cursor\ndata: g-5\n\n");
    }

    #[tokio::test]
    async fn unavailable_maps_to_503() {
        let res = unavailable(Duration::from_secs(2));
        assert_eq!(res.status(), 503);
        assert_eq!(res.headers()["retry-after"], "2");
    }
}
