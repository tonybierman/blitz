#![allow(dead_code)]
use blitz_traits::net::{Bytes, NetHandler, NetWaker};
use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Standard delay for mock responses in concurrency-sensitive tests.
/// Wide enough to give deterministic mid-flight observation windows on slow CI.
pub const RESPONSE_DELAY: Duration = Duration::from_millis(300);

pub struct CaptureHandler(pub tokio::sync::oneshot::Sender<Result<(String, Bytes), String>>);

impl NetHandler for CaptureHandler {
    fn bytes(self: Box<Self>, url: String, b: Bytes) {
        let _ = self.0.send(Ok((url, b)));
    }
}

#[derive(Default, Clone)]
pub struct CaptureWaker(pub Arc<Mutex<Vec<usize>>>);

impl NetWaker for CaptureWaker {
    fn wake(&self, id: usize) {
        self.0.lock().unwrap().push(id);
    }
}

pub fn make_url(s: &str) -> url::Url {
    url::Url::parse(s).expect("valid url")
}

pub fn write_tempfile(contents: &[u8]) -> tempfile::NamedTempFile {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(contents).unwrap();
    tmp
}

pub async fn mount_get_ok(server: &MockServer) {
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

pub async fn mount_get_body(server: &MockServer, body: &'static str) {
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

pub async fn mount_get_status(server: &MockServer, status: u16) {
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(status))
        .mount(server)
        .await;
}

pub async fn mount_post_ok(server: &MockServer) {
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

/// Poll `condition` until it returns true or `timeout` elapses, sleeping between checks.
pub async fn wait_until<F: FnMut() -> bool>(timeout: Duration, mut condition: F) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    condition()
}

/// Poll `server.received_requests().len()` until it reaches `target` or `timeout` elapses.
/// Returns the count at the moment the loop exits — caller can compare against `target`
/// to distinguish "reached the cap" from "timed out below the cap".
pub async fn wait_for_received(server: &MockServer, target: usize, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        let n = server.received_requests().await.unwrap().len();
        if n >= target || Instant::now() >= deadline {
            return n;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
