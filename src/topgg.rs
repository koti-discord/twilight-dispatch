//! Posting the bot's stats to top.gg: its server count and shard count
//! (`TOPGG_TOKEN`, `TOPGG_INTERVAL_SECS`).
//!
//! Only the process that runs shard 0 posts, so a bot split over several
//! processes posts once per interval. The server count is the number of
//! guilds in the state cache (`guild_keys`), which every process of the bot
//! shares: the guilds the bot is in, those in an outage included. The shard
//! count is the bot's shard total. The first post goes out one interval after
//! the start, so the shards have reconciled the cache by then, and the next
//! ones every interval after that.
//!
//! A post that fails is logged and not retried: the next one comes an
//! interval later. When top.gg rate limits a post, the next one waits as long
//! as it asks, if that is longer (at most [`MAX_RETRY_AFTER`]). An empty
//! cache is never posted: a server count of 0 would replace the real one.

use crate::{
    cache::keys::type_index,
    constants::GUILD_KEY,
    models::{ApiError, ApiResult},
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER, USER_AGENT},
    Method, Request, StatusCode, Uri,
};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client as HttpClient},
    rt::TokioExecutor,
};
use redis::aio::ConnectionLike;
use serde::Serialize;
use std::{error::Error, sync::Arc, time::Duration};
use tracing::{info, warn};

/// top.gg's endpoint for the stats of the project a token belongs to.
pub const METRICS_URL: &str = "https://top.gg/api/v1/projects/@me/metrics";

/// Time limit for one post, from connecting to the end of the answer.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The longest a rate limit holds back the next post.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(6 * 60 * 60);

/// The wait after a 429 that does not say how long to wait: top.gg blocks a
/// token that exceeded a limit for an hour.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(60 * 60);

/// At most this much of an answer's body is read.
const MAX_BODY: usize = 16 * 1024;

/// At most this many characters of a rejection's detail are logged.
const MAX_DETAIL: usize = 300;

const USER_AGENT_VALUE: &str = concat!(
    "twilight-dispatch/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/top-stats/twilight-dispatch)"
);

/// What is posted: `{"server_count": ..., "shard_count": ...}`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct Stats {
    /// The guilds the bot is in.
    pub server_count: u64,
    /// The bot's shard total.
    pub shard_count: u32,
}

/// What top.gg answered to a post.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    /// It took the stats (a 2xx status, normally 204).
    Posted,
    /// It rate limited the post (429): the next one may go out after this
    /// long, as its `Retry-After` header or `retry-after` body field said, in
    /// seconds (an hour when neither did, at most [`MAX_RETRY_AFTER`]).
    RateLimited(Duration),
    /// It refused the post with this status (400 or 401 for a bad token, 404,
    /// 422 for a body it does not take), and the body's `detail`.
    Rejected(StatusCode, String),
}

/// Posts stats to top.gg with a project token.
#[derive(Clone)]
pub struct Client {
    http: HttpClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    url: Uri,
    authorization: HeaderValue,
    timeout: Duration,
}

impl Client {
    /// A client for [`METRICS_URL`] that authenticates with `token`, a top.gg
    /// project token (sent as `Bearer <token>`), with [`REQUEST_TIMEOUT`].
    ///
    /// # Errors
    ///
    /// When `token` cannot be sent in a header or TLS cannot be set up.
    pub fn new(token: &str) -> ApiResult<Self> {
        Self::with_url(METRICS_URL, token, REQUEST_TIMEOUT)
    }

    /// A client that posts to `url` (`https://`, or `http://`) instead, with
    /// `timeout` for each post.
    ///
    /// # Errors
    ///
    /// As [`Client::new`], and when `url` does not parse.
    pub fn with_url(url: &str, token: &str, timeout: Duration) -> ApiResult<Self> {
        let parsed = url
            .parse::<Uri>()
            .map_err(|err| format!("invalid top.gg URL {url:?}: {err}"))?;
        let mut authorization = HeaderValue::try_from(format!("Bearer {}", token.trim()))
            .map_err(|_| "the top.gg token cannot be sent in a header".to_owned())?;
        authorization.set_sensitive(true);
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let connector = HttpsConnectorBuilder::new()
            .with_provider_and_webpki_roots(provider)
            .map_err(|err| format!("cannot set up TLS for top.gg: {err}"))?
            .https_or_http()
            .enable_http1()
            .build();
        Ok(Self {
            http: HttpClient::builder(TokioExecutor::new()).build(connector),
            url: parsed,
            authorization,
            timeout,
        })
    }

    /// Post `stats` and read the answer.
    ///
    /// # Errors
    ///
    /// When no answer came: the connection or the request failed, or the
    /// answer did not arrive in full within the timeout.
    pub async fn post(&self, stats: Stats) -> ApiResult<Outcome> {
        let body = Full::new(Bytes::from(serde_json::to_vec(&stats)?));
        let request = Request::builder()
            .method(Method::PATCH)
            .uri(self.url.clone())
            .header(AUTHORIZATION, self.authorization.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(USER_AGENT, USER_AGENT_VALUE)
            .body(body)
            .map_err(|err| format!("cannot build the top.gg request: {err}"))?;
        let answer = async {
            let response = self
                .http
                .request(request)
                .await
                .map_err(|err| format!("top.gg request failed: {}", chain(&err)))?;
            let (parts, body) = response.into_parts();
            let body = Limited::new(body, MAX_BODY)
                .collect()
                .await
                .map_err(|err| format!("cannot read top.gg's answer: {err}"))?
                .to_bytes();
            Ok::<_, ApiError>(outcome(parts.status, &parts.headers, &body))
        };
        tokio::time::timeout(self.timeout, answer)
            .await
            .map_err(|_| format!("top.gg did not answer within {:?}", self.timeout))?
    }
}

/// `err` and its sources, joined by `: ` (the client's own message is only
/// "client error (Connect)").
fn chain(err: &dyn Error) -> String {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

/// What an answer with `status`, `headers` and `body` means.
fn outcome(status: StatusCode, headers: &HeaderMap, body: &[u8]) -> Outcome {
    if status.is_success() {
        Outcome::Posted
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        Outcome::RateLimited(retry_after(headers, body))
    } else {
        Outcome::Rejected(status, detail(body))
    }
}

/// How long a 429 asks to wait: its `Retry-After` header, else its body's
/// `retry-after` (or `retry_after`), in seconds. [`DEFAULT_RETRY_AFTER`]
/// when neither is a number of seconds, and at most [`MAX_RETRY_AFTER`].
fn retry_after(headers: &HeaderMap, body: &[u8]) -> Duration {
    let from_header = headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<f64>().ok());
    let from_body = || {
        let body: serde_json::Value = serde_json::from_slice(body).ok()?;
        ["retry-after", "retry_after"]
            .into_iter()
            .find_map(|name| body.get(name)?.as_f64())
    };
    from_header
        .or_else(from_body)
        .and_then(|secs| Duration::try_from_secs_f64(secs).ok())
        .unwrap_or(DEFAULT_RETRY_AFTER)
        .min(MAX_RETRY_AFTER)
}

/// What a rejection's body says: the `detail`, else the `title`, of a
/// problem document (RFC 7807, which top.gg answers errors with), else the
/// body as text. At most [`MAX_DETAIL`] characters.
fn detail(body: &[u8]) -> String {
    let problem = serde_json::from_slice::<serde_json::Value>(body).ok();
    let text = problem
        .as_ref()
        .and_then(|problem| {
            ["detail", "title"]
                .into_iter()
                .find_map(|name| problem.get(name)?.as_str())
        })
        .map_or_else(|| String::from_utf8_lossy(body).into(), str::to_owned);
    text.trim().chars().take(MAX_DETAIL).collect()
}

/// The number of guilds in the state cache (`SCARD guild_keys`).
///
/// # Errors
///
/// When Redis fails or does not answer with a count.
pub async fn guild_count<C: ConnectionLike + Send>(conn: &mut C) -> ApiResult<u64> {
    let count = redis::cmd("SCARD")
        .arg(type_index(GUILD_KEY))
        .query_async(conn)
        .await?;
    Ok(count)
}

/// Post the cache's guild count and `shard_count` once, and log what came of
/// it. Nothing is posted when the count cannot be read or is 0. Returns how
/// long top.gg asked to wait when it rate limited the post.
pub async fn post_once<C: ConnectionLike + Send>(
    conn: &mut C,
    client: &Client,
    shard_count: u32,
) -> Option<Duration> {
    let server_count = match guild_count(conn).await {
        Ok(0) => {
            warn!("Not posting stats to top.gg: the state cache has no guilds");
            return None;
        }
        Ok(count) => count,
        Err(err) => {
            warn!("Not posting stats to top.gg: cannot count the guilds: {err}");
            return None;
        }
    };
    let stats = Stats {
        server_count,
        shard_count,
    };
    match client.post(stats).await {
        Ok(Outcome::Posted) => {
            info!("Posted {server_count} guilds and {shard_count} shards to top.gg");
            None
        }
        Ok(Outcome::RateLimited(wait)) => {
            warn!("top.gg rate limited the stats, retry after {wait:?}");
            Some(wait)
        }
        Ok(Outcome::Rejected(status, detail)) => {
            warn!("top.gg rejected the stats ({status}): {detail}");
            None
        }
        Err(err) => {
            warn!("Failed to post stats to top.gg: {err}");
            None
        }
    }
}

/// Post the stats ([`post_once`]) every `interval`, the first time one
/// interval after the start, until the task is aborted. After a rate limit
/// the next post waits as long as top.gg asked, if that is longer.
pub async fn run_job<C: ConnectionLike + Send>(
    mut conn: C,
    client: Client,
    shard_count: u32,
    interval: Duration,
) {
    let mut wait = interval;
    loop {
        tokio::time::sleep(wait).await;
        wait = post_once(&mut conn, &client, shard_count)
            .await
            .map_or(interval, |retry| retry.max(interval));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::{Cmd, Pipeline, RedisFuture, Value};
    use serde_json::json;
    use std::{fmt, time::Instant};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };

    /// A request as the fake top.gg received it.
    #[derive(Debug)]
    struct Received {
        /// The request line and the headers, as sent.
        head: String,
        body: String,
    }

    impl Received {
        fn header(&self, name: &str) -> Option<&str> {
            self.head.lines().skip(1).find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name).then_some(value.trim())
            })
        }
    }

    type Requests = mpsc::UnboundedReceiver<Received>;

    /// Read one HTTP/1.1 request with a `content-length` body (or none).
    async fn read_request(socket: &mut TcpStream) -> Received {
        let mut data = Vec::new();
        let mut buf = [0; 4096];
        let end = loop {
            if let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                break end;
            }
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0, "the request ended early");
            data.extend_from_slice(&buf[..n]);
        };
        let head = String::from_utf8(data[..end].to_vec()).unwrap();
        let mut received = Received {
            head,
            body: String::new(),
        };
        let length: usize = received
            .header("content-length")
            .map_or(0, |value| value.parse().unwrap());
        while data.len() < end + 4 + length {
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0, "the body ended early");
            data.extend_from_slice(&buf[..n]);
        }
        let body = data[end + 4..end + 4 + length].to_vec();
        received.body = String::from_utf8(body).unwrap();
        received
    }

    /// An answer of the fake top.gg: a status such as `204 No Content` and
    /// a body. The connection closes after it.
    fn answer(status: &str, body: &str) -> Option<String> {
        let head = format!("HTTP/1.1 {status}\r\nconnection: close\r\n");
        let length = body.len();
        Some(format!("{head}content-length: {length}\r\n\r\n{body}"))
    }

    /// A fake top.gg that answers one connection after another with
    /// `answers`, in order (`None`: read the request and never answer), and
    /// sends each request it reads to the receiver. Returns its URL.
    async fn fake_topgg(answers: Vec<Option<String>>) -> (String, Requests) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let url = format!("http://{address}/api/v1/projects/@me/metrics");
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            for answer in answers {
                let (mut socket, _) = listener.accept().await.unwrap();
                let _ = tx.send(read_request(&mut socket).await);
                match answer {
                    Some(answer) => {
                        socket.write_all(answer.as_bytes()).await.unwrap();
                        let _ = socket.shutdown().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            }
        });
        (url, rx)
    }

    fn client(url: &str) -> Client {
        let timeout = Duration::from_secs(5);
        Client::with_url(url, "  topgg-token ", timeout).unwrap()
    }

    fn json_body(sent: &Received) -> serde_json::Value {
        serde_json::from_str(&sent.body).unwrap()
    }

    fn error(result: ApiResult<Client>) -> String {
        result.err().unwrap().to_string()
    }

    fn answer_to(status: u16, body: &[u8]) -> Outcome {
        let status = StatusCode::from_u16(status).unwrap();
        outcome(status, &HeaderMap::new(), body)
    }

    /// Answers every command with its value, as SCARD would with a count.
    struct Redis(Value);

    impl ConnectionLike for Redis {
        fn req_packed_command<'a>(&'a mut self, _cmd: &'a Cmd) -> RedisFuture<'a, Value> {
            let reply = self.0.clone();
            Box::pin(async move { Ok(reply) })
        }

        fn req_packed_commands<'a>(
            &'a mut self,
            _pipeline: &'a Pipeline,
            _offset: usize,
            count: usize,
        ) -> RedisFuture<'a, Vec<Value>> {
            let reply = self.0.clone();
            Box::pin(async move { Ok(vec![reply; count]) })
        }

        fn get_db(&self) -> i64 {
            0
        }
    }

    /// An error with a source, as the HTTP client's are.
    #[derive(Debug)]
    struct Outer(std::io::Error);

    impl fmt::Display for Outer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("client error (Connect)")
        }
    }

    impl Error for Outer {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.0)
        }
    }

    #[tokio::test]
    async fn stats_are_patched_as_json_with_the_bearer_token() {
        let (url, mut requests) = fake_topgg(vec![answer("204 No Content", "")]).await;
        let stats = Stats {
            server_count: 420,
            shard_count: 67,
        };
        assert_eq!(client(&url).post(stats).await.unwrap(), Outcome::Posted);

        let sent = requests.recv().await.unwrap();
        let line = sent.head.lines().next().unwrap();
        assert_eq!(line, "PATCH /api/v1/projects/@me/metrics HTTP/1.1");
        assert_eq!(sent.header("authorization"), Some("Bearer topgg-token"));
        assert_eq!(sent.header("content-type"), Some("application/json"));
        assert_eq!(sent.header("user-agent"), Some(USER_AGENT_VALUE));
        let expected = json!({"server_count": 420, "shard_count": 67});
        assert_eq!(json_body(&sent), expected);
    }

    #[tokio::test]
    async fn the_client_posts_to_top_ggs_metrics_endpoint() {
        let client = Client::new("token").unwrap();
        assert_eq!(client.url, METRICS_URL);
        assert_eq!(client.timeout, REQUEST_TIMEOUT);
        assert!(client.authorization.is_sensitive());
        assert!(USER_AGENT_VALUE.starts_with("twilight-dispatch/"));
    }

    #[test]
    fn a_bad_url_or_token_is_an_error() {
        let timeout = Duration::from_secs(1);
        let bad_url = Client::with_url("http://exa mple", "token", timeout);
        assert!(error(bad_url).contains("invalid top.gg URL"));
        let bad_token = Client::with_url(METRICS_URL, "tok\nen", timeout);
        assert!(error(bad_token).contains("cannot be sent in a header"));
    }

    #[test]
    fn answers_are_read_by_status() {
        assert_eq!(answer_to(204, b""), Outcome::Posted);
        assert_eq!(answer_to(200, b"{}"), Outcome::Posted);
        let hour = Outcome::RateLimited(Duration::from_secs(3600));
        assert_eq!(answer_to(429, br#"{"retry-after": 3600}"#), hour);
        let missing = Outcome::Rejected(StatusCode::NOT_FOUND, "Project not found.".into());
        assert_eq!(answer_to(404, b"Project not found."), missing);
    }

    #[test]
    fn a_rate_limit_waits_as_long_as_it_says() {
        let secs = Duration::from_secs;
        let mut h = HeaderMap::new();
        // The body, in seconds, under either name.
        assert_eq!(retry_after(&h, br#"{"retry-after": 3600}"#), secs(3600));
        assert_eq!(retry_after(&h, br#"{"retry_after": 30}"#), secs(30));
        let ms = Duration::from_millis(1500);
        assert_eq!(retry_after(&h, br#"{"retry-after": 1.5}"#), ms);
        // Nothing usable: an hour.
        for body in [&b""[..], b"{}", br#"{"retry-after": -1}"#, b"soon"] {
            assert_eq!(retry_after(&h, body), DEFAULT_RETRY_AFTER, "{body:?}");
        }
        // The header wins over the body.
        h.insert(RETRY_AFTER, HeaderValue::from_static(" 120 "));
        assert_eq!(retry_after(&h, br#"{"retry-after": 3600}"#), secs(120));
        // A date is not a number of seconds.
        let date = HeaderValue::from_static("Wed, 21 Oct 2026 07:28:00 GMT");
        h.insert(RETRY_AFTER, date);
        assert_eq!(retry_after(&h, br#"{"retry-after": 5}"#), secs(5));
        // Capped.
        h.insert(RETRY_AFTER, HeaderValue::from_static("1e12"));
        assert_eq!(retry_after(&h, b""), MAX_RETRY_AFTER);
    }

    #[test]
    fn a_rejection_says_why() {
        let problem = br#"{"title": "Unprocessable Entity", "detail": "Payload was malformed"}"#;
        assert_eq!(detail(problem), "Payload was malformed");
        assert_eq!(detail(br#"{"title": "Not Found"}"#), "Not Found");
        assert_eq!(detail(b"  Bad Gateway\n"), "Bad Gateway");
        assert_eq!(detail(br#"{"detail": 5}"#), r#"{"detail": 5}"#);
        assert_eq!(detail(&[b'x'; 1000]).len(), MAX_DETAIL);
        assert_eq!(detail(b"\xff"), "\u{fffd}");
    }

    #[test]
    fn errors_name_their_causes() {
        let err = Outer(std::io::Error::other("connection refused"));
        assert_eq!(chain(&err), "client error (Connect): connection refused");
        assert_eq!(chain(&std::io::Error::other("alone")), "alone");
    }

    #[tokio::test]
    async fn no_answer_is_an_error() {
        // Nothing listens on a port whose listener was dropped.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let refused = client(&format!("http://{address}/metrics"));
        let stats = Stats {
            server_count: 1,
            shard_count: 1,
        };
        let err = refused.post(stats).await.unwrap_err().to_string();
        assert!(err.contains("top.gg request failed"), "{err}");

        let (url, mut requests) = fake_topgg(vec![None]).await;
        let timeout = Duration::from_millis(200);
        let silent = Client::with_url(&url, "token", timeout).unwrap();
        let err = silent.post(stats).await.unwrap_err().to_string();
        assert!(err.contains("did not answer within 200ms"), "{err}");
        assert!(requests.recv().await.is_some());
    }

    #[tokio::test]
    async fn the_cached_guild_count_is_posted_with_the_shard_total() {
        let answers = vec![
            answer("204 No Content", ""),
            answer("429 Too Many Requests", r#"{"retry-after": 120}"#),
            answer("401 Unauthorized", r#"{"detail": "Invalid token"}"#),
        ];
        let (url, mut requests) = fake_topgg(answers).await;
        let client = client(&url);
        let mut redis = Redis(Value::Int(1234));

        assert_eq!(post_once(&mut redis, &client, 4).await, None);
        let sent = requests.recv().await.unwrap();
        let expected = json!({"server_count": 1234, "shard_count": 4});
        assert_eq!(json_body(&sent), expected);

        let wait = Some(Duration::from_secs(120));
        assert_eq!(post_once(&mut redis, &client, 4).await, wait);
        assert_eq!(post_once(&mut redis, &client, 4).await, None);
        let sent = requests.recv().await.unwrap();
        assert_eq!(sent.header("authorization"), Some("Bearer topgg-token"));
        assert!(requests.recv().await.is_some());

        // The fake answers no more: the post fails.
        assert_eq!(post_once(&mut redis, &client, 4).await, None);
    }

    #[tokio::test]
    async fn an_empty_or_unreadable_count_is_not_posted() {
        let (url, mut requests) = fake_topgg(vec![]).await;
        let client = client(&url);
        assert_eq!(post_once(&mut Redis(Value::Int(0)), &client, 1).await, None);
        let unreadable = Value::SimpleString("not a count".into());
        assert_eq!(post_once(&mut Redis(unreadable), &client, 1).await, None);
        // The fake took no connection, so it has ended without a request.
        assert!(requests.recv().await.is_none());
    }

    #[tokio::test]
    async fn the_job_posts_every_interval() {
        let answers = vec![answer("204 No Content", ""); 3];
        let (url, mut requests) = fake_topgg(answers).await;
        let interval = Duration::from_millis(50);
        let started = Instant::now();
        let job = tokio::spawn(run_job(Redis(Value::Int(7)), client(&url), 2, interval));
        let expected = json!({"server_count": 7, "shard_count": 2});
        for _ in 0..3 {
            let next = tokio::time::timeout(Duration::from_secs(10), requests.recv());
            let sent = next.await.expect("a post within 10 s").unwrap();
            assert_eq!(json_body(&sent), expected);
        }
        // The first post waits one interval too.
        assert!(started.elapsed() >= Duration::from_millis(150));
        job.abort();
    }

    #[tokio::test]
    async fn after_a_rate_limit_the_job_waits_as_asked() {
        let limited = answer("429 Too Many Requests", r#"{"retry-after": 120}"#);
        let answers = vec![limited, answer("204 No Content", "")];
        let (url, mut requests) = fake_topgg(answers).await;
        let interval = Duration::from_millis(20);
        let job = tokio::spawn(run_job(Redis(Value::Int(7)), client(&url), 2, interval));
        assert!(requests.recv().await.is_some());
        // The 429 asked for 120 s: no second post soon after it.
        let second = tokio::time::timeout(Duration::from_millis(500), requests.recv());
        assert!(second.await.is_err(), "posted again during the rate limit");
        job.abort();
    }
}
