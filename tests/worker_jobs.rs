#![recursion_limit = "256"]

//! Workers: which `GUILD_CREATE` is a guild join once READY of a new session
//! has cached every guild it lists as a stub (set TEST_REDIS_URL), and that
//! `worker::spawn` always starts a worker, which drains its queue on drop.

mod common;
#[path = "support/fake_discord.rs"]
mod fake_discord;

use common::*;
use fake_discord::FakeDiscord;
use redis::aio::ConnectionLike;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use twilight_dispatch::{
    config::StateConfig,
    discord_log::DiscordLog,
    gateway::{
        chat_filter::ChatFilter,
        registry::{Registry, ShardHandle},
        runner::parse_event,
        worker::{self, Breaker, Job, Publisher, WorkerContext},
    },
    metrics::{CHAT_FILTERED, COMPACT_UNREADABLE, GUILD_EVENTS},
    models::ApiResult,
    util::Throttle,
};
use twilight_http::Client;

/// The guild join tests compare the process-wide `GUILD_EVENTS` counter, so
/// they run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The `LOG_GUILD_CHANNEL` on the fake Discord.
const GUILD_LOG: u64 = 22;

/// The routing key and message id of every published event, and its body.
/// The next `fail` publishes fail instead.
#[derive(Default)]
struct Recorder {
    published: Mutex<Vec<(String, Option<String>)>>,
    bodies: Mutex<Vec<String>>,
    fail: AtomicUsize,
}

impl Recorder {
    fn published(&self) -> Vec<(String, Option<String>)> {
        self.published.lock().unwrap().clone()
    }

    fn bodies(&self) -> Vec<String> {
        self.bodies.lock().unwrap().clone()
    }
}

impl Publisher for Recorder {
    async fn publish(
        &self,
        routing_key: &str,
        payload: Vec<u8>,
        message_id: Option<String>,
    ) -> ApiResult<()> {
        let failing = self
            .fail
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            });
        if failing.is_ok() {
            return Err("the broker is down".to_owned().into());
        }
        self.bodies
            .lock()
            .unwrap()
            .push(String::from_utf8(payload).unwrap());
        let mut published = self.published.lock().unwrap();
        published.push((routing_key.to_owned(), message_id));
        Ok(())
    }
}

/// The Redis connection of workers with the cache off, which never use it.
#[derive(Clone)]
struct NoRedis;

impl ConnectionLike for NoRedis {
    fn req_packed_command<'a>(
        &'a mut self,
        _cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        unreachable!("the cache is disabled")
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        _pipeline: &'a redis::Pipeline,
        _offset: usize,
        _count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        unreachable!("the cache is disabled")
    }

    fn get_db(&self) -> i64 {
        0
    }
}

fn context<C, P>(
    redis: C,
    publisher: Arc<P>,
    registry: Arc<Registry>,
    state: StateConfig,
    discord: DiscordLog,
) -> WorkerContext<C, P> {
    WorkerContext {
        redis,
        publisher,
        registry,
        state,
        bot_id: bot_id(),
        cache_timeout: Duration::from_secs(10),
        discord,
        breaker: Breaker::default(),
        cache_errors: Throttle::default(),
        publish_errors: Throttle::default(),
        publish_events: None,
        chat_filter: None,
        compact_message_create: false,
    }
}

fn registry(handle: &Arc<ShardHandle>) -> Arc<Registry> {
    Arc::new(Registry::new([handle.clone()]))
}

/// A job of shard 0 in session `s1`, as the shard runner builds it.
fn job(handle: &ShardHandle, kind: &str, seq: u64, d: Value) -> Job {
    let json = raw(kind, seq, d);
    Job {
        shard: 0,
        seq: Some(seq),
        token: handle.dispatch(seq),
        session: Some(Arc::from("s1")),
        event: parse_event(&json).unwrap(),
        json,
    }
}

/// A `GUILD_CREATE` of a guild the bot joined just now.
fn recent_guild(id: u64, name: &str) -> Value {
    let mut guild = guild(id, name);
    guild["joined_at"] = json!(now_iso());
    guild
}

fn joins() -> u64 {
    GUILD_EVENTS.with_label_values(&["Join"]).get()
}

/// A guild log posting to `GUILD_LOG` on the fake Discord.
fn guild_log(fake: &FakeDiscord) -> DiscordLog {
    // main() installs it; twilight's rustls connectors panic without one.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let proxy = fake.api.proxy();
    let address = proxy.trim_start_matches("http://").to_owned();
    let client = Client::builder()
        .token("Bot test".to_owned())
        .proxy(address, true)
        .ratelimiter(None)
        .build();
    DiscordLog::new(Arc::new(client), 0, GUILD_LOG)
}

/// The title and description of a posted log message's embed.
fn line(body: &Value) -> (String, String) {
    let embed = &body["embeds"][0];
    let text = |field: &str| embed[field].as_str().unwrap_or_default().to_owned();
    (text("title"), text("description"))
}

/// The guild log lines posted before a marker logged now. One task posts them
/// in order, so once the marker is there everything logged before it is too.
async fn guild_log_lines(fake: &FakeDiscord, log: &DiscordLog) -> Vec<(String, String)> {
    log.guild_leave("marker", 1);
    let path = format!("/channels/{GUILD_LOG}/messages");
    let api = &fake.api;
    api.wait_until("the marker in the guild log", |requests| {
        let mut lines: Vec<(String, String)> = requests
            .iter()
            .filter(|request| request.method == "POST" && request.path == path)
            .map(|request| line(&request.body))
            .collect();
        let marker = lines
            .iter()
            .position(|(_, description)| description == "marker (1)")?;
        lines.truncate(marker);
        Some(lines)
    })
    .await
}

/// A guild joined while no session received its `GUILD_CREATE` (while the
/// bot was disconnected, or queued in the session that ended and so not
/// applied) is listed by READY of the new session, which caches it as a stub
/// without a name. The new session's `GUILD_CREATE` is the join: counted and
/// posted once.
#[tokio::test]
async fn a_join_no_session_reported_is_logged_after_ready() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(1).await else {
        return;
    };
    let fake = FakeDiscord::start().await;
    let log = guild_log(&fake);
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let ctx = context(
        conn.clone(),
        recorder.clone(),
        registry(&handle),
        state_all(),
        log.clone(),
    );
    let before = joins();

    let stale = job(&handle, "GUILD_CREATE", 2, recent_guild(1000, "g"));
    handle.new_session(1);
    let state = state_all();
    apply(&mut conn, &state, "READY", ready([0, 1], &[1000, 2000])).await;
    let stub = get(&mut conn, "guild:1000").await.expect("a stub");
    assert!(stub.get("name").is_none(), "{stub}");

    worker::process(&ctx, &mut conn, stale).await;
    assert_eq!(joins() - before, 0, "the stale GUILD_CREATE is no join");

    let current = job(&handle, "GUILD_CREATE", 2, recent_guild(1000, "g"));
    worker::process(&ctx, &mut conn, current).await;
    assert_eq!(joins() - before, 1, "a join of the new session");
    assert_eq!(get(&mut conn, "guild:1000").await.unwrap()["name"], "g");

    // Also listed by READY, but the bot joined it long ago.
    let old = job(&handle, "GUILD_CREATE", 3, guild(2000, "h"));
    worker::process(&ctx, &mut conn, old).await;
    assert_eq!(joins() - before, 1, "a guild joined long ago is no join");

    let lines = guild_log_lines(&fake, &log).await;
    assert_eq!(lines, [("Guild Join".to_owned(), "g (1000)".to_owned())]);
    assert_eq!(recorder.published().len(), 3, "everything is published");
    assert_eq!(handle.in_flight(), 0);
}

/// A guild joined minutes ago is cached in full by its first `GUILD_CREATE`.
/// Neither the next session's `GUILD_CREATE` (READY keeps a cached guild) nor
/// the one after an outage (which only marks it unavailable) is a second
/// join, although its `joined_at` is still recent.
#[tokio::test]
async fn a_recent_join_cached_in_full_is_not_logged_again() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(2).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let ctx = context(
        conn.clone(),
        Arc::new(Recorder::default()),
        registry(&handle),
        state_all(),
        DiscordLog::disabled(),
    );
    let before = joins();

    let first = job(&handle, "GUILD_CREATE", 2, recent_guild(1000, "g"));
    worker::process(&ctx, &mut conn, first).await;
    assert_eq!(joins() - before, 1, "an unknown guild is a join");

    handle.new_session(1);
    let state = state_all();
    apply(&mut conn, &state, "READY", ready([0, 1], &[1000])).await;
    assert_eq!(get(&mut conn, "guild:1000").await.unwrap()["name"], "g");
    let resent = job(&handle, "GUILD_CREATE", 2, recent_guild(1000, "g"));
    worker::process(&ctx, &mut conn, resent).await;
    assert_eq!(joins() - before, 1, "re-sent by the next session");

    let outage = json!({"id": "1000", "unavailable": true});
    let delete = job(&handle, "GUILD_DELETE", 3, outage);
    worker::process(&ctx, &mut conn, delete).await;
    let cached = get(&mut conn, "guild:1000").await.unwrap();
    assert_eq!(cached["name"], "g", "{cached}");
    assert_eq!(cached["unavailable"], true, "{cached}");
    let back = job(&handle, "GUILD_CREATE", 4, recent_guild(1000, "g"));
    worker::process(&ctx, &mut conn, back).await;
    assert_eq!(joins() - before, 1, "back after an outage");
}

/// A guild that was never cached and is first seen unavailable (an outage's
/// `GUILD_DELETE`, or an unavailable `GUILD_CREATE`) is cached as a stub
/// without a name as well, so a `GUILD_CREATE` after it with a recent
/// `joined_at` is a join.
#[tokio::test]
async fn a_recent_join_known_only_from_an_unavailable_stub_is_logged() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(3).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let ctx = context(
        conn.clone(),
        Arc::new(Recorder::default()),
        registry(&handle),
        state_all(),
        DiscordLog::disabled(),
    );
    let before = joins();

    let outage = json!({"id": "3000", "unavailable": true});
    let delete = job(&handle, "GUILD_DELETE", 2, outage);
    worker::process(&ctx, &mut conn, delete).await;
    let unavailable = json!({"id": "4000", "unavailable": true});
    let create = job(&handle, "GUILD_CREATE", 3, unavailable);
    worker::process(&ctx, &mut conn, create).await;
    for key in ["guild:3000", "guild:4000"] {
        let stub = get(&mut conn, key).await.expect("a stub");
        assert!(stub.get("name").is_none(), "{key}: {stub}");
    }
    assert_eq!(joins() - before, 0);

    let joined = job(&handle, "GUILD_CREATE", 4, recent_guild(3000, "k"));
    worker::process(&ctx, &mut conn, joined).await;
    assert_eq!(joins() - before, 1, "after an outage's stub");
    let joined = job(&handle, "GUILD_CREATE", 5, recent_guild(4000, "m"));
    worker::process(&ctx, &mut conn, joined).await;
    assert_eq!(joins() - before, 2, "after an unavailable GUILD_CREATE");
}

/// With `PUBLISH_EVENTS` set only the listed events are published. An event
/// that is not listed is still applied to the cache and still completes its
/// sequence token, so the resume point passes it.
#[tokio::test]
async fn only_listed_events_are_published_but_every_event_is_cached_and_completed() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(4).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let mut ctx = context(
        conn.clone(),
        recorder.clone(),
        registry(&handle),
        state_all(),
        DiscordLog::disabled(),
    );
    ctx.publish_events = Some(["GUILD_DELETE".to_owned()].into());

    let create = job(&handle, "GUILD_CREATE", 2, guild(1000, "g"));
    worker::process(&ctx, &mut conn, create).await;
    assert_eq!(get(&mut conn, "guild:1000").await.unwrap()["name"], "g");
    assert_eq!(recorder.published(), [], "GUILD_CREATE is not listed");
    assert_eq!(handle.in_flight(), 0, "but its sequence completed");
    assert_eq!(resume_point(&handle), 2, "the resume point passed it");

    let delete = job(&handle, "GUILD_DELETE", 3, json!({"id": "1000"}));
    worker::process(&ctx, &mut conn, delete).await;
    assert!(get(&mut conn, "guild:1000").await.is_none());
    assert_eq!(
        recorder.published(),
        [("GUILD_DELETE".to_owned(), Some("0:s1:3".to_owned()))]
    );
    assert_eq!(handle.in_flight(), 0);
    assert_eq!(resume_point(&handle), 3);
}

/// The allow-list needs only the event name: a job without a typed event
/// (the cache is off) is filtered by it too.
#[tokio::test]
async fn the_allow_list_applies_without_the_cache() {
    let recorder = Arc::new(Recorder::default());
    let mut ctx = context(
        NoRedis,
        recorder.clone(),
        Arc::new(Registry::default()),
        StateConfig::default(),
        DiscordLog::disabled(),
    );
    ctx.publish_events = Some(["B".to_owned()].into());
    for (seq, kind) in [(2, "A"), (3, "B")] {
        let queued = Job {
            shard: 0,
            seq: Some(seq),
            token: None,
            session: None,
            json: format!(r#"{{"op":0,"t":"{kind}","s":{seq},"d":{{}}}}"#),
            event: None,
        };
        worker::process(&ctx, &mut NoRedis, queued).await;
    }
    assert_eq!(
        recorder.published(),
        [("B".to_owned(), Some("0::3".to_owned()))]
    );
}

/// The sequence the saved session would resume from. `fail` completes a token
/// before it records the failure, so `in_flight()` cannot tell a completed
/// token from a failed one; the resume point stays before a failed one.
fn resume_point(handle: &ShardHandle) -> u64 {
    handle.update(|snapshot| snapshot.session_id = Some("s1".to_owned()));
    handle.session_info().expect("a resumable session").sequence
}

/// A `MESSAGE_CREATE` of shard 0 in channel 3 of guild 1000 by user 1.
fn chat_job(handle: &ShardHandle, seq: u64, edit: impl FnOnce(&mut Value)) -> Job {
    let mut d = message(seq, 3, Some(1000), "hi");
    edit(&mut d);
    job(handle, "MESSAGE_CREATE", seq, d)
}

fn filtered(reason: &str) -> u64 {
    CHAT_FILTERED.with_label_values(&[reason]).get()
}

/// With the chat filter on, a message it drops is not published, but it is
/// still cached and its sequence token completes, like an event left out by
/// `PUBLISH_EVENTS`. A second message of the same user inside the window is
/// one of them.
#[tokio::test]
async fn filtered_messages_are_cached_and_complete_their_sequence() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(5).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let mut ctx = context(
        conn.clone(),
        recorder.clone(),
        registry(&handle),
        state_all(),
        DiscordLog::disabled(),
    );
    ctx.chat_filter = Some(ChatFilter::new(Duration::from_secs(3600)));
    let (bots, repeats) = (filtered("bot"), filtered("window"));

    worker::process(&ctx, &mut conn, chat_job(&handle, 2, |_| {})).await;
    worker::process(&ctx, &mut conn, chat_job(&handle, 3, |_| {})).await;
    let bot = chat_job(&handle, 4, |d| d["author"]["bot"] = json!(true));
    worker::process(&ctx, &mut conn, bot).await;

    assert_eq!(
        recorder.published(),
        [("MESSAGE_CREATE".to_owned(), Some("0:s1:2".to_owned()))]
    );
    for id in [2, 3, 4] {
        let key = format!("message:3:{id}");
        assert!(get(&mut conn, &key).await.is_some(), "{key} is cached");
    }
    assert_eq!(handle.in_flight(), 0, "every sequence completed");
    assert_eq!(
        resume_point(&handle),
        4,
        "the resume point passed all three"
    );
    assert_eq!(filtered("window") - repeats, 1);
    assert_eq!(filtered("bot") - bots, 1);
}

/// Only `MESSAGE_CREATE` is looked at, and only after `PUBLISH_EVENTS`: an
/// event the allow-list leaves out never reaches the filter, so it does not
/// start a window; other events pass through.
#[tokio::test]
async fn the_chat_filter_follows_the_allow_list_and_only_reads_messages() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(6).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let mut ctx = context(
        conn.clone(),
        recorder.clone(),
        registry(&handle),
        state_all(),
        DiscordLog::disabled(),
    );
    ctx.chat_filter = Some(ChatFilter::new(Duration::from_secs(3600)));
    ctx.publish_events = Some(["GUILD_CREATE".to_owned()].into());

    worker::process(&ctx, &mut conn, chat_job(&handle, 2, |_| {})).await;
    assert!(recorder.published().is_empty());
    assert_eq!(ctx.chat_filter.as_ref().unwrap().pairs(), 0);

    let create = job(&handle, "GUILD_CREATE", 3, guild(1000, "g"));
    worker::process(&ctx, &mut conn, create).await;
    assert_eq!(
        recorder.published().len(),
        1,
        "other events are not filtered"
    );
    assert_eq!(handle.in_flight(), 0);
}

/// Without `CHAT_FILTER` every message is published, bots and repeats too.
#[tokio::test]
async fn without_the_chat_filter_every_message_is_published() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(7).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let ctx = context(
        conn.clone(),
        recorder.clone(),
        registry(&handle),
        state_all(),
        DiscordLog::disabled(),
    );
    worker::process(&ctx, &mut conn, chat_job(&handle, 2, |_| {})).await;
    worker::process(&ctx, &mut conn, chat_job(&handle, 3, |_| {})).await;
    let bot = chat_job(&handle, 4, |d| d["author"]["bot"] = json!(true));
    worker::process(&ctx, &mut conn, bot).await;
    let dm = job(&handle, "MESSAGE_CREATE", 5, message(5, 3, None, "dm"));
    worker::process(&ctx, &mut conn, dm).await;
    assert_eq!(recorder.published().len(), 4);
}

/// A `MESSAGE_CREATE` by user `user` in `channel` of guild 1000, with the
/// message id `seq`.
fn message_job(handle: &ShardHandle, seq: u64, channel: u64, user: u64) -> Job {
    let mut d = message(seq, channel, Some(1000), "hi");
    d["author"] = common::user(user, "author");
    job(handle, "MESSAGE_CREATE", seq, d)
}

/// A context with the chat filter (window `window`) and the compact payload on.
fn compact_context<C, P>(
    redis: C,
    publisher: Arc<P>,
    handle: &Arc<ShardHandle>,
    state: StateConfig,
    window: Duration,
) -> WorkerContext<C, P> {
    let mut ctx = context(
        redis,
        publisher,
        registry(handle),
        state,
        DiscordLog::disabled(),
    );
    ctx.chat_filter = Some(ChatFilter::new(window));
    ctx.compact_message_create = true;
    ctx
}

/// The `d` of a published body.
fn data_of(body: &str) -> Value {
    serde_json::from_str::<Value>(body).unwrap()["d"].clone()
}

/// With the compact payload on, a published `MESSAGE_CREATE` is the same
/// envelope (routing key and message id too) around a `d` of seven fields,
/// in this order. The parent is the cached thread's parent, `null` for a
/// channel that is not a cached thread.
#[tokio::test]
async fn a_compact_message_create_has_seven_fields_and_the_threads_parent() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(8).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let ctx = compact_context(
        conn.clone(),
        recorder.clone(),
        &handle,
        state_all(),
        Duration::ZERO,
    );

    // Guild 1000 has the thread 1005 (parent 1003) and the channel 1003.
    let create = job(&handle, "GUILD_CREATE", 2, guild(1000, "g"));
    worker::process(&ctx, &mut conn, create).await;
    worker::process(&ctx, &mut conn, message_job(&handle, 3, 1005, 7)).await;
    worker::process(&ctx, &mut conn, message_job(&handle, 4, 1003, 8)).await;

    let published = recorder.published();
    assert_eq!(published[0].0, "GUILD_CREATE");
    assert_eq!(
        published[1..],
        [
            ("MESSAGE_CREATE".to_owned(), Some("0:s1:3".to_owned())),
            ("MESSAGE_CREATE".to_owned(), Some("0:s1:4".to_owned())),
        ]
    );
    let bodies = recorder.bodies();
    assert!(
        bodies[0].contains(r#""name":"g""#),
        "other events are whole"
    );
    insta::assert_snapshot!("compact_message_create_in_a_thread", bodies[1]);
    insta::assert_snapshot!("compact_message_create_in_a_channel", bodies[2]);
    assert_eq!(handle.in_flight(), 0);
}

/// `dropped` is how many messages of the (guild, user) pair the window dropped
/// since the previous published one, and starts over after it.
#[tokio::test]
async fn the_compact_body_carries_the_dropped_count() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(9).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let ctx = compact_context(
        conn.clone(),
        recorder.clone(),
        &handle,
        state_all(),
        Duration::from_millis(400),
    );

    for seq in 2..=4 {
        worker::process(&ctx, &mut conn, message_job(&handle, seq, 3, 1)).await;
    }
    tokio::time::sleep(Duration::from_millis(600)).await;
    worker::process(&ctx, &mut conn, message_job(&handle, 5, 3, 1)).await;
    worker::process(&ctx, &mut conn, message_job(&handle, 6, 3, 1)).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    worker::process(&ctx, &mut conn, message_job(&handle, 7, 3, 1)).await;

    let dropped: Vec<_> = recorder
        .bodies()
        .iter()
        .map(|body| data_of(body)["dropped"].clone())
        .collect();
    assert_eq!(dropped, [json!(0), json!(2), json!(1)]);
    assert_eq!(handle.in_flight(), 0);
}

/// A message whose publish failed does not start the window: the pair's next
/// message is published at once, with the `dropped` the failed one would have
/// carried. The same holds for the first message of a pair.
///
/// The windows are a few hundred milliseconds: the waits are between one and
/// two windows (a count older than two is reported as 0).
#[tokio::test]
async fn a_failed_publish_does_not_start_the_window() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(10).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let ctx = compact_context(
        conn.clone(),
        recorder.clone(),
        &handle,
        state_all(),
        Duration::from_millis(400),
    );

    worker::process(&ctx, &mut conn, message_job(&handle, 2, 3, 1)).await;
    worker::process(&ctx, &mut conn, message_job(&handle, 3, 3, 1)).await;
    worker::process(&ctx, &mut conn, message_job(&handle, 4, 3, 1)).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    // The next two publishes fail: user 1's third message (which would carry
    // the 2 dropped) and user 2's first.
    recorder.fail.store(2, Ordering::SeqCst);
    worker::process(&ctx, &mut conn, message_job(&handle, 5, 3, 1)).await;
    worker::process(&ctx, &mut conn, message_job(&handle, 6, 3, 2)).await;
    assert_eq!(recorder.published().len(), 1, "nothing was published");

    worker::process(&ctx, &mut conn, message_job(&handle, 5, 3, 1)).await;
    worker::process(&ctx, &mut conn, message_job(&handle, 6, 3, 2)).await;
    let dropped: Vec<_> = recorder
        .bodies()
        .iter()
        .map(|body| data_of(body)["dropped"].clone())
        .collect();
    assert_eq!(dropped, [json!(0), json!(2), json!(0)]);
}

/// With `COMPACT_MESSAGE_CREATE` off the body is the envelope around Discord's
/// own `d`, byte for byte, with the filter on.
#[tokio::test]
async fn without_the_compact_switch_the_body_is_untouched() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(11).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let mut ctx = compact_context(
        conn.clone(),
        recorder.clone(),
        &handle,
        state_all(),
        Duration::ZERO,
    );
    ctx.compact_message_create = false;

    worker::process(&ctx, &mut conn, message_job(&handle, 2, 3, 1)).await;
    let mut d = message(2, 3, Some(1000), "hi");
    d["author"] = common::user(1, "author");
    assert_eq!(
        recorder.bodies(),
        [format!(r#"{{"op":0,"t":"MESSAGE_CREATE","d":{d}}}"#)]
    );
}

/// With the compact payload on, a `MESSAGE_CREATE` that does not fit it is not
/// published (consumers expect one shape). It still completes its sequence,
/// is counted, and does not start a window for its pair.
#[tokio::test]
async fn an_unreadable_message_is_not_published_and_completes_its_sequence() {
    let _serial = SERIAL.lock().await;
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let ctx = compact_context(
        NoRedis,
        recorder.clone(),
        &handle,
        StateConfig::default(),
        Duration::from_secs(3600),
    );
    let before = COMPACT_UNREADABLE.get();

    let mut d = message(2, 3, Some(1000), "hi");
    d["author"] = common::user(1, "author");
    d.as_object_mut().unwrap().remove("timestamp");
    let unreadable = Job {
        shard: 0,
        seq: Some(2),
        token: handle.dispatch(2),
        session: Some(Arc::from("s1")),
        json: raw("MESSAGE_CREATE", 2, d),
        event: None,
    };
    worker::process(&ctx, &mut NoRedis, unreadable).await;
    assert!(recorder.published().is_empty());
    assert_eq!(handle.in_flight(), 0, "its sequence completed");
    assert_eq!(resume_point(&handle), 2, "the resume point passed it");
    assert_eq!(COMPACT_UNREADABLE.get() - before, 1);

    // Nothing was recorded for the pair; with the cache off the parent is null.
    let mut d = message(3, 1005, Some(1000), "hi");
    d["author"] = common::user(1, "author");
    let readable = Job {
        shard: 0,
        seq: Some(3),
        token: handle.dispatch(3),
        session: Some(Arc::from("s1")),
        json: raw("MESSAGE_CREATE", 3, d),
        event: None,
    };
    worker::process(&ctx, &mut NoRedis, readable).await;
    let bodies = recorder.bodies();
    assert_eq!(bodies.len(), 1);
    assert_eq!(data_of(&bodies[0])["parent_id"], Value::Null);
    assert_eq!(data_of(&bodies[0])["dropped"], 0);
    assert_eq!(resume_point(&handle), 3);
}

/// A read of the thread's parent goes through the breaker like a cache
/// update: with it open the cache is not asked, and the message is still
/// published, with a null parent.
#[tokio::test]
async fn with_the_breaker_open_a_compact_message_is_published_without_a_parent() {
    let _serial = SERIAL.lock().await;
    let Some(mut conn) = redis(12).await else {
        return;
    };
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let ctx = compact_context(
        conn.clone(),
        recorder.clone(),
        &handle,
        state_all(),
        Duration::ZERO,
    );
    let create = job(&handle, "GUILD_CREATE", 2, guild(1000, "g"));
    worker::process(&ctx, &mut conn, create).await;

    ctx.breaker.failure();
    worker::process(&ctx, &mut conn, message_job(&handle, 3, 1005, 7)).await;
    let bodies = recorder.bodies();
    assert_eq!(bodies.len(), 2, "published");
    assert_eq!(data_of(&bodies[1])["parent_id"], Value::Null);
    assert!(!ctx.breaker.allow(), "and the breaker is still open");
    assert_eq!(resume_point(&handle), 3);
}

/// A message the filter cannot read (a `bot` flag that is no boolean) skips
/// the bot rule, so with the compact payload on it is not published; with it
/// off it is published whole, as before.
#[tokio::test]
async fn a_message_the_filter_cannot_read_is_not_published_compact() {
    let _serial = SERIAL.lock().await;
    let handle = Arc::new(ShardHandle::new(0, None, Some(1)));
    let recorder = Arc::new(Recorder::default());
    let mut ctx = compact_context(
        NoRedis,
        recorder.clone(),
        &handle,
        StateConfig::default(),
        Duration::from_secs(3600),
    );
    let before = COMPACT_UNREADABLE.get();
    let unreadable = |seq: u64| {
        let mut d = message(seq, 3, Some(1000), "hi");
        d["author"] = common::user(1, "author");
        d["author"]["bot"] = json!("yes");
        Job {
            shard: 0,
            seq: Some(seq),
            token: handle.dispatch(seq),
            session: Some(Arc::from("s1")),
            json: raw("MESSAGE_CREATE", seq, d),
            event: None,
        }
    };

    worker::process(&ctx, &mut NoRedis, unreadable(2)).await;
    assert!(recorder.published().is_empty());
    assert_eq!(COMPACT_UNREADABLE.get() - before, 1);
    assert_eq!(resume_point(&handle), 2, "its sequence completed");

    ctx.compact_message_create = false;
    worker::process(&ctx, &mut NoRedis, unreadable(3)).await;
    let bodies = recorder.bodies();
    assert_eq!(bodies.len(), 1, "published whole");
    assert_eq!(data_of(&bodies[0])["author"]["bot"], "yes");
    assert_eq!(COMPACT_UNREADABLE.get() - before, 1);
    assert_eq!(resume_point(&handle), 3);
}

/// `spawn` starts at least one worker even for a count of 0 (the runner
/// indexes into the queues it returns), and a worker publishes what is still
/// queued and stops once every sender is dropped. The cache is off, so Redis
/// is never used.
#[tokio::test]
async fn spawning_no_workers_still_starts_one_that_drains_its_queue() {
    let recorder = Arc::new(Recorder::default());
    let ctx = Arc::new(context(
        NoRedis,
        recorder.clone(),
        Arc::new(Registry::default()),
        StateConfig::default(),
        DiscordLog::disabled(),
    ));
    let (queues, workers) = worker::spawn(0, ctx);
    assert_eq!((queues.len(), workers.len()), (1, 1));

    let queued = Job {
        shard: 0,
        seq: Some(2),
        token: None,
        session: None,
        json: r#"{"op":0,"t":"A","s":2,"d":{}}"#.to_owned(),
        event: None,
    };
    queues[0].send(queued).await.unwrap();
    drop(queues);
    for worker in workers {
        tokio::time::timeout(Duration::from_secs(30), worker)
            .await
            .expect("the worker drains its queue and stops")
            .unwrap();
    }
    let published = recorder.published();
    assert_eq!(published, [("A".to_owned(), Some("0::2".to_owned()))]);
}
