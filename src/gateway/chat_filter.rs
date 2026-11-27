//! The chat source filter (`CHAT_FILTER`): which `MESSAGE_CREATE` events are
//! chat worth publishing.
//!
//! A message is dropped when it comes from a bot, from a webhook, from a DM,
//! is a system message (only the types [`CHAT_TYPES`] count as chat), or when
//! the same (guild, user) pair already had a message published inside the
//! window (`CHAT_FILTER_WINDOW_SECS`). The window needs state: a bounded map
//! of the pairs seen, shared by every worker. A dropped message is still
//! cached and still completes its sequence number (see `worker`); it is only
//! not published.
//!
//! Only the few fields the rules read are taken from the raw payload.

use serde::{de::IgnoredAny, Deserialize};
use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};
use twilight_model::id::{
    marker::{GuildMarker, UserMarker},
    Id,
};

/// The message types that count as chat: 0 (default) and 19 (reply). Every
/// other type is a system message (joins, pins, boosts, calls, ...).
pub const CHAT_TYPES: [u8; 2] = [0, 19];

/// The most (guild, user) pairs the window remembers. With more live pairs
/// than this, a new pair is published without being remembered.
///
/// A pair costs about 100 bytes (key, two instants, a count, and the hash
/// table's overhead), so a full map is roughly 100 to 150 MB, the most while
/// the table is growing. It is sized for several hundred thousand live pairs
/// inside one window (about 5000 chat messages a second over 100000 guilds).
pub const MAX_PAIRS: usize = 1_000_000;

/// The shortest time between two sweeps of the expired pairs of a full map,
/// so a flood of new pairs does not walk the whole map for every message.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Why a message is not published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reason {
    /// The author is a bot.
    Bot,
    /// The message was sent by a webhook.
    Webhook,
    /// The message is a DM (it has no guild).
    Dm,
    /// The message is a system message (not one of [`CHAT_TYPES`]).
    System,
    /// A message of the same (guild, user) pair was published inside the
    /// window.
    Window,
}

impl Reason {
    /// The `reason` label of `gateway_chat_filtered`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Bot => "bot",
            Self::Webhook => "webhook",
            Self::Dm => "dm",
            Self::System => "system",
            Self::Window => "window",
        }
    }
}

/// What to do with a `MESSAGE_CREATE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Verdict {
    /// Publish it. `dropped` is how many messages of its (guild, user) pair
    /// the window dropped since the pair's previous published message (the
    /// other rules do not count); the filter has reset it to zero. The
    /// compact payload carries it. A count that waited at least twice the
    /// window for the pair's next message is stale and reported as 0.
    Publish {
        /// The window's drops since the pair's previous published message.
        dropped: u32,
    },
    /// Do not publish it.
    Drop(Reason),
    /// The rules could not read the message (no author, an id that is not a
    /// snowflake, a flag of the wrong type). Nothing was recorded. Published
    /// whole as before; with the compact payload on it is not published.
    Unreadable,
}

/// The fields of a `MESSAGE_CREATE` the rules read. A `null` or absent `bot`
/// or `type` is what Discord sends for a plain message, so they are optional
/// and the type is read as a wide integer (one above 255 is not chat).
#[derive(Deserialize)]
struct Message {
    guild_id: Option<Id<GuildMarker>>,
    author: Author,
    webhook_id: Option<IgnoredAny>,
    #[serde(rename = "type", default)]
    kind: Option<u64>,
}

#[derive(Deserialize)]
struct Author {
    id: Id<UserMarker>,
    #[serde(default)]
    bot: Option<bool>,
}

/// When a pair's last message was published, and how many were dropped since.
#[derive(Debug)]
struct Pair {
    last: Instant,
    dropped: u32,
    /// `last` and `dropped` as they were before the latest published message
    /// took them over, for [`ChatFilter::undo`]. `None` when that message
    /// created the pair.
    before: Option<(Instant, u32)>,
}

#[derive(Debug, Default)]
struct State {
    pairs: HashMap<(u64, u64), Pair>,
    swept: Option<Instant>,
}

/// The chat filter, shared by every worker.
#[derive(Debug)]
pub struct ChatFilter {
    window: Duration,
    max_pairs: usize,
    state: Mutex<State>,
}

impl ChatFilter {
    /// A filter with a per-pair window (zero turns the window off, and with
    /// it the map).
    pub fn new(window: Duration) -> Self {
        Self::with_capacity(window, MAX_PAIRS)
    }

    /// As [`ChatFilter::new`], remembering at most `max_pairs` pairs.
    pub fn with_capacity(window: Duration, max_pairs: usize) -> Self {
        Self {
            window,
            max_pairs,
            state: Mutex::default(),
        }
    }

    /// Decide on the `MESSAGE_CREATE` whose `d` is `data` (the raw JSON).
    pub fn check(&self, data: &str) -> Verdict {
        self.check_at(data, Instant::now())
    }

    fn check_at(&self, data: &str, now: Instant) -> Verdict {
        let Ok(message) = serde_json::from_str::<Message>(data) else {
            return Verdict::Unreadable;
        };
        if message.author.bot == Some(true) {
            return Verdict::Drop(Reason::Bot);
        }
        if message.webhook_id.is_some() {
            return Verdict::Drop(Reason::Webhook);
        }
        let Some(guild) = message.guild_id else {
            return Verdict::Drop(Reason::Dm);
        };
        let chat =
            u8::try_from(message.kind.unwrap_or(0)).is_ok_and(|kind| CHAT_TYPES.contains(&kind));
        if !chat {
            return Verdict::Drop(Reason::System);
        }
        self.window_at((guild.get(), message.author.id.get()), now)
    }

    /// The window rule for a message of `pair` at `now`: dropped while less
    /// than the window has passed since the pair's last published message.
    fn window_at(&self, pair: (u64, u64), now: Instant) -> Verdict {
        if self.window.is_zero() {
            return Verdict::Publish { dropped: 0 };
        }
        let mut state = self.state();
        if let Some(entry) = state.pairs.get_mut(&pair) {
            if now.saturating_duration_since(entry.last) < self.window {
                entry.dropped = entry.dropped.saturating_add(1);
                return Verdict::Drop(Reason::Window);
            }
            let dropped = std::mem::take(&mut entry.dropped);
            // A count that waited this long is not a recent burst.
            let aged = now.saturating_duration_since(entry.last) >= self.window.saturating_mul(2);
            entry.before = Some((entry.last, dropped));
            entry.last = now;
            return Verdict::Publish {
                dropped: if aged { 0 } else { dropped },
            };
        }
        // A new pair. A full map first forgets the pairs whose window is over
        // (they would publish anyway); if it is still full the pair is not
        // remembered, so the map never grows past `max_pairs`.
        if state.pairs.len() >= self.max_pairs {
            self.sweep(&mut state, now);
        }
        if state.pairs.len() < self.max_pairs {
            state.pairs.insert(
                pair,
                Pair {
                    last: now,
                    dropped: 0,
                    before: None,
                },
            );
        }
        Verdict::Publish { dropped: 0 }
    }

    /// Take back the record of the message with this `data` that
    /// [`ChatFilter::check`] just let through, because it was not published
    /// after all (the broker did not take it): the pair's last published time
    /// and drop count are what they were before it, or the pair is forgotten
    /// when that message created it. So the window does not run from a message
    /// nobody received: the pair's next message is published at once, and
    /// carries the restored count (when it comes within twice the window; a
    /// count older than that is reported as 0).
    ///
    /// Call it right after the failed publish, before the pair's next message
    /// is checked: one worker processes a guild's events one at a time, so
    /// nothing is in between.
    pub fn undo(&self, data: &str) {
        let Ok(message) = serde_json::from_str::<Message>(data) else {
            return;
        };
        let pair = (message.guild_id.map_or(0, Id::get), message.author.id.get());
        let mut state = self.state();
        let Some(entry) = state.pairs.get_mut(&pair) else {
            return;
        };
        match entry.before.take() {
            Some((last, dropped)) => {
                entry.last = last;
                entry.dropped = dropped;
            }
            None => {
                state.pairs.remove(&pair);
            }
        }
    }

    /// Forget the pairs whose window is over, at most once per
    /// [`SWEEP_INTERVAL`]. A forgotten pair loses its drop count.
    fn sweep(&self, state: &mut State, now: Instant) {
        if state
            .swept
            .is_some_and(|swept| now.saturating_duration_since(swept) < SWEEP_INTERVAL)
        {
            return;
        }
        state.swept = Some(now);
        let window = self.window;
        state
            .pairs
            .retain(|_, entry| now.saturating_duration_since(entry.last) < window);
    }

    /// How many pairs the window remembers.
    pub fn pairs(&self) -> usize {
        self.state().pairs.len()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    const WINDOW: Duration = Duration::from_secs(10);

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn chat() -> Value {
        json!({
            "id": "5", "channel_id": "2", "guild_id": "1", "content": "hi",
            "author": {"id": "3", "username": "u", "bot": false}, "type": 0
        })
    }

    fn from_user(id: &str) -> Value {
        let mut message = chat();
        message["author"]["id"] = json!(id);
        message
    }

    fn check_at(filter: &ChatFilter, message: &Value, now: Instant) -> Verdict {
        filter.check_at(&message.to_string(), now)
    }

    fn publish(dropped: u32) -> Verdict {
        Verdict::Publish { dropped }
    }

    /// The reason the stateless rules drop `message` for, if they do.
    fn drop_of(message: &Value) -> Option<Reason> {
        match ChatFilter::new(Duration::ZERO).check(&message.to_string()) {
            Verdict::Drop(reason) => Some(reason),
            Verdict::Publish { .. } => None,
            Verdict::Unreadable => panic!("unreadable: {message}"),
        }
    }

    #[test]
    fn each_source_rule_drops_its_messages() {
        let mut bot = chat();
        bot["author"]["bot"] = json!(true);
        assert_eq!(drop_of(&bot), Some(Reason::Bot));

        let mut webhook = chat();
        webhook["webhook_id"] = json!("9");
        assert_eq!(drop_of(&webhook), Some(Reason::Webhook));

        let mut dm = chat();
        dm.as_object_mut().unwrap().remove("guild_id");
        assert_eq!(drop_of(&dm), Some(Reason::Dm));

        let mut system = chat();
        system["type"] = json!(7);
        assert_eq!(drop_of(&system), Some(Reason::System));
    }

    #[test]
    fn only_default_and_reply_messages_are_chat() {
        for kind in 0..=255u8 {
            let mut message = chat();
            message["type"] = json!(kind);
            let expected = (!matches!(kind, 0 | 19)).then_some(Reason::System);
            assert_eq!(drop_of(&message), expected, "type {kind}");
        }
    }

    #[test]
    fn absent_or_null_markers_do_not_drop() {
        let mut message = chat();
        message["webhook_id"] = Value::Null;
        message["author"].as_object_mut().unwrap().remove("bot");
        message.as_object_mut().unwrap().remove("type");
        assert_eq!(drop_of(&message), None);
    }

    #[test]
    fn a_payload_that_cannot_be_read_is_unreadable() {
        let filter = ChatFilter::new(WINDOW);
        for data in [
            "{}",
            "not json",
            r#"{"author":{"id":"x"}}"#,
            r#"{"guild_id":"1","author":{"id":"3","bot":"yes"}}"#,
            r#"{"guild_id":"1","author":{"id":"3"},"type":"x"}"#,
        ] {
            assert_eq!(filter.check(data), Verdict::Unreadable, "{data}");
        }
        assert_eq!(filter.pairs(), 0);
    }

    #[test]
    fn a_null_bot_flag_is_no_bot_and_a_huge_type_is_no_chat() {
        let mut null_bot = chat();
        null_bot["author"]["bot"] = Value::Null;
        assert_eq!(drop_of(&null_bot), None);
        let mut null_type = chat();
        null_type["type"] = Value::Null;
        assert_eq!(drop_of(&null_type), None);
        let mut huge = chat();
        huge["type"] = json!(300);
        assert_eq!(drop_of(&huge), Some(Reason::System));
    }

    #[test]
    fn the_window_drops_inside_and_publishes_from_its_end() {
        let filter = ChatFilter::new(WINDOW);
        let start = Instant::now();
        let message = chat();
        assert_eq!(check_at(&filter, &message, start), publish(0));
        assert_eq!(
            check_at(&filter, &message, start + WINDOW - ms(1)),
            Verdict::Drop(Reason::Window)
        );
        // The window runs from the last published message, not from a drop.
        assert_eq!(check_at(&filter, &message, start + WINDOW), publish(1));
        assert_eq!(
            check_at(&filter, &message, start + WINDOW + ms(1)),
            Verdict::Drop(Reason::Window),
            "a new window began at the published message"
        );
    }

    #[test]
    fn the_drop_count_is_handed_over_once_and_reset() {
        let filter = ChatFilter::new(WINDOW);
        let start = Instant::now();
        let message = chat();
        check_at(&filter, &message, start);
        for second in 1..=3 {
            let at = start + Duration::from_secs(second);
            assert_eq!(
                check_at(&filter, &message, at),
                Verdict::Drop(Reason::Window)
            );
        }
        let later = start + WINDOW;
        assert_eq!(check_at(&filter, &message, later), publish(3));
        assert_eq!(check_at(&filter, &message, later + WINDOW), publish(0));
    }

    #[test]
    fn other_drop_rules_are_not_counted_and_do_not_start_a_window() {
        let filter = ChatFilter::new(WINDOW);
        let start = Instant::now();
        let mut bot = chat();
        bot["author"]["bot"] = json!(true);
        let mut system = chat();
        system["type"] = json!(7);
        assert_eq!(check_at(&filter, &bot, start), Verdict::Drop(Reason::Bot));
        assert_eq!(
            check_at(&filter, &system, start),
            Verdict::Drop(Reason::System)
        );
        assert_eq!(filter.pairs(), 0, "dropped by another rule: not remembered");

        let message = chat();
        assert_eq!(check_at(&filter, &message, start), publish(0));
        check_at(&filter, &bot, start);
        check_at(&filter, &system, start);
        assert_eq!(check_at(&filter, &message, start + WINDOW), publish(0));
    }

    #[test]
    fn pairs_do_not_affect_each_other() {
        let filter = ChatFilter::new(WINDOW);
        let start = Instant::now();
        let first = chat();
        let other_user = from_user("4");
        let mut other_guild = chat();
        other_guild["guild_id"] = json!("8");
        for message in [&first, &other_user, &other_guild] {
            assert_eq!(check_at(&filter, message, start), publish(0));
        }
        assert_eq!(filter.pairs(), 3);
        let at = start + Duration::from_secs(1);
        check_at(&filter, &first, at);
        check_at(&filter, &first, at);
        check_at(&filter, &other_user, at);
        let later = start + WINDOW;
        assert_eq!(check_at(&filter, &first, later), publish(2));
        assert_eq!(check_at(&filter, &other_user, later), publish(1));
        assert_eq!(check_at(&filter, &other_guild, later), publish(0));
    }

    #[test]
    fn a_zero_window_remembers_nothing() {
        let filter = ChatFilter::new(Duration::ZERO);
        let message = chat().to_string();
        for _ in 0..3 {
            assert_eq!(filter.check(&message), publish(0));
        }
        assert_eq!(filter.pairs(), 0);
    }

    #[test]
    fn the_map_stays_bounded() {
        let filter = ChatFilter::with_capacity(WINDOW, 2);
        let start = Instant::now();
        let messages = ["10", "11", "12"].map(from_user);
        for message in &messages {
            assert_eq!(check_at(&filter, message, start), publish(0));
        }
        assert_eq!(filter.pairs(), 2, "the third live pair is not remembered");
        assert_eq!(
            check_at(&filter, &messages[2], start + ms(1)),
            publish(0),
            "so it is published again"
        );

        // Once the window of the first two is over, a new pair sweeps them.
        let later = start + WINDOW + SWEEP_INTERVAL;
        assert_eq!(check_at(&filter, &messages[2], later), publish(0));
        assert_eq!(filter.pairs(), 1, "the expired pairs were evicted");
        assert_eq!(
            check_at(&filter, &messages[2], later + ms(1)),
            Verdict::Drop(Reason::Window)
        );
    }

    #[test]
    fn a_full_map_sweeps_at_most_once_per_interval() {
        let window = ms(100);
        let filter = ChatFilter::with_capacity(window, 1);
        let start = Instant::now();
        let [first, second, third] = ["10", "11", "12"].map(from_user);
        check_at(&filter, &first, start);
        // Full: the sweep finds the first pair alive.
        check_at(&filter, &second, start + ms(10));
        // The first pair has expired, but the last sweep was 190 ms ago: the
        // third pair is not remembered, so it is published again.
        assert_eq!(check_at(&filter, &third, start + ms(200)), publish(0));
        assert_eq!(check_at(&filter, &third, start + ms(250)), publish(0));
        // A sweep an interval after the last one makes room.
        let swept = start + ms(10) + SWEEP_INTERVAL;
        assert_eq!(check_at(&filter, &third, swept), publish(0));
        assert_eq!(
            check_at(&filter, &third, swept + ms(10)),
            Verdict::Drop(Reason::Window)
        );
        assert_eq!(filter.pairs(), 1);
    }

    #[test]
    fn the_pair_cap_is_a_million_and_new_uses_it() {
        assert_eq!(MAX_PAIRS, 1_000_000);
        assert_eq!(ChatFilter::new(WINDOW).max_pairs, MAX_PAIRS);
    }

    #[test]
    fn undo_restores_the_time_and_the_drop_count_of_a_published_message() {
        let filter = ChatFilter::new(WINDOW);
        let start = Instant::now();
        let message = chat();
        let data = message.to_string();
        check_at(&filter, &message, start);
        check_at(&filter, &message, start + ms(1));
        check_at(&filter, &message, start + ms(2));

        // Published with 2 dropped, then taken back because the broker did
        // not take it: the pair's next message finds the window as it was, so
        // it is published at once and carries the 2.
        let again = start + WINDOW + ms(5);
        assert_eq!(check_at(&filter, &message, again), publish(2));
        filter.undo(&data);
        assert_eq!(check_at(&filter, &message, again + ms(1)), publish(2));
        assert_eq!(
            check_at(&filter, &message, again + ms(2)),
            Verdict::Drop(Reason::Window),
            "and once published it starts the window"
        );
    }

    #[test]
    fn a_count_that_waited_twice_the_window_is_reported_as_zero() {
        let start = Instant::now();
        let message = chat();
        let fresh = |at: Instant| {
            let filter = ChatFilter::new(WINDOW);
            check_at(&filter, &message, start);
            check_at(&filter, &message, start + ms(1));
            check_at(&filter, &message, start + ms(2));
            (check_at(&filter, &message, at), filter)
        };
        assert_eq!(fresh(start + WINDOW * 2 - ms(1)).0, publish(2));
        let (verdict, filter) = fresh(start + WINDOW * 2);
        assert_eq!(verdict, publish(0), "stale: not a recent burst");
        assert_eq!(
            check_at(&filter, &message, start + WINDOW * 2 + ms(1)),
            Verdict::Drop(Reason::Window),
            "the window still started"
        );
        // Undo keeps the count, as the next message may come sooner.
        filter.undo(&message.to_string());
        assert_eq!(
            check_at(&filter, &message, start + WINDOW * 2 - ms(1)),
            publish(2)
        );
    }

    #[test]
    fn undo_forgets_a_pair_the_message_created() {
        let filter = ChatFilter::new(WINDOW);
        let start = Instant::now();
        let message = chat();
        assert_eq!(check_at(&filter, &message, start), publish(0));
        filter.undo(&message.to_string());
        assert_eq!(filter.pairs(), 0);
        assert_eq!(check_at(&filter, &message, start + ms(1)), publish(0));
    }

    #[test]
    fn undo_without_a_record_changes_nothing() {
        // A full map that did not remember the pair, a zero window, a payload
        // that cannot be read, and an unknown pair.
        let full = ChatFilter::with_capacity(WINDOW, 1);
        let start = Instant::now();
        check_at(&full, &chat(), start);
        let unremembered = from_user("11");
        check_at(&full, &unremembered, start);
        full.undo(&unremembered.to_string());
        assert_eq!(full.pairs(), 1);

        let none = ChatFilter::new(Duration::ZERO);
        none.undo(&chat().to_string());
        none.undo("not json");
        assert_eq!(none.pairs(), 0);
    }

    #[test]
    fn labels_name_each_reason() {
        let labels = [
            Reason::Bot,
            Reason::Webhook,
            Reason::Dm,
            Reason::System,
            Reason::Window,
        ]
        .map(Reason::label);
        assert_eq!(labels, ["bot", "webhook", "dm", "system", "window"]);
    }
}
