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
pub const MAX_PAIRS: usize = 100_000;

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
    /// compact payload carries it.
    Publish {
        /// The window's drops since the pair's previous published message.
        dropped: u32,
    },
    /// Do not publish it.
    Drop(Reason),
}

/// The fields of a `MESSAGE_CREATE` the rules read. A payload that does not
/// have them (the model would not accept it either) is published.
#[derive(Deserialize)]
struct Message {
    guild_id: Option<Id<GuildMarker>>,
    author: Author,
    webhook_id: Option<IgnoredAny>,
    #[serde(rename = "type", default)]
    kind: u8,
}

#[derive(Deserialize)]
struct Author {
    id: Id<UserMarker>,
    #[serde(default)]
    bot: bool,
}

/// When a pair's last message was published, and how many were dropped since.
#[derive(Debug)]
struct Pair {
    last: Instant,
    dropped: u32,
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
    /// A `d` that cannot be read is published.
    pub fn check(&self, data: &str) -> Verdict {
        self.check_at(data, Instant::now())
    }

    fn check_at(&self, data: &str, now: Instant) -> Verdict {
        let Ok(message) = serde_json::from_str::<Message>(data) else {
            return Verdict::Publish { dropped: 0 };
        };
        if message.author.bot {
            return Verdict::Drop(Reason::Bot);
        }
        if message.webhook_id.is_some() {
            return Verdict::Drop(Reason::Webhook);
        }
        let Some(guild) = message.guild_id else {
            return Verdict::Drop(Reason::Dm);
        };
        if !CHAT_TYPES.contains(&message.kind) {
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
            entry.last = now;
            return Verdict::Publish {
                dropped: std::mem::take(&mut entry.dropped),
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
                },
            );
        }
        Verdict::Publish { dropped: 0 }
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
    fn a_payload_that_cannot_be_read_is_published() {
        let filter = ChatFilter::new(WINDOW);
        for data in ["{}", "not json", r#"{"author":{"id":"x"}}"#] {
            assert_eq!(filter.check(data), publish(0), "{data}");
        }
        assert_eq!(filter.pairs(), 0);
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
