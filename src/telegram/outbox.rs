//! Outgoing Telegram notifications, decoupled from the engine.
//!
//! The engine side only ever calls [`Notifier::notify`]: one `try_send` into a bounded channel,
//! no await, no lock, never blocked by Telegram (a full queue drops the notice and counts it).
//! A single worker drains the queue on the Telegram runtime:
//!
//! * pacing: at most one message per `min_interval` (Telegram allows ~1/s per chat);
//! * coalescing: notices that piled up while it waited are merged into one message (up to
//!   `coalesce_chars`), so a burst costs one send instead of a growing backlog;
//! * TTL: a routine notice older than `ttl` is dropped, never delivered minutes late (critical
//!   ones never expire);
//! * bounded retries: `RetryAfter` (HTTP 429) honoured up to `max_retry_after`, transient errors
//!   retried with back-off, `max_attempts` per message, then dropped; a routine message stops
//!   retrying as soon as a critical one is waiting;
//! * HTML safety: invalid HTML is sent as plain text, and a "can't parse entities" answer is
//!   retried once as plain text, so a bad string never loses the message;
//! * critical notices (circuit breaker reports) have their own queue, served first.

use crate::telegram::html;
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::warn;

/// Flood-control wait a critical message may sit out (routine ones: `max_retry_after`).
const CRITICAL_MAX_RETRY_AFTER: Duration = Duration::from_secs(300);
const CRITICAL_MAX_ATTEMPTS: u32 = 6;
/// How often a routine message's retry wait checks for a waiting critical one.
const YIELD_CHECK_EVERY: Duration = Duration::from_millis(100);

/// Why one send failed, in the terms the retry policy needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError {
    /// Flood control (HTTP 429): wait this long before the next request.
    RetryAfter(Duration),
    /// Telegram could not parse the message's HTML entities.
    Parse(String),
    /// Network trouble or a 5xx: may work on a later attempt.
    Transient(String),
    /// Will never work (chat not found, bot blocked, bad request).
    Fatal(String),
}

/// The one Telegram call the outbox needs (a trait, so the policy can be tested without Telegram).
pub trait TelegramApi: Send + Sync + 'static {
    fn send(&self, chat_id: i64, text: String, html: bool) -> BoxFuture<'_, Result<(), SendError>>;
}

#[derive(Debug, Clone, Copy)]
pub struct OutboxPolicy {
    /// Normal notices waiting at most (the engine never waits: beyond this they are dropped).
    pub capacity: usize,
    pub critical_capacity: usize,
    pub min_interval: Duration,
    pub max_attempts: u32,
    pub max_retry_after: Duration,
    pub ttl: Duration,
    pub coalesce_chars: usize,
}

impl Default for OutboxPolicy {
    fn default() -> Self {
        Self {
            capacity: 64,
            critical_capacity: 16,
            min_interval: Duration::from_millis(1_000),
            max_attempts: 3,
            max_retry_after: Duration::from_secs(30),
            ttl: Duration::from_secs(90),
            coalesce_chars: 3_500,
        }
    }
}

#[derive(Debug, Default)]
pub struct OutboxStats {
    pub enqueued: AtomicU64,
    pub sent: AtomicU64,
    /// Notices merged into another message.
    pub coalesced: AtomicU64,
    /// Rejected at enqueue: queue full.
    pub dropped_full: AtomicU64,
    /// Dropped by the worker: older than the TTL (routine notices only).
    pub expired: AtomicU64,
    /// Notices given up after the retry budget / a fatal error (each notice of a merged message).
    pub failed: AtomicU64,
    /// Sent as plain text because the HTML was invalid or refused.
    pub sanitized: AtomicU64,
    pub retries: AtomicU64,
    /// `dropped_full + expired + failed` already announced to the operator.
    announced_losses: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OutboxSnapshot {
    pub enqueued: u64,
    pub sent: u64,
    pub coalesced: u64,
    pub dropped_full: u64,
    pub expired: u64,
    pub failed: u64,
    pub sanitized: u64,
    pub retries: u64,
}

impl OutboxStats {
    pub fn snapshot(&self) -> OutboxSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        OutboxSnapshot {
            enqueued: l(&self.enqueued),
            sent: l(&self.sent),
            coalesced: l(&self.coalesced),
            dropped_full: l(&self.dropped_full),
            expired: l(&self.expired),
            failed: l(&self.failed),
            sanitized: l(&self.sanitized),
            retries: l(&self.retries),
        }
    }

    fn losses(&self) -> u64 {
        self.dropped_full.load(Ordering::Relaxed) + self.expired.load(Ordering::Relaxed) + self.failed.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct Notice {
    chat_id: i64,
    text: String,
    at: Instant,
}

/// Engine-side handle: cheap to clone, never blocks.
#[derive(Clone)]
pub struct Notifier {
    normal: mpsc::Sender<Notice>,
    critical: mpsc::Sender<Notice>,
    stats: Arc<OutboxStats>,
}

/// Worker-side end of the queues.
pub struct OutboxReceiver {
    normal: mpsc::Receiver<Notice>,
    critical: mpsc::Receiver<Notice>,
    stats: Arc<OutboxStats>,
    policy: OutboxPolicy,
}

impl Notifier {
    pub fn channel(policy: OutboxPolicy) -> (Notifier, OutboxReceiver) {
        let (normal, normal_rx) = mpsc::channel(policy.capacity.max(1));
        let (critical, critical_rx) = mpsc::channel(policy.critical_capacity.max(1));
        let stats = Arc::new(OutboxStats::default());
        (
            Notifier { normal, critical, stats: stats.clone() },
            OutboxReceiver { normal: normal_rx, critical: critical_rx, stats, policy },
        )
    }

    fn push(&self, queue: &mpsc::Sender<Notice>, chat_id: i64, text: String) -> bool {
        if chat_id == 0 {
            return false;
        }
        match queue.try_send(Notice { chat_id, text, at: Instant::now() }) {
            Ok(()) => {
                self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(_) => {
                self.stats.dropped_full.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Queue a notice. Non-blocking: false if the queue is full (the notice is dropped and counted).
    pub fn notify(&self, chat_id: i64, text: String) -> bool {
        self.push(&self.normal, chat_id, text)
    }

    /// Queue a notice that must not wait behind routine alerts (circuit breaker report).
    pub fn notify_critical(&self, chat_id: i64, text: String) -> bool {
        self.push(&self.critical, chat_id, text)
    }

    pub fn stats(&self) -> OutboxSnapshot {
        self.stats.snapshot()
    }
}

impl OutboxReceiver {
    pub fn stats(&self) -> Arc<OutboxStats> {
        self.stats.clone()
    }
}

/// Send one message under the retry policy; true if Telegram accepted it. `yield_to` is asked
/// before every retry wait: when it says a more urgent message is waiting, this one is given up
/// instead of holding that one back for a flood-control delay.
async fn deliver(
    api: &dyn TelegramApi,
    stats: &OutboxStats,
    policy: &OutboxPolicy,
    chat_id: i64,
    text: String,
    critical: bool,
    yield_to: &(dyn Fn() -> bool + Sync),
) -> bool {
    // A critical message (breaker report, failed exit) is worth a longer flood-control wait and
    // a few more attempts than routine traffic.
    let (max_wait, max_attempts) = if critical {
        (policy.max_retry_after.max(CRITICAL_MAX_RETRY_AFTER), policy.max_attempts.max(CRITICAL_MAX_ATTEMPTS))
    } else {
        (policy.max_retry_after, policy.max_attempts)
    };
    // Waits are sliced so a routine message notices a critical one arriving mid-wait.
    let wait = |d: Duration| async move {
        let until = Instant::now() + d;
        while Instant::now() < until {
            if yield_to() {
                return false;
            }
            tokio::time::sleep((until - Instant::now()).min(YIELD_CHECK_EVERY)).await;
        }
        true
    };
    // Too long but well-formed: cut at a line, tags closed (keeps the formatting).
    let fitted = html::fit(&text, html::MAX_MESSAGE_CHARS);
    let (mut body, mut as_html) = match html::validate(&fitted) {
        Ok(()) => (fitted, true),
        Err(e) => {
            warn!("Telegram notice is not valid HTML ({}): sending it as plain text", e);
            stats.sanitized.fetch_add(1, Ordering::Relaxed);
            (html::truncate_plain(&html::to_plain(&text), html::MAX_MESSAGE_CHARS), false)
        }
    };
    let mut attempt = 0;
    loop {
        attempt += 1;
        match api.send(chat_id, body.clone(), as_html).await {
            Ok(()) => {
                stats.sent.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            // Not an attempt: the same message, as text Telegram cannot misparse.
            Err(SendError::Parse(e)) if as_html => {
                warn!("Telegram refused the HTML ({}): resending as plain text", e);
                stats.sanitized.fetch_add(1, Ordering::Relaxed);
                body = html::truncate_plain(&html::to_plain(&body), html::MAX_MESSAGE_CHARS);
                as_html = false;
                attempt -= 1;
            }
            Err(SendError::RetryAfter(delay)) if attempt < max_attempts && delay <= max_wait => {
                stats.retries.fetch_add(1, Ordering::Relaxed);
                if !wait(delay).await {
                    warn!("Routine Telegram notice given up: a critical one is waiting");
                    return false;
                }
            }
            Err(SendError::Transient(e)) if attempt < max_attempts => {
                stats.retries.fetch_add(1, Ordering::Relaxed);
                warn!("Telegram send failed ({}), retry {}/{}", e, attempt, max_attempts - 1);
                if !wait(Duration::from_millis(500) * attempt).await {
                    warn!("Routine Telegram notice given up: a critical one is waiting");
                    return false;
                }
            }
            Err(e) => {
                warn!("Telegram notice dropped after {} attempt(s): {:?}", attempt, e);
                return false;
            }
        }
    }
}

/// The worker. Runs until every [`Notifier`] is dropped.
pub async fn run_outbox(api: Arc<dyn TelegramApi>, mut rx: OutboxReceiver) {
    let policy = rx.policy;
    let stats = rx.stats.clone();
    // Critical notices (circuit-breaker reports, failed exits) never expire: late is better than
    // never for those. Routine ones older than the TTL are dropped instead of delivered late.
    let expired = |n: &Notice, critical: bool| !critical && n.at.elapsed() > policy.ttl;
    // A notice taken off the queue that did not fit into the previous message.
    let mut carry: Option<(Notice, bool)> = None;
    loop {
        let (first, critical) = match carry.take() {
            Some(c) => c,
            None => tokio::select! {
                biased;
                Some(n) = rx.critical.recv() => (n, true),
                Some(n) = rx.normal.recv() => (n, false),
                else => return,
            },
        };
        if expired(&first, critical) {
            stats.expired.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let chat_id = first.chat_id;
        let mut text = first.text;
        let mut notices = 1u64;
        // Merge what is already waiting in the same queue, for the same chat.
        loop {
            let next = if critical { rx.critical.try_recv() } else { rx.normal.try_recv() };
            let Ok(next) = next else { break };
            if expired(&next, critical) {
                stats.expired.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if next.chat_id != chat_id || html::plain_len(&text) + html::plain_len(&next.text) + 2 > policy.coalesce_chars {
                carry = Some((next, critical));
                break;
            }
            text.push_str("\n\n");
            text.push_str(&next.text);
            notices += 1;
            stats.coalesced.fetch_add(1, Ordering::Relaxed);
        }
        // Tell the operator what they did not get.
        let losses = stats.losses();
        let announced = stats.announced_losses.load(Ordering::Relaxed);
        if losses > announced {
            text = format!("⚠️ <i>{} notifikasi sebelumnya dilewati (antrean penuh / kedaluwarsa / gagal kirim).</i>\n\n{}", losses - announced, text);
        }
        // A routine message gives up its retries as soon as a critical one is waiting.
        let critical_rx = &rx.critical;
        let yield_to = move || !critical && !critical_rx.is_empty();
        if deliver(api.as_ref(), &stats, &policy, chat_id, text, critical, &yield_to).await {
            stats.announced_losses.store(losses, Ordering::Relaxed);
        } else {
            // Every notice merged into the message is lost, not just one.
            stats.failed.fetch_add(notices, Ordering::Relaxed);
        }
        tokio::time::sleep(policy.min_interval).await;
    }
}
