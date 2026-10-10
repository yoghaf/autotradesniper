//! Connection, retry and de-duplication primitives for racing several sequencer feeds.

use anyhow::{Context, Result, anyhow, bail};
use futures_util::future::select_ok;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::{TcpStream, lookup_host};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls_with_config};

/// TCP + TLS + WebSocket upgrade must finish within this, or the attempt is abandoned. Without
/// a deadline a black-holed SYN pins the feed for the OS default (minutes).
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// A host with several A/AAAA records is raced, but never more than this many sockets.
const MAX_RACED_ADDRS: usize = 4;
/// Nitro: the client asks the broadcaster to start at this feed sequence number, so a
/// reconnect gets only what is missing instead of the whole backlog cache.
pub const REQUESTED_SEQ_HEADER: &str = "Arbitrum-Requested-Sequence-Number";

pub type FeedSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Open the feed socket with `TCP_NODELAY` and resume hint.
///
/// Compared with `connect_async`: Nagle is disabled before the first byte is written (it also
/// delays our pongs / pings and the TLS handshake flights), every resolved address is raced
/// so a dead AAAA record cannot stall the connect, and the whole attempt has a deadline.
pub async fn connect_feed(url: &str, resume_seq: u64) -> Result<(FeedSocket, Response)> {
    connect_feed_within(url, resume_seq, CONNECT_TIMEOUT).await
}

/// [`connect_feed`] with an explicit deadline (tests use a short one).
pub async fn connect_feed_within(url: &str, resume_seq: u64, deadline: Duration) -> Result<(FeedSocket, Response)> {
    let mut request = url.into_client_request().context("invalid feed URL")?;
    if resume_seq > 0 {
        request.headers_mut().insert(REQUESTED_SEQ_HEADER, HeaderValue::from(resume_seq));
    }
    let uri = request.uri().clone();
    let host = uri.host().ok_or_else(|| anyhow!("feed URL has no host: {url}"))?.to_owned();
    let port = uri.port_u16().unwrap_or(match uri.scheme_str() {
        Some("ws") => 80,
        _ => 443,
    });

    tokio::time::timeout(deadline, async move {
        let tcp = connect_tcp(&host, port).await?;
        let (ws, response) = client_async_tls_with_config(request, tcp, None, None).await?;
        Ok::<_, anyhow::Error>((ws, response))
    })
    .await
    .map_err(|_| anyhow!("feed connect timed out after {:?}", deadline))?
    .map_err(|e| match e.downcast::<tokio_tungstenite::tungstenite::Error>() {
        // Keep the tungstenite error at the top of the chain: `retry_after_hint` looks for it.
        Ok(ws_err) => anyhow::Error::new(ws_err).context("Failed to connect to Arbitrum Sequencer Feed WebSocket"),
        Err(other) => other.context("Failed to connect to Arbitrum Sequencer Feed WebSocket"),
    })
}

async fn connect_tcp(host: &str, port: u16) -> Result<TcpStream> {
    let addrs: Vec<SocketAddr> = lookup_host((host, port))
        .await
        .with_context(|| format!("DNS lookup failed for {host}"))?
        .take(MAX_RACED_ADDRS)
        .collect();
    let stream = match addrs.as_slice() {
        [] => bail!("{host} resolved to no addresses"),
        [only] => TcpStream::connect(only).await?,
        many => {
            let attempts = many.iter().map(|a| Box::pin(TcpStream::connect(*a)));
            select_ok(attempts).await?.0
        }
    };
    stream.set_nodelay(true).context("failed to set TCP_NODELAY")?;
    Ok(stream)
}

/// Exponential backoff with "equal jitter": each delay is uniformly drawn from
/// `[ceiling/2, ceiling]`, where the ceiling doubles per consecutive failure up to `max`.
/// The fixed half keeps a flapping endpoint from being hammered; the random half keeps many
/// clients (or several feeds of one client) from reconnecting in lockstep.
#[derive(Clone, Debug)]
pub struct Backoff {
    min: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    pub fn new(min: Duration, max: Duration) -> Self {
        Self { min, max, attempt: 0 }
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// `unit` is a uniform sample in `[0, 1)` (injected so the schedule is testable).
    pub fn next_delay_with(&mut self, unit: f64) -> Duration {
        let ceiling = self.min.saturating_mul(1u32 << self.attempt.min(20)).min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        ceiling.mul_f64(0.5 + 0.5 * unit.clamp(0.0, 1.0))
    }

    pub fn next_delay(&mut self) -> Duration {
        self.next_delay_with(rand::random::<f64>())
    }
}

/// How many recent block keys the claim ring remembers (power of two). A message more than
/// this many blocks behind the newest one is stale for trading purposes anyway.
pub const CLAIM_WINDOW: usize = 4096;

/// Lock-free "first packet wins" registry keyed by L2 block number.
///
/// Every feed (and the HTTP bridge) calls [`ClaimRing::try_claim`] for each message right after
/// parsing it; exactly one caller gets `true` and processes the message. Unlike a high-water
/// mark this stays exact when feeds deliver out of order relative to each other: if feed B
/// skips block 101 but delivers 102, block 101 arriving later from feed A is still claimed.
pub struct ClaimRing {
    slots: Box<[AtomicU64]>,
}

impl Default for ClaimRing {
    fn default() -> Self {
        Self { slots: (0..CLAIM_WINDOW).map(|_| AtomicU64::new(0)).collect() }
    }
}

impl ClaimRing {
    #[inline]
    fn slot(&self, key: u64) -> &AtomicU64 {
        &self.slots[key as usize & (CLAIM_WINDOW - 1)]
    }

    /// True for the first claimer of `key`. False for a duplicate, and for a key so old that
    /// the ring has already moved a full window past it.
    #[inline]
    pub fn try_claim(&self, key: u64) -> bool {
        self.slot(key).fetch_max(key.saturating_add(1), Ordering::AcqRel) <= key
    }

    /// Read-only form: has `key` been claimed (or aged out of the window)?
    #[inline]
    pub fn is_claimed(&self, key: u64) -> bool {
        self.slot(key).load(Ordering::Acquire) > key
    }
}

/// Remembers when each block was first claimed so a losing feed can report how late it was.
/// One packed atomic per slot: 24 bits of key tag + 40 bits of microseconds.
pub struct WinLedger {
    epoch: Instant,
    slots: Box<[AtomicU64]>,
}

const TIME_BITS: u32 = 40;
const TIME_MASK: u64 = (1 << TIME_BITS) - 1;
/// A "lateness" above this is a stale/garbled slot, not a measurement.
const MAX_PLAUSIBLE_LATENESS_US: u64 = 60_000_000;

impl Default for WinLedger {
    fn default() -> Self {
        Self { epoch: Instant::now(), slots: (0..CLAIM_WINDOW).map(|_| AtomicU64::new(0)).collect() }
    }
}

impl WinLedger {
    #[inline]
    fn now_us(&self) -> u64 {
        self.epoch.elapsed().as_micros() as u64 & TIME_MASK
    }

    #[inline]
    fn tag(key: u64) -> u64 {
        (key & 0xFF_FFFF) << TIME_BITS
    }

    #[inline]
    pub fn record_win(&self, key: u64) {
        let slot = &self.slots[key as usize & (CLAIM_WINDOW - 1)];
        slot.store(Self::tag(key) | self.now_us(), Ordering::Relaxed);
    }

    /// Microseconds between the winner's claim and now, if the slot still describes `key`.
    #[inline]
    pub fn lateness_us(&self, key: u64) -> Option<u64> {
        let packed = self.slots[key as usize & (CLAIM_WINDOW - 1)].load(Ordering::Relaxed);
        if packed == 0 || packed & !TIME_MASK != Self::tag(key) {
            return None;
        }
        let late = self.now_us().wrapping_sub(packed & TIME_MASK) & TIME_MASK;
        (late <= MAX_PLAUSIBLE_LATENESS_US).then_some(late)
    }
}

/// `scheme://host[:port]` only: feed URLs often embed an API key in the path or query.
pub fn redact_feed_url(url: &str) -> String {
    use tokio_tungstenite::tungstenite::http::Uri;
    match url.parse::<Uri>() {
        Ok(uri) => match (uri.scheme_str(), uri.host()) {
            (Some(scheme), Some(host)) => match uri.port_u16() {
                Some(port) => format!("{scheme}://{host}:{port}"),
                None => format!("{scheme}://{host}"),
            },
            _ => "<feed>".to_string(),
        },
        Err(_) => "<invalid feed url>".to_string(),
    }
}

/// Per-endpoint counters, readable at any time without locking.
#[derive(Debug)]
pub struct FeedStats {
    /// Full URL (may carry an API key): never logged, only used to connect.
    pub(crate) url: String,
    /// `scheme://host[:port]` of the URL, safe for logs and UI.
    pub label: String,
    pub(crate) connected: AtomicBool,
    pub(crate) frames: AtomicU64,
    pub(crate) wins: AtomicU64,
    pub(crate) dups: AtomicU64,
    pub(crate) parse_errors: AtomicU64,
    pub(crate) sessions: AtomicU64,
    pub(crate) lag_sum_us: AtomicU64,
    pub(crate) lag_samples: AtomicU64,
    pub(crate) rtt_us: AtomicU64,
}

impl FeedStats {
    pub fn new(url: impl Into<String>) -> Self {
        let url = url.into();
        Self {
            label: redact_feed_url(&url),
            url,
            connected: AtomicBool::new(false),
            frames: AtomicU64::new(0),
            wins: AtomicU64::new(0),
            dups: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            sessions: AtomicU64::new(0),
            lag_sum_us: AtomicU64::new(0),
            lag_samples: AtomicU64::new(0),
            rtt_us: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> FeedStatsSnapshot {
        let samples = self.lag_samples.load(Ordering::Relaxed);
        let rtt = self.rtt_us.load(Ordering::Relaxed);
        FeedStatsSnapshot {
            label: self.label.clone(),
            connected: self.connected.load(Ordering::Relaxed),
            frames: self.frames.load(Ordering::Relaxed),
            wins: self.wins.load(Ordering::Relaxed),
            dups: self.dups.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            sessions: self.sessions.load(Ordering::Relaxed),
            avg_lag_us: (samples > 0).then(|| self.lag_sum_us.load(Ordering::Relaxed) / samples),
            rtt_us: (rtt > 0).then_some(rtt),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedStatsSnapshot {
    pub label: String,
    pub connected: bool,
    pub frames: u64,
    /// Messages this feed delivered first (it triggered the processing).
    pub wins: u64,
    /// Messages another feed (or the HTTP bridge) had already delivered.
    pub dups: u64,
    pub parse_errors: u64,
    /// Connections opened so far (1 = never reconnected).
    pub sessions: u64,
    /// Mean delay behind the winning feed over the duplicates it delivered.
    pub avg_lag_us: Option<u64>,
    /// Latest WebSocket ping round trip.
    pub rtt_us: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_the_cap_and_stays_inside_the_jitter_band() {
        let mut b = Backoff::new(Duration::from_millis(200), Duration::from_secs(5));
        let ceilings = [200u64, 400, 800, 1600, 3200, 5000, 5000, 5000];
        for ceiling in ceilings {
            let lo = b.clone().next_delay_with(0.0);
            let hi = b.clone().next_delay_with(0.999_999);
            assert_eq!(lo, Duration::from_millis(ceiling / 2), "lower band edge");
            assert!(hi <= Duration::from_millis(ceiling) && hi > Duration::from_millis(ceiling * 99 / 200));
            b.next_delay_with(0.5);
        }
        b.reset();
        assert_eq!(b.attempt(), 0);
        assert_eq!(b.next_delay_with(1.0), Duration::from_millis(200));
    }

    #[test]
    fn backoff_never_overflows_after_many_failures() {
        let mut b = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        for _ in 0..200 {
            assert!(b.next_delay() <= Duration::from_secs(30));
        }
    }

    #[test]
    fn feed_labels_never_carry_credentials() {
        assert_eq!(redact_feed_url("wss://user:pw@feed.example.com:8443/feed?apikey=SECRET"), "wss://feed.example.com:8443");
        assert_eq!(redact_feed_url("wss://feed.example.com/v2/SECRETKEY"), "wss://feed.example.com");
        assert_eq!(redact_feed_url("not a url"), "<invalid feed url>");
    }

    #[test]
    fn claim_ring_gives_each_key_to_exactly_one_claimer() {
        let ring = ClaimRing::default();
        assert!(ring.try_claim(10));
        assert!(!ring.try_claim(10));
        assert!(ring.is_claimed(10));
        assert!(!ring.is_claimed(11));
        // Out of order: 12 arrives before 11; 11 must still be accepted.
        assert!(ring.try_claim(12));
        assert!(ring.try_claim(11));
        assert!(!ring.try_claim(11));
        // Block 0 is a legal key.
        assert!(ring.try_claim(0));
        assert!(!ring.try_claim(0));
    }

    #[test]
    fn claim_ring_rejects_keys_a_full_window_behind() {
        let ring = ClaimRing::default();
        assert!(ring.try_claim(100));
        assert!(ring.try_claim(100 + CLAIM_WINDOW as u64));
        // Same slot, but the ring has moved on: the old key is stale.
        assert!(!ring.try_claim(100));
        assert!(ring.is_claimed(100));
    }

    #[test]
    fn claim_ring_is_exact_under_concurrency() {
        use std::sync::Arc;
        let ring = Arc::new(ClaimRing::default());
        let wins: u64 = (0..4)
            .map(|_| {
                let ring = ring.clone();
                std::thread::spawn(move || (0..2_000u64).filter(|k| ring.try_claim(*k)).count() as u64)
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .sum();
        assert_eq!(wins, 2_000, "every key claimed exactly once across all racers");
    }

    #[test]
    fn win_ledger_reports_lateness_only_for_the_right_key() {
        let ledger = WinLedger::default();
        assert_eq!(ledger.lateness_us(5), None);
        ledger.record_win(5);
        std::thread::sleep(Duration::from_millis(3));
        let late = ledger.lateness_us(5).expect("recorded");
        assert!((2_000..1_000_000).contains(&late), "{late}");
        // Another key mapping to the same slot is not mistaken for 5.
        assert_eq!(ledger.lateness_us(5 + CLAIM_WINDOW as u64), None);
    }
}
