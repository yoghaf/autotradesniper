use crate::config::AppConfig;
use crate::dispatcher::{BroadcasterPool, Dispatcher, StageOutcome};
use crate::payload::{self, CalldataPayloadBuilder};
use crate::types::{PrysmPattern, TradeSignal};
use alloy::primitives::{address, Address, B256, U256};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, RwLock};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

/// Image URL substring characteristic of Prysm Series operator
pub const PRYSMCLOUD_SUBSTRING: &str = "prysmcloud/token-images";

/// Prefix of every `TradeSignal::strategy_tag` emitted by this engine ("Prysm: Pola A ...").
pub const PRYSM_STRATEGY_TAG_PREFIX: &str = "Prysm";

/// Single source of truth for "is this a Prysm signal/position" (main, dispatcher, risk).
#[inline]
pub fn is_prysm_tag(tag: Option<&str>) -> bool {
    tag.is_some_and(|t| t.starts_with(PRYSM_STRATEGY_TAG_PREFIX))
}

/// A token is entered at most once within this window (Pola C fires per companion buy).
const ENTRY_DEDUP_WINDOW: Duration = Duration::from_secs(600);

/// Entry de-duplication key. While the launched token's address is unresolved (the parser
/// still reports the launchpad as the token), keying by address would collapse every launch
/// on that launchpad into one, so the triggering tx hash is used instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EntryKey {
    Token(Address),
    Tx(B256),
}

impl EntryKey {
    /// Keyed by the launch tx whenever it is known, so Pola A/B (keyed while the token is
    /// still unresolved) and a later Pola C on the resolved curve share one entry slot.
    fn for_match(meta: &LaunchMetadata, tx_hash: B256) -> Self {
        if meta.launch_tx_hash != B256::ZERO {
            EntryKey::Tx(meta.launch_tx_hash)
        } else if meta.token_address != meta.pool_address {
            EntryKey::Token(meta.token_address)
        } else {
            EntryKey::Tx(tx_hash)
        }
    }
}

/// Token and curve of a launch, read from its receipt (`T_LAUNCH` event).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchResolution {
    Resolved {
        token: Address,
        curve: Address,
        /// Live `getReserves()` right after resolution (feeds the Revert Guard quote).
        reserves: Option<(U256, U256)>,
    },
    /// The launch reverted or emitted no launch event from an allowlisted launchpad.
    Failed,
}

/// Receipt poll cadence of the background resolver. The feed is ahead of every RPC index, so
/// the receipt of block L typically appears tens to hundreds of ms after L is seen.
///
/// The cadence is a fixed-rate [`tokio::time::interval`] (no drift from the request round trip) and a
/// newly observed block cuts the wait short: see [`PrysmEngine::spawn_resolver`].
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(60);
/// Launch records (metadata, resolutions) are forgotten after this long.
const LAUNCH_RECORD_TTL: Duration = Duration::from_secs(120);
/// How often, and how long at most (retries x step = 100 ms), the primary staging waits for the
/// carrier batch of its launch to be registered when the resolution overtook the arming task.
const STAGE_ARM_RETRIES: u32 = 20;
const STAGE_ARM_RETRY_STEP: Duration = Duration::from_millis(5);

/// Read the launch outcome from a receipt. Only `T_LAUNCH` logs emitted by an allowlisted
/// launchpad count; a reverted launch (status 0) resolves to `Failed`.
pub fn parse_launch_receipt(receipt: &serde_json::Value, launchpads: &HashSet<Address>) -> LaunchResolution {
    let succeeded = receipt
        .get("status")
        .and_then(|s| s.as_str())
        .is_some_and(|s| s.eq_ignore_ascii_case("0x1"));
    if !succeeded {
        return LaunchResolution::Failed;
    }
    let logs = receipt.get("logs").and_then(|l| l.as_array()).map(Vec::as_slice).unwrap_or_default();
    for log in logs {
        let emitter = log.get("address").and_then(|a| a.as_str()).and_then(|a| a.parse::<Address>().ok());
        if !emitter.is_some_and(|a| launchpads.contains(&a)) {
            continue;
        }
        let Some(topics) = log.get("topics").and_then(|t| t.as_array()) else { continue };
        let topic = |i: usize| topics.get(i).and_then(|t| t.as_str());
        if !topic(0).is_some_and(|t0| t0.eq_ignore_ascii_case(payload::PONS_LAUNCH_TOPIC_HEX)) {
            continue;
        }
        if let (Some(token), Some(curve)) = (
            topic(1).and_then(parse_address_from_topic),
            topic(2).and_then(parse_address_from_topic),
        ) {
            return LaunchResolution::Resolved { token, curve, reserves: None };
        }
    }
    LaunchResolution::Failed
}

/// Known Companion Accounts triggering Pola C trade signals
pub const COMPANION_ACCOUNTS: [Address; 2] = [
    address!("e89c3d903e84aab1c6327c794ea8e749b60a347c"),
    address!("791ef6e64a3fb267502ceb950b57a81671974065"),
];

/// Pre-compiled zero-allocation regular expression for `.lat` top-level domain websites
static LAT_DOMAIN_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^https?://[a-z0-9-]+\.lat/?$").expect("Valid static regex for .lat domains")
});

/// Extracted token launch metadata from raw calldata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchMetadata {
    pub token_address: Address,
    pub pool_address: Address,
    pub creator_address: Address,
    pub image_url: Option<String>,
    pub website_url: Option<String>,
    pub dev_buy_eth: f64,
    pub creator_tax_bps: u32,
    pub tax_exempt_wallets: Vec<Address>,
    pub launch_block: u64,
    #[serde(skip, default = "Instant::now")]
    pub launch_instant: Instant,
    pub market_cap_usd: f64,
    /// Launch transaction (zero when unknown).
    #[serde(default)]
    pub launch_tx_hash: B256,
}

/// Detailed match report for Prysm Series Strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrysmMatchReport {
    pub token_address: Address,
    pub pattern: PrysmPattern,
    pub reason: String,
    pub dev_buy_eth: f64,
    pub creator_tax_bps: u32,
    pub market_cap_usd: f64,
    pub launch_block: u64,
    pub detected_block: u64,
    pub latency_micros: u128,
    pub guardrails_passed: bool,
    pub rejection_reason: Option<String>,
}

/// Entry order waiting until it is both due (`scheduled_block` observed) and resolved
#[derive(Debug, Clone)]
pub struct PendingPrysmOrder {
    pub report: PrysmMatchReport,
    pub token_address: Address,
    pub pool_address: Address,
    pub launch_tx_hash: B256,
    /// Fire when this block is observed; the tx then lands in the next block.
    pub scheduled_block: u64,
    /// Launch time: staleness is measured from here.
    pub created_at: Instant,
    pub size_eth: f64,
    pub dev_buy_eth: f64,
    /// Creator (Pola A/B) or companion (Pola C) whose tx triggered the entry.
    pub trigger_account: Address,
}

enum Release {
    Ready(PendingPrysmOrder, Address, Address, Option<(U256, U256)>),
    Reject(PendingPrysmOrder, String),
}

/// In-memory statistics & match counter for Prysm Series Strategy
#[derive(Debug, Default)]
pub struct PrysmStats {
    pub total_evaluated: AtomicU64,
    pub pattern_a_matches: AtomicU64,
    pub pattern_b_matches: AtomicU64,
    pub pattern_c_matches: AtomicU64,
    pub total_executed: AtomicU64,
    pub rejected_staleness: AtomicU64,
    pub rejected_mcap_ceiling: AtomicU64,
    pub recent_matches: RwLock<VecDeque<PrysmMatchReport>>,
}

impl PrysmStats {
    pub fn new() -> Self {
        Self {
            total_evaluated: AtomicU64::new(0),
            pattern_a_matches: AtomicU64::new(0),
            pattern_b_matches: AtomicU64::new(0),
            pattern_c_matches: AtomicU64::new(0),
            total_executed: AtomicU64::new(0),
            rejected_staleness: AtomicU64::new(0),
            rejected_mcap_ceiling: AtomicU64::new(0),
            recent_matches: RwLock::new(VecDeque::with_capacity(10)),
        }
    }

    pub async fn record_match(&self, report: PrysmMatchReport) {
        match report.pattern {
            PrysmPattern::PatternA => {
                self.pattern_a_matches.fetch_add(1, Ordering::SeqCst);
            }
            PrysmPattern::PatternB => {
                self.pattern_b_matches.fetch_add(1, Ordering::SeqCst);
            }
            PrysmPattern::PatternC => {
                self.pattern_c_matches.fetch_add(1, Ordering::SeqCst);
            }
        }

        if report.guardrails_passed {
            self.total_executed.fetch_add(1, Ordering::SeqCst);
        } else if let Some(ref r) = report.rejection_reason {
            if r.contains("Staleness") {
                self.rejected_staleness.fetch_add(1, Ordering::SeqCst);
            } else if r.contains("Market Cap") {
                self.rejected_mcap_ceiling.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mut recents = self.recent_matches.write().await;
        if recents.len() >= 10 {
            recents.pop_front();
        }
        recents.push_back(report);
    }
}

/// Helper function to parse 20-byte Address from a 32-byte log topic hex string
///
/// The string comes from an RPC response: slicing a non-ASCII string at a byte offset used to
/// panic (not a char boundary) inside the feed task.
pub fn parse_address_from_topic(topic_str: &str) -> Option<Address> {
    let clean = topic_str.strip_prefix("0x").unwrap_or(topic_str);
    if !clean.is_ascii() || clean.len() < 40 {
        return None;
    }
    std::str::FromStr::from_str(&clean[clean.len() - 40..]).ok()
}

/// Autonomous Heuristic Pattern Engine: Prysm Series Strategy
pub struct PrysmEngine {
    inner: Arc<Inner>,
}

/// Shared with the background receipt resolvers. Every lock here is a sync mutex that is never
/// held across an `.await`; lock order is `pending_orders` before `resolutions`.
struct Inner {
    config: AppConfig,
    stats: Arc<PrysmStats>,
    /// Resolved launches keyed by curve and by token (Pola C looks up the companion's `to`).
    recent_launches: Mutex<HashMap<Address, LaunchMetadata>>,
    /// Every marker launch on an allowlisted launchpad, by launch tx, until it ages out.
    launches_by_tx: Mutex<HashMap<B256, LaunchMetadata>>,
    resolutions: Mutex<HashMap<B256, (LaunchResolution, Instant)>>,
    pending_orders: Mutex<Vec<PendingPrysmOrder>>,
    signal_tx: mpsc::Sender<TradeSignal>,
    /// Entries already queued/dispatched.
    entered_tokens: Mutex<HashMap<EntryKey, Instant>>,
    warned_no_launchpads: AtomicBool,
    broadcaster: OnceLock<Arc<BroadcasterPool>>,
    master_address: std::sync::RwLock<Option<Address>>,
    /// Highest block observed (feed or HTTP).
    last_block: AtomicU64,
    /// Fires once per *new* highest block. Receipt resolvers wait on it next to their poll tick, so a
    /// sealed block wakes them immediately instead of at the next `sleep` expiry.
    block_tx: watch::Sender<u64>,
    /// `pending_orders.len()`, kept under the same lock: lets the per-message block hook return
    /// without touching a lock or awaiting a config `RwLock` while nothing is queued.
    pending_count: AtomicUsize,
    /// Receipt poll period in ms (see [`RECEIPT_POLL_INTERVAL`]).
    poll_ms: AtomicU64,
    /// AOT pre-signing pipeline (optional): staged transactions for orders that still wait.
    presigner: OnceLock<Arc<Dispatcher>>,
    /// One reserve refresh at a time.
    refreshing: AtomicBool,
    /// In-memory curve replica: told each resolved launch's block (snipe-tax schedule).
    mirror: OnceLock<Arc<crate::risk::curve_mirror::CurveMirror>>,
}

fn locked<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl PrysmEngine {
    pub fn new(config: AppConfig, signal_tx: mpsc::Sender<TradeSignal>) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                stats: Arc::new(PrysmStats::new()),
                recent_launches: Mutex::new(HashMap::new()),
                launches_by_tx: Mutex::new(HashMap::new()),
                resolutions: Mutex::new(HashMap::new()),
                pending_orders: Mutex::new(Vec::new()),
                signal_tx,
                entered_tokens: Mutex::new(HashMap::new()),
                warned_no_launchpads: AtomicBool::new(false),
                broadcaster: OnceLock::new(),
                master_address: std::sync::RwLock::new(None),
                last_block: AtomicU64::new(0),
                block_tx: watch::channel(0).0,
                pending_count: AtomicUsize::new(0),
                poll_ms: AtomicU64::new(RECEIPT_POLL_INTERVAL.as_millis() as u64),
                presigner: OnceLock::new(),
                refreshing: AtomicBool::new(false),
                mirror: OnceLock::new(),
            }),
        }
    }

    /// RPC pool used to resolve launch receipts (and read curve reserves) in the background.
    pub fn with_broadcaster(self, broadcaster: Arc<BroadcasterPool>) -> Self {
        let _ = self.inner.broadcaster.set(broadcaster);
        self
    }

    /// Enable the AOT pre-signing pipeline: entries that have to wait (for the launch receipt or for
    /// their target block) get their transactions signed in the background, and the target block
    /// only broadcasts stored bytes. Without it the engine behaves as before.
    pub fn with_presigner(self, dispatcher: Arc<Dispatcher>) -> Self {
        let _ = self.inner.presigner.set(dispatcher);
        self
    }

    /// Tell the in-memory curve replica when each resolved curve launched, so the launch-window
    /// buy tax of its first seconds is replayed exactly.
    pub fn with_curve_mirror(self, mirror: Arc<crate::risk::curve_mirror::CurveMirror>) -> Self {
        let _ = self.inner.mirror.set(mirror);
        self
    }

    /// Override the receipt poll period (default 60 ms). Mostly for tests.
    pub fn with_receipt_poll_interval(self, period: Duration) -> Self {
        self.inner.poll_ms.store(period.as_millis().clamp(1, u64::MAX as u128) as u64, Ordering::Relaxed);
        self
    }

    pub async fn set_master_address(&self, addr: Address) {
        *self.inner.master_address.write().unwrap_or_else(|e| e.into_inner()) = Some(addr);
    }

    /// Claim the single entry slot for `key`; false if it was claimed within the window.
    fn try_claim_entry(&self, key: EntryKey, now: Instant) -> bool {
        let mut entered = locked(&self.inner.entered_tokens);
        entered.retain(|_, at| now.saturating_duration_since(*at) < ENTRY_DEDUP_WINDOW);
        match entered.entry(key) {
            std::collections::hash_map::Entry::Occupied(_) => false,
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(now);
                true
            }
        }
    }

    pub fn stats(&self) -> Arc<PrysmStats> {
        self.inner.stats.clone()
    }

    /// Evaluator Pola A: Host Gambar Penyimpanan Prysmcloud
    /// - Mengandung string `prysmcloud/token-images`
    /// - Dev Buy: 0.005 ETH - 0.06 ETH
    /// - Creator Tax: <= 300 bps (3%)
    /// - Tax-Exempt Wallets: <= 1 wallet selain developer
    pub fn evaluate_pattern_a(meta: &LaunchMetadata) -> bool {
        // 1. Image URL substring check
        let has_image_match = meta
            .image_url
            .as_ref()
            .map(|url| url.contains(PRYSMCLOUD_SUBSTRING))
            .unwrap_or(false);

        if !has_image_match {
            return false;
        }

        // 2. Dev Buy Amount check: 0.005 ETH <= amount <= 0.06 ETH
        if meta.dev_buy_eth < 0.005 || meta.dev_buy_eth > 0.06 {
            return false;
        }

        // 3. Creator Tax check: <= 300 bps (3%)
        if meta.creator_tax_bps > 300 {
            return false;
        }

        // 4. Tax-Exempt Wallets check: <= 1 beyond developer
        let non_creator_exempt_count = meta
            .tax_exempt_wallets
            .iter()
            .filter(|&&w| w != meta.creator_address)
            .count();

        if non_creator_exempt_count > 1 {
            return false;
        }

        true
    }

    /// Evaluator Pola B: Domain Ekstensi `.lat` & Nilai Dev Buy Khas
    /// - Website regex match: `^https?://[a-z0-9-]+\.lat/?$`
    /// - Dev Buy spesifik: 0.019 ETH - 0.024 ETH (rata-rata ~0.021 ETH)
    /// - Creator Tax: <= 300 bps (3%)
    /// - Tax-Exempt Wallets: <= 1 wallet selain developer
    pub fn evaluate_pattern_b(meta: &LaunchMetadata) -> bool {
        // 1. Website regex match
        let has_website_match = meta
            .website_url
            .as_ref()
            .map(|url| LAT_DOMAIN_REGEX.is_match(url))
            .unwrap_or(false);

        if !has_website_match {
            return false;
        }

        // 2. Dev Buy Amount check: strict 0.019 ETH <= amount <= 0.024 ETH
        if meta.dev_buy_eth < 0.019 || meta.dev_buy_eth > 0.024 {
            return false;
        }

        // 3. Creator Tax check: <= 300 bps (3%)
        if meta.creator_tax_bps > 300 {
            return false;
        }

        // 4. Tax-Exempt Wallets check: <= 1 beyond developer
        let non_creator_exempt_count = meta
            .tax_exempt_wallets
            .iter()
            .filter(|&&w| w != meta.creator_address)
            .count();

        if non_creator_exempt_count > 1 {
            return false;
        }

        true
    }

    /// Evaluator Pola C: Sinyal Konfirmasi Akun Rekanan (Companion Trade Signal)
    /// - Transaksi beli dari salah satu akun rekanan
    /// - Terjadi dalam rentang waktu <= 60 detik setelah token peluncuran
    ///
    /// `token` is the companion tx's `to`: the curve (a Pons `buy`) or the token itself.
    pub fn evaluate_pattern_c(
        buyer: Address,
        token: Address,
        launch_meta: Option<&LaunchMetadata>,
        now: Instant,
    ) -> bool {
        // 1. Check if buyer is one of companion accounts
        if !COMPANION_ACCOUNTS.contains(&buyer) {
            return false;
        }

        // 2. Check timing against launch instant (<= 60s, sub-second precision)
        if let Some(meta) = launch_meta {
            if (meta.token_address == token || meta.pool_address == token)
                && now.saturating_duration_since(meta.launch_instant) <= Duration::from_secs(60)
            {
                return true;
            }
        }

        false
    }

    /// Validate Entry Guardrails
    /// Returns Ok(()) if all guardrails pass, or Err(rejection_reason)
    ///
    /// `prysm_min_block_delay` is the block the entry should *land* in, relative to the launch
    /// block L. A tx sent after observing block `current` lands in `current + 1` at the
    /// earliest, so "Block +1" means firing as soon as L itself is observed.
    pub async fn validate_entry_guardrails(
        &self,
        meta: &LaunchMetadata,
        current_block: u64,
        now: Instant,
    ) -> Result<(), String> {
        let config = &self.inner.config;
        let max_age_secs = *config.prysm_max_signal_age_secs.read().await;
        let max_mcap_usd = *config.prysm_max_mcap_usd.read().await;

        // 1. Signal Staleness check: <= 15s. Compare Durations: `as_secs()` truncation would
        //    let a 15.9s-old signal through a 15s limit.
        let age = now.saturating_duration_since(meta.launch_instant);
        if age > Duration::from_secs(max_age_secs) {
            return Err(format!(
                "Signal Staleness Exceeded: {:.1}s > {}s limit",
                age.as_secs_f64(), max_age_secs
            ));
        }

        // 2. Market Cap Ceiling Protection: <= $8,000 USD
        if meta.market_cap_usd > max_mcap_usd {
            return Err(format!(
                "Market Cap Ceiling Exceeded: ${:.2} > ${:.2} limit",
                meta.market_cap_usd, max_mcap_usd
            ));
        }

        // 3. Min Block Guard: the earliest landing block must be at least L + delay.
        let min_delay = *config.prysm_min_block_delay.read().await;
        let required_block = meta.launch_block.saturating_add(min_delay.max(1));
        let landing_block = current_block.saturating_add(1);
        if landing_block < required_block {
            return Err(format!(
                "Awaiting Min Block Guard: Landing Block #{} < Required Block #{}",
                landing_block, required_block
            ));
        }

        Ok(())
    }

    /// High-performance calldata parser to extract LaunchMetadata from raw transaction bytes
    pub fn parse_launch_calldata(
        from: Address,
        to: Address,
        value_wei: U256,
        input: &[u8],
        block_number: u64,
        simulated_mcap_usd: f64,
    ) -> Option<LaunchMetadata> {
        if input.is_empty() {
            return None;
        }

        // Byte-level prefilter: almost no calldata matches, and `from_utf8_lossy` allocates a
        // full copy for any non-UTF-8 input (i.e. nearly every transaction).
        if memchr::memmem::find(input, PRYSMCLOUD_SUBSTRING.as_bytes()).is_none()
            && memchr::memmem::find(input, b".lat").is_none()
        {
            return None;
        }

        let input_lossy = String::from_utf8_lossy(input);

        // Extract Image URL: look for prysmcloud or http/ipfs image link
        let image_url = input_lossy.find(PRYSMCLOUD_SUBSTRING).map(|idx| {
            let start = input_lossy[..idx].rfind("http").unwrap_or(idx);
            let end = input_lossy[idx..]
                .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == '\0' || c == '}')
                .map(|e| idx + e)
                .unwrap_or(input_lossy.len());
            input_lossy[start..end].trim().to_string()
        });

        // Extract Website URL: look for http/https with .lat
        let website_url = input_lossy.find(".lat").map(|idx| {
            let start = input_lossy[..idx].rfind("http").unwrap_or(idx);
            let end = input_lossy[idx..]
                .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == '\0' || c == '}')
                .map(|e| idx + e)
                .unwrap_or(input_lossy.len());
            input_lossy[start..end].trim().to_string()
        });

        // If neither image match nor website match exists, this isn't a Prysm candidate
        if image_url.is_none() && website_url.is_none() {
            return None;
        }

        // Dev Buy in ETH from the launch tx value. There is deliberately no fallback: the former
        // hard-coded 0.021 ETH default made every zero-value tx pass the Pola A/B dev-buy check.
        let dev_buy_eth = f64::from(value_wei) / 1e18;

        // Creator Tax (bps): Default to 200 bps (2%) if standard launch, max allowed is 300 bps
        let creator_tax_bps = payload::PONS_ASSUMED_CREATOR_TAX_BPS;

        // Tax-Exempt Wallets:
        let tax_exempt_wallets = vec![from]; // only developer

        // Token and curve are unknown until the launch receipt resolves them; until then both
        // name the launchpad (never a valid buy target, see `Dispatcher::dispatch_signal`).
        let token_address = to;
        let pool_address = to;

        Some(LaunchMetadata {
            token_address,
            pool_address,
            creator_address: from,
            image_url,
            website_url,
            dev_buy_eth,
            creator_tax_bps,
            tax_exempt_wallets,
            launch_block: block_number,
            launch_instant: Instant::now(),
            market_cap_usd: simulated_mcap_usd,
            launch_tx_hash: B256::ZERO,
        })
    }

    /// Primary inspection pipeline called by StreamListener for each incoming transaction
    #[allow(clippy::too_many_arguments)]
    pub async fn inspect_transaction(
        &self,
        from: Address,
        to: Option<Address>,
        value: U256,
        input: &[u8],
        tx_hash: B256,
        block_number: u64,
        start_time: Instant,
    ) {
        let config = &self.inner.config;
        if !config.is_prysm_enabled() {
            return;
        }

        self.inner.stats.total_evaluated.fetch_add(1, Ordering::Relaxed);
        let to_addr = to.unwrap_or_default();
        let now = Instant::now();

        // 1. Pola C: a companion *buy* (a sell must never trigger our entry) on the curve or
        //    token of a launch resolved within the window.
        if COMPANION_ACCOUNTS.contains(&from) && crate::stream::fast_selectors::is_copyable_eth_swap(input) {
            let launch = locked(&self.inner.recent_launches).get(&to_addr).cloned();
            if let Some(meta) = launch
                && Self::evaluate_pattern_c(from, to_addr, Some(&meta), now)
            {
                let elapsed = start_time.elapsed().as_micros();
                info!(
                    "⚡ [Prysm Pola C Matched in {} µs] Companion Account {:?} bought {:?} within 60s of launch!",
                    elapsed, from, to_addr
                );
                let report = PrysmMatchReport {
                    token_address: meta.token_address,
                    pattern: PrysmPattern::PatternC,
                    reason: format!("Companion Account {:?} buy confirmed within 60s", from),
                    dev_buy_eth: meta.dev_buy_eth,
                    creator_tax_bps: meta.creator_tax_bps,
                    market_cap_usd: meta.market_cap_usd,
                    launch_block: meta.launch_block,
                    detected_block: block_number,
                    latency_micros: elapsed,
                    guardrails_passed: false,
                    rejection_reason: None,
                };
                self.process_matched_pattern(report, meta, block_number, tx_hash, from).await;
                return;
            }
        }

        // 2. Check for Token Launch creation calldata (Pola A & Pola B)
        // Only calls into an allowlisted launchpad factory count as launches. Without this, any
        // contract (or EOA) receiving a tx whose calldata contains the marker strings would be
        // treated as a "token" and receive our entry ETH.
        if !config.prysm_launchpads.contains(&to_addr) {
            if config.prysm_launchpads.is_empty()
                && !self.inner.warned_no_launchpads.swap(true, Ordering::Relaxed)
            {
                warn!("Prysm Pola A/B disabled: PRYSM_LAUNCHPAD_FACTORIES is empty (fail-closed)");
            }
            return;
        }

        // Placeholder until the curve resolves; the real market cap is checked at release.
        let simulated_mcap = 5000.0;
        let Some(mut meta) = Self::parse_launch_calldata(from, to_addr, value, input, block_number, simulated_mcap) else {
            return;
        };
        meta.launch_tx_hash = tx_hash;
        // Resolve token + curve right away, in the background, whatever the A/B verdict:
        // Pola C may still confirm a launch that A/B rejected.
        let first_sighting = locked(&self.inner.launches_by_tx).insert(tx_hash, meta.clone()).is_none();
        if first_sighting {
            self.spawn_resolver(tx_hash);
        }

        let (pattern, match_reason) = if Self::evaluate_pattern_a(&meta) {
            (
                PrysmPattern::PatternA,
                format!(
                    "Pola A Matched: Prysmcloud storage ({}), Dev Buy {:.4} ETH, Tax {} bps",
                    meta.image_url.as_deref().unwrap_or(""),
                    meta.dev_buy_eth,
                    meta.creator_tax_bps
                ),
            )
        } else if Self::evaluate_pattern_b(&meta) {
            (
                PrysmPattern::PatternB,
                format!(
                    "Pola B Matched: .lat domain ({}), Dev Buy {:.4} ETH, Tax {} bps",
                    meta.website_url.as_deref().unwrap_or(""),
                    meta.dev_buy_eth,
                    meta.creator_tax_bps
                ),
            )
        } else {
            return;
        };

        let elapsed = start_time.elapsed().as_micros();
        info!("⚡ [Prysm {} Matched in {} µs] {}", pattern, elapsed, match_reason);
        let report = PrysmMatchReport {
            token_address: meta.token_address,
            pattern,
            reason: match_reason,
            dev_buy_eth: meta.dev_buy_eth,
            creator_tax_bps: meta.creator_tax_bps,
            market_cap_usd: meta.market_cap_usd,
            launch_block: meta.launch_block,
            detected_block: block_number,
            latency_micros: elapsed,
            guardrails_passed: false,
            rejection_reason: None,
        };
        self.process_matched_pattern(report, meta, block_number, tx_hash, from).await;
    }

    /// Poll the launch receipt off the feed path, racing every endpoint.
    ///
    /// The former code fetched the receipt *inside* `process_new_block`, i.e. on the feed
    /// reader: each attempt stalled feed ingestion for an RPC round trip (2.5 s per endpoint on
    /// a dead one), and an RPC error dropped the order.
    ///
    /// Scheduling: the wait between two polls is `select!(interval tick, newer block)`.
    /// - The tick is a fixed-rate interval, so the cadence no longer drifts by the round trip of
    ///   every request (the former `sleep(60 ms)` after each poll made the real period 60 ms + RTT)
    ///   and a slow answer is followed by an immediate poll instead of another full sleep.
    /// - A newly observed block wakes the loop at once: once block L+1 is sealed the node that
    ///   served L has almost certainly indexed L's receipt, so waiting out the rest of the tick
    ///   would only add jitter.
    fn spawn_resolver(&self, launch_tx: B256) {
        let Some(reader) = self.inner.broadcaster.get().cloned() else { return };
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let budget = Duration::from_secs(*inner.config.prysm_max_signal_age_secs.read().await);
            let deadline = Instant::now() + budget;
            let mut blocks = inner.block_tx.subscribe();
            let mut tick = tokio::time::interval(Duration::from_millis(inner.poll_ms.load(Ordering::Relaxed).max(1)));
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tick.tick().await; // the first tick is immediate: the first poll below is the "poll now"
            loop {
                if let Ok(Some(receipt)) = reader.get_transaction_receipt(launch_tx).await {
                    let mut resolution = parse_launch_receipt(&receipt, &inner.config.prysm_launchpads);
                    if let LaunchResolution::Resolved { curve, ref mut reserves, .. } = resolution {
                        let call = CalldataPayloadBuilder::build_pons_get_reserves_payload();
                        *reserves = tokio::time::timeout(Duration::from_secs(1), reader.eth_call(curve, &call))
                            .await
                            .ok()
                            .and_then(|r| r.ok())
                            .and_then(|raw| CalldataPayloadBuilder::decode_get_reserves_output(&raw));
                    }
                    Inner::record_resolution(&inner, launch_tx, resolution).await;
                    return;
                }
                if Instant::now() >= deadline {
                    warn!("[Prysm] Launch {:?} unresolved after {:?}; its orders will expire", launch_tx, budget);
                    inner.cancel_staged(launch_tx);
                    return;
                }
                tokio::select! {
                    _ = tick.tick() => {}
                    // `Err` = the sender is gone, which cannot happen while `inner` is alive.
                    _ = blocks.changed() => {}
                }
            }
        });
    }

    /// Record a launch's token/curve (normally done by the background resolver) and fire the
    /// orders that were only waiting for it.
    pub async fn record_resolution(&self, launch_tx: B256, resolution: LaunchResolution) {
        Inner::record_resolution(&self.inner, launch_tx, resolution).await;
    }

    /// Queue or fire a verified match.
    async fn process_matched_pattern(
        &self,
        report: PrysmMatchReport,
        meta: LaunchMetadata,
        current_block: u64,
        tx_hash: B256,
        trigger_account: Address,
    ) {
        let config = &self.inner.config;
        let now = Instant::now();

        // One entry per launch: Pola C fires again for every companion buy, and the same
        // launch can be seen twice (feed + HTTP bridge).
        if !self.try_claim_entry(EntryKey::for_match(&meta, tx_hash), now) {
            // A companion buy confirms a launch whose A/B entry is still waiting for its block:
            // fire that entry now instead of entering twice.
            if report.pattern == PrysmPattern::PatternC && self.inner.expedite(meta.launch_tx_hash, current_block) {
                info!("⚡ [Prysm Pola C] Companion confirmation: firing queued entry for launch {:?} now", meta.launch_tx_hash);
                self.inner.observe_block(current_block);
                Inner::release_due(&self.inner, current_block, Some(meta.launch_tx_hash)).await;
            } else {
                info!("[Prysm] Launch {:?} already queued/entered, skipping duplicate match", meta.launch_tx_hash);
            }
            return;
        }

        // Landing offset: Pola C follows the companion into the next block; Pola A/B land in
        // L + delay (at least L + 1, block L is already sealed when we see it).
        let landing_offset = if report.pattern == PrysmPattern::PatternC {
            1
        } else {
            (*config.prysm_min_block_delay.read().await).max(1)
        };
        let order = PendingPrysmOrder {
            report,
            token_address: meta.token_address,
            pool_address: meta.pool_address,
            launch_tx_hash: if meta.launch_tx_hash != B256::ZERO { meta.launch_tx_hash } else { tx_hash },
            scheduled_block: meta.launch_block.saturating_add(landing_offset - 1),
            created_at: meta.launch_instant,
            size_eth: *config.prysm_size_eth.read().await,
            dev_buy_eth: meta.dev_buy_eth,
            trigger_account,
        };
        self.inner.observe_block(current_block);
        info!(
            "⏳ [Prysm] Order armed: fire at Block #{} (lands #{}), current #{}, launch {:?}",
            order.scheduled_block, order.scheduled_block + 1, current_block, order.launch_tx_hash
        );
        // AOT: an order that cannot fire right now (receipt or target block outstanding) gets its
        // carriers signed in the background while it waits. One that can (Pola C on a resolved
        // curve, the 0-delay profile) is not staged: signing it ahead would only compete for CPU
        // with the dispatch that follows immediately.
        if self.inner.presigner.get().is_some() && self.inner.must_wait(&order, current_block) {
            let max_age = Duration::from_secs(*config.prysm_max_signal_age_secs.read().await);
            Inner::spawn_arm(&self.inner, order.launch_tx_hash, order.created_at + max_age);
        }
        {
            let mut pending = locked(&self.inner.pending_orders);
            pending.push(order);
            self.inner.pending_count.store(pending.len(), Ordering::Release);
        }
        Inner::release_due(&self.inner, current_block, None).await;
    }

    /// On each new block, fire matured orders whose launch is resolved (no I/O on this path).
    ///
    /// Called for every L2 message of the feed, so everything here is event driven and lock-light:
    /// no timer, no await while nothing is queued, and the once-per-block work (staged-packet
    /// re-validation, quote refresh, record ageing) only runs when the block number really advanced.
    pub async fn process_new_block(&self, current_block: u64) {
        if !self.inner.config.is_prysm_enabled() {
            // Switched off while orders are staged: their leased nonces must not stay held.
            if let Some(dispatcher) = self.inner.presigner.get()
                && dispatcher.has_staged()
            {
                dispatcher.cancel_all_presigned();
            }
            return;
        }
        let advanced = self.inner.observe_block(current_block);
        Inner::release_due(&self.inner, current_block, None).await;
        if advanced {
            Inner::on_block_advanced(&self.inner);
            self.inner.forget_old_launches();
        }
    }

    /// Cancel everything pending for a launch: its queued orders are dropped and its staged
    /// (pre-signed) packets are torn down, handing every leased nonce back. The entry slot of the
    /// launch stays claimed, so the same launch is not re-entered. True if anything was cancelled.
    ///
    /// Safe against a concurrent dispatch: if the batch was already claimed by the dispatcher the
    /// transactions are on their way and are not touched (exactly one of the two wins).
    pub async fn cancel_launch(&self, launch_tx: B256) -> bool {
        let dropped = {
            let mut pending = locked(&self.inner.pending_orders);
            let before = pending.len();
            pending.retain(|o| o.launch_tx_hash != launch_tx);
            self.inner.pending_count.store(pending.len(), Ordering::Release);
            before != pending.len()
        };
        let torn_down = self.inner.cancel_staged(launch_tx);
        if dropped || torn_down {
            info!("[Prysm] Launch {:?} cancelled (orders dropped: {}, staged packets released: {})", launch_tx, dropped, torn_down);
        }
        dropped || torn_down
    }

    /// Circuit breaker: drop every queued order and tear down every staged batch (leased nonces go
    /// back). Entry slots stay claimed, so none of these launches is re-entered. Returns how many
    /// orders were dropped.
    pub fn cancel_all_pending(&self) -> usize {
        let dropped = {
            let mut pending = locked(&self.inner.pending_orders);
            let n = pending.len();
            pending.clear();
            self.inner.pending_count.store(0, Ordering::Release);
            n
        };
        let released = self.inner.presigner.get().map_or(0, |d| d.cancel_all_presigned());
        if dropped + released > 0 {
            warn!("[Prysm] Circuit breaker: {} queued order(s) dropped, {} staged batch(es) released", dropped, released);
        }
        dropped
    }

    /// Orders waiting for their block or their launch receipt.
    pub fn pending_count(&self) -> usize {
        self.inner.pending_count.load(Ordering::Acquire)
    }

    /// Feed a fresher `getReserves()` reading for a resolved launch (from a refresher or a curve
    /// mirror). The next entry is quoted on it, and a staged primary whose Revert Guard moved beyond
    /// the pre-signing tolerance is signed again right away, ahead of the target block.
    pub fn update_reserves(&self, launch_tx: B256, eth_reserve: U256, token_reserve: U256) -> bool {
        Inner::set_reserves(&self.inner, launch_tx, eth_reserve, token_reserve)
    }
}

/// The numbers an entry is built from: its signal plus what the guardrails need to judge it.
struct EntryQuote {
    signal: TradeSignal,
    min_tokens_out: U256,
    /// Market cap on the reserves used, if they allow one.
    mcap_usd: Option<f64>,
}

impl Inner {
    /// Record `block` as seen. True if it is a new highest block; that wakes every receipt resolver.
    fn observe_block(&self, block: u64) -> bool {
        let prev = self.last_block.fetch_max(block, Ordering::AcqRel);
        if block > prev {
            self.block_tx.send_replace(block);
            true
        } else {
            false
        }
    }

    /// Can `order` not be sent at `current_block` yet: target block still ahead, or launch unresolved?
    fn must_wait(&self, order: &PendingPrysmOrder, current_block: u64) -> bool {
        if order.scheduled_block > current_block {
            return true;
        }
        if order.token_address != order.pool_address {
            return false;
        }
        // A failed launch will be rejected, not waited for.
        !locked(&self.resolutions).contains_key(&order.launch_tx_hash)
    }

    /// Tear down the staged packets of a launch (order rejected, cancelled, launch failed).
    fn cancel_staged(&self, launch_tx: B256) -> bool {
        self.presigner.get().is_some_and(|d| d.cancel_presigned(launch_tx))
    }

    /// Arm the staging batch of `launch_tx` in the background. Never on the feed path.
    fn spawn_arm(inner: &Arc<Inner>, launch_tx: B256, hold_until: Instant) {
        let Some(dispatcher) = inner.presigner.get().cloned() else { return };
        tokio::spawn(async move {
            dispatcher.arm_presign(launch_tx, hold_until).await;
        });
    }

    /// Once per new block: re-validate staged packets against the live fees and, for orders that
    /// are still waiting, refresh the curve reserves their Revert Guard was quoted on.
    fn on_block_advanced(inner: &Arc<Inner>) {
        let Some(dispatcher) = inner.presigner.get().cloned() else { return };
        if dispatcher.has_staged() {
            tokio::spawn(async move {
                dispatcher.revalidate_presigned().await;
            });
        }
        if inner.pending_count.load(Ordering::Acquire) > 0 {
            Inner::refresh_waiting_reserves(inner);
        }
    }

    /// Re-read `getReserves()` of every resolved launch whose order still waits for its block.
    /// A single refresh runs at a time; the feed path only spawns it.
    fn refresh_waiting_reserves(inner: &Arc<Inner>) {
        let Some(reader) = inner.broadcaster.get().cloned() else { return };
        let last = inner.last_block.load(Ordering::Acquire);
        let targets: Vec<(B256, Address)> = {
            let pending = locked(&inner.pending_orders);
            let resolutions = locked(&inner.resolutions);
            pending
                .iter()
                .filter(|o| o.scheduled_block > last)
                .filter_map(|o| match resolutions.get(&o.launch_tx_hash) {
                    Some((LaunchResolution::Resolved { curve, .. }, _)) => Some((o.launch_tx_hash, *curve)),
                    _ => None,
                })
                .collect()
        };
        if targets.is_empty() || inner.refreshing.swap(true, Ordering::AcqRel) {
            return;
        }
        let inner = inner.clone();
        tokio::spawn(async move {
            let call = CalldataPayloadBuilder::build_pons_get_reserves_payload();
            for (launch_tx, curve) in targets {
                let reading = tokio::time::timeout(Duration::from_secs(1), reader.eth_call(curve, &call))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .and_then(|raw| CalldataPayloadBuilder::decode_get_reserves_output(&raw));
                if let Some((eth, token)) = reading {
                    Inner::set_reserves(&inner, launch_tx, eth, token);
                }
            }
            inner.refreshing.store(false, Ordering::Release);
        });
    }

    /// Store fresher reserves for a resolved launch and re-quote its staged primary.
    fn set_reserves(inner: &Arc<Inner>, launch_tx: B256, eth: U256, token: U256) -> bool {
        let found = match locked(&inner.resolutions).get_mut(&launch_tx) {
            Some((LaunchResolution::Resolved { reserves, .. }, _)) => {
                *reserves = Some((eth, token));
                true
            }
            _ => false,
        };
        if found {
            Inner::stage_waiting(inner, launch_tx);
        }
        found
    }

    /// Phase 2 of the AOT pipeline: the launch of a still-waiting order resolved (or its quote moved),
    /// so sign its primary buy now. The staging call is idempotent: it keeps a staged primary whose
    /// Revert Guard is still within tolerance and signs a new one, under the same nonce, otherwise.
    fn stage_waiting(inner: &Arc<Inner>, launch_tx: B256) {
        let Some(dispatcher) = inner.presigner.get().cloned() else { return };
        let last = inner.last_block.load(Ordering::Acquire);
        let job = {
            let pending = locked(&inner.pending_orders);
            let resolutions = locked(&inner.resolutions);
            match (
                pending.iter().find(|o| o.launch_tx_hash == launch_tx && o.scheduled_block > last),
                resolutions.get(&launch_tx).map(|(r, _)| *r),
            ) {
                (Some(order), Some(LaunchResolution::Resolved { token, curve, reserves })) => Some((order.clone(), token, curve, reserves)),
                _ => None,
            }
        };
        let Some((order, token, curve, reserves)) = job else { return };
        let inner = inner.clone();
        tokio::spawn(async move {
            let quote = inner.quote_entry(&order, token, curve, reserves, last).await;
            // An entry the valuation guardrail will refuse is not worth a nonce.
            let max_mcap_usd = *inner.config.prysm_max_mcap_usd.read().await;
            if quote.mcap_usd.is_some_and(|m| m > max_mcap_usd) {
                return;
            }
            // The resolution can overtake the (equally background) arming of the carrier batch when the
            // runtime is busy: the batch is then not registered yet. Wait for it, a few short steps at
            // most and only while the order still waits for its block; this is off the hot path.
            let mut attempts = 0u32;
            let outcome = loop {
                let outcome = dispatcher.stage_primary(&quote.signal).await;
                if matches!(outcome, StageOutcome::NotArmed)
                    && attempts < STAGE_ARM_RETRIES
                    && inner.last_block.load(Ordering::Acquire) < order.scheduled_block
                {
                    attempts += 1;
                    tokio::time::sleep(STAGE_ARM_RETRY_STEP).await;
                    continue;
                }
                break outcome;
            };
            match outcome {
                StageOutcome::Staged { nonce } => {
                    info!("🧾 [Prysm] Primary pre-signed for launch {:?} (nonce {}, minTokensOut {})", launch_tx, nonce, quote.min_tokens_out)
                }
                StageOutcome::Restaged { nonce } => {
                    info!("🧾 [Prysm] Primary re-signed after a quote move for launch {:?} (nonce {})", launch_tx, nonce)
                }
                other => debug!("[Prysm] primary of {:?} not staged: {:?}", launch_tx, other),
            }
        });
    }

    /// Make queued orders of `launch_tx` due at `current_block`; true if any was queued.
    fn expedite(&self, launch_tx: B256, current_block: u64) -> bool {
        let mut found = false;
        for order in locked(&self.pending_orders).iter_mut().filter(|o| o.launch_tx_hash == launch_tx) {
            order.scheduled_block = order.scheduled_block.min(current_block);
            found = true;
        }
        found
    }

    async fn record_resolution(inner: &Arc<Inner>, launch_tx: B256, resolution: LaunchResolution) {
        locked(&inner.resolutions).insert(launch_tx, (resolution, Instant::now()));
        match resolution {
            LaunchResolution::Resolved { token, curve, .. } => {
                info!("🎉 [Prysm Receipt Resolved] Launch {:?}: Token {:?}, Curve {:?}", launch_tx, token, curve);
                let meta = locked(&inner.launches_by_tx).get(&launch_tx).cloned();
                if let Some(mut meta) = meta {
                    if let Some(mirror) = inner.mirror.get() {
                        mirror.note_launch(curve, meta.launch_block);
                    }
                    meta.token_address = token;
                    meta.pool_address = curve;
                    let mut recent = locked(&inner.recent_launches);
                    recent.insert(curve, meta.clone());
                    recent.insert(token, meta);
                }
            }
            LaunchResolution::Failed => {
                warn!("[Prysm] Launch {:?} reverted or emitted no launch event", launch_tx);
                // Nothing will ever be bought: the staged carriers must not hold their nonces.
                inner.cancel_staged(launch_tx);
            }
        }
        // Orders already due fire now instead of waiting for the next block.
        let current = inner.last_block.load(Ordering::Acquire);
        Inner::release_due(inner, current, Some(launch_tx)).await;
        // Orders still waiting for their block get their primary signed while they wait.
        if matches!(resolution, LaunchResolution::Resolved { .. }) {
            Inner::stage_waiting(inner, launch_tx);
        }
    }

    /// Take every due order (optionally only those of `only_launch`) that can be decided now.
    ///
    /// The `Pending -> Dispatched` transition happens here, under the `pending_orders` lock: an order
    /// is removed from the queue in the same critical section in which it is judged, so concurrent
    /// callers (feed blocks, the resolver, a companion confirmation) can never both release it.
    async fn release_due(inner: &Arc<Inner>, current_block: u64, only_launch: Option<B256>) {
        // Nothing queued (the usual case: this runs for every feed message): no lock, no await.
        if inner.pending_count.load(Ordering::Acquire) == 0 {
            return;
        }
        let config = &inner.config;
        let max_age = Duration::from_secs(*config.prysm_max_signal_age_secs.read().await);
        let now = Instant::now();

        let decided: Vec<Release> = {
            let mut pending = locked(&inner.pending_orders);
            if pending.is_empty() {
                return;
            }
            let resolutions = locked(&inner.resolutions);
            let mut decided = Vec::new();
            let mut i = 0;
            while i < pending.len() {
                let order = &pending[i];
                let due = order.scheduled_block <= current_block
                    && only_launch.is_none_or(|h| h == order.launch_tx_hash);
                if !due {
                    i += 1;
                    continue;
                }
                let age = now.saturating_duration_since(order.created_at);
                let outcome = if age > max_age {
                    Some(Err(format!("Signal Staleness Exceeded (queued order, {:.1}s)", age.as_secs_f64())))
                } else if order.token_address != order.pool_address {
                    Some(Ok((order.token_address, order.pool_address, None)))
                } else {
                    match resolutions.get(&order.launch_tx_hash).map(|(r, _)| *r) {
                        Some(LaunchResolution::Resolved { token, curve, reserves }) => Some(Ok((token, curve, reserves))),
                        Some(LaunchResolution::Failed) => Some(Err("Launch reverted / no launch event".to_string())),
                        None => None, // still resolving: keep it queued
                    }
                };
                match outcome {
                    None => i += 1,
                    Some(Ok((token, curve, reserves))) => decided.push(Release::Ready(pending.swap_remove(i), token, curve, reserves)),
                    Some(Err(reason)) => decided.push(Release::Reject(pending.swap_remove(i), reason)),
                }
            }
            inner.pending_count.store(pending.len(), Ordering::Release);
            decided
        };

        for release in decided {
            match release {
                Release::Ready(order, token, curve, reserves) => inner.fire(order, token, curve, reserves, current_block).await,
                Release::Reject(mut order, reason) => {
                    warn!("⚠️ [Prysm Guardrail Rejection] {}: launch {:?}", reason, order.launch_tx_hash);
                    inner.cancel_staged(order.launch_tx_hash);
                    order.report.rejection_reason = Some(reason);
                    inner.stats.record_match(order.report).await;
                }
            }
        }
    }

    /// Build the Pons buy (Revert Guard quoted on the real post-launch reserves) of `order`.
    ///
    /// Shared by the hot path ([`Self::fire`]) and the staging path, so a pre-signed primary and the
    /// signal that later claims it are built by the very same code from the same numbers.
    async fn quote_entry(&self, order: &PendingPrysmOrder, token: Address, curve: Address, reserves: Option<(U256, U256)>, current_block: u64) -> EntryQuote {
        let config = &self.config;
        let size_wei = U256::from((order.size_eth * 1e18) as u128);
        // Live reserves when the resolver got them; otherwise the initial curve plus the dev
        // buy that the launch tx itself made.
        let (eth_reserve, token_reserve) = reserves.unwrap_or_else(|| {
            payload::pons_reserves_after_buy(U256::from((order.dev_buy_eth.max(0.0) * 1e18) as u128))
        });
        let mcap_usd = payload::pons_mcap_eth(eth_reserve, token_reserve).map(|mcap_eth| mcap_eth * payload::eth_usd_reference());

        let slippage = *config.prysm_slippage_percent.read().await;
        // Revert Guard: minTokensOut quoted for the Block +1 snipe tax (6.18% + 1% fee) plus
        // slippage; a fill under the 99% launch-block tax gets ~0 tokens and reverts.
        let min_tokens_out = CalldataPayloadBuilder::calculate_pons_buy_revert_guard(
            size_wei,
            Some(eth_reserve),
            Some(token_reserve),
            slippage,
        );
        // The dispatcher rewrites the recipient per account; the master is only a default.
        let recipient = self
            .master_address
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .unwrap_or(Address::ZERO);
        let buy_payload = CalldataPayloadBuilder::build_pons_buy_payload(size_wei, min_tokens_out, recipient);

        let signal = TradeSignal {
            tx_hash: order.launch_tx_hash,
            monitored_account: order.trigger_account,
            target_token: token,
            pool_or_router: curve,
            entry_amount_wei: size_wei,
            swap_calldata: buy_payload,
            detected_at: Instant::now(),
            block_number: current_block,
            strategy_tag: Some(format!("{}: {}", PRYSM_STRATEGY_TAG_PREFIX, order.report.pattern)),
        };
        EntryQuote { signal, min_tokens_out, mcap_usd }
    }

    /// Enqueue the entry of a matured order. Its pre-signed packets (if any) are claimed by the
    /// dispatcher that receives the signal.
    async fn fire(&self, mut order: PendingPrysmOrder, token: Address, curve: Address, reserves: Option<(U256, U256)>, current_block: u64) {
        let config = &self.config;
        let quote = self.quote_entry(&order, token, curve, reserves, current_block).await;

        // Valuation guardrail on real numbers (the detection-time figure is a placeholder).
        let max_mcap_usd = *config.prysm_max_mcap_usd.read().await;
        if let Some(mcap_usd) = quote.mcap_usd {
            order.report.market_cap_usd = mcap_usd;
        }
        if order.report.market_cap_usd > max_mcap_usd {
            let reason = format!(
                "Market Cap Ceiling Exceeded at release: ${:.2} > ${:.2}",
                order.report.market_cap_usd, max_mcap_usd
            );
            warn!("⚠️ [Prysm Guardrail Rejection] {} for {:?}", reason, token);
            self.cancel_staged(order.launch_tx_hash);
            order.report.rejection_reason = Some(reason);
            self.stats.record_match(order.report).await;
            return;
        }

        match self.signal_tx.try_send(quote.signal) {
            Ok(()) => {
                info!(
                    "🚀 [Prysm Entry Fired at Block #{}] {} Token {:?} on Curve {:?}, {:.4} ETH, minTokensOut {}",
                    current_block, order.report.pattern, token, curve, order.size_eth, quote.min_tokens_out
                );
                order.report.token_address = token;
                order.report.guardrails_passed = true;
                self.stats.record_match(order.report).await;
            }
            Err(e) => {
                warn!("Failed to enqueue matured Prysm order: {}", e);
                self.cancel_staged(order.launch_tx_hash);
            }
        }
    }

    fn forget_old_launches(&self) {
        let now = Instant::now();
        let fresh = |at: &Instant| now.saturating_duration_since(*at) < LAUNCH_RECORD_TTL;
        locked(&self.recent_launches).retain(|_, m| fresh(&m.launch_instant));
        locked(&self.launches_by_tx).retain(|_, m| fresh(&m.launch_instant));
        locked(&self.resolutions).retain(|_, (_, at)| fresh(at));
    }
}
