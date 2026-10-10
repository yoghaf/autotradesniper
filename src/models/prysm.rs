use crate::config::AppConfig;
use crate::types::{PrysmPattern, TradeSignal};
use alloy::primitives::{address, Address, B256, U256};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

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
    fn for_match(meta: &LaunchMetadata, tx_hash: B256) -> Self {
        if meta.token_address != meta.pool_address {
            EntryKey::Token(meta.token_address)
        } else {
            EntryKey::Tx(tx_hash)
        }
    }
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

/// Pending entry order awaiting minimum block delay guard (+3 blocks)
#[derive(Debug, Clone)]
pub struct PendingPrysmOrder {
    pub report: PrysmMatchReport,
    pub token_address: Address,
    pub pool_address: Address,
    pub launch_tx_hash: B256,
    pub scheduled_block: u64,
    pub created_at: Instant,
    pub size_eth: f64,
    pub swap_calldata: Vec<u8>,
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
pub fn parse_address_from_topic(topic_str: &str) -> Option<Address> {
    let clean = topic_str.trim_start_matches("0x");
    if clean.len() < 40 {
        return None;
    }
    let addr_part = &clean[clean.len() - 40..];
    std::str::FromStr::from_str(addr_part).ok()
}

/// Autonomous Heuristic Pattern Engine: Prysm Series Strategy
pub struct PrysmEngine {
    config: AppConfig,
    stats: Arc<PrysmStats>,
    recent_launches: Arc<RwLock<HashMap<Address, LaunchMetadata>>>,
    pending_orders: Arc<RwLock<Vec<PendingPrysmOrder>>>,
    signal_tx: mpsc::Sender<TradeSignal>,
    /// Entries already queued/dispatched (sync mutex: never held across an await).
    entered_tokens: Mutex<HashMap<EntryKey, Instant>>,
    warned_no_launchpads: AtomicBool,
    broadcaster: Option<Arc<crate::dispatcher::BroadcasterPool>>,
    master_address: Arc<RwLock<Option<Address>>>,
}

impl PrysmEngine {
    pub fn new(config: AppConfig, signal_tx: mpsc::Sender<TradeSignal>) -> Self {
        Self {
            config,
            stats: Arc::new(PrysmStats::new()),
            recent_launches: Arc::new(RwLock::new(HashMap::new())),
            pending_orders: Arc::new(RwLock::new(Vec::new())),
            signal_tx,
            entered_tokens: Mutex::new(HashMap::new()),
            warned_no_launchpads: AtomicBool::new(false),
            broadcaster: None,
            master_address: Arc::new(RwLock::new(None)),
        }
    }

    pub fn with_broadcaster(mut self, broadcaster: Arc<crate::dispatcher::BroadcasterPool>) -> Self {
        self.broadcaster = Some(broadcaster);
        self
    }

    pub async fn set_master_address(&self, addr: Address) {
        *self.master_address.write().await = Some(addr);
    }

    /// Claim the single entry slot for `key`; false if it was claimed within the window.
    fn try_claim_entry(&self, key: EntryKey, now: Instant) -> bool {
        let mut entered = self.entered_tokens.lock().unwrap_or_else(|e| e.into_inner());
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
        self.stats.clone()
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
            if meta.token_address == token
                && now.saturating_duration_since(meta.launch_instant) <= Duration::from_secs(60)
            {
                return true;
            }
        }

        false
    }

    /// Validate Entry Guardrails
    /// Returns Ok(()) if all guardrails pass, or Err(rejection_reason)
    pub async fn validate_entry_guardrails(
        &self,
        meta: &LaunchMetadata,
        current_block: u64,
        now: Instant,
    ) -> Result<(), String> {
        let max_age_secs = *self.config.prysm_max_signal_age_secs.read().await;
        let max_mcap_usd = *self.config.prysm_max_mcap_usd.read().await;

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

        // 3. Min Block Guard (+3 blocks)
        let min_delay = *self.config.prysm_min_block_delay.read().await;
        let required_block = meta.launch_block.saturating_add(min_delay);
        if current_block < required_block {
            return Err(format!(
                "Awaiting Min Block Guard: Current Block #{} < Required Block #{}",
                current_block, required_block
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
        let image_url = if input_lossy.contains(PRYSMCLOUD_SUBSTRING) {
            // Find start and end of url
            if let Some(idx) = input_lossy.find(PRYSMCLOUD_SUBSTRING) {
                let start = input_lossy[..idx].rfind("http").unwrap_or(idx);
                let end = input_lossy[idx..]
                    .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == '\0' || c == '}')
                    .map(|e| idx + e)
                    .unwrap_or(input_lossy.len());
                Some(input_lossy[start..end].trim().to_string())
            } else {
                Some(format!("https://vercel-storage.com/{}/sample.png", PRYSMCLOUD_SUBSTRING))
            }
        } else {
            None
        };

        // Extract Website URL: look for http/https with .lat
        let website_url = if let Some(idx) = input_lossy.find(".lat") {
            let start = input_lossy[..idx].rfind("http").unwrap_or(idx);
            let end = input_lossy[idx..]
                .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == '\0' || c == '}')
                .map(|e| idx + e)
                .unwrap_or(input_lossy.len());
            Some(input_lossy[start..end].trim().to_string())
        } else {
            None
        };

        // If neither image match nor website match exists, this isn't a Prysm candidate
        if image_url.is_none() && website_url.is_none() {
            return None;
        }

        // Dev Buy in ETH from the launch tx value. There is deliberately no fallback: the former
        // hard-coded 0.021 ETH default made every zero-value tx pass the Pola A/B dev-buy check.
        let dev_buy_eth = f64::from(value_wei) / 1e18;

        // Creator Tax (bps): Default to 200 bps (2%) if standard launch, max allowed is 300 bps
        let creator_tax_bps = 200;

        // Tax-Exempt Wallets:
        let tax_exempt_wallets = vec![from]; // only developer

        // Token Address: derive from `to` or create deterministic target
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
        })
    }

    /// Primary inspection pipeline called by StreamListener for each incoming transaction
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
        if !self.config.is_prysm_enabled() {
            return;
        }

        self.stats.total_evaluated.fetch_add(1, Ordering::Relaxed);
        let to_addr = to.unwrap_or_default();
        let now = Instant::now();

        // 1. Check for Companion Accounts Buy (Pola C)
        if COMPANION_ACCOUNTS.contains(&from) {
            // Clone and release the read guard before any await below.
            let launch = self.recent_launches.read().await.get(&to_addr).cloned();
            if let Some(meta) = launch.as_ref() {
                if Self::evaluate_pattern_c(from, to_addr, Some(meta), now) {
                    let elapsed = start_time.elapsed().as_micros();
                    info!(
                        "⚡ [Prysm Pola C Matched in {} µs] Companion Account {:?} bought token {:?} within 60s of launch!",
                        elapsed, from, to_addr
                    );

                    let report = PrysmMatchReport {
                        token_address: to_addr,
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

                    self.process_matched_pattern(report, meta.clone(), block_number, input.to_vec(), tx_hash).await;
                    return;
                }
            }
        }

        // 2. Check for Token Launch creation calldata (Pola A & Pola B)
        // Only calls into an allowlisted launchpad factory count as launches. Without this, any
        // contract (or EOA) receiving a tx whose calldata contains the marker strings would be
        // treated as a "token" and receive our entry ETH.
        if !self.config.prysm_launchpads.contains(&to_addr) {
            if self.config.prysm_launchpads.is_empty()
                && !self.warned_no_launchpads.swap(true, Ordering::Relaxed)
            {
                warn!("Prysm Pola A/B disabled: PRYSM_LAUNCHPAD_FACTORIES is empty (fail-closed)");
            }
            return;
        }

        // Assume initial launch market cap starts at ~$4,500 - $6,000 USD
        let simulated_mcap = 5000.0;
        if let Some(meta) = Self::parse_launch_calldata(from, to_addr, value, input, block_number, simulated_mcap) {
            // Save into recent launches registry for companion tracking (retained for 120s)
            self.recent_launches.write().await.insert(meta.token_address, meta.clone());

            let mut matched_pattern = None;
            let mut match_reason = String::new();

            if Self::evaluate_pattern_a(&meta) {
                matched_pattern = Some(PrysmPattern::PatternA);
                match_reason = format!(
                    "Pola A Matched: Prysmcloud storage ({}), Dev Buy {:.4} ETH, Tax {} bps",
                    meta.image_url.as_deref().unwrap_or(""),
                    meta.dev_buy_eth,
                    meta.creator_tax_bps
                );
            } else if Self::evaluate_pattern_b(&meta) {
                matched_pattern = Some(PrysmPattern::PatternB);
                match_reason = format!(
                    "Pola B Matched: .lat domain ({}), Dev Buy {:.4} ETH, Tax {} bps",
                    meta.website_url.as_deref().unwrap_or(""),
                    meta.dev_buy_eth,
                    meta.creator_tax_bps
                );
            }

            if let Some(pattern) = matched_pattern {
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

                self.process_matched_pattern(report, meta, block_number, input.to_vec(), tx_hash).await;
            }
        }
    }

    /// Process a verified pattern match through entry guardrails
    async fn process_matched_pattern(
        &self,
        mut report: PrysmMatchReport,
        meta: LaunchMetadata,
        current_block: u64,
        swap_calldata: Vec<u8>,
        tx_hash: B256,
    ) {
        let now = Instant::now();
        let min_block_delay = if report.pattern == PrysmPattern::PatternC {
            0
        } else {
            *self.config.prysm_min_block_delay.read().await
        };
        let required_block = meta.launch_block.saturating_add(min_block_delay);

        // One entry per token: Pola C fires again for every companion buy, and the same launch
        // can be seen twice (feed + HTTP bridge).
        if !self.try_claim_entry(EntryKey::for_match(&meta, tx_hash), now) {
            info!("[Prysm] Token {:?} already queued/entered, skipping duplicate match", meta.token_address);
            return;
        }

        // Release happens when block `required_block` is *observed*, so the entry tx lands in
        // block `required_block + 1` or later.
        if current_block < required_block {
            info!(
                "⏳ [Prysm Min Block Guard Armed] Queueing order for Block #{} (Current: #{}, Target: Token {:?})",
                required_block, current_block, meta.token_address
            );

            let pending = PendingPrysmOrder {
                report: report.clone(),
                token_address: meta.token_address,
                pool_address: meta.pool_address,
                launch_tx_hash: tx_hash,
                scheduled_block: required_block,
                created_at: now,
                size_eth: *self.config.prysm_size_eth.read().await,
                swap_calldata,
            };

            self.pending_orders.write().await.push(pending);
            return;
        }

        // Validate guardrails immediately
        let guardrails_res = if report.pattern == PrysmPattern::PatternC {
            let max_age_secs = *self.config.prysm_max_signal_age_secs.read().await;
            let max_mcap_usd = *self.config.prysm_max_mcap_usd.read().await;
            let age = now.saturating_duration_since(meta.launch_instant);
            if age > Duration::from_secs(max_age_secs) {
                Err(format!(
                    "Signal Staleness Exceeded: {:.1}s > {}s limit",
                    age.as_secs_f64(), max_age_secs
                ))
            } else if meta.market_cap_usd > max_mcap_usd {
                Err(format!(
                    "Market Cap Ceiling Exceeded: ${:.2} > ${:.2} limit",
                    meta.market_cap_usd, max_mcap_usd
                ))
            } else {
                Ok(())
            }
        } else {
            self.validate_entry_guardrails(&meta, current_block, now).await
        };

        match guardrails_res {
            Ok(_) => {
                report.guardrails_passed = true;
                self.stats.record_match(report.clone()).await;
                self.dispatch_prysm_entry(meta, report.pattern, swap_calldata, tx_hash).await;
            }
            Err(rejection) => {
                warn!("⚠️ [Prysm Guardrail Rejection] {}: {:?}", rejection, meta.token_address);
                report.guardrails_passed = false;
                report.rejection_reason = Some(rejection);
                self.stats.record_match(report).await;
            }
        }
    }

    /// On each new block confirmation, check and execute any matured pending orders
    pub async fn process_new_block(&self, current_block: u64) {
        if !self.config.is_prysm_enabled() {
            return;
        }

        let now = Instant::now();
        let mut ready_orders = Vec::new();
        {
            let mut pending_lock = self.pending_orders.write().await;
            if !pending_lock.is_empty() {
                let mut i = 0;
                while i < pending_lock.len() {
                    if current_block >= pending_lock[i].scheduled_block {
                        ready_orders.push(pending_lock.swap_remove(i));
                    } else {
                        i += 1;
                    }
                }
            }
        }

        let max_age = Duration::from_secs(*self.config.prysm_max_signal_age_secs.read().await);
        let max_mcap_usd = *self.config.prysm_max_mcap_usd.read().await;
        for mut order in ready_orders {
            let age = now.saturating_duration_since(order.created_at);

            if age > max_age {
                warn!(
                    "⚠️ [Prysm Pending Order Expired] Age {:.1}s > {}s limit for Token {:?}",
                    age.as_secs_f64(), max_age.as_secs(), order.token_address
                );
                order.report.rejection_reason = Some("Signal Staleness Exceeded (queued order)".to_string());
                self.stats.record_match(order.report).await;
                continue;
            }

            // Queued orders get the same valuation guardrail as immediate ones.
            if order.report.market_cap_usd > max_mcap_usd {
                warn!(
                    "⚠️ [Prysm Guardrail Rejection] Market Cap Ceiling Exceeded at release: ${:.2} > ${:.2} for {:?}",
                    order.report.market_cap_usd, max_mcap_usd, order.token_address
                );
                order.report.rejection_reason = Some("Market Cap Ceiling Exceeded (queued order)".to_string());
                self.stats.record_match(order.report).await;
                continue;
            }

            // If token address is not resolved yet (e.g. token == pool == launchpad/factory),
            // resolve token and curve addresses from the on-chain launch receipt!
            if order.token_address == order.pool_address && order.launch_tx_hash != B256::ZERO {
                if let Some(ref broadcaster) = self.broadcaster {
                    match broadcaster.get_transaction_receipt(order.launch_tx_hash).await {
                        Ok(Some(receipt)) => {
                            if let Some(logs) = receipt.get("logs").and_then(|l| l.as_array()) {
                                let mut resolved = false;
                                for log in logs {
                                    if let Some(topics) = log.get("topics").and_then(|t| t.as_array()) {
                                        if topics.len() >= 3 {
                                            let t0 = topics[0].as_str().unwrap_or_default();
                                            if t0.eq_ignore_ascii_case(crate::payload::PONS_LAUNCH_TOPIC_HEX) {
                                                if let (Some(t1), Some(t2)) = (topics[1].as_str(), topics[2].as_str()) {
                                                    if let (Some(token_addr), Some(curve_addr)) = (
                                                        parse_address_from_topic(t1),
                                                        parse_address_from_topic(t2),
                                                    ) {
                                                        info!(
                                                            "🎉 [Prysm Receipt Resolved] Token: {:?}, Curve: {:?}",
                                                            token_addr, curve_addr
                                                        );
                                                        order.token_address = token_addr;
                                                        order.pool_address = curve_addr;
                                                        resolved = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                if !resolved {
                                    warn!("Receipt for {:?} found but T_LAUNCH log not found yet, retrying next block...", order.launch_tx_hash);
                                    if age <= max_age {
                                        self.pending_orders.write().await.push(order);
                                        continue;
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            // Node has not indexed receipt yet, wait for next block
                            if age <= max_age {
                                info!("Receipt for {:?} not indexed yet, retrying next block...", order.launch_tx_hash);
                                self.pending_orders.write().await.push(order);
                                continue;
                            }
                        }
                        Err(e) => {
                            warn!("Receipt query error for {:?}: {}", order.launch_tx_hash, e);
                        }
                    }
                }
            }

            // If token and pool are resolved to the curve, construct real buy calldata!
            let (target_token, target_pool, swap_data) = if order.token_address != order.pool_address {
                let size_wei = U256::from((order.size_eth * 1e18) as u128);
                let recipient = self.master_address.read().await.unwrap_or(Address::ZERO);
                let slippage = *self.config.prysm_slippage_percent.read().await;
                // Revert Guard: quote with Block +1 6.18% snipe tax and slippage.
                // If this order accidentally lands in Block 0 with 99% tax, EVM reverts safely on-chain!
                let min_tokens_out = crate::payload::CalldataPayloadBuilder::calculate_pons_buy_revert_guard(
                    size_wei,
                    None,
                    None,
                    slippage,
                );
                let buy_payload = crate::payload::CalldataPayloadBuilder::build_pons_buy_payload(
                    size_wei,
                    min_tokens_out,
                    recipient,
                );
                (order.token_address, order.pool_address, buy_payload)
            } else {
                (order.token_address, order.pool_address, order.swap_calldata)
            };

            info!(
                "🚀 [Prysm Min Block Guard Fired at Block #{}] Executing queued order for Token {:?} on Curve {:?}",
                current_block, target_token, target_pool
            );

            let signal = TradeSignal {
                tx_hash: order.launch_tx_hash,
                monitored_account: COMPANION_ACCOUNTS[0],
                target_token,
                pool_or_router: target_pool,
                entry_amount_wei: U256::from((order.size_eth * 1e18) as u128),
                swap_calldata: swap_data,
                detected_at: Instant::now(),
                block_number: current_block,
                strategy_tag: Some(format!("{}: {}", PRYSM_STRATEGY_TAG_PREFIX, order.report.pattern)),
            };

            match self.signal_tx.try_send(signal) {
                Ok(()) => {
                    order.report.guardrails_passed = true;
                    self.stats.record_match(order.report).await;
                }
                Err(e) => warn!("Failed to enqueue matured Prysm order: {}", e),
            }
        }

        // Cleanup stale launch records older than 120s
        let mut launches = self.recent_launches.write().await;
        if !launches.is_empty() {
            launches.retain(|_, v| v.launch_instant.elapsed() < Duration::from_secs(120));
        }
    }

    /// Dispatch verified entry into trading execution channel
    async fn dispatch_prysm_entry(
        &self,
        meta: LaunchMetadata,
        pattern: PrysmPattern,
        swap_calldata: Vec<u8>,
        tx_hash: B256,
    ) {
        let size_eth = *self.config.prysm_size_eth.read().await;
        let size_wei = U256::from((size_eth * 1e18) as u128);
        let slippage = *self.config.prysm_slippage_percent.read().await;

        info!(
            "🎯 [Prysm Series Entry Dispatched] Pattern: {}, Size: {:.4} ETH, Slippage: {:.1}%, Token: {:?}",
            pattern, size_eth, slippage, meta.token_address
        );

        let swap_payload = if meta.token_address != meta.pool_address {
            let recipient = self.master_address.read().await.unwrap_or(Address::ZERO);
            let min_tokens_out = crate::payload::CalldataPayloadBuilder::calculate_pons_buy_revert_guard(
                size_wei,
                None,
                None,
                slippage,
            );
            crate::payload::CalldataPayloadBuilder::build_pons_buy_payload(
                size_wei,
                min_tokens_out,
                recipient,
            )
        } else {
            swap_calldata
        };

        let signal = TradeSignal {
            tx_hash,
            monitored_account: meta.creator_address,
            target_token: meta.token_address,
            pool_or_router: meta.pool_address,
            entry_amount_wei: size_wei,
            swap_calldata: swap_payload,
            detected_at: Instant::now(),
            block_number: meta.launch_block,
            strategy_tag: Some(format!("{}: {}", PRYSM_STRATEGY_TAG_PREFIX, pattern)),
        };

        if let Err(e) = self.signal_tx.try_send(signal) {
            warn!("Failed to dispatch Prysm trade signal: {}", e);
        }
    }
}
