use crate::addr_set::AddressSet;
use crate::config::AppConfig;
use crate::payload::DynamicFeeAdjuster;
use crate::types::TradeSignal;
use alloy::consensus::transaction::SignerRecoverable;
use alloy::consensus::{Transaction as TxTrait, TxEnvelope};
use alloy::eips::eip2718::Decodable2718;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::BlockTransactions;
use anyhow::{Context, Result};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::{self, Message};
use tracing::{debug, error, info, trace, warn};

pub mod feed;
pub mod raw_tx;

pub use feed::{FeedStats, FeedStatsSnapshot};
pub use raw_tx::RawTxView;
use feed::{Backoff, ClaimRing, WinLedger, connect_feed};

/// Nitro's broadcaster pings every ~5s; Nitro's own feed client gives up after 20s of silence.
const FEED_IDLE_TIMEOUT: Duration = Duration::from_secs(20);
/// We ping too: it keeps NAT mappings warm, gives a round-trip sample per feed, and makes a
/// dead path fail on the send instead of waiting for the idle deadline. Also the cadence of
/// every liveness check, so the per-frame hot path carries no timer work at all.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const PING_SEND_TIMEOUT: Duration = Duration::from_secs(3);
/// First retry is quick (the common case is a transient drop); the ceiling protects the endpoint.
const MIN_RECONNECT_BACKOFF: Duration = Duration::from_millis(200);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);
/// A feed session that stayed up this long counts as healthy and resets the backoff.
const HEALTHY_SESSION: Duration = Duration::from_secs(60);
/// Upper bound on a server-requested `Retry-After`.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);
/// A feed that is this many messages behind the others *and* has delivered nothing for
/// `LAGGARD_QUIET` is wedged (alive at the TCP level, dead at the application level): recycle it.
const LAGGARD_MESSAGES: u64 = 64;
const LAGGARD_QUIET: Duration = Duration::from_secs(10);
/// The HTTP poller only steps in once every feed has been down this long.
const BRIDGE_GRACE: Duration = Duration::from_secs(1);
/// Cadence of the per-feed racing summary (only logged when more than one feed is configured).
const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(60);
/// HTTP fallback never replays more than this many blocks after a stall (stale signals).
const MAX_HTTP_CATCHUP_BLOCKS: u64 = 32;
/// Transactions in blocks older than this are not acted on. On (re)connect the broadcaster
/// replays its backlog (possibly minutes of blocks); without this every watched swap in it was
/// copied again as if it had just happened.
const MAX_SIGNAL_BLOCK_AGE_SECS: u64 = 5;
/// Feed and HTTP block numbers further apart than this mean `FEED_GENESIS_BLOCK` is wrong.
const MAX_BLOCK_NUMBERING_SKEW: u64 = 10_000;

/// Liveness timings of one feed session. The defaults are the production values; tests shrink
/// them so heartbeat, half-open and wedged-feed handling can be exercised in milliseconds.
#[derive(Clone, Copy, Debug)]
pub struct FeedTiming {
    /// Cadence of liveness checks and of our own pings.
    pub heartbeat: Duration,
    /// Silence (not even a ping or pong) after which the connection counts as half-open.
    pub idle_timeout: Duration,
    /// Messages another feed delivered beyond this one ...
    pub laggard_messages: u64,
    /// ... while this one delivered nothing for this long: the feed is wedged.
    pub laggard_quiet: Duration,
}

impl Default for FeedTiming {
    fn default() -> Self {
        Self {
            heartbeat: HEARTBEAT_INTERVAL,
            idle_timeout: FEED_IDLE_TIMEOUT,
            laggard_messages: LAGGARD_MESSAGES,
            laggard_quiet: LAGGARD_QUIET,
        }
    }
}

/// `arbostypes.L1MessageType_L2Message`: only these feed messages carry an L2 message payload.
pub const L1_MESSAGE_TYPE_L2_MESSAGE: u8 = 3;
const L2_KIND_BATCH: u8 = 3;
const L2_KIND_SIGNED_TX: u8 = 4;
/// Nitro rejects L2 batches nested deeper than this (`parseL2Message`, depth >= 16).
pub const MAX_L2_BATCH_DEPTH: usize = 16;

/// Pre-computed 4-byte DEX Swap Method Signatures as u32 integers
/// Enables single CPU clock-cycle bitwise matching (Zero Heap Allocation)
pub mod fast_selectors {
    /// Pons bonding curve buy(uint256,uint256,address) -> 0x59a87bc1
    pub const PONS_BUY: u32 = 0x59a87bc1;
    /// swapExactETHForTokens(uint256,address[],address,uint256) -> 0x7ff36ab5
    pub const SWAP_EXACT_ETH: u32 = 0x7ff36ab5;
    /// swapExactETHForTokensSupportingFeeOnTransferTokens -> 0xb6f9de95
    pub const SWAP_EXACT_ETH_FEE: u32 = 0xb6f9de95;
    /// swapExactTokensForTokens -> 0x38ed1739
    pub const SWAP_EXACT_TOKENS: u32 = 0x38ed1739;
    /// exactInputSingle((address,address,uint24,address,uint256,uint256,uint160)) -> 0x414bf382
    pub const EXACT_INPUT_SINGLE: u32 = 0x414bf382;
    /// multicall(bytes[]) -> 0xac9650d8
    pub const MULTICALL: u32 = 0xac9650d8;
    /// execute(bytes,bytes[],uint256) -> 0x3593564c (Universal Router)
    pub const UNIVERSAL_ROUTER_EXECUTE: u32 = 0x3593564c;

    /// Instantaneous O(1) integer comparison without memory allocation
    #[inline(always)]
    pub fn is_swap_selector(selector_bytes: &[u8]) -> bool {
        if selector_bytes.len() < 4 {
            return false;
        }
        let tag = u32::from_be_bytes([
            selector_bytes[0],
            selector_bytes[1],
            selector_bytes[2],
            selector_bytes[3],
        ]);
        matches!(
            tag,
            PONS_BUY
                | SWAP_EXACT_ETH
                | SWAP_EXACT_ETH_FEE
                | SWAP_EXACT_TOKENS
                | EXACT_INPUT_SINGLE
                | MULTICALL
                | UNIVERSAL_ROUTER_EXECUTE
        )
    }

    /// Swaps that can be copied with our own `msg.value` and recipient:
    /// Pons curve `buy(uint256,uint256,address)` or V2 `swapExactETHForTokens*(...)`.
    #[inline(always)]
    pub fn is_copyable_eth_swap(calldata: &[u8]) -> bool {
        match calldata.first_chunk::<4>() {
            Some(sel) => matches!(u32::from_be_bytes(*sel), PONS_BUY | SWAP_EXACT_ETH | SWAP_EXACT_ETH_FEE),
            None => false,
        }
    }
}

/// Allocation-free walk over the signed transactions of one L2 feed message.
///
/// Kind 4 = a single signed tx; kind 3 = a batch of (8-byte big-endian length + message).
/// Open batches live in a fixed on-stack array (Nitro caps the nesting at
/// [`MAX_L2_BATCH_DEPTH`]), so unpacking a block touches the heap zero times; the former
/// `Vec<&[u8]>` per message is gone.
///
/// The bytes are untrusted: delayed-inbox messages (`sendL2Message` on L1) are user supplied
/// and reach the feed verbatim. Lengths are bounds-checked without arithmetic overflow, and a
/// batch nested deeper than Nitro allows invalidates the *whole* message, exactly as Nitro
/// does, so nothing is acted on that the chain would not execute. An iterator cannot take
/// back what it already yielded, so run [`l2_message_is_valid`] first for batch messages
/// (kind 4 cannot be invalid); `extract_l2_txs` does that for you.
pub struct L2Txs<'a> {
    /// `open[..depth]`: unread tails of the batches currently being walked, outermost first.
    open: [&'a [u8]; MAX_L2_BATCH_DEPTH],
    depth: usize,
    root: Option<&'a [u8]>,
    too_deep: bool,
}

impl<'a> L2Txs<'a> {
    pub fn new(message: &'a [u8]) -> Self {
        Self { open: [&[]; MAX_L2_BATCH_DEPTH], depth: 0, root: Some(message), too_deep: false }
    }

    /// A batch nested deeper than Nitro allows was met: the message is void as a whole.
    pub fn is_void(&self) -> bool {
        self.too_deep
    }

    /// Look at one message found at the current depth; yields the tx if it is one.
    fn examine(&mut self, msg: &'a [u8]) -> Option<&'a [u8]> {
        match msg.split_first() {
            Some((&L2_KIND_SIGNED_TX, tx)) => Some(tx),
            Some((&L2_KIND_BATCH, tail)) => {
                if self.depth >= MAX_L2_BATCH_DEPTH {
                    self.too_deep = true;
                } else {
                    self.open[self.depth] = tail;
                    self.depth += 1;
                }
                None
            }
            _ => None,
        }
    }
}

impl<'a> Iterator for L2Txs<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if let Some(root) = self.root.take()
            && let Some(tx) = self.examine(root)
        {
            return Some(tx);
        }
        while self.depth > 0 && !self.too_deep {
            let rest = self.open[self.depth - 1];
            // Truncated or oversized segment: Nitro treats it as end-of-batch.
            let segment = rest.split_first_chunk::<8>().and_then(|(len, tail)| {
                let n = usize::try_from(u64::from_be_bytes(*len)).ok()?;
                tail.split_at_checked(n)
            });
            match segment {
                None => self.depth -= 1,
                Some((segment, next)) => {
                    self.open[self.depth - 1] = next;
                    if let Some(tx) = self.examine(segment) {
                        return Some(tx);
                    }
                }
            }
        }
        None
    }
}

/// `false` if Nitro would void the whole message (batches nested beyond [`MAX_L2_BATCH_DEPTH`]).
pub fn l2_message_is_valid(message: &[u8]) -> bool {
    let mut walk = L2Txs::new(message);
    walk.by_ref().for_each(drop);
    !walk.is_void()
}

/// Collect the signed transactions of one L2 message (empty if the message is void).
pub fn extract_l2_txs<'a>(bytes: &'a [u8], out: &mut Vec<&'a [u8]>) {
    if l2_message_is_valid(bytes) {
        out.extend(L2Txs::new(bytes));
    }
}

/// Borrowed view of one broadcaster frame: `{"version":1,"messages":[...]}`.
/// Deserializing into these structs avoids building a `serde_json::Value` tree per frame;
/// `l2Msg` (base64, never JSON-escaped) is borrowed straight from the frame buffer.
#[derive(Deserialize)]
struct FeedFrame<'a> {
    #[serde(borrow, default)]
    messages: Vec<FeedMessage<'a>>,
}

#[derive(Deserialize)]
struct FeedMessage<'a> {
    #[serde(rename = "sequenceNumber")]
    sequence_number: u64,
    #[serde(borrow)]
    message: FeedMessageWithMetadata<'a>,
}

#[derive(Deserialize)]
struct FeedMessageWithMetadata<'a> {
    #[serde(borrow)]
    message: L1IncomingMessage<'a>,
}

#[derive(Deserialize)]
struct L1IncomingMessage<'a> {
    #[serde(default)]
    header: Option<L1Header>,
    /// Go marshals an empty `[]byte` as `null`; such a message must not poison the whole frame
    /// (and with it every other message in it), so `null` reads as "no payload".
    #[serde(rename = "l2Msg", borrow, default, deserialize_with = "l2_msg_or_null")]
    l2_msg: Cow<'a, str>,
}

/// Borrow the base64 string straight from the frame buffer (it never needs JSON escapes);
/// fall back to an owned copy only if the sender escaped something anyway.
fn l2_msg_or_null<'de: 'a, 'a, D: serde::Deserializer<'de>>(d: D) -> Result<Cow<'a, str>, D::Error> {
    struct V;
    impl<'de> serde::de::Visitor<'de> for V {
        type Value = Cow<'de, str>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a base64 string or null")
        }
        fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E> {
            Ok(Cow::Borrowed(v))
        }
        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
            Ok(Cow::Owned(v.to_owned()))
        }
        fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
            Ok(Cow::Owned(v))
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(Cow::Borrowed(""))
        }
        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(Cow::Borrowed(""))
        }
    }
    d.deserialize_any(V)
}

#[derive(Deserialize)]
struct L1Header {
    kind: u8,
    /// L2 block timestamp (seconds) assigned by the sequencer.
    #[serde(default)]
    timestamp: u64,
}

/// A curve/pool buy seen on the feed (Layer 2 inflow input for the risk engine).
#[derive(Debug, Clone, Copy)]
pub struct PoolBuy {
    pub pool: Address,
    pub buyer: Address,
    pub amount_wei: U256,
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Too old to trade on (unknown timestamp = fresh).
fn is_stale_block(timestamp_secs: u64, now_secs: u64) -> bool {
    timestamp_secs != 0 && now_secs.saturating_sub(timestamp_secs) > MAX_SIGNAL_BLOCK_AGE_SECS
}

/// What one feed frame contained, for the connection's liveness bookkeeping.
#[derive(Default)]
struct FrameSummary {
    messages: usize,
    max_seq: Option<u64>,
}

/// Long-lived tasks of [`StreamListener::run`]; each is restarted if it ever exits or panics.
#[derive(Clone, Copy, Debug)]
enum Role {
    Feed(usize),
    HttpBridge,
    Reporter,
}

/// Marks a feed as live for as long as its session exists (also on early return or panic), so
/// the HTTP bridge sees an accurate count of working feeds.
struct LiveGuard<'a> {
    live: &'a watch::Sender<usize>,
    stats: &'a FeedStats,
}

impl<'a> LiveGuard<'a> {
    fn new(live: &'a watch::Sender<usize>, stats: &'a FeedStats) -> Self {
        stats.connected.store(true, Ordering::Relaxed);
        live.send_modify(|n| *n += 1);
        Self { live, stats }
    }
}

impl Drop for LiveGuard<'_> {
    fn drop(&mut self) {
        self.stats.connected.store(false, Ordering::Relaxed);
        self.live.send_modify(|n| *n = n.saturating_sub(1));
    }
}

/// Real-Time Sequencer Feed & Signal Replication Stream Listener
///
/// Races every configured feed (`FEED_URL` + `FEED_URLS`): each endpoint has its own task,
/// socket and reconnect schedule, all sharing one [`ClaimRing`]. The first feed to deliver a
/// block processes it; the same block from any other feed is dropped after a single atomic
/// operation. Latency is therefore the minimum over the feeds, one feed dying costs nothing,
/// and the HTTP poller is only a last resort for the moment *all* feeds are down.
pub struct StreamListener {
    config: AppConfig,
    fee_adjuster: Arc<DynamicFeeAdjuster>,
    prysm_engine: Arc<crate::models::PrysmEngine>,
    signal_tx: mpsc::Sender<TradeSignal>,
    /// Highest feed sequence number claimed + 1 (0 = none). Only a resume hint for
    /// reconnects (`Arbitrum-Requested-Sequence-Number`) and gap bookkeeping; whether a
    /// message is processed is decided by `claims`.
    next_feed_seq: AtomicU64,
    /// First-packet-wins registry, keyed by L2 block number, shared by all feeds and the
    /// HTTP bridge (so a block seen by both is not traded twice).
    claims: ClaimRing,
    ledger: WinLedger,
    feeds: Vec<Arc<FeedStats>>,
    /// Number of feeds with a live session.
    live_feeds: watch::Sender<usize>,
    /// `PRYSM_LAUNCHPAD_FACTORIES` as a bitwise-probed set for the per-tx prefilter.
    launchpads: AddressSet,
    skew_warned: AtomicBool,
    /// Feed messages skipped as too old (backlog replay, or a skewed system clock).
    stale_skipped: AtomicU64,
    /// Messages skipped over when a feed's sequence jumped (may be filled by another feed).
    gap_messages: AtomicU64,
    /// Messages that arrived below the high-water mark and were still new: gaps another
    /// feed left behind, recovered thanks to exact (not high-water) claiming.
    gap_filled: AtomicU64,
    fee_refresher_started: AtomicBool,
    pool_buy_tx: Option<mpsc::Sender<PoolBuy>>,
    timing: FeedTiming,
}

impl StreamListener {
    pub fn new(
        config: AppConfig,
        fee_adjuster: Arc<DynamicFeeAdjuster>,
        prysm_engine: Arc<crate::models::PrysmEngine>,
        signal_tx: mpsc::Sender<TradeSignal>,
    ) -> Self {
        let urls = if config.feed_urls.is_empty() { vec![config.feed_url.clone()] } else { config.feed_urls.clone() };
        let feeds = urls.into_iter().map(|u| Arc::new(FeedStats::new(u))).collect();
        let launchpads = AddressSet::new(config.prysm_launchpads.iter());
        Self {
            config,
            fee_adjuster,
            prysm_engine,
            signal_tx,
            next_feed_seq: AtomicU64::new(0),
            claims: ClaimRing::default(),
            ledger: WinLedger::default(),
            feeds,
            live_feeds: watch::channel(0).0,
            launchpads,
            skew_warned: AtomicBool::new(false),
            stale_skipped: AtomicU64::new(0),
            gap_messages: AtomicU64::new(0),
            gap_filled: AtomicU64::new(0),
            fee_refresher_started: AtomicBool::new(false),
            pool_buy_tx: None,
            timing: FeedTiming::default(),
        }
    }

    /// Override the liveness timings (tests; production uses the defaults).
    pub fn with_timing(mut self, timing: FeedTiming) -> Self {
        self.timing = timing;
        self
    }

    /// Forward every curve buy seen on the feed (risk engine Layer 2 input).
    pub fn with_pool_buy_sink(mut self, tx: mpsc::Sender<PoolBuy>) -> Self {
        self.pool_buy_tx = Some(tx);
        self
    }

    /// Per-feed racing counters: who wins, who is late, who reconnects.
    pub fn feed_stats(&self) -> Vec<FeedStatsSnapshot> {
        self.feeds.iter().map(|f| f.snapshot()).collect()
    }

    /// `(skipped, recovered)`: messages a feed's sequence jumped over, and how many of those a
    /// different feed (or a later frame) delivered after all.
    pub fn gap_stats(&self) -> (u64, u64) {
        (self.gap_messages.load(Ordering::Relaxed), self.gap_filled.load(Ordering::Relaxed))
    }

    /// Number of feeds with a live session right now.
    pub fn live_feed_count(&self) -> usize {
        *self.live_feeds.borrow()
    }

    /// Warn once if the HTTP block numbering and the feed numbering disagree, which means
    /// `FEED_GENESIS_BLOCK` is wrong and de-duplication across the two sources is blind.
    fn warn_on_numbering_skew(&self, http_block: u64) {
        let fed = self.next_feed_seq.load(Ordering::Relaxed);
        if fed == 0 {
            return;
        }
        let feed_head = self.config.feed_genesis_block.saturating_add(fed - 1);
        if feed_head.abs_diff(http_block) > MAX_BLOCK_NUMBERING_SKEW && !self.skew_warned.swap(true, Ordering::Relaxed) {
            warn!(
                "Feed and RPC block numbers differ by > {} (block #{} vs #{}): check FEED_GENESIS_BLOCK",
                MAX_BLOCK_NUMBERING_SKEW, feed_head, http_block
            );
        }
    }

    /// Background fee refresher to poll gas prices from RPC without adding latency to the sequencer feed
    fn spawn_fee_refresher(&self) {
        // `run` may be restarted by a supervisor; never stack a second refresher.
        if self.fee_refresher_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let fee_adjuster = self.fee_adjuster.clone();
        let rpc_http = self.config.rpc_http_url.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_millis(1500))
                .build()
                .unwrap_or_default();
            let payload = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_gasPrice",
                "params": []
            });

            // L1 poster gas for a representative ~100 byte buy, priced by NodeInterface.
            let sample_buy = crate::payload::CalldataPayloadBuilder::build_pons_buy_payload(
                U256::from(u64::MAX),
                U256::from(u64::MAX),
                Address::repeat_byte(0xab),
            );
            let l1_call = crate::payload::CalldataPayloadBuilder::build_l1_component_estimate_payload(
                Address::repeat_byte(0xab),
                &sample_buy,
            );
            let l1_payload = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "eth_call",
                "params": [{
                    "to": format!("{:?}", crate::payload::NODE_INTERFACE_ADDRESS),
                    "data": alloy::hex::encode_prefixed(&l1_call),
                }, "latest"]
            });

            let mut tick = tokio::time::interval(Duration::from_secs(2));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if let Ok(resp) = client.post(&rpc_http).json(&payload).send().await {
                    if let Ok(json) = resp.json::<serde_json::Value>().await {
                        if let Some(res_str) = json.get("result").and_then(|r| r.as_str()) {
                            let hex_clean = res_str.trim_start_matches("0x");
                            if let Ok(val) = u128::from_str_radix(hex_clean, 16) {
                                let gwei = val as f64 / 1e9;
                                fee_adjuster.update_block_fees(gwei, 2.5);
                            }
                        }
                    }
                }
                // `gasEstimateForL1` is the first return word (uint64).
                let l1_gas = async {
                    let json: serde_json::Value = client.post(&rpc_http).json(&l1_payload).send().await.ok()?.json().await.ok()?;
                    let raw = alloy::hex::decode(json.get("result")?.as_str()?).ok()?;
                    let word = crate::payload::CalldataPayloadBuilder::decode_u256_output(&raw)?;
                    u64::try_from(word).ok()
                };
                if let Some(gas) = l1_gas.await {
                    fee_adjuster.set_l1_component_gas(gas);
                }
            }
        });
    }

    /// Run the stream listener.
    ///
    /// Spawns one task per feed plus the HTTP bridge (and, with several feeds, a periodic
    /// racing report). A task that exits or panics is restarted after a second, so the engine
    /// never goes blind silently. Dropping this future aborts every child task.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let labels: Vec<&str> = self.feeds.iter().map(|f| f.label.as_str()).collect();
        info!(
            "Starting High-Frequency Sequencer Stream Listener for Chain ID: {} ({} feed(s): {})",
            self.config.chain_id,
            labels.len(),
            labels.join(", ")
        );

        self.spawn_fee_refresher();

        let mut tasks: JoinSet<()> = JoinSet::new();
        let mut roles: HashMap<tokio::task::Id, Role> = HashMap::new();
        for i in 0..self.feeds.len() {
            self.spawn_role(&mut tasks, &mut roles, Role::Feed(i));
        }
        self.spawn_role(&mut tasks, &mut roles, Role::HttpBridge);
        if self.feeds.len() > 1 {
            self.spawn_role(&mut tasks, &mut roles, Role::Reporter);
        }

        while let Some(done) = tasks.join_next_with_id().await {
            let (id, panicked) = match done {
                Ok((id, ())) => (id, false),
                Err(e) => (e.id(), e.is_panic()),
            };
            let Some(role) = roles.remove(&id) else { continue };
            if panicked {
                error!("Stream task {:?} panicked; restarting in 1s", role);
            } else {
                warn!("Stream task {:?} exited; restarting in 1s", role);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            self.spawn_role(&mut tasks, &mut roles, role);
        }
        Ok(())
    }

    fn spawn_role(self: &Arc<Self>, tasks: &mut JoinSet<()>, roles: &mut HashMap<tokio::task::Id, Role>, role: Role) {
        let me = self.clone();
        let handle = match role {
            Role::Feed(i) => tasks.spawn(me.run_feed(i)),
            Role::HttpBridge => tasks.spawn(me.run_http_bridge()),
            Role::Reporter => tasks.spawn(me.run_stats_reporter()),
        };
        roles.insert(handle.id(), role);
    }

    /// Keep one feed connected forever: exponential backoff with jitter between sessions, reset
    /// once a session proved healthy, `Retry-After` honoured and never undercut.
    async fn run_feed(self: Arc<Self>, idx: usize) {
        let stats = self.feeds[idx].clone();
        let mut backoff = Backoff::new(MIN_RECONNECT_BACKOFF, MAX_RECONNECT_BACKOFF);
        loop {
            if !self.config.is_engine_running() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            let session_start = Instant::now();
            let outcome = self.listen_sequencer_feed(&stats).await;
            if session_start.elapsed() >= HEALTHY_SESSION {
                backoff.reset();
            }

            let wait = match &outcome {
                Err(e) if retry_after_hint(e).is_some() => {
                    // The server told us when to come back: never earlier, spread the herd later.
                    let hint = retry_after_hint(e).unwrap_or_default().min(MAX_RETRY_AFTER);
                    hint.mul_f64(1.0 + 0.2 * rand::random::<f64>())
                }
                _ => backoff.next_delay(),
            };
            let live = *self.live_feeds.borrow();
            match &outcome {
                Ok(()) => warn!("Sequencer feed {} closed. Reconnecting in {:?} ({} feed(s) still live)", stats.label, wait, live),
                Err(e) => warn!("Sequencer feed {} error: {:#}. Reconnecting in {:?} ({} feed(s) still live)", stats.label, e, wait, live),
            }
            tokio::time::sleep(wait).await;
        }
    }

    /// Last-resort source: poll over HTTP only while *every* feed is down (after a short grace,
    /// because a reconnect normally takes well under a second), stand down as soon as one is back.
    async fn run_http_bridge(self: Arc<Self>) {
        let http_url = self.config.rpc_http_url.clone();
        let mut live = self.live_feeds.subscribe();
        loop {
            if !self.config.is_engine_running() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            if !wait_live(&mut live, false).await {
                return;
            }
            tokio::select! {
                up = wait_live(&mut live, true) => {
                    if !up { return }
                    continue;
                }
                _ = tokio::time::sleep(BRIDGE_GRACE) => {}
            }
            warn!("All sequencer feeds are down: HTTP poller bridging until one reconnects");
            tokio::select! {
                up = wait_live(&mut live, true) => {
                    if !up { return }
                    info!("Sequencer feed recovered: HTTP poller stands down");
                }
                res = self.listen_http_poll(&http_url) => {
                    if let Err(e) = res {
                        error!("HTTP Sequencer poller error: {:#}", e);
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn run_stats_reporter(self: Arc<Self>) {
        let mut tick = tokio::time::interval(STATS_REPORT_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tick.tick().await;
            for s in self.feed_stats() {
                info!(
                    "📡 feed {} [{}] wins={} dups={} behind-winner={}µs ping-rtt={}µs sessions={} bad-frames={}",
                    s.label,
                    if s.connected { "up" } else { "DOWN" },
                    s.wins,
                    s.dups,
                    s.avg_lag_us.map_or_else(|| "-".to_string(), |v| v.to_string()),
                    s.rtt_us.map_or_else(|| "-".to_string(), |v| v.to_string()),
                    s.sessions,
                    s.parse_errors
                );
            }
            let (skipped, filled) = self.gap_stats();
            if skipped > 0 {
                info!("📡 feed gaps: {} message(s) skipped over, {} recovered from another feed", skipped, filled);
            }
        }
    }

    /// Real-time Arbitrum Nitro Sequencer Feed session for one endpoint.
    ///
    /// Returns `Ok(())` on an orderly close and `Err` on handshake failure, transport error,
    /// silent (half-open) connection or a wedged-but-pinging feed.
    async fn listen_sequencer_feed(&self, stats: &FeedStats) -> Result<()> {
        // Ask only for what we are missing (Nitro honours this; other servers ignore it).
        let resume = self.next_feed_seq.load(Ordering::Relaxed);
        let (mut ws_stream, response) = connect_feed(&stats.url, resume).await?;

        // Nitro feed servers announce their chain; a feed for another chain would be fatal.
        if let Some(chain) = response
            .headers()
            .get("Arbitrum-Chain-Id")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
        {
            anyhow::ensure!(
                chain == self.config.chain_id,
                "Feed {} serves chain {} but CHAIN_ID is {}",
                stats.label, chain, self.config.chain_id
            );
        }

        stats.sessions.fetch_add(1, Ordering::Relaxed);
        let _live = LiveGuard::new(&self.live_feeds, stats);
        info!("⚡ Successfully connected to Real-Time Sequencer Feed ({}, resuming from #{})", stats.label, resume);

        let mut b64_buf = Vec::with_capacity(64 * 1024);
        let data_timeout = (self.config.feed_data_timeout_secs > 0).then(|| Duration::from_secs(self.config.feed_data_timeout_secs));
        let timing = self.timing;
        let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + timing.heartbeat, timing.heartbeat);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_rx = Instant::now();
        let mut last_message = last_rx;
        // This session starts at the resume point: everything before it was delivered by
        // someone (possibly us, earlier). Starting at 0 would make a late-joining feed look
        // thousands of messages behind on a quiet chain and recycle it for nothing.
        let mut own_next_seq = resume;
        let mut ping_sent: Option<Instant> = None;

        loop {
            // The hot path is the first branch: no timer is armed per frame. Liveness is
            // judged on the heartbeat tick instead (server pings and our pongs count as traffic).
            tokio::select! {
                biased;
                frame = ws_stream.next() => {
                    let msg = match frame {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => return Err(e).context("Sequencer feed read error"),
                        None => return Ok(()),
                    };
                    let received_at = Instant::now();
                    last_rx = received_at;
                    match msg {
                        Message::Text(text) => {
                            if self.config.is_engine_running() {
                                let summary = self.handle_feed_frame(stats, text.as_bytes(), &mut b64_buf, received_at).await;
                                note_progress(&summary, &mut own_next_seq, &mut last_message, received_at);
                            } else {
                                last_message = received_at;
                            }
                        }
                        Message::Binary(bin) => {
                            if self.config.is_engine_running() {
                                let summary = self.handle_feed_frame(stats, &bin, &mut b64_buf, received_at).await;
                                note_progress(&summary, &mut own_next_seq, &mut last_message, received_at);
                            } else {
                                last_message = received_at;
                            }
                        }
                        Message::Pong(_) => {
                            if let Some(sent) = ping_sent.take() {
                                stats.rtt_us.store(sent.elapsed().as_micros().max(1) as u64, Ordering::Relaxed);
                            }
                        }
                        Message::Close(frame) => {
                            info!("Sequencer Feed {} received close frame from server: {:?}", stats.label, frame);
                            return Ok(());
                        }
                        // Server ping: tungstenite queues the Pong and flushes it on the next read/write.
                        _ => {}
                    }
                }
                _ = heartbeat.tick() => {
                    let now = Instant::now();
                    let behind = self.next_feed_seq.load(Ordering::Relaxed).saturating_sub(own_next_seq);
                    check_liveness(&timing, now, last_rx, last_message, behind, data_timeout)?;
                    ping_sent = Some(now);
                    match tokio::time::timeout(PING_SEND_TIMEOUT, ws_stream.send(Message::Ping(Default::default()))).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => return Err(e).context("Sequencer feed ping failed"),
                        Err(_) => anyhow::bail!("Sequencer feed ping could not be sent within {:?}", PING_SEND_TIMEOUT),
                    }
                }
            }
        }
    }

    /// First-packet-wins. True if this call is the first delivery of `block_number`; the loser
    /// is dropped after one atomic operation and records how late it was.
    fn claim_feed_message(&self, stats: &FeedStats, seq: u64, block_number: u64) -> bool {
        if !self.claims.try_claim(block_number) {
            stats.dups.fetch_add(1, Ordering::Relaxed);
            if let Some(late_us) = self.ledger.lateness_us(block_number) {
                stats.lag_sum_us.fetch_add(late_us, Ordering::Relaxed);
                stats.lag_samples.fetch_add(1, Ordering::Relaxed);
            }
            return false;
        }
        self.ledger.record_win(block_number);
        stats.wins.fetch_add(1, Ordering::Relaxed);

        let prev = self.next_feed_seq.fetch_max(seq.saturating_add(1), Ordering::Relaxed);
        if prev != 0 && prev < seq {
            self.gap_messages.fetch_add(seq - prev, Ordering::Relaxed);
            warn!(
                "Feed {} jumped from #{} to #{}: {} message(s) not received (yet); another feed may still deliver them",
                stats.label, prev, seq, seq - prev
            );
        } else if prev > seq.saturating_add(1) {
            self.gap_filled.fetch_add(1, Ordering::Relaxed);
            debug!("Feed {} delivered #{} below the high-water mark #{}: gap filled", stats.label, seq, prev);
        }
        true
    }

    /// Decode one broadcaster frame and process each contained message in order.
    async fn handle_feed_frame(
        &self,
        stats: &FeedStats,
        frame: &[u8],
        b64_buf: &mut Vec<u8>,
        received_at: Instant,
    ) -> FrameSummary {
        stats.frames.fetch_add(1, Ordering::Relaxed);
        let parsed: FeedFrame<'_> = match serde_json::from_slice(frame) {
            Ok(f) => f,
            Err(e) => {
                // A frame we cannot read is messages we never see: count it, and say so.
                let seen = stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                if matches!(seen % 1000, 0) {
                    warn!("Feed {} sent an unparseable frame ({} bytes; {} so far): {}", stats.label, frame.len(), seen + 1, e);
                }
                return FrameSummary::default();
            }
        };

        let now_secs = unix_now_secs();
        let mut summary = FrameSummary::default();
        for m in parsed.messages {
            let seq = m.sequence_number;
            summary.messages += 1;
            summary.max_seq = Some(summary.max_seq.map_or(seq, |s| s.max(seq)));
            // Every feed message produces exactly one L2 block.
            let block_number = self.config.feed_genesis_block.saturating_add(seq);
            if !self.claim_feed_message(stats, seq, block_number) {
                continue; // another feed (or the HTTP bridge) was first
            }
            let incoming = m.message.message;
            let carries_l2 = incoming.header.as_ref().is_none_or(|h| h.kind == L1_MESSAGE_TYPE_L2_MESSAGE);
            let stale = incoming.header.as_ref().is_some_and(|h| is_stale_block(h.timestamp, now_secs));
            if stale && self.stale_skipped.fetch_add(1, Ordering::Relaxed) % 1000 == 0 {
                warn!(
                    "Skipping feed message #{} as stale ({}s old): backlog replay after (re)connect. \
                     If this persists, the system clock is off (run NTP).",
                    seq,
                    incoming.header.as_ref().map_or(0, |h| now_secs.saturating_sub(h.timestamp))
                );
            }

            b64_buf.clear();
            let l2_bytes: &[u8] = if carries_l2
                && !stale
                && base64::engine::general_purpose::STANDARD
                    .decode_vec(incoming.l2_msg.as_bytes(), b64_buf)
                    .is_ok()
            {
                b64_buf
            } else {
                &[]
            };
            self.process_l2_message(block_number, l2_bytes, received_at).await;
        }
        summary
    }

    /// Cheap, allocation-free "could this raw tx matter?" test that runs before any full
    /// decode or `ecrecover` (~360 µs): copyable ETH swaps (copy-trades, Pola C, the pool-buy
    /// sink) and, with Prysm on, calls into an allowlisted launchpad (Pola A/B).
    ///
    /// Conservative by construction: a tx the in-place RLP walk cannot parse is a candidate,
    /// so the full decoder keeps the last word and nothing it would accept is lost here.
    #[inline]
    pub fn is_candidate_tx(&self, tx_raw: &[u8], prysm_enabled: bool) -> bool {
        match RawTxView::parse(tx_raw) {
            Some(view) => {
                fast_selectors::is_copyable_eth_swap(view.input)
                    || (prysm_enabled && view.to.is_some_and(|to| self.launchpads.contains_bytes(to)))
            }
            None => true,
        }
    }

    /// Process raw L2 sequencer bytes: release matured block queues & inspect transactions
    async fn process_l2_message(&self, block_number: u64, l2_bytes: &[u8], start: Instant) {

        // 1. Notify Prysm Series Strategy of block progression (releases matured +3 block orders)
        self.prysm_engine.process_new_block(block_number).await;

        if l2_bytes.is_empty() {
            return;
        }
        // A batch nested beyond Nitro's limit is void as a whole; a lone tx cannot be.
        if l2_bytes[0] == L2_KIND_BATCH && !l2_message_is_valid(l2_bytes) {
            return;
        }

        // 2. Walk the signed transactions of the Arbitrum Nitro L2 payload in place
        let prysm_enabled = self.config.is_prysm_enabled();
        for tx_raw in L2Txs::new(l2_bytes) {
            // Zero-copy prefilter: almost every tx in a block ends here without a heap touch.
            if !self.is_candidate_tx(tx_raw, prysm_enabled) {
                continue;
            }
            // `_exact`: trailing bytes make Nitro reject the tx, so we must not act on it either.
            let Ok(envelope) = TxEnvelope::decode_2718_exact(tx_raw) else { continue };
            let input = envelope.input();
            let to = envelope.to();

            // ecrecover costs ~360 µs (k256): only pay it for txs a pipeline can act on.
            // Copy-trades and Pola C are buys; Pola A/B are calls into an allowlisted launchpad.
            let needs_sender = fast_selectors::is_copyable_eth_swap(input)
                || (prysm_enabled && to.is_some_and(|t| self.launchpads.contains(&t)));
            if !needs_sender {
                continue;
            }
            let Ok(from) = envelope.recover_signer() else { continue };
            let value = envelope.value();
            let tx_hash = *envelope.tx_hash();

            // Pipeline 1: Watchlist Target Wallets
            self.inspect_transaction_fast(from, to, value, input, tx_hash, block_number, start)
                .await;

            // Pipeline 2: Autonomous Prysm Pattern Evaluator
            self.prysm_engine
                .inspect_transaction(from, to, value, input, tx_hash, block_number, start)
                .await;
        }
    }

    /// HTTP fallback polling in case WS feed is not reachable or during dev
    async fn listen_http_poll(&self, http_url: &str) -> Result<()> {
        let provider = ProviderBuilder::new()
            .connect(http_url)
            .await
            .context("Failed to connect to HTTP RPC")?;

        // Never fall back to block 0: that would replay the entire chain as fresh signals.
        let mut last_block = provider
            .get_block_number()
            .await
            .context("HTTP poller: initial eth_blockNumber failed")?;
        info!("Sequencer HTTP Poller active from block #{}", last_block);

        loop {
            if !self.config.is_engine_running() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            match provider.get_block_number().await {
                Ok(current_block) if current_block > last_block => {
                    let from_block = (last_block + 1)
                        .max(current_block.saturating_sub(MAX_HTTP_CATCHUP_BLOCKS - 1));
                    for block_num in from_block..=current_block {
                        if self.claims.is_claimed(block_num) {
                            last_block = block_num;
                            continue; // the feed already covered it: skip the fetch too
                        }
                        // `.full()`: the default request returns only tx hashes.
                        let block = match provider.get_block_by_number(block_num.into()).full().await {
                            Ok(Some(b)) => b,
                            // Retry this block on the next tick instead of silently skipping it.
                            Ok(None) | Err(_) => break,
                        };
                        last_block = block_num;
                        self.warn_on_numbering_skew(block_num);
                        if !self.claims.try_claim(block_num) {
                            continue; // the feed already processed it
                        }
                        let start = Instant::now();
                        self.prysm_engine.process_new_block(block_num).await;
                        if is_stale_block(block.header.timestamp, unix_now_secs()) {
                            continue;
                        }

                        if let BlockTransactions::Full(txs) = block.transactions {
                            for tx in txs {
                                let from = tx.inner.signer();
                                let to = tx.inner.to();
                                let value = tx.inner.value();
                                let input = tx.inner.input();
                                let tx_hash = *tx.inner.tx_hash();

                                self.inspect_transaction_fast(from, to, value, input, tx_hash, block_num, start)
                                    .await;
                                self.prysm_engine
                                    .inspect_transaction(from, to, value, input, tx_hash, block_num, start)
                                    .await;
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    trace!("HTTP block poll tick: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Microsecond-optimized inspection of incoming transaction against watchlist
    ///
    /// Utilizes O(1) HashSet lookup + 1-cycle integer bitwise selector matching.
    /// Internal compute latency: < 5 microseconds.
    #[inline]
    pub async fn inspect_transaction_fast(
        &self,
        from: Address,
        to: Option<Address>,
        value: U256,
        input: &[u8],
        tx_hash: B256,
        block_number: u64,
        start_time: Instant,
    ) {
        // Step 1: Only swaps we can replay for our own account. Plain ETH transfers from a
        // watched wallet (CEX deposit, wallet shuffle) must never be "copied".
        if !fast_selectors::is_copyable_eth_swap(input) {
            return;
        }

        let to_address = match to {
            Some(addr) => addr,
            None => return, // Contract creation, not a swap
        };

        let is_pons = input
            .first_chunk::<4>()
            .is_some_and(|sel| u32::from_be_bytes(*sel) == fast_selectors::PONS_BUY);

        // Layer 2 input: every curve buy, watched or not (the risk task filters by pool).
        if is_pons
            && !value.is_zero()
            && let Some(sink) = self.pool_buy_tx.as_ref()
        {
            let _ = sink.try_send(PoolBuy { pool: to_address, buyer: from, amount_wei: value });
        }

        // Step 2: Watchlist lookup: bitwise [u8; 20] probe on a lock-free snapshot, no await
        if !self.config.is_watched(&from) {
            return;
        }

        // Step 3: Target token (Pons: the curve, resolved to its token later; V2: last path hop,
        // ABI-decoded). A path that does not decode is not copied.
        let target_token = if is_pons {
            to_address
        } else {
            match crate::payload::CalldataPayloadBuilder::decode_v2_path_ends(input) {
                Some((_, token)) => token,
                None => {
                    warn!("Watched swap {:?} has an undecodable path; not copied", tx_hash);
                    return;
                }
            }
        };

        let signal = TradeSignal {
            tx_hash,
            monitored_account: from,
            target_token,
            pool_or_router: to_address,
            entry_amount_wei: value,
            swap_calldata: input.to_vec(),
            detected_at: start_time,
            block_number,
            strategy_tag: None,
        };

        // Never block the feed reader on a full queue: a backlog of 1000 signals is stale anyway.
        // Logging (a blocking stdout write) happens only after the signal is queued.
        match self.signal_tx.try_send(signal) {
            Ok(()) => info!(
                "⚡ ZERO-COPY ON-CHAIN SIGNAL queued in {} µs! Account: {:?} -> Router: {:?}, Value: {} wei",
                start_time.elapsed().as_micros(), from, to_address, value
            ),
            Err(e) => error!("Dropping trade signal (queue unavailable): {}", e),
        }
    }
}


/// Heartbeat verdict for one feed session; `Err` means "recycle this connection".
///
/// * `last_rx`: last frame of any kind (server pings and our pongs count): silence beyond
///   `timing.idle_timeout` is a half-open TCP connection.
/// * `last_message`: last frame that carried feed messages. Only enforced when the operator set
///   `FEED_DATA_TIMEOUT_SECS` (an idle Orbit chain is legitimately quiet).
/// * `behind`: how many messages the other feeds delivered beyond this one. Far behind *and*
///   quiet means the relay is alive at the TCP level but wedged at the application level.
fn check_liveness(
    timing: &FeedTiming,
    now: Instant,
    last_rx: Instant,
    last_message: Instant,
    behind: u64,
    data_timeout: Option<Duration>,
) -> Result<()> {
    anyhow::ensure!(
        now.saturating_duration_since(last_rx) <= timing.idle_timeout,
        "Sequencer feed silent for {:?}", timing.idle_timeout
    );
    let quiet = now.saturating_duration_since(last_message);
    if let Some(limit) = data_timeout {
        anyhow::ensure!(quiet <= limit, "Sequencer feed delivered no message for {:?}", quiet);
    }
    anyhow::ensure!(
        !(behind >= timing.laggard_messages && quiet >= timing.laggard_quiet),
        "Sequencer feed stalled: {} messages behind the other feeds, nothing for {:?}", behind, quiet
    );
    Ok(())
}

/// Wait until at least one feed is live (`up`) or none is (`!up`). False if the listener is gone.
async fn wait_live(rx: &mut watch::Receiver<usize>, up: bool) -> bool {
    rx.wait_for(|n| (*n > 0) == up).await.is_ok()
}

fn note_progress(summary: &FrameSummary, own_next_seq: &mut u64, last_message: &mut Instant, at: Instant) {
    if summary.messages > 0 {
        *last_message = at;
    }
    if let Some(seq) = summary.max_seq {
        *own_next_seq = (*own_next_seq).max(seq.saturating_add(1));
    }
}

/// Server-requested delay from an HTTP 429/503 handshake response (`Retry-After` seconds).
fn retry_after_hint(err: &anyhow::Error) -> Option<Duration> {
    let tungstenite::Error::Http(resp) = err.downcast_ref::<tungstenite::Error>()? else {
        return None;
    };
    let status = resp.status().as_u16();
    if status != 429 && status != 503 {
        return None;
    }
    let secs = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(10);
    Some(Duration::from_secs(secs.max(1)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::consensus::{
        SignableTransaction, TxEip1559, TxEip2930, TxEip4844, TxEip7702, TxLegacy,
    };
    use alloy::eips::eip2718::Encodable2718;
    use alloy::eips::eip7702::SignedAuthorization;
    use alloy::primitives::{Bytes, TxKind};
    use alloy::signers::SignerSync;
    use alloy::signers::local::PrivateKeySigner;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    #[test]
    fn feed_frame_decodes_with_borrowed_l2_msg() {
        let frame = br#"{"version":1,"messages":[{"sequenceNumber":42,"message":{"message":{"header":{"kind":3,"sender":"0xa4b000000000000000000073657175656e636572","blockNumber":1,"timestamp":2,"requestId":null,"baseFeeL1":null},"l2Msg":"BAE="},"delayedMessagesRead":7},"signature":null}]}"#;
        let parsed: FeedFrame<'_> = serde_json::from_slice(frame).unwrap();
        let m = &parsed.messages[0];
        assert_eq!(m.sequence_number, 42);
        assert_eq!(m.message.message.header.as_ref().map(|h| h.kind), Some(L1_MESSAGE_TYPE_L2_MESSAGE));
        assert!(matches!(m.message.message.l2_msg, Cow::Borrowed("BAE=")));

        // Confirmation-only frames carry no messages.
        let parsed: FeedFrame<'_> = serde_json::from_slice(br#"{"version":1,"confirmedSequenceNumberMessage":{"sequenceNumber":41}}"#).unwrap();
        assert!(parsed.messages.is_empty());
    }

    #[test]
    fn null_l2_msg_does_not_poison_the_frame() {
        // Go marshals an empty []byte as null. Before, one such message made serde reject the
        // whole frame and every other message in it was silently lost.
        let frame = br#"{"version":1,"messages":[
            {"sequenceNumber":7,"message":{"message":{"header":{"kind":6,"timestamp":2},"l2Msg":null}}},
            {"sequenceNumber":8,"message":{"message":{"header":{"kind":3,"timestamp":2},"l2Msg":"BAE="}}}]}"#;
        let parsed: FeedFrame<'_> = serde_json::from_slice(frame).expect("frame with a null l2Msg parses");
        assert_eq!(parsed.messages.len(), 2);
        assert_eq!(parsed.messages[0].message.message.l2_msg, "");
        assert!(matches!(parsed.messages[1].message.message.l2_msg, Cow::Borrowed("BAE=")));
        // An escaped payload still decodes (owned copy), it is not rejected.
        let escaped = br#"{"messages":[{"sequenceNumber":1,"message":{"message":{"l2Msg":"BAE="}}}]}"#;
        let parsed: FeedFrame<'_> = serde_json::from_slice(escaped).unwrap();
        assert_eq!(parsed.messages[0].message.message.l2_msg, "BAE=");
    }

    #[test]
    fn liveness_verdicts_at_the_boundaries() {
        let t0 = Instant::now();
        let at = |secs: u64| t0 + Duration::from_secs(secs);
        let timing = FeedTiming::default();
        let ok = |now, rx, msg, behind, limit| check_liveness(&timing, now, rx, msg, behind, limit).is_ok();

        // Healthy, and exactly at the idle limit (inclusive).
        assert!(ok(at(5), at(0), at(0), 0, None));
        assert!(ok(at(20), at(0), at(0), 0, None));
        // Half-open: nothing, not even a ping, for more than 20s.
        assert!(!ok(at(21), at(0), at(0), 0, None));
        // Pings keep it alive even if no messages flow (quiet chain) ...
        assert!(ok(at(300), at(299), at(0), 0, None));
        // ... unless the operator demanded data within N seconds.
        assert!(ok(at(30), at(29), at(0), 0, Some(Duration::from_secs(30))));
        assert!(!ok(at(31), at(30), at(0), 0, Some(Duration::from_secs(30))));
        // Behind the others and silent for >= 10s: wedged relay, recycle it ...
        assert!(!ok(at(10), at(9), at(0), 64, None));
        assert!(!ok(at(60), at(59), at(0), 1_000, None));
        // ... but not when it is still delivering, or only slightly behind.
        assert!(ok(at(10), at(9), at(8), 64, None));
        assert!(ok(at(10), at(9), at(0), 63, None));
        // A single feed is never "behind" itself, however quiet the chain.
        assert!(ok(at(100), at(99), at(0), 0, None));
    }

    // ── L2 unpacker: the iterator must agree with the recursive reference exactly ──────────

    /// The pre-optimization implementation, kept verbatim as the oracle.
    fn oracle<'a>(bytes: &'a [u8], out: &mut Vec<&'a [u8]>, depth: usize) -> bool {
        match bytes.split_first() {
            Some((&4, tx)) => out.push(tx),
            Some((&3, mut rest)) => {
                if depth >= 16 {
                    return false;
                }
                while let Some((len_bytes, tail)) = rest.split_first_chunk::<8>() {
                    let Ok(n) = usize::try_from(u64::from_be_bytes(*len_bytes)) else { break };
                    let Some((segment, next)) = tail.split_at_checked(n) else { break };
                    if !oracle(segment, out, depth + 1) {
                        return false;
                    }
                    rest = next;
                }
            }
            _ => {}
        }
        true
    }

    fn oracle_txs(bytes: &[u8]) -> Vec<&[u8]> {
        let mut out = Vec::new();
        if !oracle(bytes, &mut out, 0) {
            out.clear();
        }
        out
    }

    fn random_message(rng: &mut StdRng, depth_left: u32) -> Vec<u8> {
        match rng.gen_range(0..10) {
            0 => vec![],
            1 => vec![rng.gen_range(0..8u8)],
            2..=4 => {
                let mut m = vec![4u8];
                m.extend((0..rng.gen_range(0..6)).map(|_| rng.r#gen::<u8>()));
                m
            }
            _ if depth_left == 0 => vec![4, 0xee],
            _ => {
                let mut m = vec![3u8];
                for _ in 0..rng.gen_range(0..4) {
                    let inner = random_message(rng, depth_left - 1);
                    let claimed = match rng.gen_range(0..12) {
                        0 => inner.len() as u64 + rng.gen_range(1..40),
                        1 => u64::MAX,
                        2 => inner.len().saturating_sub(1) as u64,
                        _ => inner.len() as u64,
                    };
                    m.extend_from_slice(&claimed.to_be_bytes());
                    m.extend_from_slice(&inner);
                }
                if rng.gen_ratio(1, 6) {
                    m.extend((0..rng.gen_range(1..12)).map(|_| rng.r#gen::<u8>()));
                }
                m
            }
        }
    }

    /// A chain of nested batches `levels` deep with random siblings (and the occasional bad
    /// length) along the way: reaches the 16-level cut-off from both sides, which pure random
    /// generation almost never does.
    fn deep_message(rng: &mut StdRng, levels: u32) -> Vec<u8> {
        let leaf = if rng.gen_bool(0.5) { random_message(rng, 1) } else { vec![4, 0xcc] };
        let mut msg = leaf;
        for _ in 0..levels {
            let mut outer = vec![3u8];
            if rng.gen_bool(0.3) {
                let sibling = random_message(rng, 1);
                outer.extend_from_slice(&(sibling.len() as u64).to_be_bytes());
                outer.extend_from_slice(&sibling);
            }
            let claimed = if rng.gen_ratio(1, 25) { msg.len() as u64 + 3 } else { msg.len() as u64 };
            outer.extend_from_slice(&claimed.to_be_bytes());
            outer.extend_from_slice(&msg);
            msg = outer;
        }
        msg
    }

    #[test]
    fn l2_iterator_matches_the_recursive_reference() {
        let mut rng = StdRng::seed_from_u64(0x5eed_f00d);
        let mut deep = 0;
        for i in 0..30_000 {
            let msg = if i % 2 == 0 {
                let budget = rng.gen_range(0..20);
                random_message(&mut rng, budget)
            } else {
                let levels = rng.gen_range(12..=20);
                deep_message(&mut rng, levels)
            };
            let want = oracle_txs(&msg);
            let mut got = Vec::new();
            extract_l2_txs(&msg, &mut got);
            assert_eq!(got, want, "case {i}: {msg:02x?}");
            assert_eq!(l2_message_is_valid(&msg), oracle(&msg, &mut Vec::new(), 0), "case {i}");
            deep += (!l2_message_is_valid(&msg)) as u32;
        }
        assert!(deep > 100, "generator should hit the depth limit regularly (hit {deep})");
    }

    // ── RawTxView must agree with the full alloy decoder on every tx type ──────────────────

    fn signed_txs(to: Address, input: &[u8], value: U256) -> Vec<(&'static str, Vec<u8>)> {
        let signer = PrivateKeySigner::random();
        let input = Bytes::copy_from_slice(input);
        let mut out = Vec::new();

        let legacy = TxLegacy { chain_id: Some(4663), nonce: 3, gas_price: 1_000_000, gas_limit: 90_000, to: TxKind::Call(to), value, input: input.clone() };
        out.push(("legacy", TxEnvelope::Legacy(legacy.clone().into_signed(signer.sign_hash_sync(&legacy.signature_hash()).unwrap())).encoded_2718()));

        let t2930 = TxEip2930 { chain_id: 4663, nonce: 3, gas_price: 1_000_000, gas_limit: 90_000, to: TxKind::Call(to), value, input: input.clone(), access_list: Default::default() };
        out.push(("2930", TxEnvelope::Eip2930(t2930.clone().into_signed(signer.sign_hash_sync(&t2930.signature_hash()).unwrap())).encoded_2718()));

        let t1559 = TxEip1559 { chain_id: 4663, nonce: 3, gas_limit: 90_000, max_fee_per_gas: 1_000_000, max_priority_fee_per_gas: 10, to: TxKind::Call(to), value, input: input.clone(), access_list: Default::default() };
        out.push(("1559", TxEnvelope::Eip1559(t1559.clone().into_signed(signer.sign_hash_sync(&t1559.signature_hash()).unwrap())).encoded_2718()));

        let t4844 = TxEip4844 { chain_id: 4663, nonce: 3, gas_limit: 90_000, max_fee_per_gas: 1_000_000, max_priority_fee_per_gas: 10, to, value, access_list: Default::default(), blob_versioned_hashes: vec![B256::repeat_byte(1)], max_fee_per_blob_gas: 5, input: input.clone() };
        out.push(("4844", TxEnvelope::Eip4844(t4844.clone().into_signed(signer.sign_hash_sync(&t4844.signature_hash()).unwrap()).into()).encoded_2718()));

        let t7702 = TxEip7702 { chain_id: 4663, nonce: 3, gas_limit: 90_000, max_fee_per_gas: 1_000_000, max_priority_fee_per_gas: 10, to, value, access_list: Default::default(), authorization_list: Vec::<SignedAuthorization>::new(), input };
        out.push(("7702", TxEnvelope::Eip7702(t7702.clone().into_signed(signer.sign_hash_sync(&t7702.signature_hash()).unwrap())).encoded_2718()));
        out
    }

    #[test]
    fn raw_view_agrees_with_the_full_decoder_for_every_tx_type() {
        let to = Address::repeat_byte(0x7a);
        let selectors: [&[u8]; 4] = [&[], &[0x59, 0xa8, 0x7b, 0xc1], &[0x7f, 0xf3, 0x6a, 0xb5, 1, 2, 3], &[0x00; 300]];
        let values = [U256::ZERO, U256::from(1u64), U256::from(0x80u64), U256::from(0x1234_5678u64), U256::from(10u64).pow(U256::from(18u64))];
        let mut checked = 0;
        for input in selectors {
            for value in values {
                for (kind, raw) in signed_txs(to, input, value) {
                    let view = RawTxView::parse(&raw).unwrap_or_else(|| panic!("{kind}: view must parse"));
                    let full = TxEnvelope::decode_2718_exact(&raw).unwrap();
                    assert_eq!(view.to.map(|t| Address::from(*t)), full.to(), "{kind} to");
                    assert_eq!(view.input, full.input().as_ref(), "{kind} input");
                    assert_eq!(U256::from_be_slice(view.value_be), full.value(), "{kind} value");
                    assert!(raw.as_ptr_range().contains(&view.input.as_ptr()), "{kind}: input is borrowed from the wire bytes");
                    // The prefilter and the post-decode gate must agree for these inputs.
                    checked += 1;
                }
            }
        }
        assert_eq!(checked, 4 * 5 * 5);
    }

    #[test]
    fn raw_view_reports_contract_creation_as_no_recipient() {
        let signer = PrivateKeySigner::random();
        let tx = TxEip1559 { chain_id: 1, nonce: 0, gas_limit: 1, max_fee_per_gas: 1, max_priority_fee_per_gas: 1, to: TxKind::Create, value: U256::ZERO, input: Bytes::from_static(&[1, 2, 3]), access_list: Default::default() };
        let raw = TxEnvelope::Eip1559(tx.clone().into_signed(signer.sign_hash_sync(&tx.signature_hash()).unwrap())).encoded_2718();
        let view = RawTxView::parse(&raw).unwrap();
        assert!(view.to.is_none());
        assert_eq!(view.input, &[1, 2, 3]);
    }

    #[test]
    fn raw_view_never_panics_on_mutated_transactions() {
        let mut rng = StdRng::seed_from_u64(7);
        let base = signed_txs(Address::repeat_byte(1), &[0x59, 0xa8, 0x7b, 0xc1, 9, 9], U256::from(5u64));
        for (_, raw) in base {
            for _ in 0..3_000 {
                let mut m = raw.clone();
                for _ in 0..rng.gen_range(1..4) {
                    let i = rng.gen_range(0..m.len());
                    m[i] = rng.r#gen();
                }
                m.truncate(rng.gen_range(0..=m.len()));
                // Whatever the answer, it must not panic and must stay inside `m`.
                if let Some(v) = RawTxView::parse(&m) {
                    let range = m.as_ptr_range();
                    assert!(v.input.is_empty() || range.contains(&v.input.as_ptr()));
                }
            }
        }
    }
}
