//! In-memory replica of Pons bonding-curve reserves, driven by the sequencer feed.
//!
//! # Why
//! The 100 ms risk loop used to value every position with an `eth_call getReserves()` per tick
//! (600 calls per minute per position, 50-200 ms each, exposed to HTTP 429). The feed already
//! shows every trade, usually before RPC nodes have even executed its block.
//!
//! # What the feed can and cannot give
//! A Nitro sequencer feed carries *transactions*, not receipts or event logs. A curve trade is
//! therefore applied by **simulating** its calldata (`buy` / `sell`) on the local reserves with the
//! same integer curve math the RPC path uses. The result is exact as long as (a) every trade on the
//! curve is a direct call we can decode, (b) we predict correctly whether it reverts and (c) the
//! fee + creator tax are right. None of that can be *proven* from the feed alone, so the replica
//! is built as **anchor + journal**:
//!
//! * an *anchor* is an exact `getReserves()` reading pinned to an explicit block number `B`
//!   (`eth_call` at `B`, never `latest`, so it is known which trades it already contains);
//! * the *journal* holds every decoded trade of blocks `> B`, ordered by `(block, index)`;
//! * the state is the anchor replayed through the journal, recomputed whenever a trade arrives out
//!   of order (racing feeds), so the order of arrival never changes the result.
//!
//! Anchors are taken when a curve starts being tracked, whenever the replica cannot vouch for
//! itself (a missed block, a block we could not inspect, a call into the curve we cannot decode, a
//! contract that mentions the curve, a borderline revert prediction, a reorg, a reverted exit), and
//! periodically (`verify_every`). Each anchor measures the drift of the prediction it replaces and,
//! from the trades between two anchors, calibrates the curve's real fee + creator tax.
//!
//! # Freshness
//! A read is `Fresh` only if the curve is anchored, nothing is suspect, the feed is alive (a block
//! within `stale_after`; every block the replica sees is an implicit "no change" event for curves it
//! does not touch) and no missing block above the anchor is older than `gap_grace`. Otherwise the
//! read is `Stale` and the caller falls back to RPC, exactly as before the replica existed.

use crate::addr_set::AddressSet;
use crate::dispatcher::BroadcasterPool;
use crate::payload::{
    CalldataPayloadBuilder, ERC20_APPROVE_SELECTOR, PONS_ASSUMED_CREATOR_TAX_BPS, PONS_BASE_FEE_BPS, PONS_BUY_SELECTOR,
    PONS_INITIAL_TOKEN_RESERVE_WEI, PONS_SELL_SELECTOR,
};
use crate::stream::{l2_message_is_fully_visible, l2_message_is_valid, L2Txs, RawTxView};
use alloy::primitives::{keccak256, Address, B256, U256};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{watch, Notify};
use tracing::{debug, error, info, warn};

/// Block timestamps remembered for the snipe-tax schedule.
const TS_RING: usize = 8192;
/// Recent price points kept per curve for the volatility estimate.
const PRICE_POINTS: usize = 16;
/// Open gaps kept individually; beyond this they are merged into one lost range.
const MAX_HOLES: usize = 64;
/// Launch blocks remembered for curves that are not (yet) tracked.
const MAX_LAUNCHES: usize = 1024;
/// Largest fee + tax the calibration considers (base fee 1% + creator tax capped at 10%).
const MAX_CALIBRATED_BPS: u32 = 2_000;
/// Verification cadence for a curve whose reserves broke the constant-product invariant between
/// two anchors (the trade model does not describe it: re-anchor much more often).
const SUSPECT_MODEL_VERIFY: Duration = Duration::from_secs(2);
/// Cadence of the feed / RPC block-numbering cross-check.
const NUMBERING_CHECK_EVERY: Duration = Duration::from_secs(60);
/// Pause between two attempts of one pinned read while the node has not imported the block yet.
const ANCHOR_RETRY_STEP: Duration = Duration::from_millis(100);
/// Ceiling of the per-curve back-off after failed anchors (a pool whose `getReserves` reverts).
const MAX_ANCHOR_BACKOFF: Duration = Duration::from_secs(30);
/// Until the block numbering is verified exactly, it is re-checked this often.
const NUMBERING_PROBE_EVERY: Duration = Duration::from_millis(500);
/// Recent (block, first tx hash) samples kept for the numbering probe, one per this many blocks.
const PROBES: usize = 8;
const PROBE_EVERY_BLOCKS: u64 = 16;

/// Tuning of the replica. The defaults are production values.
#[derive(Clone, Copy, Debug)]
pub struct MirrorPolicy {
    pub enabled: bool,
    /// No feed block for this long: every curve is stale (the RPC fallback takes over).
    pub stale_after: Duration,
    /// A missing block may still arrive from a slower feed; after this it counts as lost.
    pub gap_grace: Duration,
    /// Exact re-anchor of every tracked curve at least this often, even when nothing looks wrong.
    pub verify_every: Duration,
    /// Minimum spacing between two anchor attempts of one curve.
    pub anchor_retry: Duration,
    /// One pinned read waits at most this long for the node to import the block.
    pub anchor_timeout: Duration,
    /// Anchor this many blocks below the feed head (the node usually trails the feed slightly).
    pub anchor_lag_blocks: u64,
    /// Trades kept per curve between two anchors; more forces a re-anchor.
    pub max_journal: usize,
    /// Price-driven exits computed on unconfirmed (feed-predicted) state are re-checked against an
    /// exact reading pinned to the trigger block before anything is sold.
    pub confirm_exits: bool,
    /// A trade whose predicted output is this close (bps) to its minimum may go either way: the
    /// curve becomes suspect until the next anchor.
    pub borderline_bps: u32,
    /// Calldata bytes searched for a tracked curve's address (indirect calls through routers).
    pub indirect_scan_bytes: usize,
    /// Feed and RPC block numbers further apart than this disable the replica (`FEED_GENESIS_BLOCK`
    /// wrong, or a node far behind): a pinned read would then target the wrong block.
    pub numbering_tolerance: u64,
}

impl Default for MirrorPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            stale_after: Duration::from_millis(2_000),
            gap_grace: Duration::from_millis(500),
            verify_every: Duration::from_secs(30),
            anchor_retry: Duration::from_millis(200),
            anchor_timeout: Duration::from_secs(3),
            anchor_lag_blocks: 1,
            max_journal: 1_024,
            confirm_exits: true,
            borderline_bps: 10,
            indirect_scan_bytes: 4_096,
            numbering_tolerance: 2_000,
        }
    }
}

impl MirrorPolicy {
    /// `MIRROR_ENABLED`, `MIRROR_STALE_AFTER_MS`, `MIRROR_GAP_GRACE_MS`, `MIRROR_VERIFY_SECS`,
    /// `MIRROR_CONFIRM_EXITS`, `MIRROR_ANCHOR_LAG_BLOCKS`.
    pub fn from_env() -> Self {
        let mut p = Self::default();
        let flag = |key: &str| {
            std::env::var(key)
                .ok()
                .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off" | "no"))
        };
        let num = |key: &str| std::env::var(key).ok().and_then(|v| v.trim().parse::<u64>().ok());
        if let Some(on) = flag("MIRROR_ENABLED") {
            p.enabled = on;
        }
        if let Some(on) = flag("MIRROR_CONFIRM_EXITS") {
            p.confirm_exits = on;
        }
        if let Some(ms) = num("MIRROR_STALE_AFTER_MS") {
            p.stale_after = Duration::from_millis(ms.max(100));
        }
        if let Some(ms) = num("MIRROR_GAP_GRACE_MS") {
            p.gap_grace = Duration::from_millis(ms);
        }
        if let Some(s) = num("MIRROR_VERIFY_SECS") {
            p.verify_every = Duration::from_secs(s.max(1));
        }
        if let Some(n) = num("MIRROR_ANCHOR_LAG_BLOCKS") {
            p.anchor_lag_blocks = n;
        }
        p
    }
}

// ── Curve model ─────────────────────────────────────────────────────────────

/// Pons V2 launch-window buy tax by whole seconds since launch:
/// `snipeTaxStartBps >> (elapsed * 14 / snipeTaxSeconds)` with 9900 bps over 3 s, i.e. 9900, 618, 19, 0.
pub fn snipe_tax_bps(elapsed_secs: u64) -> u32 {
    if elapsed_secs >= 3 { 0 } else { 9_900u32 >> ((elapsed_secs * 14) / 3) }
}

/// Default fee + creator tax on a trade's quote side until the curve is calibrated.
pub const DEFAULT_TRADE_BPS: u32 = PONS_BASE_FEE_BPS + PONS_ASSUMED_CREATOR_TAX_BPS;

/// One buy on `(eth, tok)`: `(eth', tok', tokens_out)`. Fees and taxes are taken from the quote
/// before pricing and leave the reserves, so `eth * tok` is invariant (up to integer rounding).
/// Same rounding as [`CalldataPayloadBuilder::calculate_pons_tokens_out`].
///
/// Checked arithmetic throughout (`U256` operators wrap silently, and the amounts come from
/// untrusted calldata): `None` when any step would overflow.
pub fn curve_buy(eth: U256, tok: U256, quote_in: U256, total_bps: u32) -> Option<(U256, U256, U256)> {
    let tax = quote_in.checked_mul(U256::from(total_bps))? / U256::from(10_000u64);
    let net = quote_in.saturating_sub(tax);
    let denom = eth.checked_add(net)?;
    if denom.is_zero() {
        return Some((eth, tok, U256::ZERO));
    }
    let out = tok.checked_mul(net)? / denom;
    Some((denom, tok.checked_sub(out)?, out))
}

/// One sell on `(eth, tok)`: `(eth', tok', eth_out)`; the curve pays the gross amount, fee and tax
/// are withheld from it. Same rounding as [`CalldataPayloadBuilder::calculate_pons_eth_out`].
/// `None` when any step would overflow.
pub fn curve_sell(eth: U256, tok: U256, tokens_in: U256, total_bps: u32) -> Option<(U256, U256, U256)> {
    let denom = tok.checked_add(tokens_in)?;
    if denom.is_zero() {
        return Some((eth, tok, U256::ZERO));
    }
    let gross = eth.checked_mul(tokens_in)? / denom;
    let tax = gross.checked_mul(U256::from(total_bps))? / U256::from(10_000u64);
    Some((eth.checked_sub(gross)?, denom, gross.saturating_sub(tax)))
}

/// Nobody can hold more than the whole supply: a sell of more certainly reverts.
const MAX_SELLABLE: u128 = PONS_INITIAL_TOKEN_RESERVE_WEI;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TradeKind {
    /// `buy(ethIn, minTokensOut, recipient)` with `msg.value = ethIn`.
    Buy { quote_in: U256, min_out: U256 },
    /// `sell(tokensIn, minEthOut, recipient)`.
    Sell { tokens_in: U256, min_out: U256 },
}

/// A decoded curve call, positioned in the chain's order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trade {
    pub block: u64,
    /// Position of the transaction inside its block's L2 message.
    pub index: u32,
    pub kind: TradeKind,
}

impl Trade {
    fn key(&self) -> (u64, u32) {
        (self.block, self.index)
    }
}

/// Result of replaying a journal on a base state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayOutcome {
    pub eth: U256,
    pub tok: U256,
    /// Trades predicted to execute.
    pub executed: u32,
    /// Trades predicted to revert on their minimum output (not applied).
    pub reverted: u32,
    /// First block holding a trade whose outcome is too close to call (or that would empty the curve).
    pub uncertain_block: Option<u64>,
}

/// Replay `trades` (sorted) on `(eth, tok)`. `snipe(block)` gives the launch-window buy tax.
pub fn replay(
    mut eth: U256,
    mut tok: U256,
    trades: &[Trade],
    total_bps: u32,
    borderline_bps: u32,
    snipe: impl Fn(u64) -> u32,
) -> ReplayOutcome {
    let mut out = ReplayOutcome { eth, tok, executed: 0, reverted: 0, uncertain_block: None };
    let close_call = |got: U256, min: U256| {
        // |got - min| <= min * borderline / 10_000
        let diff = if got > min { got - min } else { min - got };
        !min.is_zero() && diff * U256::from(10_000u64) <= min * U256::from(borderline_bps)
    };
    for t in trades {
        match t.kind {
            TradeKind::Buy { quote_in, min_out } => {
                // Overflow: no sane trade does that; it cannot be modelled, so it is not applied
                // and the curve becomes suspect (the next exact reading settles it).
                let Some((e2, t2, got)) = curve_buy(eth, tok, quote_in, total_bps.saturating_add(snipe(t.block))) else {
                    out.uncertain_block.get_or_insert(t.block);
                    out.reverted += 1;
                    continue;
                };
                if close_call(got, min_out) || got >= tok {
                    out.uncertain_block.get_or_insert(t.block);
                }
                if got < min_out || got >= tok {
                    out.reverted += 1;
                    continue;
                }
                (eth, tok) = (e2, t2);
            }
            TradeKind::Sell { tokens_in, min_out } => {
                if tokens_in > U256::from(MAX_SELLABLE) {
                    out.reverted += 1; // certain: more than the supply
                    continue;
                }
                let Some((e2, t2, got)) = curve_sell(eth, tok, tokens_in, total_bps) else {
                    out.uncertain_block.get_or_insert(t.block);
                    out.reverted += 1;
                    continue;
                };
                if close_call(got, min_out) {
                    out.uncertain_block.get_or_insert(t.block);
                }
                if got < min_out {
                    out.reverted += 1;
                    continue;
                }
                (eth, tok) = (e2, t2);
            }
        }
        out.executed += 1;
    }
    out.eth = eth;
    out.tok = tok;
    out
}

// ── Public read side ────────────────────────────────────────────────────────

/// A curve state the replica vouches for.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CurveSnapshot {
    pub curve: Address,
    pub eth_reserve: U256,
    pub token_reserve: U256,
    /// Highest feed block seen when the snapshot was taken.
    pub as_of_block: u64,
    /// Block of the exact reading this state is built on.
    pub anchor_block: u64,
    /// Trades predicted on top of the anchor (0: this *is* the chain state at `anchor_block`).
    pub unconfirmed_trades: u32,
    /// Fee + creator tax withheld from a sell (calibrated, else [`DEFAULT_TRADE_BPS`]).
    pub exit_fee_bps: u32,
    /// Largest one-step adverse price move among the recent price points, in bps.
    pub volatility_bps: u32,
    /// Valuations of blocks in `(validated_from, validated_through]` rest only on trades that a
    /// chain of exact readings confirmed to the wei (a trailing peak set there was real). A reading
    /// that missed its prediction, or the first reading after a reorg, starts a new, empty range.
    pub validated_from: u64,
    pub validated_through: u64,
}

impl CurveSnapshot {
    /// ETH a sell of `tokens` would return on this state, after fee and tax. The very same integer
    /// function the RPC path uses, so equal reserves give a bit-identical valuation.
    #[inline]
    pub fn value_of(&self, tokens: U256) -> U256 {
        CalldataPayloadBuilder::calculate_pons_eth_out(self.eth_reserve, self.token_reserve, tokens, self.exit_fee_bps, 0)
    }

    pub fn reserves(&self) -> (U256, U256) {
        (self.eth_reserve, self.token_reserve)
    }
}

/// Why a read cannot be served from memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaleReason {
    Disabled,
    Untracked,
    /// Tracked, no exact reading yet.
    NoAnchor,
    /// No feed block within `stale_after`.
    FeedSilent,
    /// A block above the anchor is missing (or could not be inspected) beyond `gap_grace`.
    Gap,
    /// A transaction touched the curve in a way the replica cannot decode.
    Indirect,
    /// A trade's revert prediction was too close to call.
    Uncertain,
    /// More trades than `max_journal` since the last anchor.
    JournalFull,
    /// The feed delivered different content for a block already processed.
    Reorg,
    /// Our own sell reverted: the replica was too optimistic.
    ExitReverted,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MirrorRead {
    Fresh(CurveSnapshot),
    Stale(StaleReason),
}

impl MirrorRead {
    pub fn fresh(self) -> Option<CurveSnapshot> {
        match self {
            MirrorRead::Fresh(s) => Some(s),
            MirrorRead::Stale(_) => None,
        }
    }
}

/// Counters of the replica's life (all monotonic except the gauges marked so).
#[derive(Debug, Default)]
pub struct MirrorStats {
    pub blocks: AtomicU64,
    pub trades_applied: AtomicU64,
    pub trades_reverted: AtomicU64,
    pub uncertain: AtomicU64,
    pub indirect_touches: AtomicU64,
    pub gaps: AtomicU64,
    pub uninspected_blocks: AtomicU64,
    pub reorgs: AtomicU64,
    pub anchors_ok: AtomicU64,
    pub anchors_failed: AtomicU64,
    /// Anchors whose prediction differed from the chain by at least 1 wei.
    pub drift_corrections: AtomicU64,
    /// Largest prediction error seen, in parts per billion of the ETH reserve (gauge, max).
    pub max_drift_ppb: AtomicU64,
    pub calibrations: AtomicU64,
    /// Anchor pairs whose `eth * tok` moved beyond rounding (trade model does not fit the curve).
    pub invariant_breaks: AtomicU64,
    pub reads_fresh: AtomicU64,
    pub reads_stale: AtomicU64,
    pub exit_confirmations: AtomicU64,
    pub phantom_exits_rejected: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MirrorStatsSnapshot {
    pub blocks: u64,
    pub trades_applied: u64,
    pub trades_reverted: u64,
    pub uncertain: u64,
    pub indirect_touches: u64,
    pub gaps: u64,
    pub uninspected_blocks: u64,
    pub reorgs: u64,
    pub anchors_ok: u64,
    pub anchors_failed: u64,
    pub drift_corrections: u64,
    pub max_drift_ppb: u64,
    pub calibrations: u64,
    pub invariant_breaks: u64,
    pub reads_fresh: u64,
    pub reads_stale: u64,
    pub exit_confirmations: u64,
    pub phantom_exits_rejected: u64,
}

impl MirrorStats {
    fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MirrorStatsSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        MirrorStatsSnapshot {
            blocks: l(&self.blocks),
            trades_applied: l(&self.trades_applied),
            trades_reverted: l(&self.trades_reverted),
            uncertain: l(&self.uncertain),
            indirect_touches: l(&self.indirect_touches),
            gaps: l(&self.gaps),
            uninspected_blocks: l(&self.uninspected_blocks),
            reorgs: l(&self.reorgs),
            anchors_ok: l(&self.anchors_ok),
            anchors_failed: l(&self.anchors_failed),
            drift_corrections: l(&self.drift_corrections),
            max_drift_ppb: l(&self.max_drift_ppb),
            calibrations: l(&self.calibrations),
            invariant_breaks: l(&self.invariant_breaks),
            reads_fresh: l(&self.reads_fresh),
            reads_stale: l(&self.reads_stale),
            exit_confirmations: l(&self.exit_confirmations),
            phantom_exits_rejected: l(&self.phantom_exits_rejected),
        }
    }
}

// ── Internal state ──────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
struct Base {
    block: u64,
    eth: U256,
    tok: U256,
    at: Instant,
}

#[derive(Debug)]
struct Slot {
    refs: u32,
    /// Head when tracking began: trades of blocks up to here are covered by the first anchor.
    tracked_since: u64,
    base: Option<Base>,
    /// Trades of blocks `> floor()`, sorted by `(block, index)`.
    journal: Vec<Trade>,
    /// `base` replayed through `journal`.
    state: Option<ReplayOutcome>,
    suspect: Option<StaleReason>,
    /// An anchor clears `suspect` only if it is at least this recent.
    min_anchor_block: u64,
    inflight: bool,
    last_attempt: Option<Instant>,
    /// Consecutive failed anchors (exponential back-off).
    failures: u32,
    /// Calibrated fee + creator tax.
    trade_bps: Option<u32>,
    /// `eth * tok` moved between two anchors: verify much more often.
    model_suspect: bool,
    /// Contiguous range of blocks whose predicted trades exact readings confirmed (see `CurveSnapshot`).
    validated_from: u64,
    validated_through: u64,
    launch_block: Option<u64>,
    prices: VecDeque<(u64, f64)>,
}

impl Slot {
    fn new(tracked_since: u64, launch_block: Option<u64>) -> Self {
        Self {
            refs: 1,
            tracked_since,
            base: None,
            journal: Vec::new(),
            state: None,
            suspect: None,
            min_anchor_block: 0,
            inflight: false,
            last_attempt: None,
            failures: 0,
            trade_bps: None,
            model_suspect: false,
            validated_from: 0,
            validated_through: 0,
            launch_block,
            prices: VecDeque::with_capacity(PRICE_POINTS),
        }
    }

    /// Blocks up to here are accounted for (by the anchor, or by the first anchor to come).
    fn floor(&self) -> u64 {
        self.base.map_or(self.tracked_since, |b| b.block)
    }

    fn bps(&self) -> u32 {
        self.trade_bps.unwrap_or(DEFAULT_TRADE_BPS)
    }

    fn mark_suspect(&mut self, reason: StaleReason, at_block: u64) {
        if self.suspect.is_none() {
            self.suspect = Some(reason);
        }
        self.min_anchor_block = self.min_anchor_block.max(at_block);
    }

    fn record_price(&mut self, block: u64) {
        let Some(st) = self.state else { return };
        if st.tok.is_zero() {
            return;
        }
        let price = f64::from(st.eth) / f64::from(st.tok);
        match self.prices.back_mut() {
            Some((b, p)) if *b == block => *p = price,
            _ => {
                if self.prices.len() == PRICE_POINTS {
                    self.prices.pop_front();
                }
                self.prices.push_back((block, price));
            }
        }
    }

    fn volatility_bps(&self) -> u32 {
        self.prices
            .iter()
            .zip(self.prices.iter().skip(1))
            .map(|((_, a), (_, b))| if *a > 0.0 && b < a { ((a - b) / a * 10_000.0) as u32 } else { 0 })
            .max()
            .unwrap_or(0)
    }
}

#[derive(Clone, Copy, Debug)]
struct Hole {
    start: u64,
    end: u64,
    since: Instant,
    /// Will never be filled (block delivered but not inspectable).
    lost: bool,
}

struct State {
    slots: HashMap<Address, Slot>,
    head: u64,
    last_block_at: Option<Instant>,
    holes: Vec<Hole>,
    ts: Box<[(u64, u64)]>,
    launches: HashMap<Address, u64>,
    /// Feed and RPC heads too far apart (may recover: a node catching up).
    skew_disabled: Option<String>,
    /// A feed block's transaction is not in the RPC block of that number (FEED_GENESIS_BLOCK off).
    probe_disabled: Option<String>,
    numbering_checked: Option<Instant>,
    numbering_verified: bool,
    /// Consecutive probe blocks whose sampled tx the RPC block lacked.
    probe_misses: u32,
    last_probe_block: u64,
}

impl State {
    fn disabled(&self) -> bool {
        self.skew_disabled.is_some() || self.probe_disabled.is_some()
    }

    fn ts_of(&self, block: u64) -> Option<u64> {
        let (b, ts) = self.ts[(block as usize) % TS_RING];
        (b == block && ts != 0).then_some(ts)
    }

    fn add_hole(&mut self, start: u64, end: u64, since: Instant, lost: bool) {
        if self.holes.len() >= MAX_HOLES {
            let lo = self.holes.iter().map(|h| h.start).min().unwrap_or(start).min(start);
            let hi = self.holes.iter().map(|h| h.end).max().unwrap_or(end).max(end);
            self.holes.clear();
            self.holes.push(Hole { start: lo, end: hi, since, lost: true });
            return;
        }
        self.holes.push(Hole { start, end, since, lost });
    }

    /// `block` arrived after all: remove it from the gap it was part of.
    fn fill_hole(&mut self, block: u64) {
        let Some(i) = self.holes.iter().position(|h| !h.lost && h.start <= block && block <= h.end) else { return };
        let h = self.holes.remove(i);
        if h.start < block {
            self.holes.push(Hole { end: block - 1, ..h });
        }
        if block < h.end {
            self.holes.push(Hole { start: block + 1, ..h });
        }
    }

    /// Gaps nobody cares about any more (below every tracked curve's floor).
    fn prune_holes(&mut self) {
        match self.slots.values().map(Slot::floor).min() {
            Some(floor) => self.holes.retain(|h| h.end > floor),
            None => self.holes.clear(),
        }
    }

    /// Highest block of a gap above `floor` that is past its grace (or lost).
    fn lost_gap_above(&self, floor: u64, now: Instant, grace: Duration) -> Option<u64> {
        self.holes
            .iter()
            .filter(|h| h.end > floor && (h.lost || now.saturating_duration_since(h.since) >= grace))
            .map(|h| h.end)
            .max()
    }

    fn snipe_for(&self, launch_block: Option<u64>) -> impl Fn(u64) -> u32 + '_ {
        let launch_ts = launch_block.and_then(|b| self.ts_of(b));
        move |block| match (launch_ts, self.ts_of(block)) {
            (Some(l), Some(t)) => snipe_tax_bps(t.saturating_sub(l)),
            _ => 0,
        }
    }

    /// Recompute `slot`'s state from its anchor and journal.
    fn recompute(&self, slot: &mut Slot, borderline_bps: u32) -> Option<u64> {
        let base = slot.base?;
        let snipe = self.snipe_for(slot.launch_block);
        let out = replay(base.eth, base.tok, &slot.journal, slot.bps(), borderline_bps, snipe);
        slot.state = Some(out);
        out.uncertain_block
    }
}

struct Index {
    set: AddressSet,
    needles: Vec<[u8; 20]>,
}

/// The replica. Shared (`Arc`) by the feed (writer), the risk engine (reader) and its own anchor
/// worker (exact readings).
pub struct CurveMirror {
    policy: MirrorPolicy,
    state: Mutex<State>,
    /// Hot-path prefilter: tracked curves (lock held only for a probe / a short scan).
    index: RwLock<Index>,
    tracked: AtomicUsize,
    /// Bumped whenever a tracked curve's state changes: wakes the risk loop.
    changes: watch::Sender<u64>,
    /// Highest feed block: wakes receipt waits.
    head_tx: watch::Sender<u64>,
    anchor_wake: Notify,
    reader: OnceLock<Arc<BroadcasterPool>>,
    stats: MirrorStats,
    /// Curves whose state changed during the block being ingested.
    block_changed: AtomicU64,
    /// `(block, hash of its first signed tx)`: proves feed and RPC number blocks alike.
    probes: Mutex<VecDeque<(u64, B256)>>,
}

fn word(input: &[u8], i: usize) -> Option<U256> {
    input.get(4 + 32 * i..4 + 32 * (i + 1)).map(U256::from_be_slice)
}

impl CurveMirror {
    pub fn new(policy: MirrorPolicy) -> Self {
        Self {
            policy,
            state: Mutex::new(State {
                slots: HashMap::new(),
                head: 0,
                last_block_at: None,
                holes: Vec::new(),
                ts: vec![(0u64, 0u64); TS_RING].into_boxed_slice(),
                launches: HashMap::new(),
                skew_disabled: None,
                probe_disabled: None,
                numbering_checked: None,
                numbering_verified: false,
                probe_misses: 0,
                last_probe_block: 0,
            }),
            index: RwLock::new(Index { set: AddressSet::default(), needles: Vec::new() }),
            tracked: AtomicUsize::new(0),
            changes: watch::channel(0).0,
            head_tx: watch::channel(0).0,
            anchor_wake: Notify::new(),
            reader: OnceLock::new(),
            stats: MirrorStats::default(),
            block_changed: AtomicU64::new(0),
            probes: Mutex::new(VecDeque::with_capacity(PROBES)),
        }
    }

    pub fn policy(&self) -> &MirrorPolicy {
        &self.policy
    }

    pub fn stats(&self) -> MirrorStatsSnapshot {
        self.stats.snapshot()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Fires on every change of a tracked curve's state.
    pub fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    /// Fires once per new highest feed block.
    pub fn subscribe_head(&self) -> watch::Receiver<u64> {
        self.head_tx.subscribe()
    }

    pub fn head(&self) -> u64 {
        self.lock().head
    }

    pub fn tracked_count(&self) -> usize {
        self.tracked.load(Ordering::Acquire)
    }

    fn notify_change(&self) {
        self.changes.send_modify(|v| *v = v.wrapping_add(1));
    }

    fn rebuild_index(&self, st: &State) {
        let addrs: Vec<Address> = st.slots.keys().copied().collect();
        let set = AddressSet::new(addrs.iter());
        let needles = addrs.iter().map(|a| a.0 .0).collect();
        *self.index.write().unwrap_or_else(|e| e.into_inner()) = Index { set, needles };
        self.tracked.store(addrs.len(), Ordering::Release);
    }

    // ── Tracking ────────────────────────────────────────────────────────────

    /// Start (or add a reference to) the replica of `curve`. The first exact reading is requested
    /// right away; until it lands, reads are `Stale(NoAnchor)`.
    pub fn track(&self, curve: Address) {
        if !self.policy.enabled {
            return;
        }
        let mut st = self.lock();
        let head = st.head;
        let launch = st.launches.get(&curve).copied();
        let created = match st.slots.get_mut(&curve) {
            Some(slot) => {
                slot.refs += 1;
                false
            }
            None => {
                st.slots.insert(curve, Slot::new(head, launch));
                true
            }
        };
        if created {
            // Index writers run with the state lock held; the feed takes the index first and the
            // state only after releasing it, so the two locks are never held in opposite order.
            self.rebuild_index(&st);
            drop(st);
            self.anchor_wake.notify_one();
            debug!("[mirror] tracking curve {:?}", curve);
        }
    }

    /// Drop one reference; the replica is discarded with the last one.
    pub fn untrack(&self, curve: Address) {
        let mut st = self.lock();
        let gone = match st.slots.get_mut(&curve) {
            Some(slot) if slot.refs > 1 => {
                slot.refs -= 1;
                false
            }
            Some(_) => {
                st.slots.remove(&curve);
                true
            }
            None => false,
        };
        if gone {
            st.prune_holes();
            self.rebuild_index(&st);
        }
    }

    /// Launch block of `curve` (snipe-tax schedule of its first seconds).
    pub fn note_launch(&self, curve: Address, launch_block: u64) {
        let mut st = self.lock();
        if st.launches.len() >= MAX_LAUNCHES {
            st.launches.clear();
        }
        st.launches.insert(curve, launch_block);
        if let Some(slot) = st.slots.get_mut(&curve) {
            slot.launch_block = Some(launch_block);
        }
    }

    // ── Feed ingestion ──────────────────────────────────────────────────────

    /// A feed block begins. `inspected = false` means its transactions will not be shown to the
    /// replica (stale backlog, undecodable payload, a message kind that can carry L1-originated
    /// calls): it becomes a lost gap for every curve anchored below it.
    pub fn begin_block(&self, block: u64, timestamp_secs: u64, inspected: bool) {
        let now = Instant::now();
        let mut st = self.lock();
        st.ts[(block as usize) % TS_RING] = (block, timestamp_secs);
        // The grace is for racing feeds reordering blocks by milliseconds. A gap that opens after
        // the feed was quiet for longer than that is blocks nobody delivered: lost right away.
        let after_silence = st.last_block_at.is_some_and(|t| now.saturating_duration_since(t) >= self.policy.gap_grace);
        st.last_block_at = Some(now);
        let mut new_head = false;
        if st.head == 0 {
            st.head = block;
            new_head = true;
        } else if block > st.head {
            if block > st.head + 1 {
                let (start, end) = (st.head + 1, block - 1);
                st.add_hole(start, end, now, after_silence);
                MirrorStats::bump(&self.stats.gaps);
            }
            st.head = block;
            new_head = true;
        } else {
            st.fill_hole(block);
        }
        if !inspected {
            st.add_hole(block, block, now, true);
            MirrorStats::bump(&self.stats.uninspected_blocks);
        }
        MirrorStats::bump(&self.stats.blocks);
        let head = st.head;
        drop(st);
        if new_head {
            self.head_tx.send_replace(head);
        }
    }

    /// The block's transactions have all been shown: publish the changes.
    pub fn end_block(&self, _block: u64) {
        if self.block_changed.swap(0, Ordering::AcqRel) > 0 {
            self.notify_change();
        }
    }

    /// One raw transaction of the current block (borrowed straight from the feed buffer).
    /// Allocation-free; returns immediately while nothing is tracked.
    #[inline]
    pub fn observe_raw_tx(&self, block: u64, index: u32, raw: &[u8]) {
        if self.tracked.load(Ordering::Acquire) == 0 {
            return;
        }
        if index == 0 {
            // Numbering probe: one tx hash every 16 blocks, only while a curve is tracked.
            let mut probes = self.probes.lock().unwrap_or_else(|e| e.into_inner());
            if probes.back().is_none_or(|(b, _)| block >= b.saturating_add(PROBE_EVERY_BLOCKS) || block < *b) {
                if probes.len() == PROBES {
                    probes.pop_front();
                }
                probes.push_back((block, keccak256(raw)));
            }
        }
        let Some(view) = RawTxView::parse(raw) else { return };
        let value = if view.value_be.len() <= 32 { U256::from_be_slice(view.value_be) } else { U256::MAX };
        self.observe(block, index, view.to.map(|t| Address::from(*t)), value, view.input);
    }

    /// One decoded transaction of the current block (HTTP bridge, tests).
    pub fn observe(&self, block: u64, index: u32, to: Option<Address>, value: U256, input: &[u8]) {
        if self.tracked.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut hits: [Option<Address>; 8] = [None; 8];
        let mut n_hits = 0;
        // Beyond 8 matches in one tx (pathological): spill to the heap rather than miss one.
        let mut more_hits: Vec<Address> = Vec::new();
        let direct = {
            let idx = self.index.read().unwrap_or_else(|e| e.into_inner());
            match to {
                Some(to) if idx.set.contains(&to) => Some(to),
                _ => {
                    // Not a call to a tracked curve. An approval naming it is harmless; anything else
                    // that carries its address (router, aggregator, token transfer) may move it.
                    if input.first_chunk::<4>() != Some(&ERC20_APPROVE_SELECTOR) && !input.is_empty() {
                        let scan = &input[..input.len().min(self.policy.indirect_scan_bytes)];
                        for needle in &idx.needles {
                            if memchr::memmem::find(scan, needle).is_some() {
                                if n_hits < hits.len() {
                                    hits[n_hits] = Some(Address::from(*needle));
                                    n_hits += 1;
                                } else {
                                    more_hits.push(Address::from(*needle));
                                }
                            }
                        }
                    }
                    None
                }
            }
        };
        if let Some(curve) = direct {
            let trade = match input.first_chunk::<4>() {
                Some(sel) if *sel == PONS_BUY_SELECTOR => match (word(input, 0), word(input, 1)) {
                    // The curve takes msg.value; a call whose ethIn disagrees may revert: suspect.
                    (Some(eth_in), Some(min_out)) if eth_in == value => Some(TradeKind::Buy { quote_in: value, min_out }),
                    _ => None,
                },
                Some(sel) if *sel == PONS_SELL_SELECTOR => match (word(input, 0), word(input, 1)) {
                    (Some(tokens_in), Some(min_out)) if value.is_zero() => Some(TradeKind::Sell { tokens_in, min_out }),
                    _ => None,
                },
                // A view call cannot arrive as a transaction; any other call may change the state.
                _ => None,
            };
            match trade {
                Some(kind) => self.push_trade(curve, Trade { block, index, kind }),
                None => self.touch(curve, block, StaleReason::Indirect),
            }
        }
        for curve in hits.iter().take(n_hits).flatten().chain(more_hits.iter()) {
            self.touch(*curve, block, StaleReason::Indirect);
        }
    }

    /// Walk a whole L2 message (tests and tools; the stream calls `observe_raw_tx` from its own walk).
    pub fn ingest_l2_message(&self, block: u64, timestamp_secs: u64, l2: &[u8]) {
        self.begin_block(block, timestamp_secs, l2.is_empty() || l2_message_is_fully_visible(l2));
        if !l2.is_empty() && l2_message_is_valid(l2) {
            for (i, raw) in L2Txs::new(l2).enumerate() {
                self.observe_raw_tx(block, i as u32, raw);
            }
        }
        self.end_block(block);
    }

    fn touch(&self, curve: Address, block: u64, reason: StaleReason) {
        let mut st = self.lock();
        if let Some(slot) = st.slots.get_mut(&curve)
            && block > slot.floor()
        {
            slot.mark_suspect(reason, block);
            MirrorStats::bump(&self.stats.indirect_touches);
            drop(st);
            self.block_changed.fetch_add(1, Ordering::AcqRel);
            self.anchor_wake.notify_one();
        }
    }

    fn push_trade(&self, curve: Address, trade: Trade) {
        let borderline = self.policy.borderline_bps;
        let mut st = self.lock();
        let st = &mut *st;
        let Some(mut slot) = st.slots.remove(&curve) else { return };
        if trade.block > slot.floor() {
            let pos = slot.journal.partition_point(|t| t.key() < trade.key());
            let duplicate = slot.journal.get(pos).is_some_and(|t| t.key() == trade.key());
            if !duplicate {
                let appended = pos == slot.journal.len();
                slot.journal.insert(pos, trade);
                if slot.journal.len() > self.policy.max_journal {
                    // Bounded memory: forget the journal, require an anchor beyond every trade it
                    // held (an out-of-order insert is not necessarily the newest one).
                    let newest = slot.journal.last().map_or(trade.block, |t| t.block).max(trade.block).max(st.head);
                    slot.journal.clear();
                    slot.state = None;
                    slot.mark_suspect(StaleReason::JournalFull, newest);
                } else if slot.base.is_some() {
                    let uncertain = match (appended, slot.state) {
                        // In order (the usual case): one step on the current state.
                        (true, Some(prev)) => {
                            let snipe = st.snipe_for(slot.launch_block);
                            let step = replay(prev.eth, prev.tok, std::slice::from_ref(&trade), slot.bps(), borderline, snipe);
                            self.stats.trades_reverted.fetch_add(u64::from(step.reverted), Ordering::Relaxed);
                            slot.state = Some(ReplayOutcome {
                                eth: step.eth,
                                tok: step.tok,
                                executed: prev.executed + step.executed,
                                reverted: prev.reverted + step.reverted,
                                uncertain_block: prev.uncertain_block.or(step.uncertain_block),
                            });
                            step.uncertain_block
                        }
                        // Out of order (a gap filled late): replay from the anchor.
                        _ => {
                            let before = slot.state.map_or(0, |s| s.reverted);
                            let uncertain = st.recompute(&mut slot, borderline);
                            let after = slot.state.map_or(0, |s| s.reverted);
                            self.stats.trades_reverted.fetch_add(u64::from(after.saturating_sub(before)), Ordering::Relaxed);
                            uncertain
                        }
                    };
                    if let Some(b) = uncertain {
                        slot.mark_suspect(StaleReason::Uncertain, b);
                        MirrorStats::bump(&self.stats.uncertain);
                        self.anchor_wake.notify_one();
                    }
                    slot.record_price(trade.block);
                }
                MirrorStats::bump(&self.stats.trades_applied);
                self.block_changed.fetch_add(1, Ordering::AcqRel);
            }
        }
        st.slots.insert(curve, slot);
    }

    /// The feed re-delivered `block` with different content: what we applied from `block` on may
    /// not be the canonical chain.
    pub fn on_reorg(&self, block: u64) {
        let mut st = self.lock();
        let head = st.head;
        let now = Instant::now();
        st.add_hole(block, head.max(block), now, true);
        for slot in st.slots.values_mut() {
            slot.journal.retain(|t| t.block < block);
            if slot.base.is_some_and(|b| b.block >= block) {
                // Even the anchor may describe the abandoned branch.
                slot.base = None;
                slot.state = None;
            }
            slot.mark_suspect(StaleReason::Reorg, head.max(block));
        }
        drop(st);
        MirrorStats::bump(&self.stats.reorgs);
        warn!("[mirror] reorg at block {}: every curve re-anchors", block);
        self.notify_change();
        self.anchor_wake.notify_one();
    }

    /// The state of `curve` proved wrong (e.g. our sell reverted on a quote taken from it).
    pub fn invalidate(&self, curve: Address, reason: StaleReason) {
        let mut st = self.lock();
        let head = st.head;
        if let Some(slot) = st.slots.get_mut(&curve) {
            slot.mark_suspect(reason, head);
        }
        drop(st);
        self.notify_change();
        self.anchor_wake.notify_one();
    }

    // ── Reads ───────────────────────────────────────────────────────────────

    /// The current state of `curve` if the replica can vouch for it.
    pub fn read(&self, curve: &Address) -> MirrorRead {
        let read = self.read_at(curve, Instant::now(), true);
        match read {
            MirrorRead::Fresh(_) => MirrorStats::bump(&self.stats.reads_fresh),
            MirrorRead::Stale(_) => MirrorStats::bump(&self.stats.reads_stale),
        }
        read
    }

    /// The replica's best state regardless of freshness (anchored curves only).
    pub fn snapshot_any(&self, curve: &Address) -> Option<CurveSnapshot> {
        self.read_at(curve, Instant::now(), false).fresh()
    }

    fn read_at(&self, curve: &Address, now: Instant, strict: bool) -> MirrorRead {
        let st = self.lock();
        if strict && st.disabled() {
            return MirrorRead::Stale(StaleReason::Disabled);
        }
        let Some(slot) = st.slots.get(curve) else { return MirrorRead::Stale(StaleReason::Untracked) };
        let (Some(base), Some(state)) = (slot.base, slot.state) else {
            return MirrorRead::Stale(slot.suspect.unwrap_or(StaleReason::NoAnchor));
        };
        if strict {
            if let Some(reason) = slot.suspect {
                return MirrorRead::Stale(reason);
            }
            if st.last_block_at.is_none_or(|t| now.saturating_duration_since(t) > self.policy.stale_after) {
                return MirrorRead::Stale(StaleReason::FeedSilent);
            }
            if st.lost_gap_above(base.block, now, self.policy.gap_grace).is_some() {
                return MirrorRead::Stale(StaleReason::Gap);
            }
        }
        MirrorRead::Fresh(CurveSnapshot {
            curve: *curve,
            eth_reserve: state.eth,
            token_reserve: state.tok,
            as_of_block: st.head,
            anchor_block: base.block,
            unconfirmed_trades: slot.journal.len() as u32,
            exit_fee_bps: slot.bps(),
            volatility_bps: slot.volatility_bps(),
            validated_from: slot.validated_from,
            validated_through: slot.validated_through,
        })
    }

    // ── Anchors ─────────────────────────────────────────────────────────────

    /// Fold an exact reading of `curve` after block `block` into the replica: drop the journal up
    /// to `block`, replay the rest on it, record how far the prediction was off, calibrate.
    /// Returns false if a newer anchor already landed (or the curve is no longer tracked).
    pub fn apply_anchor(&self, curve: Address, block: u64, eth: U256, tok: U256) -> bool {
        let borderline = self.policy.borderline_bps;
        let now = Instant::now();
        let mut guard = self.lock();
        let st = &mut *guard;
        let Some(mut slot) = st.slots.remove(&curve) else { return false };
        slot.inflight = false;
        slot.failures = 0;
        if slot.base.is_some_and(|b| b.block > block) || block < slot.tracked_since {
            st.slots.insert(curve, slot);
            return false;
        }

        let floor = slot.floor();
        let clean_window = slot.suspect.is_none() && !st.holes.iter().any(|h| h.end > floor && h.start <= block);
        // Does this reading confirm, to the wei, every trade predicted since the previous one?
        let mut validated = false;
        if let Some(old) = slot.base {
            let upto = slot.journal.partition_point(|t| t.block <= block);
            let window = &slot.journal[..upto];
            let snipe = st.snipe_for(slot.launch_block);
            let predicted = replay(old.eth, old.tok, window, slot.bps(), borderline, &snipe);
            if !eth.is_zero() {
                let err = if predicted.eth > eth { predicted.eth - eth } else { eth - predicted.eth };
                let tok_err = if predicted.tok > tok { predicted.tok - tok } else { tok - predicted.tok };
                // Both reserves to the wei: the trades the prediction applied are the chain's.
                validated = err.is_zero() && tok_err.is_zero();
                if !err.is_zero() {
                    // The recent price path was computed on a wrong state: do not let it widen
                    // exit slippage (volatility) any longer.
                    slot.prices.clear();
                    MirrorStats::bump(&self.stats.drift_corrections);
                    let ppb = (err * U256::from(1_000_000_000u64) / eth).saturating_to::<u64>();
                    self.stats.max_drift_ppb.fetch_max(ppb, Ordering::Relaxed);
                    debug!("[mirror] {:?} anchor at {}: prediction off by {} wei ({} ppb)", curve, block, err, ppb);
                }
            }
            // Constant product on net amounts: `eth * tok` only moves by rounding between anchors.
            let (k_old, k_new) = (old.eth * old.tok, eth * tok);
            let dk = if k_old > k_new { k_old - k_new } else { k_new - k_old };
            if !k_old.is_zero() && dk * U256::from(1_000_000_000u64) > k_old {
                if !slot.model_suspect {
                    warn!(
                        "[mirror] {:?}: reserves product moved {} ppb between blocks {} and {}: the curve does not follow the \
                         modelled constant product; re-anchoring every {:?}",
                        curve,
                        (dk * U256::from(1_000_000_000u64) / k_old).saturating_to::<u64>(),
                        old.block,
                        block,
                        SUSPECT_MODEL_VERIFY
                    );
                }
                slot.model_suspect = true;
                MirrorStats::bump(&self.stats.invariant_breaks);
            } else if clean_window
                && let Some(bps) = calibrate(old, window, eth, &snipe, borderline)
                && slot.trade_bps != Some(bps)
            {
                info!("[mirror] {:?}: fee + creator tax calibrated to {} bps (was {})", curve, bps, slot.bps());
                slot.trade_bps = Some(bps);
                MirrorStats::bump(&self.stats.calibrations);
            }
        }

        let contiguous = slot.base.is_some_and(|old| old.block <= slot.validated_through && old.block >= slot.validated_from);
        if validated && contiguous {
            slot.validated_through = slot.validated_through.max(block);
        } else if validated {
            // The window (previous reading, this one] is proven, nothing before it.
            let from = slot.base.map_or(block, |b| b.block);
            (slot.validated_from, slot.validated_through) = (from, block);
        } else {
            // First reading, first after a reorg, or a missed prediction: nothing up to here is
            // proven; a new range starts empty at this block.
            (slot.validated_from, slot.validated_through) = (block, block);
        }
        slot.journal.retain(|t| t.block > block);
        slot.base = Some(Base { block, eth, tok, at: now });
        if slot.suspect.is_some() && block >= slot.min_anchor_block {
            slot.suspect = None;
        }
        if let Some(b) = st.recompute(&mut slot, borderline) {
            slot.mark_suspect(StaleReason::Uncertain, b);
        }
        slot.record_price(st.head.max(block));
        st.slots.insert(curve, slot);
        st.prune_holes();
        drop(guard);
        MirrorStats::bump(&self.stats.anchors_ok);
        self.notify_change();
        true
    }

    fn anchor_failed(&self, curve: Address) {
        let mut st = self.lock();
        if let Some(slot) = st.slots.get_mut(&curve) {
            slot.inflight = false;
            slot.failures = slot.failures.saturating_add(1);
            if slot.failures == 5 {
                warn!("[mirror] {:?}: no exact reading after 5 attempts (getReserves reverting?); backing off", curve);
            }
        }
        MirrorStats::bump(&self.stats.anchors_failed);
    }

    /// Curves that need an exact reading now, with the block to read at. Marks them in flight.
    fn due_anchors(&self, now: Instant) -> Vec<(Address, u64)> {
        let mut guard = self.lock();
        let st = &mut *guard;
        if st.disabled() || st.head == 0 {
            return Vec::new();
        }
        let head = st.head;
        let mut jobs = Vec::new();
        let policy = self.policy;
        let lost: Vec<(Address, Option<u64>)> =
            st.slots.iter().map(|(a, s)| (*a, st.lost_gap_above(s.floor(), now, policy.gap_grace))).collect();
        for (addr, lost_gap) in lost {
            let Some(slot) = st.slots.get_mut(&addr) else { continue };
            let backoff = policy.anchor_retry.saturating_mul(1u32 << slot.failures.min(8)).min(MAX_ANCHOR_BACKOFF);
            if slot.inflight || slot.last_attempt.is_some_and(|t| now.saturating_duration_since(t) < backoff) {
                continue;
            }
            let verify = if slot.model_suspect { SUSPECT_MODEL_VERIFY } else { policy.verify_every };
            let needed = slot.base.is_none()
                || slot.suspect.is_some()
                || lost_gap.is_some()
                || slot.base.is_some_and(|b| now.saturating_duration_since(b.at) >= verify);
            if !needed {
                continue;
            }
            let floor = slot.tracked_since.max(slot.min_anchor_block).max(slot.floor()).max(lost_gap.unwrap_or(0));
            let target = floor.max(head.saturating_sub(policy.anchor_lag_blocks)).min(head.max(floor));
            slot.inflight = true;
            slot.last_attempt = Some(now);
            jobs.push((addr, target));
        }
        jobs
    }

    /// One pinned `getReserves()` at `block`, retried while the node has not imported the block.
    async fn read_reserves_at(reader: &BroadcasterPool, curve: Address, block: u64, budget: Duration) -> Option<(U256, U256)> {
        let deadline = Instant::now() + budget;
        let call = CalldataPayloadBuilder::build_pons_get_reserves_payload();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            let res = tokio::time::timeout(left, reader.eth_call_at(curve, &call, Some(block))).await;
            match res {
                Ok(Ok(raw)) => match CalldataPayloadBuilder::decode_get_reserves_output(&raw) {
                    Some(r) => return Some(r),
                    None => return None, // not a curve (short / empty output): retrying cannot help
                },
                // A JSON-RPC error that is not "block not imported yet" is the call itself failing on
                // this contract (revert, not a curve): retrying cannot help. Transport errors retry.
                Ok(Err(e)) if !retryable_read_error(&e) => return None,
                _ => {}
            }
            tokio::time::sleep(ANCHOR_RETRY_STEP.min(deadline.saturating_duration_since(Instant::now()))).await;
        }
    }

    async fn anchor_now(self: &Arc<Self>, reader: &BroadcasterPool, curve: Address, block: u64) -> bool {
        match Self::read_reserves_at(reader, curve, block, self.policy.anchor_timeout).await {
            Some((eth, tok)) => self.apply_anchor(curve, block, eth, tok),
            None => {
                self.anchor_failed(curve);
                false
            }
        }
    }

    /// Feed and RPC must number blocks alike, or a pinned read targets the wrong block.
    ///
    /// Two checks: the heads must be close (a wrong `FEED_GENESIS_BLOCK` is usually far off, a
    /// node far behind is unusable too), and, exactly, the first transaction the feed showed in a
    /// recent block must be in the RPC block of the same number (catches an offset of one block).
    async fn check_numbering(&self, reader: &BroadcasterPool) {
        let due = {
            let st = self.lock();
            let every = if st.numbering_verified { NUMBERING_CHECK_EVERY } else { NUMBERING_PROBE_EVERY };
            st.head != 0 && st.numbering_checked.is_none_or(|t| t.elapsed() >= every)
        };
        if !due {
            return;
        }
        let Ok(rpc_head) = reader.block_number().await else { return };
        {
            let mut st = self.lock();
            st.numbering_checked = Some(Instant::now());
            let skew = rpc_head.abs_diff(st.head);
            if skew > self.policy.numbering_tolerance {
                if st.skew_disabled.is_none() {
                    error!(
                        "[mirror] feed block #{} vs RPC block #{} ({} apart): FEED_GENESIS_BLOCK is wrong or the node is far behind. \
                         In-memory valuation disabled, every read goes to RPC.",
                        st.head, rpc_head, skew
                    );
                }
                st.skew_disabled = Some(format!("feed #{} vs rpc #{}", st.head, rpc_head));
                return;
            } else if st.skew_disabled.take().is_some() {
                info!("[mirror] feed and RPC heads agree again (skew {}): in-memory valuation re-enabled", skew);
            }
        }
        let probe = self.probes.lock().unwrap_or_else(|e| e.into_inner()).iter().rev().find(|(b, _)| *b <= rpc_head).copied();
        let Some((block, hash)) = probe else { return };
        let Ok(rpc_block) = reader.call_rpc("eth_getBlockByNumber", serde_json::json!([format!("0x{:x}", block), false])).await else {
            return;
        };
        let Some(txs) = rpc_block.get("transactions").and_then(|t| t.as_array()) else { return };
        let wanted = format!("{:?}", hash);
        let found = txs.iter().any(|t| t.as_str().is_some_and(|h| h.eq_ignore_ascii_case(&wanted)));
        let mut st = self.lock();
        if block == st.last_probe_block && !found {
            return; // this sample already counted
        }
        st.last_probe_block = block;
        if found {
            st.probe_misses = 0;
            if !st.numbering_verified {
                info!("[mirror] block numbering verified: feed block #{} == RPC block #{}", block, block);
            }
            st.numbering_verified = true;
            if st.probe_disabled.take().is_some() {
                info!("[mirror] numbering probe matches again: in-memory valuation re-enabled");
            }
        } else {
            // One miss can be a delayed-inbox tx Nitro rejected (never in any block): two distinct
            // samples missing is a numbering offset.
            st.probe_misses += 1;
            if st.probe_misses < 2 {
                return;
            }
            if st.probe_disabled.is_none() {
                error!(
                    "[mirror] tx {} seen in feed block #{} is not in RPC block #{}: FEED_GENESIS_BLOCK is off. \
                     In-memory valuation disabled, every read goes to RPC.",
                    wanted, block, block
                );
            }
            st.numbering_verified = false;
            st.probe_disabled = Some(format!("feed block #{} != rpc block #{}", block, block));
        }
    }

    pub fn is_disabled(&self) -> bool {
        self.lock().disabled()
    }

    /// Feed and RPC block numbering proven identical by a transaction hash (see `check_numbering`).
    pub fn numbering_verified(&self) -> bool {
        self.lock().numbering_verified
    }

    /// Background task: takes the exact readings the replica needs (first anchor, suspect curves,
    /// lost gaps, periodic verification). Idle while nothing is tracked.
    pub fn spawn_anchor_worker(self: &Arc<Self>, reader: Arc<BroadcasterPool>) -> tokio::task::JoinHandle<()> {
        let _ = self.reader.set(reader.clone());
        let mirror = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = mirror.anchor_wake.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
                if mirror.tracked_count() == 0 {
                    continue;
                }
                mirror.check_numbering(&reader).await;
                for (curve, block) in mirror.due_anchors(Instant::now()) {
                    let (m, r) = (mirror.clone(), reader.clone());
                    tokio::spawn(async move {
                        m.anchor_now(&r, curve, block).await;
                    });
                }
            }
        })
    }

    /// Exact reading of `curve` pinned to `block` (the block whose trades triggered an exit, or the
    /// curve's anchor if that is newer). It is folded into the replica, and returned *alone*: the
    /// verdict on a move must not rest on trades the chain has not confirmed yet (a later phantom
    /// sell would otherwise confirm an earlier one). `None`: no reader, or no answer in time.
    pub async fn confirm(&self, curve: Address, block: u64) -> Option<CurveSnapshot> {
        MirrorStats::bump(&self.stats.exit_confirmations);
        let (target, exit_fee_bps, volatility_bps, anchored) = {
            let st = self.lock();
            let slot = st.slots.get(&curve);
            (
                block.max(slot.map_or(0, Slot::floor)),
                slot.map_or(DEFAULT_TRADE_BPS, Slot::bps),
                slot.map_or(0, Slot::volatility_bps),
                // Already read exactly at (or after) that block: no need to ask again.
                slot.and_then(|s| s.base).filter(|b| b.block >= block && s_suspect_free(slot)),
            )
        };
        let (eth, tok) = match anchored {
            Some(base) => (base.eth, base.tok),
            None => {
                let reader = self.reader.get()?.clone();
                let (eth, tok) = Self::read_reserves_at(&reader, curve, target, self.policy.anchor_timeout).await?;
                self.apply_anchor(curve, target, eth, tok);
                (eth, tok)
            }
        };
        let target = anchored.map_or(target, |b| b.block);
        Some(CurveSnapshot {
            curve,
            eth_reserve: eth,
            token_reserve: tok,
            as_of_block: target,
            anchor_block: target,
            unconfirmed_trades: 0,
            exit_fee_bps,
            volatility_bps,
            validated_from: 0,
            validated_through: 0,
        })
    }

    /// Exact reserves after `block` (pinned read), folded in if newer than the curve's anchor.
    /// Used to verify a past valuation (e.g. a trailing-stop peak). `None`: no reader / no answer.
    pub async fn reserves_at(&self, curve: Address, block: u64) -> Option<(U256, U256)> {
        let reader = self.reader.get()?.clone();
        let (eth, tok) = Self::read_reserves_at(&reader, curve, block, self.policy.anchor_timeout).await?;
        self.apply_anchor(curve, block, eth, tok);
        Some((eth, tok))
    }

    /// Use `reader` for exact readings without starting the background worker (tests, tools).
    pub fn set_reader(&self, reader: Arc<BroadcasterPool>) {
        let _ = self.reader.set(reader);
    }

    /// Take every exact reading that is due now and wait for them (tests, tools). Returns how
    /// many succeeded.
    pub async fn anchor_due_now(self: &Arc<Self>) -> usize {
        let Some(reader) = self.reader.get().cloned() else { return 0 };
        self.check_numbering(&reader).await;
        let jobs = self.due_anchors(Instant::now());
        let mut ok = 0;
        for (curve, block) in jobs {
            ok += usize::from(self.anchor_now(&reader, curve, block).await);
        }
        ok
    }

    pub(crate) fn note_phantom_rejected(&self) {
        MirrorStats::bump(&self.stats.phantom_exits_rejected);
    }
}

/// Errors worth retrying a pinned read for: everything (block not imported yet, rate limits,
/// transient node errors, timeouts) except the call itself failing on the contract.
fn retryable_read_error(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    !(e.contains("revert") || e.contains("invalid opcode"))
}

fn s_suspect_free(slot: Option<&Slot>) -> bool {
    slot.is_some_and(|s| s.suspect.is_none())
}

/// Fee + creator tax (bps) that makes the trades between two anchors land exactly on the second
/// one. Only buys depend on it (a sell's effect on the reserves is the gross amount), and the ETH
/// reserve after the window is monotonic in it, so a binary search finds it. `None` when the window
/// holds no buy large enough to resolve 1 bps, or no value reproduces the reading within rounding.
fn calibrate(old: Base, window: &[Trade], eth_actual: U256, snipe: &impl Fn(u64) -> u32, borderline: u32) -> Option<u32> {
    let resolvable = window.iter().any(|t| matches!(t.kind, TradeKind::Buy { quote_in, .. } if quote_in >= U256::from(10u64).pow(U256::from(12u64))));
    if !resolvable {
        return None;
    }
    let eth_at = |bps: u32| replay(old.eth, old.tok, window, bps, borderline, snipe).eth;
    let (mut lo, mut hi) = (0u32, MAX_CALIBRATED_BPS);
    // eth_at is non-increasing in bps: find the smallest bps with eth_at(bps) <= actual.
    if eth_at(lo) < eth_actual || eth_at(hi) > eth_actual {
        return None;
    }
    while lo < hi {
        let mid = (lo + hi) / 2;
        if eth_at(mid) <= eth_actual { hi = mid } else { lo = mid + 1 }
    }
    let candidates = [lo.saturating_sub(1), lo];
    let best = candidates
        .into_iter()
        .min_by_key(|b| {
            let e = eth_at(*b);
            if e > eth_actual { e - eth_actual } else { eth_actual - e }
        })?;
    let e = eth_at(best);
    let residual = if e > eth_actual { e - eth_actual } else { eth_actual - e };
    // Integer rounding: a few wei per trade.
    (residual <= U256::from(4u64 * (window.len() as u64 + 1))).then_some(best)
}

/// Slippage for an exit sell: the base tolerance widened by recent adverse volatility (other sells
/// that may land before ours), never below the base and never above the cap (90% emergency, 25%
/// otherwise, unless the base itself is already wider after failed attempts).
pub fn adaptive_exit_slippage(base_percent: f64, volatility_bps: u32, emergency: bool) -> f64 {
    let base = if base_percent.is_finite() { base_percent.clamp(0.0, 99.0) } else { 5.0 };
    let cap = if emergency { 90.0 } else { 25.0 };
    let widened = base + 1.5 * (volatility_bps as f64 / 100.0);
    base.max(widened.min(cap))
}
