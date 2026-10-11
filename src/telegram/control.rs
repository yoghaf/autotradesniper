//! Control plane: what the operator's buttons *do* to the running engine, and the telemetry the
//! screens show, independent of Telegram (so it can be tested without it).
//!
//! Every action is applied to the shared runtime state and then propagated to the pipelines
//! that cached anything derived from it:
//!
//! * carrier count / single-multi mode / padding switch: the new account layout is derived and
//!   the chain nonce of every carrier that starts taking part is read *before* the change is
//!   committed (a carrier whose nonce cannot be read aborts the change); after the commit the
//!   staged AOT batches are reshaped (slots dropped or added and signed) before their block;
//! * fee preset: the `(tip, max fee)` pair is swapped atomically, the fee adjuster's floor moves
//!   with it, and staged packets outside the tolerance are re-signed under their own nonce;
//! * pause: intake stops; queued Prysm orders and staged batches are dropped;
//! * circuit breaker: latch + intake off with two atomic stores, queues cut, every open position
//!   liquidated through the receipt-confirmed exit, and an entry still in flight is liquidated
//!   when it registers. Intake cannot be resumed until the operator releases the latch, which is
//!   refused while anything of the liquidation is still running.
//!
//! Isolation: the Telegram side never touches an engine lock. Everything that reads or writes
//! engine state runs as a task on the engine runtime (`EngineHandles::engine`), which the Telegram
//! side only awaits, with a time budget. Engine locks are therefore only ever held by engine
//! tasks (a writer granted on the Telegram thread could otherwise keep engine readers waiting
//! until that thread is polled again), engine connections and timers never live on the Telegram
//! thread, and a slow Telegram never slows the engine.

use crate::config::AppConfig;
use crate::dispatcher::{Dispatcher, PresignSnapshot, ReshapeReport};
use crate::models::{PrysmEngine, PrysmMatchReport};
use crate::payload::{DynamicFeeAdjuster, TARGET_CALLDATA_BUFFER_SIZE};
use crate::risk::curve_mirror::{CurveMirror, MirrorRead, MirrorStatsSnapshot};
use crate::risk::{PanicReport, RiskManager};
use crate::stream::{FeedStatsSnapshot, StreamListener};
use crate::telegram::outbox::{Notifier, OutboxSnapshot};
use crate::types::{ExecutionMode, GasRebalanceResult, Position, PrysmExitMode};
use crate::wallet::WalletManager;
use alloy::primitives::Address;
use std::future::Future;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Carrier counts offered by the keyboard (callback data is client-controlled: nothing else is accepted).
pub const CARRIER_CHOICES: [usize; 4] = [1, 2, 3, 5];
/// Balance reads for the dashboard give up after this.
const BALANCE_BUDGET: Duration = Duration::from_secs(3);
/// Nonce sync / balance check of carriers that start taking part gives up after this.
const NONCE_SYNC_BUDGET: Duration = Duration::from_secs(4);
/// How long the Telegram side waits for an engine read (snapshot, lists).
const READ_BUDGET: Duration = Duration::from_secs(5);
/// How long the Telegram side waits for a settings change (the change itself always completes).
const APPLY_BUDGET: Duration = Duration::from_secs(15);
/// Wallet replacement (persist + derive + nonce sync). Below the Telegram handler budget; a
/// replacement that completes later is reported as a critical notice.
const WALLET_BUDGET: Duration = Duration::from_secs(20);
/// Gas top-up of the carriers (sequential sends from the master).
const REBALANCE_BUDGET: Duration = Duration::from_secs(90);

/// Everything the control plane acts on. `engine` is the runtime the engine was started on.
#[derive(Clone)]
pub struct EngineHandles {
    pub config: AppConfig,
    pub wallet: WalletManager,
    pub risk: Arc<RiskManager>,
    pub prysm: Arc<PrysmEngine>,
    pub positions: Arc<RwLock<Vec<Position>>>,
    pub dispatcher: Option<Arc<Dispatcher>>,
    pub fee: Option<Arc<DynamicFeeAdjuster>>,
    pub stream: Option<Arc<StreamListener>>,
    pub mirror: Option<Arc<CurveMirror>>,
    pub engine: tokio::runtime::Handle,
}

/// The three fee presets of the settings keyboard: `(tip, max fee cap)` in Gwei. The tip is a
/// floor: the fee adjuster only bids above it when the network reports higher tips.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeePreset {
    Normal,
    Optimal,
    Race,
}

impl FeePreset {
    pub const ALL: [FeePreset; 3] = [FeePreset::Normal, FeePreset::Optimal, FeePreset::Race];

    pub fn gwei(self) -> (f64, f64) {
        match self {
            FeePreset::Normal => (1.0, 5.0),
            FeePreset::Optimal => (2.5, 10.0),
            FeePreset::Race => (5.0, 20.0),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            FeePreset::Normal => "Normal",
            FeePreset::Optimal => "Optimal",
            FeePreset::Race => "Balap",
        }
    }

    /// Callback data (kept identical to the buttons of older messages still in the chat).
    pub fn callback(self) -> &'static str {
        match self {
            FeePreset::Normal => "cfg_fee:1.0:5.0",
            FeePreset::Optimal => "cfg_fee:2.5:10.0",
            FeePreset::Race => "cfg_fee:5.0:20.0",
        }
    }

    /// The preset the current numbers correspond to, if any.
    pub fn matching(priority: f64, max: f64) -> Option<FeePreset> {
        FeePreset::ALL.into_iter().find(|p| {
            let (pp, pm) = p.gwei();
            (pp - priority).abs() < 1e-9 && (pm - max).abs() < 1e-9
        })
    }
}

/// A state-changing button. Parsed with an exact whitelist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    SetCarriers(usize),
    SetFeePreset(FeePreset),
    /// Padding (block-booking carriers + master padding) on / off.
    SetPadding(bool),
    /// Single / multi-account execution. Like every button here the target is in the callback,
    /// so a stale or repeated press can never flip the engine the "wrong" way.
    SetMode(ExecutionMode),
    /// Intake to the given state.
    Intake(bool),
    /// Clear the latch of the breaker with this epoch and resume intake (refused while it still
    /// liquidates, and when a newer breaker fired since the button was drawn).
    ReleaseBreaker(u64),
    Panic,
}

impl Action {
    pub fn parse(data: &str) -> Option<Action> {
        if let Some(n) = data.strip_prefix("cfg_sub:").or_else(|| data.strip_prefix("tr_sub:")) {
            return n.parse().ok().filter(|n| CARRIER_CHOICES.contains(n)).map(Action::SetCarriers);
        }
        if let Some(p) = FeePreset::ALL.into_iter().find(|p| p.callback() == data) {
            return Some(Action::SetFeePreset(p));
        }
        if let Some(epoch) = data.strip_prefix("breaker_release:") {
            return epoch.parse().ok().filter(|e| *e > 0 && !epoch.starts_with('+')).map(Action::ReleaseBreaker);
        }
        match data {
            "cfg_buffer:94500" | "cfg_buffer_default" => Some(Action::SetPadding(true)),
            "cfg_buffer:0" => Some(Action::SetPadding(false)),
            "cfg_mode:single" => Some(Action::SetMode(ExecutionMode::Single)),
            "cfg_mode:multi" => Some(Action::SetMode(ExecutionMode::MultiAccount)),
            "intake:on" => Some(Action::Intake(true)),
            "intake:off" => Some(Action::Intake(false)),
            "op_panic" => Some(Action::Panic),
            _ => None,
        }
    }
}

/// What applying a runtime change did to the engine.
#[derive(Debug, Default, Clone)]
pub struct ApplyReport {
    /// One line per changed parameter, e.g. "Carrier: 2 → 5".
    pub changes: Vec<String>,
    /// The requested state was already in force: nothing was touched.
    pub unchanged: bool,
    /// The change was refused or could not be prepared: the previous state stays in force.
    pub aborted: bool,
    /// The engine has not finished within the Telegram side's budget: it completes on its own.
    pub pending: bool,
    pub presign: ReshapeReport,
    /// Accounts derived after the change (master included).
    pub accounts: usize,
    /// Carriers that start taking part whose nonce was read from the chain.
    pub nonces_synced: usize,
    pub warnings: Vec<String>,
}

impl ApplyReport {
    fn refused(why: impl Into<String>) -> Self {
        Self { aborted: true, warnings: vec![why.into()], ..Self::default() }
    }

    fn unchanged(what: impl Into<String>) -> Self {
        Self { unchanged: true, changes: vec![what.into()], ..Self::default() }
    }
}

/// Runtime parameters as the dispatcher will apply them next.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SettingsView {
    pub mode: ExecutionMode,
    pub carriers: usize,
    pub padding_on: bool,
    pub buffer_size: usize,
    pub priority_gwei: f64,
    pub max_fee_gwei: f64,
    pub preset: Option<FeePreset>,
    pub target_profit_percent: f64,
    pub time_decay_exit_secs: u64,
    pub gas_limit: u64,
    pub trade_eth: f64,
}

impl SettingsView {
    /// Satellites that take part in an entry right now.
    pub fn active_carriers(&self) -> usize {
        if self.mode == ExecutionMode::MultiAccount && self.padding_on { self.carriers } else { 0 }
    }

    /// Padding the carriers carry (0 when they do not take part).
    pub fn carrier_padding_bytes(&self) -> usize {
        if self.active_carriers() == 0 {
            0
        } else if self.buffer_size > 0 {
            self.buffer_size
        } else {
            TARGET_CALLDATA_BUFFER_SIZE
        }
    }

    pub fn master_padding_bytes(&self) -> usize {
        if self.padding_on { self.buffer_size } else { 0 }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AccountView {
    pub index: usize,
    pub role: String,
    pub address: Address,
    pub balance_eth: Option<f64>,
    /// A carrier in the active layout below the operational minimum.
    pub low: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FeedView {
    pub stats: FeedStatsSnapshot,
    /// Share of the blocks this feed delivered first, in percent of all wins.
    pub win_pct: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CurveView {
    pub curve: Address,
    pub read: MirrorRead,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MirrorView {
    pub disabled: bool,
    pub numbering_verified: bool,
    pub head: u64,
    pub curves: Vec<CurveView>,
    pub stats: MirrorStatsSnapshot,
}

#[derive(Debug, Clone)]
pub struct PositionView {
    pub position: Position,
    /// How the loop values it right now: `Some(true)` in memory, `Some(false)` RPC fallback,
    /// `None` not a mirrored curve.
    pub in_memory: Option<bool>,
}

/// One consistent look at the whole system for the screens.
#[derive(Debug, Clone)]
pub struct SystemSnapshot {
    pub engine_running: bool,
    /// Epoch of the latched circuit breaker, if one is latched.
    pub breaker: Option<u64>,
    pub chain_id: u64,
    pub settings: SettingsView,
    pub accounts: Vec<AccountView>,
    pub feeds: Vec<FeedView>,
    pub live_feeds: usize,
    /// `(skipped, recovered)` feed messages.
    pub gaps: (u64, u64),
    pub mirror: Option<MirrorView>,
    pub presign: Option<(bool, usize, PresignSnapshot)>,
    pub open: Vec<PositionView>,
    pub recent_closed: Vec<Position>,
    pub closed_count: usize,
    pub exits_in_flight: usize,
    pub entries_in_flight: usize,
    pub prysm_pending: usize,
    pub watchlist: usize,
    pub outbox: Option<OutboxSnapshot>,
    pub master_address: Option<Address>,
}

/// Outcome of the circuit breaker, ready to be shown.
#[derive(Debug, Clone, Default)]
pub struct BreakerOutcome {
    pub prysm_orders_dropped: usize,
    pub presign_batches_released: usize,
    /// Entries already on their way when the breaker fired (each liquidates itself on registration).
    pub entries_in_flight: usize,
    pub report: PanicReport,
}

/// Prysm model settings for its screen.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PrysmView {
    pub enabled: bool,
    pub size_eth: f64,
    pub exit_mode: PrysmExitMode,
    pub max_mcap_usd: f64,
    pub pending: usize,
}

/// Prysm match counters for its statistics screen.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PrysmCounts {
    pub evaluated: u64,
    pub a: u64,
    pub b: u64,
    pub c: u64,
    pub executed: u64,
    pub rejected_stale: u64,
    pub rejected_mcap: u64,
}

// ───────────────────── engine-side bodies (run on the engine runtime) ─────────────────────

async fn read_settings(c: &AppConfig) -> SettingsView {
    let (priority_gwei, max_fee_gwei) = c.fee_caps().await;
    let mode = *c.execution_mode.read().await;
    let carriers = *c.sub_account_count.read().await;
    let buffer_size = *c.buffer_size.read().await;
    let target_profit_percent = *c.target_profit_percent.read().await;
    let time_decay_exit_secs = *c.time_decay_exit_seconds.read().await;
    let gas_limit = *c.gas_limit.read().await;
    let trade_eth = *c.default_trade_eth.read().await;
    SettingsView {
        mode,
        carriers,
        padding_on: c.carrier_padding_enabled(),
        buffer_size,
        priority_gwei,
        max_fee_gwei,
        preset: FeePreset::matching(priority_gwei, max_fee_gwei),
        target_profit_percent,
        time_decay_exit_secs,
        gas_limit,
        trade_eth,
    }
}

/// Before a layout change is committed: derive the accounts of `carriers` (PBKDF2, blocking
/// pool) so the first dispatch under the new layout finds them cached, and read the chain nonce
/// and balance of the carriers in `activate` (account indices that start taking part). False if
/// the change must not be committed: accounts not derivable, or a joining carrier whose nonce
/// could not be read (it would lease from a stale cursor and its packets would be rejected).
async fn prepare_layout(h: &EngineHandles, carriers: usize, activate: RangeInclusive<usize>, report: &mut ApplyReport) -> bool {
    let accounts = match h.wallet.prepare_accounts(carriers).await {
        Ok(a) => a,
        Err(e) => {
            report.aborted = true;
            report.warnings.push(format!("Akun carrier tidak bisa diturunkan ({:#}): setelan lama tetap berlaku", e));
            return false;
        }
    };
    report.accounts = accounts.len();
    let joining: Vec<(usize, Address)> = accounts.iter().filter(|a| activate.contains(&a.index)).map(|a| (a.index, a.address)).collect();
    let addresses: Vec<Address> = joining.iter().map(|(_, a)| *a).collect();
    let syncs = addresses.iter().map(|addr| {
        let w = h.wallet.clone();
        let addr = *addr;
        async move { matches!(tokio::time::timeout(NONCE_SYNC_BUDGET, w.resync_nonce(addr)).await, Ok(Ok(_))) }
    });
    let (synced, balances) = tokio::join!(futures_util::future::join_all(syncs), h.wallet.balances_of(&addresses, NONCE_SYNC_BUDGET));
    report.nonces_synced = synced.iter().filter(|ok| **ok).count();
    let unsynced: Vec<String> = joining.iter().zip(&synced).filter(|(_, ok)| !**ok).map(|((i, _), _)| format!("#{}", i)).collect();
    if !unsynced.is_empty() {
        report.aborted = true;
        report.warnings.push(format!(
            "Nonce carrier {} tidak terbaca dari RPC: setelan lama tetap berlaku (carrier dengan nonce basi akan ditolak node)",
            unsynced.join(", ")
        ));
        return false;
    }
    let min_eth = *h.config.min_sub_account_eth.read().await;
    for ((index, _), balance) in joining.iter().zip(balances) {
        match balance {
            Some(b) if b < min_eth => {
                report.warnings.push(format!("Carrier #{} saldo {:.5} ETH < minimum {:.4} ETH: jalankan Rebalance", index, b, min_eth))
            }
            None => report.warnings.push(format!("Saldo carrier #{} tidak terbaca", index)),
            _ => {}
        }
    }
    true
}

/// After a commit: re-shape and re-sign the staged AOT batches for the new parameters.
async fn reshape(h: &EngineHandles, report: &mut ApplyReport) {
    if let Some(d) = &h.dispatcher {
        report.presign = d.reshape_presigned().await;
    }
}

/// Drop queued Prysm orders and staged batches (batches counted first: the orders' own teardown
/// would release them uncounted).
async fn release_pending(h: &EngineHandles, report: &mut ApplyReport) {
    let batches = h.dispatcher.as_ref().map_or(0, |d| d.cancel_all_presigned());
    let orders = h.prysm.cancel_all_pending();
    if orders + batches > 0 {
        report.changes.push(format!("{} order Prysm antre dibatalkan, {} batch pre-sign dilepas", orders, batches));
    }
}

async fn apply_carriers(h: EngineHandles, n: usize) -> ApplyReport {
    if !CARRIER_CHOICES.contains(&n) {
        return ApplyReport::refused(format!("Jumlah carrier {} tidak tersedia", n));
    }
    let before = read_settings(&h.config).await;
    if before.carriers == n && before.mode == ExecutionMode::MultiAccount {
        return ApplyReport::unchanged(format!("Carrier tetap {}", n));
    }
    let mut report = ApplyReport::default();
    let active_after = if before.padding_on { n } else { 0 };
    if !prepare_layout(&h, n, before.active_carriers() + 1..=active_after, &mut report).await {
        return report;
    }
    h.config.set_carrier_count(n).await;
    report.changes.push(format!("Carrier: {} → {}", before.carriers, n));
    if before.mode != ExecutionMode::MultiAccount {
        report.changes.push("Mode: Single → MultiAccount".into());
    }
    if !before.padding_on {
        report.warnings.push("Padding 0 KB aktif: carrier ikut entry setelah padding 95 KB dinyalakan".into());
    }
    info!("[control] carriers {} -> {}", before.carriers, n);
    reshape(&h, &mut report).await;
    report
}

async fn apply_fee_preset(h: EngineHandles, preset: FeePreset) -> ApplyReport {
    let (old_p, old_m) = h.config.fee_caps().await;
    let (p, m) = preset.gwei();
    let floor_matches = h.fee.as_ref().is_none_or(|f| (f.priority_floor_gwei() - p).abs() < 1e-9);
    if FeePreset::matching(old_p, old_m) == Some(preset) && floor_matches {
        return ApplyReport::unchanged(format!("Fee tetap {}", preset.label()));
    }
    // Floor first: a packet built in between already uses the new tip, capped by the old or new
    // max (both valid); the pair itself is swapped atomically.
    if let Some(fee) = &h.fee {
        fee.set_priority_floor_gwei(p);
    }
    h.config.set_fee_preset(p, m).await;
    let mut report = ApplyReport::default();
    report.changes.push(format!("Fee: {:.1}G tip / {:.1}G max → {} ({:.1}G / {:.1}G)", old_p, old_m, preset.label(), p, m));
    info!("[control] fee preset {:?}", preset);
    reshape(&h, &mut report).await;
    report
}

async fn apply_padding(h: EngineHandles, on: bool) -> ApplyReport {
    let before = read_settings(&h.config).await;
    let target_buffer = if on { TARGET_CALLDATA_BUFFER_SIZE } else { 0 };
    if before.padding_on == on && before.buffer_size == target_buffer {
        return ApplyReport::unchanged(format!("Padding tetap {}", if on { "95 KB" } else { "0 KB" }));
    }
    let mut report = ApplyReport::default();
    let active_after = if on && before.mode == ExecutionMode::MultiAccount { before.carriers } else { 0 };
    if !prepare_layout(&h, before.carriers, before.active_carriers() + 1..=active_after, &mut report).await {
        return report;
    }
    h.config.set_carrier_padding(on, TARGET_CALLDATA_BUFFER_SIZE).await;
    report.changes.push(format!(
        "Padding: {} → {}",
        if before.padding_on { "95 KB" } else { "0 KB" },
        if on { "95 KB (carrier + master)" } else { "0 KB (tanpa carrier, master tanpa padding)" }
    ));
    info!("[control] padding {}", if on { "on" } else { "off" });
    reshape(&h, &mut report).await;
    report
}

async fn apply_mode(h: EngineHandles, next: ExecutionMode) -> ApplyReport {
    let before = read_settings(&h.config).await;
    if before.mode == next {
        return ApplyReport::unchanged(format!("Mode tetap {}", next));
    }
    let mut report = ApplyReport::default();
    let active_after = if next == ExecutionMode::MultiAccount && before.padding_on { before.carriers } else { 0 };
    if !prepare_layout(&h, before.carriers, before.active_carriers() + 1..=active_after, &mut report).await {
        return report;
    }
    h.config.set_execution_mode(next).await;
    report.changes.push(format!("Mode: {} → {}", before.mode, next));
    reshape(&h, &mut report).await;
    report
}

async fn apply_intake(h: EngineHandles, running: bool) -> ApplyReport {
    if running && h.config.breaker_engaged() {
        return ApplyReport::refused(
            "Circuit breaker masih terkunci: lepas dulu lewat tombol 🔓 setelah likuidasi selesai",
        );
    }
    let mut report = if h.config.is_engine_running() == running {
        ApplyReport::unchanged(format!("Engine intake tetap {}", if running { "AKTIF" } else { "PAUSE" }))
    } else {
        h.config.set_engine_running(running).await;
        ApplyReport { changes: vec![format!("Engine intake → {}", if running { "AKTIF" } else { "PAUSE" })], ..ApplyReport::default() }
    };
    if !running {
        release_pending(&h, &mut report).await;
    }
    report
}

async fn apply_release_breaker(h: EngineHandles, breaker_runs: Arc<AtomicUsize>, epoch: u64) -> ApplyReport {
    match h.config.breaker_epoch() {
        None => return ApplyReport::unchanged("Circuit breaker tidak terkunci"),
        Some(latched) if latched != epoch => {
            return ApplyReport::refused("Tombol ini untuk breaker sebelumnya: breaker yang lebih baru sedang terkunci");
        }
        Some(_) => {}
    }
    let running = breaker_runs.load(Ordering::SeqCst);
    let exits = h.risk.exits_in_flight();
    let entries = h.dispatcher.as_ref().map_or(0, |d| d.entries_in_flight());
    if running + exits + entries > 0 {
        return ApplyReport::refused(format!(
            "Likuidasi belum selesai (breaker aktif {}, exit berjalan {}, entri di jalan {}): coba lagi sebentar",
            running, exits, entries
        ));
    }
    // Compare-and-swap on the epoch: a breaker that fires right now keeps its own latch (and
    // with it intake off, whatever the store below does).
    if !h.config.release_breaker(epoch) {
        return ApplyReport::refused("Breaker baru terpicu saat melepas: tetap terkunci");
    }
    h.config.set_engine_running(true).await;
    warn!("[control] circuit breaker #{} released by the operator: intake resumed", epoch);
    ApplyReport { changes: vec!["Circuit breaker dilepas · Engine intake → AKTIF".into()], ..ApplyReport::default() }
}

async fn wallet_blocked(h: &EngineHandles, rebalancing: &AtomicUsize) -> Option<String> {
    if rebalancing.load(Ordering::SeqCst) > 0 {
        return Some("rebalance saldo sedang berjalan".into());
    }
    if h.config.is_engine_running() {
        return Some("intake masih AKTIF: pause dulu (tombol Intake)".into());
    }
    if let Some(d) = &h.dispatcher {
        if d.entries_in_flight() > 0 {
            return Some(format!("{} entri masih dalam pengiriman", d.entries_in_flight()));
        }
        if d.has_staged() {
            return Some(format!("{} batch pre-sign masih memegang nonce", d.staged_count()));
        }
    }
    let open = h.positions.read().await.iter().filter(|p| p.is_open).count();
    if open > 0 {
        return Some(format!("{} posisi masih terbuka", open));
    }
    if h.risk.exits_in_flight() > 0 {
        return Some("exit masih berjalan".into());
    }
    let pending = h.prysm.pending_count();
    if pending > 0 {
        return Some(format!("{} order Prysm masih antre", pending));
    }
    None
}

/// Engine part of the dashboard: every copy of engine state, plus the account list whose
/// balances the caller reads over its own connection.
async fn engine_snapshot(h: EngineHandles) -> (SystemSnapshot, Vec<(usize, String, Address)>, f64) {
    let c = &h.config;
    let settings = read_settings(c).await;
    let min_carrier_eth = *c.min_sub_account_eth.read().await;
    let accounts: Vec<(usize, String, Address)> = match h.wallet.get_all_accounts_shared().await {
        Ok(list) => list.iter().map(|a| (a.index, a.role.clone(), a.address)).collect(),
        Err(e) => {
            warn!("[control] account listing failed: {:#}", e);
            Vec::new()
        }
    };

    let (feeds, live_feeds, gaps) = match &h.stream {
        Some(s) => {
            let stats = s.feed_stats();
            let total: u64 = stats.iter().map(|f| f.wins).sum();
            let feeds = stats
                .into_iter()
                .map(|f| FeedView { win_pct: if total > 0 { f.wins as f64 * 100.0 / total as f64 } else { 0.0 }, stats: f })
                .collect();
            (feeds, s.live_feed_count(), s.gap_stats())
        }
        None => (Vec::new(), 0, (0, 0)),
    };

    let curves: Vec<(Address, MirrorRead)> = h.mirror.as_ref().map(|m| m.curve_overview()).unwrap_or_default();
    let mirror = h.mirror.as_ref().map(|m| MirrorView {
        disabled: m.is_disabled(),
        numbering_verified: m.numbering_verified(),
        head: m.head(),
        curves: curves.iter().map(|(curve, read)| CurveView { curve: *curve, read: *read }).collect(),
        stats: m.stats(),
    });

    let (open, recent_closed, closed_count) = {
        let positions = h.positions.read().await;
        let open: Vec<Position> = positions.iter().filter(|p| p.is_open).cloned().collect();
        let closed_count = positions.iter().filter(|p| !p.is_open).count();
        let recent: Vec<Position> = positions.iter().filter(|p| !p.is_open).rev().take(5).cloned().collect();
        (open, recent, closed_count)
    };
    let open = open
        .into_iter()
        .map(|p| {
            let in_memory = Address::from_str(&p.pool_address)
                .ok()
                .and_then(|pool| curves.iter().find(|(c, _)| *c == pool))
                .map(|(_, r)| matches!(r, MirrorRead::Fresh(_)));
            PositionView { position: p, in_memory }
        })
        .collect();

    let snapshot = SystemSnapshot {
        engine_running: c.is_engine_running(),
        breaker: c.breaker_epoch(),
        chain_id: c.chain_id,
        settings,
        accounts: Vec::new(),
        feeds,
        live_feeds,
        gaps,
        mirror,
        presign: h.dispatcher.as_ref().map(|d| (d.presign_policy().enabled, d.staged_count(), d.presign_snapshot())),
        open,
        recent_closed,
        closed_count,
        exits_in_flight: h.risk.exits_in_flight(),
        entries_in_flight: h.dispatcher.as_ref().map_or(0, |d| d.entries_in_flight()),
        prysm_pending: h.prysm.pending_count(),
        watchlist: c.watchlist_accounts.read().await.len(),
        outbox: None,
        master_address: accounts.first().map(|(_, _, a)| *a),
    };
    (snapshot, accounts, min_carrier_eth)
}

// ───────────────────────────── the control plane ─────────────────────────────

#[derive(Clone)]
pub struct ControlPlane {
    h: EngineHandles,
    notifier: Option<Notifier>,
    /// State changes are applied one at a time, on the engine runtime (two presses never
    /// interleave their prepare / commit / reshape steps, even if the Telegram side stopped
    /// waiting). The circuit breaker never takes it.
    apply_lock: Arc<tokio::sync::Mutex<()>>,
    /// Circuit-breaker liquidations still running.
    breaker_runs: Arc<AtomicUsize>,
    /// Rebalances running (they send from the master: no wallet swap meanwhile). They do not hold
    /// `apply_lock`: a stuck RPC there must not hold up pause or settings.
    rebalancing: Arc<AtomicUsize>,
}

impl ControlPlane {
    pub fn new(handles: EngineHandles) -> Self {
        Self {
            h: handles,
            notifier: None,
            apply_lock: Arc::new(tokio::sync::Mutex::new(())),
            breaker_runs: Arc::new(AtomicUsize::new(0)),
            rebalancing: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Outbox statistics to show on the dashboard; critical reports go through it.
    pub fn with_notifier(mut self, notifier: Notifier) -> Self {
        self.notifier = Some(notifier);
        self
    }

    pub fn handles(&self) -> &EngineHandles {
        &self.h
    }

    pub fn config(&self) -> &AppConfig {
        &self.h.config
    }

    pub fn notifier(&self) -> Option<&Notifier> {
        self.notifier.as_ref()
    }

    /// Run `f` on the engine runtime; wait at most `budget` for it here (it completes anyway).
    async fn on_engine<F, Fut, T>(&self, budget: Duration, f: F) -> Result<T, String>
    where
        F: FnOnce(EngineHandles) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let task = self.h.engine.spawn(f(self.h.clone()));
        match tokio::time::timeout(budget, task).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) if e.is_panic() => Err("engine task panicked".into()),
            Ok(Err(_)) => Err("engine runtime is shutting down".into()),
            Err(_) => Err(format!("engine did not answer within {} s", budget.as_secs())),
        }
    }

    /// A serialized state change on the engine runtime.
    async fn apply_with<F, Fut>(&self, f: F) -> ApplyReport
    where
        F: FnOnce(EngineHandles) -> Fut + Send + 'static,
        Fut: Future<Output = ApplyReport> + Send + 'static,
    {
        let lock = self.apply_lock.clone();
        let res = self
            .on_engine(APPLY_BUDGET, move |h| async move {
                let _serial = lock.lock().await;
                f(h).await
            })
            .await;
        res.unwrap_or_else(|e| ApplyReport {
            pending: true,
            warnings: vec![format!("{}: perubahan tetap diselesaikan engine, cek layar setelan sebentar lagi", e)],
            ..ApplyReport::default()
        })
    }

    pub async fn settings(&self) -> Result<SettingsView, String> {
        self.on_engine(READ_BUDGET, |h| async move { read_settings(&h.config).await }).await
    }

    /// Apply a state-changing action. The circuit breaker is not applied here: it must never
    /// wait (see [`Self::trigger_breaker`]).
    pub async fn apply(&self, action: Action) -> ApplyReport {
        match action {
            Action::SetCarriers(n) => self.apply_with(move |h| apply_carriers(h, n)).await,
            Action::SetFeePreset(p) => self.apply_with(move |h| apply_fee_preset(h, p)).await,
            Action::SetPadding(on) => self.apply_with(move |h| apply_padding(h, on)).await,
            Action::SetMode(mode) => self.apply_with(move |h| apply_mode(h, mode)).await,
            Action::Intake(on) => self.set_intake(on).await,
            Action::ReleaseBreaker(epoch) => {
                let runs = self.breaker_runs.clone();
                self.apply_with(move |h| apply_release_breaker(h, runs, epoch)).await
            }
            Action::Panic => ApplyReport::refused("circuit breaker goes through trigger_breaker"),
        }
    }

    pub async fn set_carriers(&self, n: usize) -> ApplyReport {
        self.apply(Action::SetCarriers(n)).await
    }

    pub async fn set_fee_preset(&self, preset: FeePreset) -> ApplyReport {
        self.apply(Action::SetFeePreset(preset)).await
    }

    pub async fn set_padding(&self, on: bool) -> ApplyReport {
        self.apply(Action::SetPadding(on)).await
    }

    /// Pause (`false`) or resume (`true`) intake. Pausing drops queued Prysm orders and staged
    /// batches (their nonces go back); open positions stay supervised. Resuming is refused while
    /// the circuit breaker is latched.
    pub async fn set_intake(&self, running: bool) -> ApplyReport {
        self.apply_with(move |h| apply_intake(h, running)).await
    }

    /// Take-profit target in percent (0, 1000].
    pub async fn set_take_profit(&self, tp: f64) -> ApplyReport {
        if !(tp.is_finite() && tp > 0.0 && tp <= 1000.0) {
            return ApplyReport::refused(format!("Target profit {} di luar rentang", tp));
        }
        self.apply_with(move |h| async move {
            let old = std::mem::replace(&mut *h.config.target_profit_percent.write().await, tp);
            let _ = h.config.save_persisted_state().await;
            ApplyReport { changes: vec![format!("Target profit: +{:.1}% → +{:.1}%", old, tp)], ..ApplyReport::default() }
        })
        .await
    }

    /// Time-decay exit in seconds [1, 3600].
    pub async fn set_time_decay(&self, secs: u64) -> ApplyReport {
        if !(1..=3600).contains(&secs) {
            return ApplyReport::refused(format!("Time-decay {} s di luar rentang", secs));
        }
        self.apply_with(move |h| async move {
            let old = std::mem::replace(&mut *h.config.time_decay_exit_seconds.write().await, secs);
            let _ = h.config.save_persisted_state().await;
            ApplyReport { changes: vec![format!("Time-decay exit: {} s → {} s", old, secs)], ..ApplyReport::default() }
        })
        .await
    }

    /// Prysm model on / off. Off drops its queued orders and their staged batches.
    pub async fn toggle_prysm(&self) -> ApplyReport {
        self.apply_with(|h| async move {
            let on = h.config.toggle_prysm_strategy().await;
            let mut report = ApplyReport { changes: vec![format!("Seri Prysm → {}", if on { "AKTIF" } else { "NONAKTIF" })], ..ApplyReport::default() };
            if !on {
                release_pending(&h, &mut report).await;
            }
            report
        })
        .await
    }

    pub async fn set_prysm_size(&self, size_eth: f64) -> ApplyReport {
        if !(size_eth.is_finite() && size_eth > 0.0) {
            return ApplyReport::refused("Modal harus angka ETH lebih dari 0");
        }
        self.apply_with(move |h| async move {
            h.config.set_prysm_size_eth(size_eth).await;
            ApplyReport { changes: vec![format!("Modal Seri Prysm → {:.4} ETH", size_eth)], ..ApplyReport::default() }
        })
        .await
    }

    pub async fn toggle_prysm_exit(&self) -> ApplyReport {
        self.apply_with(|h| async move {
            let mode = h.config.toggle_prysm_exit_mode().await;
            ApplyReport { changes: vec![format!("Mode exit Prysm → {}", mode)], ..ApplyReport::default() }
        })
        .await
    }

    pub async fn add_watchlist(&self, address: Address) -> ApplyReport {
        self.apply_with(move |h| async move {
            h.config.add_watchlist_account(address).await;
            ApplyReport { changes: vec![format!("Watchlist + {:?}", address)], ..ApplyReport::default() }
        })
        .await
    }

    pub async fn remove_watchlist(&self, address: Address) -> ApplyReport {
        self.apply_with(move |h| async move {
            h.config.remove_watchlist_account(&address).await;
            ApplyReport { changes: vec![format!("Watchlist − {:?}", address)], ..ApplyReport::default() }
        })
        .await
    }

    /// Circuit breaker. The latch is set and intake stops *now*, with two atomic stores, before
    /// this returns (no lock, no await); queued Prysm orders, staged batches and every open
    /// position are then handled on the engine runtime, each position through the
    /// receipt-confirmed exit. The returned handle resolves with the full report: callers must
    /// not make the operator wait for it.
    pub fn trigger_breaker(&self) -> tokio::task::JoinHandle<BreakerOutcome> {
        self.breaker_runs.fetch_add(1, Ordering::SeqCst);
        let epoch = self.h.config.engage_breaker();
        warn!("[control] CIRCUIT BREAKER #{}: intake stopped and latched, liquidating every open position", epoch);
        let h = self.h.clone();
        let runs = self.breaker_runs.clone();
        self.h.engine.spawn(async move {
            struct Done(Arc<AtomicUsize>);
            impl Drop for Done {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let _done = Done(runs);
            // Queues first (synchronous): nothing new can be dispatched while the sells go out.
            // Batches armed after this teardown take themselves down (intake is off).
            let entries_in_flight = h.dispatcher.as_ref().map_or(0, |d| d.entries_in_flight());
            let presign_batches_released = h.dispatcher.as_ref().map_or(0, |d| d.cancel_all_presigned());
            let prysm_orders_dropped = h.prysm.cancel_all_pending();
            let report = h.risk.panic_sell_all_report().await;
            BreakerOutcome { prysm_orders_dropped, presign_batches_released, entries_in_flight, report }
        })
    }

    /// Is the breaker latched (resume refused until released)?
    pub fn breaker_engaged(&self) -> bool {
        self.h.config.breaker_engaged()
    }

    /// Epoch of the latched breaker (what its release button must name).
    pub fn breaker(&self) -> Option<u64> {
        self.h.config.breaker_epoch()
    }

    /// Why the master mnemonic cannot be replaced right now, if it cannot: intake must be paused,
    /// and nothing may be in flight, staged, open or queued (the next exit would otherwise try
    /// to sell from the new, empty wallet).
    pub async fn wallet_replacement_blocked(&self) -> Option<String> {
        let rebalancing = self.rebalancing.clone();
        match self.on_engine(READ_BUDGET, move |h| async move { wallet_blocked(&h, &rebalancing).await }).await {
            Ok(blocked) => blocked,
            Err(e) => Some(e),
        }
    }

    /// Import (`Some`) or generate (`None`) the master mnemonic, guarded by
    /// [`Self::wallet_replacement_blocked`] (checked again under the apply lock). Returns the phrase
    /// now in force. If the engine needs longer than `WALLET_BUDGET`, the outcome (with a
    /// generated phrase, which exists nowhere else but the state file) follows as a critical
    /// notice to `notify_chat`, so it is never lost to the operator.
    pub async fn replace_wallet(&self, phrase: Option<String>, notify_chat: i64) -> Result<String, String> {
        let lock = self.apply_lock.clone();
        let rebalancing = self.rebalancing.clone();
        let h = self.h.clone();
        let task = self.h.engine.spawn(async move {
            let _serial = lock.lock().await;
            if let Some(why) = wallet_blocked(&h, &rebalancing).await {
                return Err(why);
            }
            let generated = phrase.is_none();
            let phrase = match phrase {
                Some(p) => h.wallet.set_mnemonic(&p).await.map(|_| p.trim().to_string()),
                None => h.wallet.generate_new_mnemonic().await,
            }
            .map_err(|e| format!("{:#}", e))?;
            // Prysm skips launches by our own master: it must know the new one.
            let master = h.wallet.get_master_account().await.ok().map(|m| m.address);
            if let Some(m) = master {
                h.prysm.set_master_address(m).await;
            }
            Ok((phrase, generated, master))
        });
        let mut task = task;
        tokio::select! {
            done = &mut task => match done {
                Ok(r) => r.map(|(p, _, _)| p),
                Err(e) => Err(if e.is_panic() { "engine task panicked".into() } else { "engine runtime is shutting down".into() }),
            },
            _ = tokio::time::sleep(WALLET_BUDGET) => {
                let notifier = self.notifier.clone();
                tokio::spawn(async move {
                    let text = match task.await {
                        Ok(Ok((phrase, generated, master))) => {
                            let master = master.map(|m| format!("{:?}", m)).unwrap_or_else(|| "?".into());
                            if generated {
                                format!(
                                    "🔑 <b>AKUN INDUK BARU AKTIF (selesai terlambat)</b>\nAyah: <code>{}</code>\n<tg-spoiler>{}</tg-spoiler>\n⚠️ <i>Simpan frasa ini offline lalu hapus pesan ini.</i>",
                                    crate::telegram::html::escape(&master),
                                    crate::telegram::html::escape(&phrase)
                                )
                            } else {
                                format!("✅ <b>Mnemonic diimpor (selesai terlambat)</b>\nAyah: <code>{}</code>", crate::telegram::html::escape(&master))
                            }
                        }
                        Ok(Err(_)) => "❌ <b>Penggantian akun induk gagal</b> (selesai terlambat): dompet lama tetap berlaku.".to_string(),
                        Err(_) => "❌ <b>Penggantian akun induk terhenti</b>: periksa log engine.".to_string(),
                    };
                    if let Some(n) = notifier {
                        n.notify_critical(notify_chat, text);
                    }
                });
                Err(format!(
                    "engine belum selesai dalam {} s: hasilnya dikirim sebagai notifikasi begitu selesai",
                    WALLET_BUDGET.as_secs()
                ))
            }
        }
    }

    /// Top up carriers below the minimum from the master (engine runtime: it allocates master
    /// nonces). Does not hold `apply_lock`; every RPC inside is time-bounded.
    pub async fn rebalance(&self) -> Result<Vec<GasRebalanceResult>, String> {
        let rebalancing = self.rebalancing.clone();
        self.on_engine(REBALANCE_BUDGET, move |h| async move {
            if rebalancing.fetch_add(1, Ordering::SeqCst) > 0 {
                rebalancing.fetch_sub(1, Ordering::SeqCst);
                return Err("rebalance lain sedang berjalan".to_string());
            }
            struct Done(Arc<AtomicUsize>);
            impl Drop for Done {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let _done = Done(rebalancing);
            h.wallet.rebalance_gas().await.map_err(|e| format!("{:#}", e))
        })
        .await
        .and_then(|r| r)
    }

    pub async fn master_address(&self) -> Result<Address, String> {
        self.on_engine(READ_BUDGET, |h| async move { h.wallet.get_master_account().await.map(|a| a.address).map_err(|e| format!("{:#}", e)) })
            .await
            .and_then(|r| r)
    }

    pub async fn watchlist(&self) -> Result<Vec<Address>, String> {
        self.on_engine(READ_BUDGET, |h| async move { h.config.watchlist_accounts.read().await.iter().copied().collect() }).await
    }

    pub async fn open_positions(&self) -> Result<Vec<Position>, String> {
        self.on_engine(READ_BUDGET, |h| async move { h.positions.read().await.iter().filter(|p| p.is_open).cloned().collect() }).await
    }

    pub async fn prysm_view(&self) -> Result<PrysmView, String> {
        self.on_engine(READ_BUDGET, |h| async move {
            let c = &h.config;
            let size_eth = *c.prysm_size_eth.read().await;
            let exit_mode = *c.prysm_exit_mode.read().await;
            let max_mcap_usd = *c.prysm_max_mcap_usd.read().await;
            PrysmView { enabled: c.is_prysm_enabled(), size_eth, exit_mode, max_mcap_usd, pending: h.prysm.pending_count() }
        })
        .await
    }

    pub async fn prysm_stats(&self) -> Result<(PrysmCounts, Vec<PrysmMatchReport>, f64), String> {
        self.on_engine(READ_BUDGET, |h| async move {
            let stats = h.prysm.stats();
            let l = |c: &std::sync::atomic::AtomicU64| c.load(Ordering::Relaxed);
            let counts = PrysmCounts {
                evaluated: l(&stats.total_evaluated),
                a: l(&stats.pattern_a_matches),
                b: l(&stats.pattern_b_matches),
                c: l(&stats.pattern_c_matches),
                executed: l(&stats.total_executed),
                rejected_stale: l(&stats.rejected_staleness),
                rejected_mcap: l(&stats.rejected_mcap_ceiling),
            };
            let recent: Vec<PrysmMatchReport> = stats.recent_matches.read().await.iter().cloned().collect();
            let max_mcap = *h.config.prysm_max_mcap_usd.read().await;
            (counts, recent, max_mcap)
        })
        .await
    }

    /// Everything the dashboard shows. Engine state is copied on the engine runtime; balances are
    /// read here, over a connection of their own, within `BALANCE_BUDGET`.
    pub async fn snapshot(&self, with_balances: bool) -> Result<SystemSnapshot, String> {
        let (mut snap, accounts, min_carrier_eth) = self.on_engine(READ_BUDGET, engine_snapshot).await?;
        if with_balances && !accounts.is_empty() {
            let addresses: Vec<Address> = accounts.iter().map(|(_, _, a)| *a).collect();
            let balances = self.h.wallet.balances_of(&addresses, BALANCE_BUDGET).await;
            let active = snap.settings.active_carriers();
            snap.accounts = accounts
                .into_iter()
                .zip(balances)
                .map(|((index, role, address), balance_eth)| AccountView {
                    index,
                    low: index > 0 && index <= active && balance_eth.is_some_and(|b| b < min_carrier_eth),
                    role,
                    address,
                    balance_eth,
                })
                .collect();
        } else {
            snap.accounts = accounts
                .into_iter()
                .map(|(index, role, address)| AccountView { index, role, address, balance_eth: None, low: false })
                .collect();
        }
        snap.outbox = self.notifier.as_ref().map(|n| n.stats());
        Ok(snap)
    }
}
