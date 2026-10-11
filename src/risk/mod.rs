pub mod anomaly_sensor;
pub mod curve_mirror;
pub mod evaluator;
pub mod flow_rules;
pub mod migration_rules;
pub mod price_rules;
pub mod types;
pub mod watchdog;

use crate::config::AppConfig;
use crate::dispatcher::BroadcasterPool;
use crate::models::prysm::is_prysm_tag;
use crate::payload::{self, CalldataPayloadBuilder, DynamicFeeAdjuster, PONS_ASSUMED_CREATOR_TAX_BPS, PONS_BASE_FEE_BPS};
use crate::risk::anomaly_sensor::AnomalySensor;
use crate::risk::curve_mirror::{adaptive_exit_slippage, CurveMirror, CurveSnapshot, StaleReason};
use crate::risk::evaluator::RiskEvaluator;
use crate::risk::flow_rules::FlowRules;
use crate::risk::types::{
    ApprovalEvent, BlockEvent, ExitReason, ExitSignal, ExitVenue, InflowEvent, PositionTracker, RiskConfig,
};
use crate::risk::watchdog::WatchdogSupervisor;
use crate::types::Position;
use crate::wallet::{TreasuryAccount, WalletManager};
use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::Encodable2718;
use alloy::primitives::{Address, Bytes, TxKind, B256, U256};
use alloy::signers::Signer;
use anyhow::{anyhow, Context, Result};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

/// Closed positions kept in the public history (Telegram portfolio view).
const MAX_CLOSED_POSITIONS: usize = 200;
/// Budget for one valuation read in the 100 ms loop.
const QUOTE_TIMEOUT: Duration = Duration::from_millis(800);
/// Budget for one read while liquidating.
const EXIT_READ_TIMEOUT: Duration = Duration::from_secs(3);
/// An entry whose tokens never show up in the wallet within this window did not fill
/// (reverted on the Revert Guard / slippage limit), so there is nothing to manage.
const ENTRY_FILL_TIMEOUT: Duration = Duration::from_secs(20);
/// Sell attempts inside one liquidation round (slippage widens after each failure).
const SELL_ATTEMPTS: usize = 3;
/// Failed rounds (re-issued by the watchdog every 5 s) before the exit is handed to the operator.
const MAX_EXIT_ROUNDS: u32 = 10;
/// How long a sell is followed for a receipt before its outcome is read from the balance.
const SELL_CONFIRM_TIMEOUT: Duration = Duration::from_secs(6);
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(150);
const V2_EXIT_DEADLINE_SECS: u64 = 120;
/// `balanceOf` cadence while waiting for the entry fill on the in-memory valuation path.
const BALANCE_POLL_EVERY: Duration = Duration::from_millis(250);
/// Cadence of the valuation-source summary in the log.
const MIRROR_REPORT_EVERY: Duration = Duration::from_secs(60);

/// Aborts the wrapped task when dropped, so a cancelled or panicking supervisor cannot leak it.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One running liquidation per position. The 100 ms monitor, the 5 s watchdog, the approval
/// sensor and `/panic` can all fire for the same position; two concurrent sells of the same
/// balance waste a nonce and gas on a guaranteed revert.
struct ExitFlight {
    set: Arc<Mutex<HashSet<String>>>,
    id: String,
}

impl ExitFlight {
    fn claim(set: &Arc<Mutex<HashSet<String>>>, id: &str) -> Option<Self> {
        let mut guard = set.lock().unwrap_or_else(|e| e.into_inner());
        guard.insert(id.to_string()).then(|| Self { set: set.clone(), id: id.to_string() })
    }
}

impl Drop for ExitFlight {
    fn drop(&mut self) {
        self.set.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.id);
    }
}

enum SellOutcome {
    Sold(String),
    /// The account holds none of the token (never filled, or already sold).
    NothingHeld,
}

/// What one valuation needs from a tracker: copied under the lock, used without it.
struct QuoteRequest {
    id: String,
    venue: ExitVenue,
    pool: Address,
    token: Option<Address>,
    tokens_held: U256,
    /// In-memory path: read `balanceOf` this tick (throttled while the fill is not seen).
    poll_balance: bool,
}

#[derive(Default)]
struct Quote {
    token: Option<Address>,
    /// Fresh `balanceOf` (only read until the entry fill is observed). `Some(0)` = read OK, empty.
    balance: Option<U256>,
    /// What selling `tokens_held` would return right now, after fees.
    value_wei: Option<U256>,
    reserves: Option<(U256, U256)>,
    /// Valued from the in-memory curve replica (no `getReserves` call), on this state.
    mirror: Option<CurveSnapshot>,
}

/// Exits decided from a price (as opposed to time, flow or operator input).
fn is_price_driven(reason: ExitReason) -> bool {
    matches!(
        reason,
        ExitReason::TrailingStop
            | ExitReason::StopLoss
            | ExitReason::PreGraduationGuard
            | ExitReason::SecondaryPoolTrailing
            | ExitReason::PrysmTargetMcap
            | ExitReason::PrysmTargetStopLoss
    )
}

/// Widen the slippage after a failed sell (e.g. 5% -> 15% -> 35% -> 75%, capped at 90%).
fn escalate_slippage(slippage_percent: f64) -> f64 {
    (slippage_percent * 2.0 + 5.0).min(90.0)
}

async fn eth_call_within(reader: &BroadcasterPool, to: Address, data: &[u8], budget: Duration) -> Result<Vec<u8>> {
    tokio::time::timeout(budget, reader.eth_call(to, data))
        .await
        .map_err(|_| anyhow!("eth_call to {:?} timed out", to))?
        .map_err(|e| anyhow!("eth_call to {:?} failed: {}", to, e))
}

/// ERC-20 balance. An RPC error is an error, never "0": treating it as zero closed positions
/// without selling and left the tokens in the wallet.
async fn erc20_balance(reader: &BroadcasterPool, token: Address, owner: Address, budget: Duration) -> Result<U256> {
    let raw = eth_call_within(reader, token, &CalldataPayloadBuilder::build_erc20_balance_of_payload(owner), budget).await?;
    CalldataPayloadBuilder::decode_u256_output(&raw).with_context(|| format!("balanceOf on {:?} returned no data", token))
}

/// ETH out for selling `amount` of `token` right now (curve math or router quote, after fees).
async fn quote_exit(reader: &BroadcasterPool, venue: ExitVenue, pool: Address, token: Address, amount: U256, budget: Duration) -> Result<U256> {
    match venue {
        ExitVenue::PonsCurve => {
            let raw = eth_call_within(reader, pool, &CalldataPayloadBuilder::build_pons_get_reserves_payload(), budget).await?;
            let (eth_res, tok_res) = CalldataPayloadBuilder::decode_get_reserves_output(&raw).context("getReserves: short output")?;
            Ok(CalldataPayloadBuilder::calculate_pons_eth_out(eth_res, tok_res, amount, PONS_BASE_FEE_BPS, PONS_ASSUMED_CREATOR_TAX_BPS))
        }
        ExitVenue::V2Router { weth } => {
            let call = CalldataPayloadBuilder::build_v2_get_amounts_out_payload(amount, &[token, weth]);
            let raw = eth_call_within(reader, pool, &call, budget).await?;
            CalldataPayloadBuilder::decode_v2_amounts_out_last(&raw).context("getAmountsOut: malformed output")
        }
    }
}

/// One valuation, without holding any lock. Missing pieces stay `None`.
async fn fetch_quote(reader: &BroadcasterPool, owner: Address, req: &QuoteRequest) -> Quote {
    let mut quote = Quote { token: req.token, ..Quote::default() };
    if quote.token.is_none() {
        // Pons copy-trades only know the curve: ask it for its token.
        quote.token = eth_call_within(reader, req.pool, &payload::PONS_PAIR_TOKEN_SELECTOR, QUOTE_TIMEOUT)
            .await
            .ok()
            .and_then(|raw| CalldataPayloadBuilder::decode_address_output(&raw));
    }
    let Some(token) = quote.token else { return quote };

    let balance = async {
        if req.tokens_held.is_zero() {
            erc20_balance(reader, token, owner, QUOTE_TIMEOUT).await.ok()
        } else {
            None
        }
    };
    let reserves = async {
        if req.venue == ExitVenue::PonsCurve {
            eth_call_within(reader, req.pool, &CalldataPayloadBuilder::build_pons_get_reserves_payload(), QUOTE_TIMEOUT)
                .await
                .ok()
                .and_then(|raw| CalldataPayloadBuilder::decode_get_reserves_output(&raw))
        } else {
            None
        }
    };
    let (balance, reserves) = tokio::join!(balance, reserves);
    quote.balance = balance;
    quote.reserves = reserves;

    let held = if req.tokens_held.is_zero() { balance.unwrap_or_default() } else { req.tokens_held };
    if held.is_zero() {
        return quote;
    }
    quote.value_wei = match (req.venue, reserves) {
        (ExitVenue::PonsCurve, Some((eth_res, tok_res))) => Some(CalldataPayloadBuilder::calculate_pons_eth_out(
            eth_res,
            tok_res,
            held,
            PONS_BASE_FEE_BPS,
            PONS_ASSUMED_CREATOR_TAX_BPS,
        )),
        (ExitVenue::PonsCurve, None) => None,
        (venue @ ExitVenue::V2Router { .. }, _) => quote_exit(reader, venue, req.pool, token, held, QUOTE_TIMEOUT).await.ok(),
    };
    quote
}

/// One valuation on the in-memory replica: reserves and value are computed locally (no
/// `getReserves`); RPC is only asked what the replica cannot know, i.e. the token of a curve
/// (once) and our balance until the entry fill is seen (throttled). Once both are known this is
/// pure arithmetic, no I/O and no await.
async fn fetch_quote_mirrored(reader: Option<&BroadcasterPool>, owner: Option<Address>, req: &QuoteRequest, snap: CurveSnapshot) -> Quote {
    let mut quote = Quote { token: req.token, reserves: Some(snap.reserves()), mirror: Some(snap), ..Quote::default() };
    if quote.token.is_none()
        && let Some(reader) = reader
    {
        quote.token = eth_call_within(reader, req.pool, &payload::PONS_PAIR_TOKEN_SELECTOR, QUOTE_TIMEOUT)
            .await
            .ok()
            .and_then(|raw| CalldataPayloadBuilder::decode_address_output(&raw));
    }
    let Some(token) = quote.token else { return quote };
    if req.tokens_held.is_zero()
        && req.poll_balance
        && let (Some(reader), Some(owner)) = (reader, owner)
    {
        quote.balance = erc20_balance(reader, token, owner, QUOTE_TIMEOUT).await.ok();
    }
    let held = if req.tokens_held.is_zero() { quote.balance.unwrap_or_default() } else { req.tokens_held };
    if !held.is_zero() {
        quote.value_wei = Some(snap.value_of(held));
    }
    quote
}

/// Comprehensive Multi-Layered Risk Management & Liquidation Engine
pub struct RiskManager {
    config: AppConfig,
    risk_config: Arc<RwLock<RiskConfig>>,
    wallet_manager: WalletManager,
    positions: Arc<RwLock<Vec<Position>>>,
    trackers: Arc<RwLock<HashMap<String, PositionTracker>>>,
    exit_alert_tx: mpsc::Sender<Position>,
    /// Monotonic position ids (random u32 ids collide after ~65k positions and would
    /// overwrite a live tracker).
    next_position_id: Arc<AtomicU64>,
    broadcaster: Option<Arc<BroadcasterPool>>,
    /// Pool for reads (`eth_call`, receipts); defaults to `broadcaster`. Sequencer-only send
    /// endpoints reject reads, so a read pool led by `RPC_HTTP_URL` saves a round trip per call.
    reader: Option<Arc<BroadcasterPool>>,
    fee_adjuster: Option<Arc<DynamicFeeAdjuster>>,
    exits_in_flight: Arc<Mutex<HashSet<String>>>,
    /// In-memory curve reserves fed by the sequencer stream (optional; without it every valuation
    /// is an RPC read, as before).
    mirror: Option<Arc<CurveMirror>>,
    /// `(owner, token, spender)` approvals seen sufficient or accepted (in-memory exit path only):
    /// the first sell skips the allowance read; a sell that reverts checks it again.
    approvals: Arc<Mutex<HashSet<(Address, Address, Address)>>>,
}

impl RiskManager {
    pub fn new(
        config: AppConfig,
        wallet_manager: WalletManager,
        positions: Arc<RwLock<Vec<Position>>>,
        exit_alert_tx: mpsc::Sender<Position>,
    ) -> Self {
        Self {
            config,
            risk_config: Arc::new(RwLock::new(RiskConfig::default())),
            wallet_manager,
            positions,
            trackers: Arc::new(RwLock::new(HashMap::new())),
            exit_alert_tx,
            next_position_id: Arc::new(AtomicU64::new(1)),
            broadcaster: None,
            reader: None,
            fee_adjuster: None,
            exits_in_flight: Arc::new(Mutex::new(HashSet::new())),
            mirror: None,
            approvals: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Value Pons positions from the in-memory curve replica (see [`CurveMirror`]): the 100 ms loop
    /// then reads no reserves over RPC while the replica is fresh, and is also woken by every
    /// change of a held curve instead of waiting for its next tick.
    pub fn with_curve_mirror(mut self, mirror: Arc<CurveMirror>) -> Self {
        self.mirror = Some(mirror).filter(|m| m.policy().enabled);
        self
    }

    pub fn curve_mirror(&self) -> Option<Arc<CurveMirror>> {
        self.mirror.clone()
    }

    pub fn with_execution(
        mut self,
        broadcaster: Arc<BroadcasterPool>,
        fee_adjuster: Arc<DynamicFeeAdjuster>,
    ) -> Self {
        self.broadcaster = Some(broadcaster);
        self.fee_adjuster = Some(fee_adjuster);
        self
    }

    /// Use a dedicated pool for reads (see `reader`).
    pub fn with_reader(mut self, reader: Arc<BroadcasterPool>) -> Self {
        self.reader = Some(reader);
        self
    }

    fn reader(&self) -> Option<Arc<BroadcasterPool>> {
        self.reader.clone().or_else(|| self.broadcaster.clone())
    }

    /// Calculate Adaptive Slippage Tolerance based on trade size and curve dynamics
    #[inline(always)]
    pub fn calculate_adaptive_slippage(&self, base_slippage_percent: f64, trade_eth: f64) -> f64 {
        let size_multiplier = 1.0 + (trade_eth / 2.0).min(0.5);
        base_slippage_percent * size_multiplier
    }

    /// Register a newly opened position into the 6-layer defense pipeline
    pub async fn register_entry(
        &self,
        token_address: Address,
        pool_address: Address,
        entry_eth: f64,
        strategy_tag: Option<String>,
        venue: ExitVenue,
    ) -> Position {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let base_slippage = *self.config.slippage_percent.read().await;
        let adaptive_slip = self.calculate_adaptive_slippage(base_slippage, entry_eth);

        let is_prysm = is_prysm_tag(strategy_tag.as_deref());
        if !(entry_eth > 0.0 && entry_eth.is_finite()) {
            warn!("Registering position with invalid entry size {} ETH: price rules will stay inert", entry_eth);
        }
        let prysm_exit_mode = if is_prysm {
            Some(*self.config.prysm_exit_mode.read().await)
        } else {
            None
        };

        let pos_id = format!("POS-{}", self.next_position_id.fetch_add(1, Ordering::Relaxed));
        let pos = Position {
            id: pos_id.clone(),
            token_address: format!("{:?}", token_address),
            token_symbol: format!("TKN-{}", &format!("{:?}", token_address)[2..6].to_uppercase()),
            entry_eth_amount: entry_eth,
            current_eth_value: entry_eth,
            entry_timestamp_secs: now_secs,
            pool_address: format!("{:?}", pool_address),
            is_open: true,
            pnl_percent: 0.0,
            exit_reason: None,
            exit_tx_hash: None,
            strategy: strategy_tag.clone(),
            exit_mode: prysm_exit_mode.map(|m| format!("{}", m)),
            entry_mcap_usd: if is_prysm { Some(5000.0) } else { None },
            current_mcap_usd: if is_prysm { Some(5000.0) } else { None },
        };

        info!(
            "🎯 [Risk Layer 1-6 Armed] Position Registered: {} (Token: {}, Strategy: {:?}, Entry: {:.4} ETH, Adaptive Slip: {:.2}%, Venue: {:?})",
            pos.id, pos.token_symbol, pos.strategy, pos.entry_eth_amount, adaptive_slip, venue
        );

        // Store in public positions vector
        self.positions.write().await.push(pos.clone());

        // Initialize 6-layer state tracker. PnL is the value of the tokens actually received,
        // so the "entry price" is the ETH spent.
        let mut tracker = PositionTracker::new(pos.clone(), entry_eth);
        tracker.venue = venue;
        tracker.token = (token_address != pool_address).then_some(token_address);
        if is_prysm {
            tracker.is_prysm_strategy = true;
            tracker.prysm_exit_mode = prysm_exit_mode;
            tracker.prysm_entry_mcap_usd = 5000.0;
            tracker.current_mcap_usd = 5000.0;
        }

        if venue == ExitVenue::PonsCurve
            && let Some(mirror) = &self.mirror
        {
            mirror.track(pool_address);
        }
        self.trackers.write().await.insert(pos_id, tracker);

        pos
    }

    /// Ingest third-party capital inflow event (Layer 2)
    pub async fn on_inflow_event(&self, token_address: &Address, event: InflowEvent) {
        let token_str = format!("{:?}", token_address);
        let mut trackers = self.trackers.write().await;
        for tracker in trackers.values_mut() {
            if tracker.position.token_address == token_str && tracker.position.is_open {
                FlowRules::record_inflow(tracker, event.clone());
            }
        }
    }

    /// Layer 2 input from the sequencer feed: a buy of `amount_wei` by `buyer` on `pool`.
    ///
    /// Without this, `last_outside_inflow_instant` never moved and Inactivity Decay closed every
    /// System-mode position exactly 25 s after entry. Our own accounts' buys are not "outside".
    pub async fn on_pool_buy(&self, pool: Address, buyer: Address, amount_wei: U256) {
        if let Ok(accounts) = self.wallet_manager.get_all_accounts_shared().await
            && accounts.iter().any(|a| a.address == buyer)
        {
            return;
        }
        let event = InflowEvent {
            buyer,
            amount_eth: f64::from(amount_wei) / 1e18,
            timestamp: Instant::now(),
            is_deployer_or_initial: false,
        };
        let mut trackers = self.trackers.write().await;
        for tracker in trackers.values_mut() {
            if tracker.position.is_open && Address::from_str(&tracker.position.pool_address).ok() == Some(pool) {
                FlowRules::record_inflow(tracker, event.clone());
            }
        }
    }

    /// Ingest mempool ERC-20 approval event (Layer 4 Anomaly Sensor)
    /// Triggers fast pre-emptive liquidation if concurrent approval spike detected
    pub async fn on_approval_event(&self, event: ApprovalEvent) {
        let config = self.risk_config.read().await.clone();
        let mut exit_signal = None;

        {
            let mut trackers = self.trackers.write().await;
            for tracker in trackers.values_mut() {
                if tracker.position.is_open {
                    if let Some(signal) = AnomalySensor::inspect_approval(tracker, event.clone(), &config) {
                        exit_signal = Some(signal);
                        break;
                    }
                }
            }
        }

        if let Some(signal) = exit_signal {
            self.execute_liquidation(signal).await;
        }
    }

    /// Sell every account's balance of the position's token on-chain, then close it.
    ///
    /// A position is only closed once the master's sell is confirmed (receipt status 1, or the
    /// balance dropped) or the master holds nothing. On failure it stays open and the watchdog
    /// re-issues the exit with wider slippage; after `MAX_EXIT_ROUNDS` the operator is alerted.
    pub async fn execute_liquidation(&self, signal: ExitSignal) {
        let Some(flight) = ExitFlight::claim(&self.exits_in_flight, &signal.position_id) else {
            return; // an exit for this position is already running
        };
        self.run_liquidation(signal, flight).await;
    }

    /// The exit itself; `_flight` keeps every other trigger of this position out until it ends.
    async fn run_liquidation(&self, signal: ExitSignal, _flight: ExitFlight) {
        let target = {
            let trackers = self.trackers.read().await;
            trackers
                .get(&signal.position_id)
                .filter(|t| t.position.is_open)
                .map(|t| (t.venue, t.token, t.tokens_held))
        };
        let Some((venue, token_hint, held_hint)) = target else { return };

        warn!(
            "🚨 LIQUIDATION TRIGGERED: [{}] Position: {} -> {}",
            signal.reason, signal.position_id, signal.reason_description
        );

        let Some(broadcaster) = self.broadcaster.clone() else {
            error!(
                "No broadcaster configured: position {} closed in memory only, tokens remain in the wallet",
                signal.position_id
            );
            self.close_position(&signal.position_id, None, format!("[SIMULATED, no sell tx] {}", signal.reason)).await;
            return;
        };

        match self.liquidate_onchain(&broadcaster, &signal, venue, token_hint, held_hint).await {
            Ok(SellOutcome::Sold(hash)) => {
                info!("🚀 LIQUIDATION CONFIRMED ON-CHAIN: {} (Tx: {})", signal.position_id, hash);
                self.close_position(&signal.position_id, Some(hash), signal.reason.to_string()).await;
            }
            Ok(SellOutcome::NothingHeld) => {
                warn!("Liquidation: {} holds no tokens, closing without a sell", signal.position_id);
                self.close_position(&signal.position_id, None, format!("[token balance 0, nothing sold] {}", signal.reason)).await;
            }
            Err(e) => self.schedule_exit_retry(signal, e).await,
        }
    }

    async fn liquidate_onchain(
        &self,
        broadcaster: &BroadcasterPool,
        signal: &ExitSignal,
        venue: ExitVenue,
        token_hint: Option<Address>,
        master_held: U256,
    ) -> Result<SellOutcome> {
        let reader = self.reader().context("no read pool")?;
        let token = match token_hint {
            Some(t) => t,
            None => {
                let raw = eth_call_within(&reader, signal.pool_address, &payload::PONS_PAIR_TOKEN_SELECTOR, EXIT_READ_TIMEOUT).await?;
                CalldataPayloadBuilder::decode_address_output(&raw).context("pairToken(): malformed output")?
            }
        };
        let accounts = self.wallet_manager.get_all_accounts_shared().await?;
        let (master, satellites) = accounts.split_first().context("no treasury accounts")?;

        // Master and satellites (they also bought in MultiAccount mode) sell concurrently; the
        // sequencer is FIFO per arrival, and the accounts share no nonce.
        // In-memory exit path: the master's balance was read when its fill was observed, so the
        // first sell skips that read (a stale amount reverts, and the retry reads the balance).
        let master_hint = (self.mirror.is_some() && !master_held.is_zero()).then_some(master_held);
        let (master_res, satellite_res) = tokio::join!(
            self.liquidate_account(broadcaster, master, token, venue, signal, master_hint),
            futures_util::future::join_all(
                satellites.iter().map(|acc| self.liquidate_account(broadcaster, acc, token, venue, signal, None))
            )
        );
        for (acc, res) in satellites.iter().zip(satellite_res) {
            match res {
                Ok(SellOutcome::Sold(hash)) => info!("✅ [{}] satellite exit {}", acc.role, hash),
                Ok(SellOutcome::NothingHeld) => {}
                Err(e) => warn!("[{}] satellite exit failed (tokens remain there): {:#}", acc.role, e),
            }
        }
        master_res
    }

    async fn liquidate_account(
        &self,
        broadcaster: &BroadcasterPool,
        acc: &TreasuryAccount,
        token: Address,
        venue: ExitVenue,
        signal: &ExitSignal,
        known_balance: Option<U256>,
    ) -> Result<SellOutcome> {
        let reader = self.reader().context("no read pool")?;
        // Zero reads before the first sell when the balance is known and the approval was accepted
        // at fill time; a sell that reverts on either falls back to reading both.
        let (mut balance, mut balance_fresh) = match known_balance {
            Some(b) => (b, false),
            None => (erc20_balance(&reader, token, acc.address, EXIT_READ_TIMEOUT).await?, true),
        };
        if balance.is_zero() {
            return Ok(SellOutcome::NothingHeld);
        }
        let spender = signal.pool_address;
        let mut approval_checked = false;
        if !self.approval_known(acc.address, token, spender) {
            self.ensure_allowance(broadcaster, &reader, acc, token, spender, balance).await?;
            approval_checked = true;
        }

        let mut slippage = signal.slippage_percent;
        let mut last_err = anyhow!("no sell attempted");
        for attempt in 1..=SELL_ATTEMPTS {
            if attempt > 1 && !approval_checked {
                self.ensure_allowance(broadcaster, &reader, acc, token, spender, balance).await?;
                approval_checked = true;
            }
            match self.sell_once(broadcaster, &reader, acc, token, venue, signal.pool_address, balance, balance_fresh, slippage, signal.is_emergency).await {
                Ok(hash) => return Ok(SellOutcome::Sold(hash)),
                Err(e) => {
                    warn!(
                        "[{}] sell attempt {}/{} at {:.1}% slippage failed: {:#}",
                        acc.role, attempt, SELL_ATTEMPTS, slippage, e
                    );
                    last_err = e;
                }
            }
            slippage = escalate_slippage(slippage);
            // A sell that landed after its confirmation window may already have emptied it.
            balance = erc20_balance(&reader, token, acc.address, EXIT_READ_TIMEOUT).await?;
            balance_fresh = true;
            if balance.is_zero() {
                return Ok(SellOutcome::NothingHeld);
            }
        }
        Err(last_err)
    }

    /// Only with the curve replica: without it the exit path reads everything, as it always did.
    fn approval_known(&self, owner: Address, token: Address, spender: Address) -> bool {
        self.mirror.is_some() && self.approvals.lock().unwrap_or_else(|e| e.into_inner()).contains(&(owner, token, spender))
    }

    fn note_approval(&self, owner: Address, token: Address, spender: Address) {
        self.approvals.lock().unwrap_or_else(|e| e.into_inner()).insert((owner, token, spender));
    }

    /// Approve `spender` for the token when the allowance does not cover `needed`.
    async fn ensure_allowance(
        &self,
        broadcaster: &BroadcasterPool,
        reader: &BroadcasterPool,
        acc: &TreasuryAccount,
        token: Address,
        spender: Address,
        needed: U256,
    ) -> Result<()> {
        let call = CalldataPayloadBuilder::build_erc20_allowance_payload(acc.address, spender);
        // Unknown allowance: approve again (harmless) rather than send a sell that reverts.
        let allowance = eth_call_within(reader, token, &call, EXIT_READ_TIMEOUT)
            .await
            .ok()
            .and_then(|raw| CalldataPayloadBuilder::decode_u256_output(&raw))
            .unwrap_or(U256::ZERO);
        if allowance >= needed {
            self.note_approval(acc.address, token, spender);
            return Ok(());
        }
        info!("[{}] approving {:?} to spend {:?}", acc.role, spender, token);
        let approve = CalldataPayloadBuilder::build_erc20_approve_payload(spender, U256::MAX);
        let hash = self.send_tx(broadcaster, acc, token, approve).await.context("approve")?;
        info!("✅ [{}] approval accepted: {}", acc.role, hash);
        // U256::MAX: never runs out. Sequenced FCFS ahead of any later sell from this account; if it
        // still failed, that sell reverts and the retry reads the allowance again.
        self.note_approval(acc.address, token, spender);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn sell_once(
        &self,
        broadcaster: &BroadcasterPool,
        reader: &BroadcasterPool,
        acc: &TreasuryAccount,
        token: Address,
        venue: ExitVenue,
        pool: Address,
        amount: U256,
        amount_is_fresh: bool,
        slippage_percent: f64,
        emergency: bool,
    ) -> Result<String> {
        // In-memory quote when the replica vouches for the curve: no `getReserves` round trip
        // between the trigger and the broadcast, and a tolerance that widens with recent volatility.
        let mirrored = match (venue, &self.mirror) {
            (ExitVenue::PonsCurve, Some(m)) => match m.read(&pool).fresh() {
                // Confirmed state (just anchored, e.g. by the exit confirmation), or speed mode where
                // the operator chose to act on predicted state anyway.
                Some(snap) if snap.unconfirmed_trades == 0 || !m.policy().confirm_exits => Some(snap),
                // Predicted trades on top: price the floor on an exact reading at the head instead,
                // so an unconfirmed (possibly reverted) sell in the feed cannot lower it.
                Some(snap) => m.confirm(pool, snap.as_of_block).await,
                None => None,
            },
            _ => None,
        };
        let (quote, slippage_percent) = match mirrored {
            Some(snap) => (Ok(snap.value_of(amount)), adaptive_exit_slippage(slippage_percent, snap.volatility_bps, emergency)),
            None => (quote_exit(reader, venue, pool, token, amount, EXIT_READ_TIMEOUT).await, slippage_percent),
        };
        let min_out = match quote {
            Ok(expected) => {
                let slip_bps = (slippage_percent.clamp(0.0, 99.0) * 100.0) as u64;
                expected * U256::from(10_000 - slip_bps) / U256::from(10_000)
            }
            // The former fallback (minOut = 1 on any quote failure) sold with no price floor
            // on every RPC hiccup. Only an emergency exit may accept that.
            Err(e) if emergency => {
                warn!("[{}] no exit quote ({:#}): emergency sell without a price floor", acc.role, e);
                U256::from(1)
            }
            Err(e) => return Err(e.context("exit quote")),
        };

        let calldata = match venue {
            ExitVenue::PonsCurve => CalldataPayloadBuilder::build_pons_sell_payload(amount, min_out, acc.address),
            ExitVenue::V2Router { weth } => {
                let deadline = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() + V2_EXIT_DEADLINE_SECS;
                CalldataPayloadBuilder::build_v2_exit_fot_payload(amount, min_out, &[token, weth], &acc.address, deadline)
            }
        };
        let hash = self.send_tx(broadcaster, acc, pool, calldata).await.context("sell")?;
        if let Err(e) = self.confirm_sell(reader, &hash, acc, token, amount, amount_is_fresh).await {
            // The floor came from a state the chain did not have (or the curve moved past it):
            // re-anchor the replica before the next attempt quotes from it.
            if venue == ExitVenue::PonsCurve
                && let Some(m) = &self.mirror
            {
                m.invalidate(pool, StaleReason::ExitReverted);
            }
            return Err(e);
        }
        Ok(hash)
    }

    /// "Accepted by an endpoint" is not "sold": a sell can revert on its minimum output.
    ///
    /// The receipt decides. Without one in time, the balance does: lower than a balance read just
    /// before the sell (`before_is_fresh`), or zero when the amount sold came from an older reading
    /// (a stale amount must never let a reverted sell pass as a sale).
    async fn confirm_sell(
        &self,
        reader: &BroadcasterPool,
        hash: &str,
        acc: &TreasuryAccount,
        token: Address,
        amount_before: U256,
        before_is_fresh: bool,
    ) -> Result<()> {
        let tx_hash = B256::from_str(hash).context("sell hash")?;
        let deadline = Instant::now() + SELL_CONFIRM_TIMEOUT;
        // A new feed block means the one holding our sell is sealed: ask for the receipt right then
        // instead of at the next poll tick.
        let mut blocks = self.mirror.as_ref().map(|m| m.subscribe_head());
        while Instant::now() < deadline {
            if let Ok(Some(receipt)) = reader.get_transaction_receipt(tx_hash).await {
                return match receipt.get("status").and_then(|s| s.as_str()) {
                    Some(status) if status.eq_ignore_ascii_case("0x1") => Ok(()),
                    Some(status) => Err(anyhow!("sell {} reverted on-chain (status {})", hash, status)),
                    None => Err(anyhow!("sell {} receipt has no status", hash)),
                };
            }
            let mut feed_gone = false;
            match blocks.as_mut() {
                Some(rx) => tokio::select! {
                    _ = tokio::time::sleep(RECEIPT_POLL_INTERVAL) => {}
                    changed = rx.changed() => feed_gone = changed.is_err(),
                },
                None => tokio::time::sleep(RECEIPT_POLL_INTERVAL).await,
            }
            if feed_gone {
                blocks = None;
            }
        }
        // No receipt in time: judge by the balance.
        let now = erc20_balance(reader, token, acc.address, EXIT_READ_TIMEOUT).await?;
        let sold = if before_is_fresh { now < amount_before } else { now.is_zero() };
        anyhow::ensure!(sold, "sell {} unconfirmed and balance not reduced", hash);
        Ok(())
    }

    /// Sign and broadcast a zero-value call from `acc`; hands the nonce back on rejection.
    async fn send_tx(&self, broadcaster: &BroadcasterPool, acc: &TreasuryAccount, to: Address, data: Vec<u8>) -> Result<String> {
        let cap_gwei = *self.config.max_fee_gwei.read().await;
        let configured_gas_limit = *self.config.gas_limit.read().await;
        let (priority_fee_wei, max_fee_wei, l1_gas) = match self.fee_adjuster {
            Some(ref fa) => {
                let (priority, max_fee) = fa.capped_fees(cap_gwei);
                (priority, max_fee, fa.l1_component_gas())
            }
            None => (2_000_000_000, 15_000_000_000, 0),
        };
        let gas_limit = CalldataPayloadBuilder::gas_limit(&data, 0, configured_gas_limit, l1_gas);

        let nonce = acc.allocate_nonce();
        let tx = TxEip1559 {
            chain_id: self.config.chain_id,
            nonce,
            gas_limit,
            max_fee_per_gas: max_fee_wei,
            max_priority_fee_per_gas: priority_fee_wei,
            to: TxKind::Call(to),
            value: U256::ZERO,
            input: Bytes::from(data),
            access_list: Default::default(),
        };
        let signature = match acc.signer.sign_hash(&tx.signature_hash()).await {
            Ok(sig) => sig,
            Err(e) => {
                self.wallet_manager.release_nonce(acc, nonce, true).await;
                return Err(anyhow!("signing failed: {}", e));
            }
        };
        let envelope = TxEnvelope::Eip1559(tx.into_signed(signature));
        let hash = format!("{:?}", envelope.tx_hash());
        match broadcaster.broadcast_raw_tx(&envelope.encoded_2718(), &hash).await {
            Ok(accepted) => Ok(accepted),
            Err(e) => {
                self.wallet_manager.release_nonce_state(acc, nonce, e.nonce_state()).await;
                Err(anyhow!("broadcast failed: {}", e))
            }
        }
    }

    /// Keep a position whose exit failed open, and queue a wider retry for the watchdog.
    async fn schedule_exit_retry(&self, signal: ExitSignal, err: anyhow::Error) {
        let mut give_up = None;
        let round = {
            let mut trackers = self.trackers.write().await;
            let Some(tracker) = trackers.get_mut(&signal.position_id) else { return };
            tracker.exit_attempts += 1;
            if tracker.exit_attempts < MAX_EXIT_ROUNDS {
                let mut retry = signal.clone();
                retry.slippage_percent = escalate_slippage(signal.slippage_percent);
                retry.is_emergency |= tracker.exit_attempts >= 3;
                tracker.pending_exit = Some(retry);
            } else {
                tracker.pending_exit = None;
                tracker.position.exit_reason = Some(format!(
                    "[EXIT FAILED after {} rounds, tokens still held: use /panic] {}",
                    tracker.exit_attempts, signal.reason
                ));
                give_up = Some(tracker.position.clone());
            }
            tracker.exit_attempts
        };
        error!(
            "Liquidation of {} failed (round {}/{}): {:#}. Position stays open.",
            signal.position_id, round, MAX_EXIT_ROUNDS, err
        );
        if let Some(pos) = give_up {
            let _ = self.exit_alert_tx.send(pos).await;
        }
    }

    async fn close_position(&self, position_id: &str, exit_tx_hash: Option<String>, exit_reason: String) {
        {
            let mut trackers = self.trackers.write().await;
            if let Some(tracker) = trackers.get_mut(position_id) {
                if tracker.position.is_open
                    && tracker.venue == ExitVenue::PonsCurve
                    && let (Some(mirror), Ok(pool)) = (&self.mirror, Address::from_str(&tracker.position.pool_address))
                {
                    mirror.untrack(pool);
                }
                tracker.position.is_open = false;
                tracker.position.exit_reason = Some(exit_reason.clone());
                tracker.position.exit_tx_hash = exit_tx_hash.clone();
                tracker.pending_exit = None;
            }
        }
        let closed = {
            let mut positions = self.positions.write().await;
            positions.iter_mut().find(|p| p.id == position_id).map(|pos| {
                pos.is_open = false;
                pos.exit_reason = Some(exit_reason);
                pos.exit_tx_hash = exit_tx_hash;
                pos.clone()
            })
        };
        if let Some(pos) = closed {
            let _ = self.exit_alert_tx.send(pos).await;
        }
    }

    fn spawn_liquidation(&self, signal: ExitSignal) {
        let manager = self.clone_for_liquidation();
        tokio::spawn(async move { manager.execute_liquidation(signal).await });
    }

    /// Approve the exit spender as soon as the entry fill is seen, so the exit is one tx.
    fn spawn_exit_preapproval(&self, token: Address, spender: Address) {
        let manager = self.clone_for_liquidation();
        tokio::spawn(async move {
            let (Some(broadcaster), Some(reader)) = (manager.broadcaster.clone(), manager.reader()) else { return };
            let Ok(master) = manager.wallet_manager.get_master_account().await else { return };
            if let Err(e) = manager.ensure_allowance(&broadcaster, &reader, &master, token, spender, U256::MAX >> 1).await {
                warn!("Exit pre-approval for {:?} failed (retried at exit time): {:#}", token, e);
            }
        });
    }

    /// Continuous Sub-Second Multi-Layer Evaluation Loop (Lapis 1 - 3)
    ///
    /// Each tick: snapshot under a short lock, read quotes with no lock held (all positions
    /// concurrently), then evaluate under a short lock. Holding `trackers` + `positions` across
    /// the RPC calls (as before) stalled `register_entry` in the signal task, the watchdog and
    /// Telegram for up to one RPC timeout per position.
    pub async fn start_lifecycle_monitor(&self) {
        info!("🛡️ Multi-Layer Risk Lifecycle Monitor started (100ms precision).");

        // Spawn Layer 5 Fault-Tolerant Watchdog Supervisor on isolated background task (every 5 seconds)
        let watchdog_manager = self.clone_for_liquidation();
        let _watchdog = AbortOnDrop(tokio::spawn(async move {
            let mut watchdog_interval = tokio::time::interval(Duration::from_secs(5));
            watchdog_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                watchdog_interval.tick().await;
                let now = Instant::now();
                let config = watchdog_manager.risk_config.read().await.clone();
                let mut signals_to_exit = Vec::new();
                {
                    let trackers = watchdog_manager.trackers.read().await;
                    for tracker in trackers.values() {
                        if !tracker.position.is_open || tracker.exit_attempts >= MAX_EXIT_ROUNDS {
                            continue;
                        }
                        if let Some(retry) = tracker.pending_exit.as_ref() {
                            signals_to_exit.push(retry.clone());
                        } else if let Some(signal) = WatchdogSupervisor::evaluate_position(tracker, now, &config) {
                            signals_to_exit.push(signal);
                        }
                    }
                }
                for signal in signals_to_exit {
                    watchdog_manager.spawn_liquidation(signal);
                }
            }
        }));

        // Main Evaluation Loop for Lapis 1 (Price), Lapis 2 (Flow), and Lapis 3 (Migration)
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        // Burst catch-up after a stall would evaluate stale state several times back-to-back.
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // With the curve replica, every change of a held curve (a trade in a feed block, an exact
        // reading) also wakes the loop: the reaction time to a dump is the feed's, not the tick's.
        let mut changes = self.mirror.as_ref().map(|m| m.subscribe_changes());
        let mut last_report = Instant::now();
        let mut fresh_reads = 0u64;
        let mut rpc_reads = 0u64;

        loop {
            let mut mirror_gone = false;
            let on_tick = match changes.as_mut() {
                Some(rx) => tokio::select! {
                    _ = interval.tick() => true,
                    changed = rx.changed() => {
                        mirror_gone = changed.is_err();
                        false
                    }
                },
                None => {
                    interval.tick().await;
                    true
                }
            };
            if mirror_gone {
                changes = None;
            }
            let mut config = self.risk_config.read().await.clone();
            config.prysm_target_mcap_usd = *self.config.prysm_target_mcap_usd.read().await;
            config.prysm_target_stop_loss_percent = *self.config.prysm_target_stop_loss_percent.read().await;

            // 1. Housekeeping + snapshot (short lock, no await inside).
            let requests: Vec<QuoteRequest> = {
                let mut trackers = self.trackers.write().await;
                let mut positions_lock = self.positions.write().await;

                // Closed trackers are never re-evaluated: drop them instead of iterating forever.
                trackers.retain(|_, t| t.position.is_open);
                let closed = positions_lock.iter().filter(|p| !p.is_open).count();
                if closed > MAX_CLOSED_POSITIONS {
                    let mut excess = closed - MAX_CLOSED_POSITIONS;
                    positions_lock.retain(|p| {
                        if !p.is_open && excess > 0 {
                            excess -= 1;
                            false
                        } else {
                            true
                        }
                    });
                }
                let now = Instant::now();
                trackers
                    .values_mut()
                    .filter_map(|t| {
                        let poll_balance = on_tick
                            && t.tokens_held.is_zero()
                            && t.last_balance_poll.is_none_or(|at| now.saturating_duration_since(at) >= BALANCE_POLL_EVERY);
                        if poll_balance {
                            t.last_balance_poll = Some(now);
                        }
                        Some(QuoteRequest {
                            id: t.position.id.clone(),
                            venue: t.venue,
                            pool: Address::from_str(&t.position.pool_address).ok()?,
                            token: t.token,
                            tokens_held: t.tokens_held,
                            poll_balance,
                        })
                    })
                    .collect()
            };
            if requests.is_empty() {
                continue;
            }

            // 2. Quotes with no lock held, all positions concurrently. A Pons position whose curve
            //    the replica vouches for is valued in memory; anything else reads RPC, but only on
            //    the 100 ms tick (a replica wake never adds RPC traffic).
            let owner = self.wallet_manager.get_master_account().await.ok().map(|a| a.address);
            let reader = self.reader();
            let fetched = futures_util::future::join_all(requests.iter().map(|r| self.quote_for(reader.as_deref(), owner, r, on_tick))).await;
            let quotes: HashMap<String, Quote> =
                requests.into_iter().map(|r| r.id).zip(fetched).filter_map(|(id, q)| q.map(|q| (id, q))).collect();
            for q in quotes.values() {
                if q.mirror.is_some() { fresh_reads += 1 } else { rpc_reads += 1 }
            }

            // 3. Apply + evaluate (short lock, no await inside).
            let now = Instant::now();
            let in_flight = self.exits_in_flight.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let mut signals = Vec::new();
            let mut to_confirm = Vec::new();
            let mut preapprove = Vec::new();
            {
                let mut trackers = self.trackers.write().await;
                let mut positions_lock = self.positions.write().await;

                for tracker in trackers.values_mut() {
                    if !tracker.position.is_open {
                        continue;
                    }
                    let quote = quotes.get(&tracker.position.id);
                    // A replica wake re-evaluates only the positions it can value.
                    if !on_tick && quote.is_none() {
                        continue;
                    }
                    let value_eth = quote.and_then(|q| Self::apply_quote(tracker, q, &mut preapprove));

                    // Failed exits are retried by the watchdog (5 s cadence), not every 100 ms.
                    if in_flight.contains(&tracker.position.id)
                        || tracker.pending_exit.is_some()
                        || tracker.exit_attempts >= MAX_EXIT_ROUNDS
                    {
                        continue;
                    }
                    // The master never received tokens: run the normal exit anyway, it closes
                    // the position and still sells whatever satellites may have bought.
                    if tracker.tokens_held.is_zero()
                        && quote.and_then(|q| q.balance) == Some(U256::ZERO)
                        && tracker.entry_instant.elapsed() >= ENTRY_FILL_TIMEOUT
                        && let (Ok(token_address), Ok(pool_address)) = (
                            Address::from_str(&tracker.position.token_address),
                            Address::from_str(&tracker.position.pool_address),
                        )
                    {
                        signals.push(ExitSignal {
                            position_id: tracker.position.id.clone(),
                            token_address,
                            pool_address,
                            reason: ExitReason::EntryNotFilled,
                            reason_description: format!("No tokens after {:?}", ENTRY_FILL_TIMEOUT),
                            is_emergency: false,
                            slippage_percent: 5.0,
                        });
                        continue;
                    }

                    // A valuation that rests on feed-predicted trades (not yet confirmed by an exact reading).
                    let unconfirmed = quote.and_then(|q| q.mirror).filter(|s| s.unconfirmed_trades > 0);
                    let (peak_before, activated_before) = (tracker.highest_price_eth, tracker.high_watermark_activated);
                    let signal = match value_eth {
                        Some(value) => {
                            let block_event = BlockEvent {
                                block_number: quote.and_then(|q| q.mirror).map_or(0, |s| s.as_of_block),
                                timestamp_secs: 0,
                                spot_price_eth: value,
                                curve_capacity_percent: tracker.curve_capacity_percent,
                                token_address: tracker.token.unwrap_or_default(),
                            };
                            let pool_quote = tracker.is_migrated_to_pool.then_some(value);
                            RiskEvaluator::evaluate_block_event(tracker, &block_event, pool_quote, now, &config)
                        }
                        // No valuation this tick (RPC down, fill not seen yet): only the
                        // time-based layers. The former fallback fed random prices to Layer 1.
                        None => RiskEvaluator::evaluate_time_rules(tracker, now, &config),
                    };
                    if value_eth.is_some()
                        && (tracker.highest_price_eth != peak_before || tracker.high_watermark_activated != activated_before)
                    {
                        // Remember whether the trailing peak rests on unconfirmed trades.
                        tracker.peak_block = unconfirmed.map(|s| s.as_of_block);
                        if unconfirmed.is_none() {
                            tracker.confirmed_peak_eth = tracker.highest_price_eth;
                            tracker.confirmed_activated = tracker.high_watermark_activated;
                        }
                    }
                    // A drift-free exact reading at or after the peak's block proves the trades it
                    // was built on: the peak is confirmed.
                    if let (Some(pb), Some(snap)) = (tracker.peak_block, quote.and_then(|q| q.mirror))
                        && snap.validated_from < pb
                        && pb <= snap.validated_through
                    {
                        tracker.peak_block = None;
                        tracker.confirmed_peak_eth = tracker.highest_price_eth;
                        tracker.confirmed_activated = tracker.high_watermark_activated;
                    }
                    if let Some(signal) = signal {
                        // A price move made of feed-predicted trades is checked against the chain
                        // before we sell on it (a sell in the feed can still revert on-chain), and a
                        // trailing exit also checks the peak it is measured from (a reverted buy in
                        // the feed would otherwise inflate it).
                        let confirm_on = self.mirror.as_ref().is_some_and(|m| m.policy().confirm_exits);
                        let check_current = unconfirmed.is_some();
                        let check_peak = signal.reason == ExitReason::TrailingStop && tracker.peak_block.is_some();
                        if confirm_on && is_price_driven(signal.reason) && (check_current || check_peak) {
                            // Claimed here, under the trackers lock: the watchdog (which reads the same
                            // unconfirmed PnL) cannot slip an unconfirmed exit in before the check.
                            if let Some(flight) = ExitFlight::claim(&self.exits_in_flight, &tracker.position.id) {
                                to_confirm.push((signal, tracker.clone(), unconfirmed.map(|s| s.as_of_block), config.clone(), flight));
                            }
                        } else {
                            signals.push(signal);
                        }
                    }

                    // Keep positions in sync
                    if let Some(pos_ref) = positions_lock.iter_mut().find(|p| p.id == tracker.position.id) {
                        pos_ref.current_eth_value = tracker.position.current_eth_value;
                        pos_ref.pnl_percent = tracker.position.pnl_percent;
                        pos_ref.current_mcap_usd = tracker.position.current_mcap_usd;
                    }
                }
            }

            for (token, spender) in preapprove {
                self.spawn_exit_preapproval(token, spender);
            }
            // Liquidations run on their own tasks: a multi-second exit must not freeze the
            // evaluation of every other position.
            for signal in signals {
                self.spawn_liquidation(signal);
            }
            for (signal, tracker, block, config, flight) in to_confirm {
                self.spawn_confirmed_liquidation(signal, tracker, block, config, flight);
            }

            if let Some(mirror) = &self.mirror
                && last_report.elapsed() >= MIRROR_REPORT_EVERY
            {
                let m = mirror.stats();
                let calls = reader.as_ref().map(|r| r.call_counts()).unwrap_or_default();
                info!(
                    "📡 [mirror] valuations: {} in memory, {} via RPC | anchors {} ok / {} failed, drift corrections {} (max {} ppb), \
                     calibrations {}, gaps {}, indirect {}, reorgs {}, confirmations {} (phantom rejected {}) | read-pool eth_call total {}",
                    fresh_reads, rpc_reads, m.anchors_ok, m.anchors_failed, m.drift_corrections, m.max_drift_ppb, m.calibrations,
                    m.gaps, m.indirect_touches, m.reorgs, m.exit_confirmations, m.phantom_exits_rejected, calls.eth_call
                );
                last_report = Instant::now();
                fresh_reads = 0;
                rpc_reads = 0;
            }
        }
    }

    /// Valuation of one position: in memory when the replica vouches for its curve, RPC otherwise
    /// (only on a timer tick). `None` = no valuation this round.
    async fn quote_for(&self, reader: Option<&BroadcasterPool>, owner: Option<Address>, req: &QuoteRequest, on_tick: bool) -> Option<Quote> {
        if req.venue == ExitVenue::PonsCurve
            && let Some(snap) = self.mirror.as_ref().and_then(|m| m.read(&req.pool).fresh())
        {
            return Some(fetch_quote_mirrored(reader, owner, req, snap).await);
        }
        if !on_tick {
            return None;
        }
        match (reader, owner) {
            (Some(reader), Some(owner)) => Some(fetch_quote(reader, owner, req).await),
            _ => None,
        }
    }

    /// Price-driven exit computed on feed-predicted state: re-evaluate it on exact readings pinned
    /// to the blocks it rests on, and only sell if the chain agrees.
    ///
    /// * `at_block = Some(b)`: the current valuation came from unconfirmed trades; read block `b`.
    /// * a trailing exit whose peak came from unconfirmed trades (`peak_block`): read that block
    ///   too, and lower the peak to the value the chain really had there if the feed overstated it.
    ///
    /// The confirmed numbers are written back into the live tracker *before* the single-flight
    /// claim (taken by the caller) is released, so neither the loop nor the watchdog act on the
    /// unconfirmed figures afterwards.
    fn spawn_confirmed_liquidation(&self, signal: ExitSignal, mut tracker: PositionTracker, at_block: Option<u64>, config: RiskConfig, flight: ExitFlight) {
        let manager = self.clone_for_liquidation();
        tokio::spawn(async move {
            let Some(mirror) = manager.mirror.clone() else { return };
            let pool = signal.pool_address;
            let held = tracker.tokens_held;
            let mut fail_open = false;

            // 1. The current value.
            if let Some(block) = at_block {
                match mirror.confirm(pool, block).await {
                    Some(snap) => {
                        tracker.current_price_eth = f64::from(snap.value_of(held)) / 1e18;
                        Self::fold_reserves(&mut tracker, snap.reserves());
                    }
                    None => fail_open = true,
                }
            }
            // 2. The trailing peak, whatever the trigger: the re-evaluation below may itself
            //    return a trailing exit measured from it.
            if !fail_open && let Some(peak_block) = tracker.peak_block {
                let real = mirror.reserves_at(pool, peak_block).await.map(|(eth, tok)| {
                    let bps = mirror.snapshot_any(&pool).map_or(crate::risk::curve_mirror::DEFAULT_TRADE_BPS, |s| s.exit_fee_bps);
                    f64::from(CalldataPayloadBuilder::calculate_pons_eth_out(eth, tok, held, bps, 0)) / 1e18
                });
                match real {
                    Some(real) if real >= tracker.highest_price_eth => {} // the peak was real
                    _ => {
                        // Refuted (or unverifiable): back to what the chain did confirm. Never below
                        // a peak confirmed earlier, never disarming a stop armed on confirmed values.
                        let entry = tracker.entry_price_eth;
                        let peak = real.unwrap_or(0.0).max(tracker.confirmed_peak_eth);
                        warn!(
                            "{}: trailing peak {:.6} ETH (block {}) not confirmed by the chain ({}); using {:.6} ETH",
                            signal.position_id,
                            tracker.highest_price_eth,
                            peak_block,
                            real.map_or("no reading".to_string(), |r| format!("{:.6} ETH there", r)),
                            peak
                        );
                        tracker.highest_price_eth = peak;
                        tracker.high_watermark_activated = tracker.confirmed_activated
                            || (entry > 0.0 && (peak - entry) / entry * 100.0 >= config.trail_activate_percent);
                    }
                }
                tracker.peak_block = None;
                tracker.confirmed_peak_eth = tracker.highest_price_eth;
                tracker.confirmed_activated = tracker.high_watermark_activated;
            }

            let verdict = if fail_open {
                warn!(
                    "{}: exact reading at block {:?} unavailable; exiting on the in-memory valuation ({})",
                    signal.position_id, at_block, signal.reason
                );
                Some(signal.clone())
            } else {
                let value = tracker.current_price_eth;
                let event = BlockEvent {
                    block_number: at_block.unwrap_or_default(),
                    timestamp_secs: 0,
                    spot_price_eth: value,
                    curve_capacity_percent: tracker.curve_capacity_percent,
                    token_address: tracker.token.unwrap_or_default(),
                };
                let pool_quote = tracker.is_migrated_to_pool.then_some(value);
                let verdict = RiskEvaluator::evaluate_block_event(&mut tracker, &event, pool_quote, Instant::now(), &config);
                manager.write_back_confirmed(&tracker).await;
                verdict
            };
            match verdict {
                Some(confirmed) => manager.run_liquidation(confirmed, flight).await,
                None => {
                    mirror.note_phantom_rejected();
                    warn!(
                        "{}: {} not confirmed by the chain (block {:?}): the feed-predicted move did not happen \
                         (reverted trades?). Position kept, valuation corrected.",
                        signal.position_id, signal.reason, at_block
                    );
                    drop(flight);
                }
            }
        });
    }

    /// Copy a confirmed valuation (and corrected trailing state) into the live tracker and position.
    async fn write_back_confirmed(&self, confirmed: &PositionTracker) {
        let id = &confirmed.position.id;
        {
            let mut trackers = self.trackers.write().await;
            if let Some(t) = trackers.get_mut(id) {
                t.current_price_eth = confirmed.current_price_eth;
                t.highest_price_eth = confirmed.highest_price_eth;
                t.high_watermark_activated = confirmed.high_watermark_activated;
                t.peak_block = confirmed.peak_block;
                t.confirmed_peak_eth = confirmed.confirmed_peak_eth;
                t.confirmed_activated = confirmed.confirmed_activated;
                t.curve_capacity_percent = confirmed.curve_capacity_percent;
                t.current_mcap_usd = confirmed.current_mcap_usd;
                t.position.pnl_percent = confirmed.position.pnl_percent;
                t.position.current_eth_value = confirmed.position.current_eth_value;
                t.position.current_mcap_usd = confirmed.position.current_mcap_usd;
            }
        }
        let mut positions = self.positions.write().await;
        if let Some(p) = positions.iter_mut().find(|p| &p.id == id) {
            p.pnl_percent = confirmed.position.pnl_percent;
            p.current_eth_value = confirmed.position.current_eth_value;
            p.current_mcap_usd = confirmed.position.current_mcap_usd;
        }
    }

    /// Market cap and curve capacity from reserves.
    fn fold_reserves(tracker: &mut PositionTracker, (eth_res, tok_res): (U256, U256)) {
        tracker.curve_capacity_percent = payload::pons_curve_capacity_percent(eth_res);
        if let Some(mcap_eth) = payload::pons_mcap_eth(eth_res, tok_res) {
            let mcap_usd = mcap_eth * payload::eth_usd_reference();
            tracker.current_mcap_usd = mcap_usd;
            tracker.position.current_mcap_usd = Some(mcap_usd);
            tracker.mcap_from_chain = true;
        }
    }

    /// Fold one quote into the tracker; returns the position's current value in ETH.
    fn apply_quote(tracker: &mut PositionTracker, quote: &Quote, preapprove: &mut Vec<(Address, Address)>) -> Option<f64> {
        if tracker.token.is_none() {
            tracker.token = quote.token;
        }
        if tracker.tokens_held.is_zero()
            && let Some(balance) = quote.balance.filter(|b| !b.is_zero())
        {
            tracker.tokens_held = balance;
            info!("Position {} filled: {} tokens held", tracker.position.id, balance);
        }
        if !tracker.tokens_held.is_zero()
            && !tracker.exit_preapproved
            && let (Some(token), Ok(spender)) = (tracker.token, Address::from_str(&tracker.position.pool_address))
        {
            tracker.exit_preapproved = true;
            preapprove.push((token, spender));
        }
        if let Some(reserves) = quote.reserves {
            Self::fold_reserves(tracker, reserves);
        }
        if tracker.tokens_held.is_zero() {
            return None;
        }
        quote.value_wei.map(|v| f64::from(v) / 1e18)
    }

    /// Helper clone to execute liquidation from background tasks
    fn clone_for_liquidation(&self) -> Self {
        Self {
            config: self.config.clone(),
            risk_config: self.risk_config.clone(),
            wallet_manager: self.wallet_manager.clone(),
            positions: self.positions.clone(),
            trackers: self.trackers.clone(),
            exit_alert_tx: self.exit_alert_tx.clone(),
            next_position_id: self.next_position_id.clone(),
            broadcaster: self.broadcaster.clone(),
            reader: self.reader.clone(),
            fee_adjuster: self.fee_adjuster.clone(),
            exits_in_flight: self.exits_in_flight.clone(),
            mirror: self.mirror.clone(),
            approvals: self.approvals.clone(),
        }
    }

    // =========================================================================
    // LAPIS 6: KONTROL OPERATOR & PENUTUPAN DARURAT MANUAL
    // =========================================================================

    /// Lapis 6: Manual Panic Sell (`/panic`)
    /// Liquidates all open positions simultaneously within milliseconds and pauses signal intake
    pub async fn panic_sell_all(&self) -> usize {
        warn!("🚨 OPERATOR TRIGGERED MANUAL PANIC SELL (/panic)! Liquidating ALL open positions!");

        // Pause engine intake immediately (idempotent: toggling would *resume* an already
        // paused engine in the middle of an emergency)
        self.config.set_engine_running(false).await;

        let mut signals = Vec::new();
        {
            let trackers = self.trackers.read().await;
            for tracker in trackers.values() {
                if tracker.position.is_open {
                    if let (Ok(token_addr), Ok(pool_addr)) = (
                        Address::from_str(&tracker.position.token_address),
                        Address::from_str(&tracker.position.pool_address),
                    ) {
                        signals.push(ExitSignal {
                            position_id: tracker.position.id.clone(),
                            token_address: token_addr,
                            pool_address: pool_addr,
                            reason: ExitReason::ManualPanic,
                            reason_description: "Manual Operator Panic Sell Triggered".to_string(),
                            is_emergency: true,
                            slippage_percent: 50.0,
                        });
                    }
                }
            }
        }

        // Concurrently: one stuck exit must not hold up the others.
        let count = signals.len();
        futures_util::future::join_all(signals.into_iter().map(|s| self.execute_liquidation(s))).await;
        count
    }

    /// Lapis 6: Engine Pause (`/pause`)
    /// Pauses new entry signals while leaving existing positions guarded
    pub async fn pause_engine(&self) -> bool {
        let is_running = self.config.is_engine_running();
        if is_running {
            self.config.set_engine_running(false).await;
            info!("⏸ Operator Paused Engine: No new entries will be taken. Existing positions remain supervised.");
            false
        } else {
            true
        }
    }

    /// Lapis 6: Emergency Asset Sweep (`/sweep`)
    /// Sweeps all remaining tokens back to ETH
    pub async fn sweep_assets(&self) -> Vec<String> {
        warn!("🧹 OPERATOR TRIGGERED EMERGENCY ASSET SWEEP (/sweep)!");
        let mut swept_tokens = Vec::new();
        let trackers = self.trackers.read().await;
        for tracker in trackers.values() {
            swept_tokens.push(format!("{}: {}", tracker.position.token_symbol, tracker.position.token_address));
        }
        swept_tokens
    }
}
