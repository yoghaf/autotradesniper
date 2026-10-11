pub mod anomaly_sensor;
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
}

#[derive(Default)]
struct Quote {
    token: Option<Address>,
    /// Fresh `balanceOf` (only read until the entry fill is observed). `Some(0)` = read OK, empty.
    balance: Option<U256>,
    /// What selling `tokens_held` would return right now, after fees.
    value_wei: Option<U256>,
    reserves: Option<(U256, U256)>,
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
        }
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
        let Some(_flight) = ExitFlight::claim(&self.exits_in_flight, &signal.position_id) else {
            return; // an exit for this position is already running
        };
        let target = {
            let trackers = self.trackers.read().await;
            trackers
                .get(&signal.position_id)
                .filter(|t| t.position.is_open)
                .map(|t| (t.venue, t.token))
        };
        let Some((venue, token_hint)) = target else { return };

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

        match self.liquidate_onchain(&broadcaster, &signal, venue, token_hint).await {
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
        let (master_res, satellite_res) = tokio::join!(
            self.liquidate_account(broadcaster, master, token, venue, signal),
            futures_util::future::join_all(
                satellites.iter().map(|acc| self.liquidate_account(broadcaster, acc, token, venue, signal))
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
    ) -> Result<SellOutcome> {
        let reader = self.reader().context("no read pool")?;
        let mut balance = erc20_balance(&reader, token, acc.address, EXIT_READ_TIMEOUT).await?;
        if balance.is_zero() {
            return Ok(SellOutcome::NothingHeld);
        }
        self.ensure_allowance(broadcaster, &reader, acc, token, signal.pool_address, balance).await?;

        let mut slippage = signal.slippage_percent;
        let mut last_err = anyhow!("no sell attempted");
        for attempt in 1..=SELL_ATTEMPTS {
            match self.sell_once(broadcaster, &reader, acc, token, venue, signal.pool_address, balance, slippage, signal.is_emergency).await {
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
            if balance.is_zero() {
                return Ok(SellOutcome::NothingHeld);
            }
        }
        Err(last_err)
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
            return Ok(());
        }
        info!("[{}] approving {:?} to spend {:?}", acc.role, spender, token);
        let approve = CalldataPayloadBuilder::build_erc20_approve_payload(spender, U256::MAX);
        let hash = self.send_tx(broadcaster, acc, token, approve).await.context("approve")?;
        info!("✅ [{}] approval accepted: {}", acc.role, hash);
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
        slippage_percent: f64,
        emergency: bool,
    ) -> Result<String> {
        let min_out = match quote_exit(reader, venue, pool, token, amount, EXIT_READ_TIMEOUT).await {
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
        self.confirm_sell(reader, &hash, acc, token, amount).await?;
        Ok(hash)
    }

    /// "Accepted by an endpoint" is not "sold": a sell can revert on its minimum output.
    async fn confirm_sell(&self, reader: &BroadcasterPool, hash: &str, acc: &TreasuryAccount, token: Address, amount_before: U256) -> Result<()> {
        let tx_hash = B256::from_str(hash).context("sell hash")?;
        let deadline = Instant::now() + SELL_CONFIRM_TIMEOUT;
        while Instant::now() < deadline {
            if let Ok(Some(receipt)) = reader.get_transaction_receipt(tx_hash).await {
                return match receipt.get("status").and_then(|s| s.as_str()) {
                    Some(status) if status.eq_ignore_ascii_case("0x1") => Ok(()),
                    Some(status) => Err(anyhow!("sell {} reverted on-chain (status {})", hash, status)),
                    None => Err(anyhow!("sell {} receipt has no status", hash)),
                };
            }
            tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
        }
        // No receipt in time: judge by the balance.
        let now = erc20_balance(reader, token, acc.address, EXIT_READ_TIMEOUT).await?;
        anyhow::ensure!(now < amount_before, "sell {} unconfirmed and balance unchanged", hash);
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

        loop {
            interval.tick().await;
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
                trackers
                    .values()
                    .filter_map(|t| {
                        Some(QuoteRequest {
                            id: t.position.id.clone(),
                            venue: t.venue,
                            pool: Address::from_str(&t.position.pool_address).ok()?,
                            token: t.token,
                            tokens_held: t.tokens_held,
                        })
                    })
                    .collect()
            };
            if requests.is_empty() {
                continue;
            }

            // 2. Quotes with no lock held, all positions concurrently.
            let owner = self.wallet_manager.get_master_account().await.ok().map(|a| a.address);
            let quotes: HashMap<String, Quote> = match (self.reader(), owner) {
                (Some(reader), Some(owner)) => {
                    let fetched = futures_util::future::join_all(requests.iter().map(|r| fetch_quote(&reader, owner, r))).await;
                    requests.into_iter().map(|r| r.id).zip(fetched).collect()
                }
                _ => HashMap::new(),
            };

            // 3. Apply + evaluate (short lock, no await inside).
            let now = Instant::now();
            let in_flight = self.exits_in_flight.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let mut signals = Vec::new();
            let mut preapprove = Vec::new();
            {
                let mut trackers = self.trackers.write().await;
                let mut positions_lock = self.positions.write().await;

                for tracker in trackers.values_mut() {
                    if !tracker.position.is_open {
                        continue;
                    }
                    let quote = quotes.get(&tracker.position.id);
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

                    let signal = match value_eth {
                        Some(value) => {
                            let block_event = BlockEvent {
                                block_number: 0,
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
                    if let Some(signal) = signal {
                        signals.push(signal);
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
        if let Some((eth_res, tok_res)) = quote.reserves {
            tracker.curve_capacity_percent = payload::pons_curve_capacity_percent(eth_res);
            if let Some(mcap_eth) = payload::pons_mcap_eth(eth_res, tok_res) {
                let mcap_usd = mcap_eth * payload::eth_usd_reference();
                tracker.current_mcap_usd = mcap_usd;
                tracker.position.current_mcap_usd = Some(mcap_usd);
                tracker.mcap_from_chain = true;
            }
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
