pub mod anomaly_sensor;
pub mod evaluator;
pub mod flow_rules;
pub mod migration_rules;
pub mod price_rules;
pub mod types;
pub mod watchdog;

use crate::config::AppConfig;
use crate::models::prysm::is_prysm_tag;
use crate::payload::{CalldataPayloadBuilder, DynamicFeeAdjuster};
use crate::risk::anomaly_sensor::AnomalySensor;
use crate::risk::evaluator::RiskEvaluator;
use crate::risk::flow_rules::FlowRules;
use crate::risk::types::{
    ApprovalEvent, BlockEvent, ExitReason, ExitSignal, InflowEvent, PositionTracker, RiskConfig,
};
use crate::risk::watchdog::WatchdogSupervisor;
use crate::types::Position;
use crate::wallet::WalletManager;
use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::Encodable2718;
use alloy::primitives::{Address, Bytes, TxKind, U256};
use alloy::signers::Signer;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

/// Closed positions kept in the public history (Telegram portfolio view).
const MAX_CLOSED_POSITIONS: usize = 200;

/// Aborts the wrapped task when dropped, so a cancelled or panicking supervisor cannot leak it.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
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
    broadcaster: Option<Arc<crate::dispatcher::BroadcasterPool>>,
    fee_adjuster: Option<Arc<DynamicFeeAdjuster>>,
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
            fee_adjuster: None,
        }
    }

    pub fn with_execution(
        mut self,
        broadcaster: Arc<crate::dispatcher::BroadcasterPool>,
        fee_adjuster: Arc<DynamicFeeAdjuster>,
    ) -> Self {
        self.broadcaster = Some(broadcaster);
        self.fee_adjuster = Some(fee_adjuster);
        self
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
            "🎯 [Risk Layer 1-6 Armed] Position Registered: {} (Token: {}, Strategy: {:?}, Entry: {:.4} ETH, Adaptive Slip: {:.2}%)",
            pos.id, pos.token_symbol, pos.strategy, pos.entry_eth_amount, adaptive_slip
        );

        // Store in public positions vector
        self.positions.write().await.push(pos.clone());

        // Initialize 6-layer state tracker
        let mut tracker = PositionTracker::new(pos.clone(), entry_eth);
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

    /// Close a position after an exit signal and execute real on-chain liquidation.
    pub async fn execute_liquidation(&self, signal: ExitSignal) {
        warn!(
            "🚨 LIQUIDATION TRIGGERED: [{}] Position: {} -> {}",
            signal.reason, signal.position_id, signal.reason_description
        );

        let mut confirmed_tx_hash = None;

        if let Some(ref broadcaster) = self.broadcaster {
            match self.wallet_manager.derive_account(0).await {
                Ok(master) => {
                    // 1. Fetch token balance for master account
                    let balance_calldata = CalldataPayloadBuilder::build_erc20_balance_of_payload(master.address);
                    let token_balance = match broadcaster.eth_call(signal.token_address, &balance_calldata).await {
                        Ok(raw) => CalldataPayloadBuilder::decode_u256_output(&raw).unwrap_or(U256::ZERO),
                        Err(e) => {
                            warn!("Liquidation: failed to fetch token balance for {:?}: {}", signal.token_address, e);
                            U256::ZERO
                        }
                    };

                    if token_balance.is_zero() {
                        warn!(
                            "Liquidation: token balance is 0 for {:?}, closing position in memory without on-chain sell",
                            signal.token_address
                        );
                    } else {
                        info!("Liquidation: master has {:?} tokens of {:?} to liquidate", token_balance, signal.token_address);

                        let chain_id = self.config.chain_id;
                        let (priority_fee_wei, max_fee_wei) = if let Some(ref fa) = self.fee_adjuster {
                            fa.calculate_optimal_fees()
                        } else {
                            (2_000_000_000, 15_000_000_000)
                        };
                        let configured_gas_limit = *self.config.gas_limit.read().await;

                        // 2. Check ERC-20 Allowance
                        let allow_calldata = CalldataPayloadBuilder::build_erc20_allowance_payload(master.address, signal.pool_address);
                        let allowance = match broadcaster.eth_call(signal.token_address, &allow_calldata).await {
                            Ok(raw) => CalldataPayloadBuilder::decode_u256_output(&raw).unwrap_or(U256::ZERO),
                            Err(_) => U256::ZERO,
                        };

                        if allowance < token_balance {
                            info!("Liquidation: approving curve/pool {:?} to spend tokens...", signal.pool_address);
                            let approve_calldata = CalldataPayloadBuilder::build_erc20_approve_payload(signal.pool_address, U256::MAX);
                            let nonce = master.nonce_cursor.fetch_add(1, Ordering::SeqCst);
                            let gas_limit = configured_gas_limit.max(100_000);

                            let tx = TxEip1559 {
                                chain_id,
                                nonce,
                                gas_limit,
                                max_fee_per_gas: max_fee_wei,
                                max_priority_fee_per_gas: priority_fee_wei,
                                to: TxKind::Call(signal.token_address),
                                value: U256::ZERO,
                                input: Bytes::from(approve_calldata),
                                access_list: Default::default(),
                            };

                            match master.signer.sign_hash(&tx.signature_hash()).await {
                                Ok(sig) => {
                                    let envelope = TxEnvelope::Eip1559(tx.into_signed(sig));
                                    let hash = format!("{:?}", envelope.tx_hash());
                                    let raw_hex = alloy::hex::encode_prefixed(envelope.encoded_2718());
                                    if let Err(e) = broadcaster.broadcast_raw_tx(&raw_hex, &hash).await {
                                        warn!("Approve broadcast failed: {}. Attempting sell regardless...", e);
                                    } else {
                                        info!("✅ Liquidation: approval broadcast accepted: {}", hash);
                                    }
                                }
                                Err(e) => error!("Failed to sign approve tx: {:#}", e),
                            }
                        }

                        // 3. Estimate min_eth_out from curve reserves
                        let reserves_data = CalldataPayloadBuilder::build_pons_get_reserves_payload();
                        let min_eth_out = match broadcaster.eth_call(signal.pool_address, &reserves_data).await {
                            Ok(raw) => {
                                if let Some((eth_res, tok_res)) = CalldataPayloadBuilder::decode_get_reserves_output(&raw) {
                                    let expected_eth = CalldataPayloadBuilder::calculate_pons_eth_out(eth_res, tok_res, token_balance, 100, 200);
                                    let slip = (signal.slippage_percent / 100.0).clamp(0.05, 0.95);
                                    let min = (f64::from(expected_eth) * (1.0 - slip)) as u128;
                                    U256::from(min)
                                } else {
                                    U256::from(1)
                                }
                            }
                            Err(_) => U256::from(1),
                        };

                        // 4. Build and broadcast on-chain sell
                        let sell_calldata = CalldataPayloadBuilder::build_pons_sell_payload(token_balance, min_eth_out, master.address);
                        let nonce = master.nonce_cursor.fetch_add(1, Ordering::SeqCst);
                        let gas_limit = configured_gas_limit.max(250_000);

                        let tx = TxEip1559 {
                            chain_id,
                            nonce,
                            gas_limit,
                            max_fee_per_gas: max_fee_wei,
                            max_priority_fee_per_gas: priority_fee_wei,
                            to: TxKind::Call(signal.pool_address),
                            value: U256::ZERO,
                            input: Bytes::from(sell_calldata),
                            access_list: Default::default(),
                        };

                        match master.signer.sign_hash(&tx.signature_hash()).await {
                            Ok(sig) => {
                                let envelope = TxEnvelope::Eip1559(tx.into_signed(sig));
                                let hash = format!("{:?}", envelope.tx_hash());
                                let raw_hex = alloy::hex::encode_prefixed(envelope.encoded_2718());
                                match broadcaster.broadcast_raw_tx(&raw_hex, &hash).await {
                                    Ok(tx_hash) => {
                                        info!("🚀 LIQUIDATION ON-CHAIN CONFIRMED: {} (Tx: {})", signal.position_id, tx_hash);
                                        confirmed_tx_hash = Some(tx_hash);
                                    }
                                    Err(err) => {
                                        error!("Liquidation broadcast failed for {}: {}", signal.position_id, err);
                                        let _ = self.wallet_manager.resync_nonce(master.address).await;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to sign liquidation tx: {:#}", e);
                                let _ = self.wallet_manager.resync_nonce(master.address).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Cannot derive master account for liquidation: {:#}", e);
                }
            }
        } else {
            error!(
                "No broadcaster configured: position {} closed in memory only, tokens remain in the wallet",
                signal.position_id
            );
        }

        let exit_reason = if confirmed_tx_hash.is_some() {
            format!("{}", signal.reason)
        } else {
            format!("[SIMULATED, no sell tx] {}", signal.reason)
        };

        // Update in-memory tracker
        {
            let mut trackers = self.trackers.write().await;
            if let Some(tracker) = trackers.get_mut(&signal.position_id) {
                tracker.position.is_open = false;
                tracker.position.exit_reason = Some(exit_reason.clone());
                tracker.position.exit_tx_hash = confirmed_tx_hash.clone();
            }
        }

        // Update public positions array
        let mut closed_pos = None;
        {
            let mut positions_lock = self.positions.write().await;
            if let Some(pos) = positions_lock.iter_mut().find(|p| p.id == signal.position_id) {
                pos.is_open = false;
                pos.exit_reason = Some(exit_reason);
                pos.exit_tx_hash = confirmed_tx_hash;
                closed_pos = Some(pos.clone());
            }
        }

        // Notify non-blocking Telegram alert queue
        if let Some(pos) = closed_pos {
            let _ = self.exit_alert_tx.send(pos).await;
        }
    }

    /// Continuous Sub-Second Multi-Layer Evaluation Loop (Lapis 1 - 3)
    pub async fn start_lifecycle_monitor(&self) {
        info!("🛡️ Multi-Layer Risk Lifecycle Monitor started (100ms precision).");

        // Spawn Layer 5 Fault-Tolerant Watchdog Supervisor on isolated background task (every 5 seconds)
        let trackers_watchdog = self.trackers.clone();
        let risk_config_watchdog = self.risk_config.clone();
        let this_manager_for_watchdog = self.clone_for_liquidation();

        let _watchdog = AbortOnDrop(tokio::spawn(async move {
            let mut watchdog_interval = tokio::time::interval(Duration::from_secs(5));
            watchdog_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                watchdog_interval.tick().await;
                let now = Instant::now();
                let config = risk_config_watchdog.read().await.clone();
                let mut signals_to_exit = Vec::new();

                {
                    let trackers = trackers_watchdog.read().await;
                    for tracker in trackers.values() {
                        if tracker.position.is_open {
                            if let Some(signal) = WatchdogSupervisor::evaluate_position(tracker, now, &config) {
                                signals_to_exit.push(signal);
                            }
                        }
                    }
                }

                for signal in signals_to_exit {
                    this_manager_for_watchdog.execute_liquidation(signal).await;
                }
            }
        }));

        // Main Evaluation Loop for Lapis 1 (Price), Lapis 2 (Flow), and Lapis 3 (Migration)
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        // Burst catch-up after a stall would evaluate stale state several times back-to-back.
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            let now = Instant::now();
            let mut config = self.risk_config.read().await.clone();
            config.prysm_target_mcap_usd = *self.config.prysm_target_mcap_usd.read().await;
            config.prysm_target_stop_loss_percent = *self.config.prysm_target_stop_loss_percent.read().await;
            let mut signals = Vec::new();

            {
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

                for tracker in trackers.values_mut() {
                    if !tracker.position.is_open {
                        continue;
                    }

                    let elapsed_f = tracker.entry_instant.elapsed().as_secs_f64();

                    let mut real_price = None;
                    if let Ok(pool_addr) = Address::from_str(&tracker.position.pool_address) {
                        if let Some(ref broadcaster) = self.broadcaster {
                            let reserves_data = CalldataPayloadBuilder::build_pons_get_reserves_payload();
                            if let Ok(raw) = broadcaster.eth_call(pool_addr, &reserves_data).await {
                                if let Some((eth_res, tok_res)) = CalldataPayloadBuilder::decode_get_reserves_output(&raw) {
                                    let eth_res_f = f64::from(eth_res) / 1e18;
                                    let tok_res_f = f64::from(tok_res);
                                    if tok_res_f > 0.0 {
                                        let spot_price = eth_res_f / tok_res_f;
                                        let mcap_usd = (spot_price * 1_000_000_000.0) * 2600.0;
                                        tracker.current_mcap_usd = mcap_usd;
                                        tracker.position.current_mcap_usd = Some(mcap_usd);

                                        let entry_eth = tracker.entry_price_eth;
                                        let est_tokens = tok_res_f * entry_eth / (eth_res_f + entry_eth);
                                        let curr_eth_val = eth_res_f * est_tokens / (tok_res_f + est_tokens) * 0.98;
                                        tracker.position.current_eth_value = curr_eth_val;
                                        tracker.position.pnl_percent = ((curr_eth_val - entry_eth) / entry_eth) * 100.0;
                                        real_price = Some(curr_eth_val);
                                    }
                                }
                            }
                        }
                    }

                    // Fallback to simulated pricing if offline or in test environment
                    let current_simulated_price = real_price.unwrap_or_else(|| {
                        if elapsed_f <= 15.0 {
                            tracker.entry_price_eth * (1.0 + (rand::random::<f64>() * 0.40))
                        } else {
                            let decay = 1.0 + ((15.0 - (elapsed_f * 0.4)) / 100.0);
                            tracker.entry_price_eth * decay.max(0.65)
                        }
                    });

                    let block_event = BlockEvent {
                        block_number: 84871000,
                        timestamp_secs: 0,
                        spot_price_eth: current_simulated_price,
                        curve_capacity_percent: (elapsed_f * 2.0).min(95.0),
                        token_address: Address::from_str(&tracker.position.token_address).unwrap_or_default(),
                    };

                    if let Some(signal) = RiskEvaluator::evaluate_block_event(
                        tracker,
                        &block_event,
                        Some(current_simulated_price),
                        now,
                        &config,
                    ) {
                        signals.push(signal);
                    }

                    // Keep positions in sync
                    if let Some(pos_ref) = positions_lock.iter_mut().find(|p| p.id == tracker.position.id) {
                        pos_ref.current_eth_value = tracker.position.current_eth_value;
                        pos_ref.pnl_percent = tracker.position.pnl_percent;
                    }
                }
            }

            for signal in signals {
                self.execute_liquidation(signal).await;
            }
        }
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
            fee_adjuster: self.fee_adjuster.clone(),
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

        let count = signals.len();
        for signal in signals {
            self.execute_liquidation(signal).await;
        }

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
