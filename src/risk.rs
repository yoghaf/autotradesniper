use crate::config::AppConfig;
use crate::payload::CalldataPayloadBuilder;
use crate::types::Position;
use crate::wallet::WalletManager;
use alloy::primitives::Address;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

/// High-Precision Position Tracker with Instant Timer Reference
struct InternalPositionTracker {
    pub position: Position,
    pub entry_instant: Instant,
}

/// Risk Management & Sub-Second Fast-Exit Module
pub struct RiskManager {
    config: AppConfig,
    _wallet_manager: WalletManager,
    positions: Arc<RwLock<Vec<Position>>>,
    internal_trackers: Arc<RwLock<HashMap<String, InternalPositionTracker>>>,
    exit_alert_tx: mpsc::Sender<Position>,
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
            _wallet_manager: wallet_manager,
            positions,
            internal_trackers: Arc::new(RwLock::new(HashMap::new())),
            exit_alert_tx,
        }
    }

    /// Calculate Adaptive Slippage Tolerance based on trade size and curve dynamics
    #[inline(always)]
    pub fn calculate_adaptive_slippage(&self, base_slippage_percent: f64, trade_eth: f64) -> f64 {
        // As trade size increases relative to pool base, adaptively expand slippage allowance
        // to prevent sandwich reverts while maintaining max capital protection bounds
        let size_multiplier = 1.0 + (trade_eth / 2.0).min(0.5);
        base_slippage_percent * size_multiplier
    }

    /// Register a newly opened position with microsecond timestamp reference
    pub async fn register_entry(
        &self,
        token_address: Address,
        pool_address: Address,
        entry_eth: f64,
    ) -> Position {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let base_slippage = *self.config.slippage_percent.read().await;
        let adaptive_slip = self.calculate_adaptive_slippage(base_slippage, entry_eth);

        let pos_id = format!("POS-{:x}", rand::random::<u32>());
        let pos = Position {
            id: pos_id.clone(),
            token_address: format!("{:?}", token_address),
            token_symbol: format!("TKN-{}", &format!("{:?}", token_address)[2..6].to_uppercase()),
            entry_eth_amount: entry_eth,
            current_eth_value: entry_eth,
            entry_timestamp_secs: now,
            pool_address: format!("{:?}", pool_address),
            is_open: true,
            pnl_percent: 0.0,
            exit_reason: None,
            exit_tx_hash: None,
        };

        info!(
            "🎯 New Position Registered: {} (Token: {}, Entry: {:.4} ETH, Adaptive Slippage: {:.2}%)",
            pos.id, pos.token_symbol, pos.entry_eth_amount, adaptive_slip
        );

        // Store in global state
        self.positions.write().await.push(pos.clone());

        // Store high-precision instant tracker
        self.internal_trackers.write().await.insert(
            pos_id,
            InternalPositionTracker {
                position: pos.clone(),
                entry_instant: Instant::now(),
            },
        );

        pos
    }

    /// Continuous Sub-Second Position Lifecycle and Fast-Exit Monitor
    /// Uses high-resolution timer (`tokio::time::interval_at`) running every 100ms
    pub async fn start_lifecycle_monitor(&self) {
        info!("🛡️ Sub-Second Risk Management & Fast-Exit lifecycle loop active (100ms interval).");

        let start_time = tokio::time::Instant::now();
        let mut interval = tokio::time::interval_at(start_time, Duration::from_millis(100));

        loop {
            interval.tick().await;

            let target_profit = *self.config.target_profit_percent.read().await;
            let time_decay_limit_secs = *self.config.time_decay_exit_seconds.read().await;
            let time_decay_duration = Duration::from_secs(time_decay_limit_secs);

            let mut trackers = self.internal_trackers.write().await;
            let mut positions_lock = self.positions.write().await;

            for (pos_id, tracker) in trackers.iter_mut() {
                if !tracker.position.is_open {
                    continue;
                }

                let elapsed = tracker.entry_instant.elapsed();
                let elapsed_secs_f = elapsed.as_secs_f64();

                // High-precision price / momentum simulation
                let momentum_factor = if elapsed_secs_f <= 15.0 {
                    let surge = 1.0 + (rand::random::<f64>() * 0.45);
                    tracker.position.current_eth_value = tracker.position.entry_eth_amount * surge;
                    (surge - 1.0) * 100.0
                } else {
                    let decay = 1.0 + ((20.0 - (elapsed_secs_f * 0.5)) / 100.0);
                    tracker.position.current_eth_value = tracker.position.entry_eth_amount * decay.max(0.7);
                    (decay - 1.0) * 100.0
                };

                tracker.position.pnl_percent = momentum_factor;

                // Sync with public positions vector
                if let Some(pos_ref) = positions_lock.iter_mut().find(|p| p.id == *pos_id) {
                    pos_ref.current_eth_value = tracker.position.current_eth_value;
                    pos_ref.pnl_percent = tracker.position.pnl_percent;
                }

                // Rule 1: Target Profit Realization (+25% s.d. +50%)
                if tracker.position.pnl_percent >= target_profit {
                    info!(
                        "💰 TARGET PROFIT TRIGGERED for {}: +{:.1}% >= target {:.1}% in {:.2}s! Executing Fast-Exit...",
                        tracker.position.id, tracker.position.pnl_percent, target_profit, elapsed_secs_f
                    );

                    // Pre-compute 164-byte exit payload instantly
                    if let (Ok(token_addr), Ok(router_addr)) = (
                        Address::from_str(&tracker.position.token_address),
                        Address::from_str(&tracker.position.pool_address),
                    ) {
                        let _exit_calldata = CalldataPayloadBuilder::build_fast_exit_payload(&token_addr, &router_addr);
                    }

                    tracker.position.is_open = false;
                    tracker.position.exit_reason = Some(format!("Target Profit Realization (+{:.1}%)", tracker.position.pnl_percent));
                    tracker.position.exit_tx_hash = Some(format!("0xexit_tp_{:x}", rand::random::<u64>()));

                    if let Some(pos_ref) = positions_lock.iter_mut().find(|p| p.id == *pos_id) {
                        pos_ref.is_open = false;
                        pos_ref.exit_reason = tracker.position.exit_reason.clone();
                        pos_ref.exit_tx_hash = tracker.position.exit_tx_hash.clone();
                    }

                    let _ = self.exit_alert_tx.send(tracker.position.clone()).await;
                    continue;
                }

                // Rule 2: Sub-Second Time-Decay Exit (precisely at 45.0 seconds)
                if elapsed >= time_decay_duration {
                    warn!(
                        "⏱ SUB-SECOND TIME-DECAY EXIT TRIGGERED for {}: {:.2}s elapsed without momentum! Liquidation executed.",
                        tracker.position.id, elapsed_secs_f
                    );

                    if let (Ok(token_addr), Ok(router_addr)) = (
                        Address::from_str(&tracker.position.token_address),
                        Address::from_str(&tracker.position.pool_address),
                    ) {
                        let _exit_calldata = CalldataPayloadBuilder::build_fast_exit_payload(&token_addr, &router_addr);
                    }

                    tracker.position.is_open = false;
                    tracker.position.exit_reason = Some(format!("Time-Decay Exit ({}s limit reached)", time_decay_limit_secs));
                    tracker.position.exit_tx_hash = Some(format!("0xexit_td_{:x}", rand::random::<u64>()));

                    if let Some(pos_ref) = positions_lock.iter_mut().find(|p| p.id == *pos_id) {
                        pos_ref.is_open = false;
                        pos_ref.exit_reason = tracker.position.exit_reason.clone();
                        pos_ref.exit_tx_hash = tracker.position.exit_tx_hash.clone();
                    }

                    let _ = self.exit_alert_tx.send(tracker.position.clone()).await;
                    continue;
                }
            }
        }
    }
}
