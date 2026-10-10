use crate::types::Position;
use alloy::primitives::{Address, U256};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Instant;

/// 6-Layer Defensive Exit Reasons
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExitReason {
    /// Layer 1: High-Watermark Trailing (-15% from peak after +20% activated)
    TrailingStop,
    /// Layer 1: Hard Stop-Loss (-20% from entry price)
    StopLoss,
    /// Layer 2: Quiet Market Evaluation (< 0.1 ETH outside inflow at 30 seconds)
    QuietMarket,
    /// Layer 2: Inactivity Decay (No outside inflow for 25 consecutive seconds)
    InactivityDecay,
    /// Layer 3: Pre-Graduation Guard (Bonding curve capacity >= 90%)
    PreGraduationGuard,
    /// Layer 3: Secondary Pool Trailing (Uniswap v4/v3 pool trailing exit)
    SecondaryPoolTrailing,
    /// Layer 4: Mempool Approval Anomaly (>= 3 early holders approve curve simultaneously)
    ApprovalAnomaly,
    /// Layer 5: Fault-Tolerant Watchdog Drawdown (>= 30% loss)
    WatchdogDrawdown,
    /// Layer 5: Fault-Tolerant Watchdog Timeout (Max hold 600s / 10 minutes exceeded)
    WatchdogTimeout,
    /// Layer 5: Secondary Pool Quote Timeout (Unavailable quote for 30 seconds)
    PoolQuoteTimeout,
    /// Layer 6: Operator Manual Panic Sell (/panic)
    ManualPanic,
    /// Layer 6: Operator Emergency Asset Sweep (/sweep)
    EmergencySweep,
    /// Prysm Strategy Target Exit: Take-Profit Market Cap (>= $10,000 USD)
    PrysmTargetMcap,
    /// Prysm Strategy Target Exit: Strict Stop-Loss (-30%)
    PrysmTargetStopLoss,
    /// The master's entry never delivered tokens (buy reverted on the Revert Guard/slippage)
    EntryNotFilled,
}

impl std::fmt::Display for ExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExitReason::TrailingStop => write!(f, "L1: Adaptive High-Watermark Trailing (-15% from peak)"),
            ExitReason::StopLoss => write!(f, "L1: Hard Stop-Loss (-20% from entry)"),
            ExitReason::QuietMarket => write!(f, "L2: Quiet Market (< 0.1 ETH inflow at 30s)"),
            ExitReason::InactivityDecay => write!(f, "L2: Inactivity Decay (25s without outside buy)"),
            ExitReason::PreGraduationGuard => write!(f, "L3: Pre-Graduation Guard (Curve capacity >= 90%)"),
            ExitReason::SecondaryPoolTrailing => write!(f, "L3: Secondary Pool Trailing (Uniswap v4)"),
            ExitReason::ApprovalAnomaly => write!(f, "L4: Approval Anomaly (>= 3 early holders approved curve)"),
            ExitReason::WatchdogDrawdown => write!(f, "L5: Watchdog Drawdown (>= 30% extreme loss)"),
            ExitReason::WatchdogTimeout => write!(f, "L5: Watchdog Max Hold Timeout (600s exceeded)"),
            ExitReason::PoolQuoteTimeout => write!(f, "L5: Watchdog Pool Quote Timeout (30s unavailable)"),
            ExitReason::ManualPanic => write!(f, "L6: Operator Manual Panic Sell (/panic)"),
            ExitReason::EmergencySweep => write!(f, "L6: Operator Emergency Asset Sweep (/sweep)"),
            ExitReason::PrysmTargetMcap => write!(f, "Prysm Target: Take-Profit Target Reached ($10k MCap)"),
            ExitReason::PrysmTargetStopLoss => write!(f, "Prysm Target: Strict Stop-Loss (-30% Deficit)"),
            ExitReason::EntryNotFilled => write!(f, "Entry not filled (buy reverted or never landed)"),
        }
    }
}

/// Comprehensive Risk Management Configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// L1: Trailing stop activation threshold (default +20% -> 0.20)
    pub trail_activate_percent: f64,
    /// L1: Trailing stop drawdown distance from peak (default -15% -> 0.15)
    pub trail_distance_percent: f64,
    /// L1: Hard stop loss threshold from entry (default -20% -> 0.20)
    pub stop_loss_percent: f64,

    /// L2: Quiet market evaluation timing in seconds (default 30s)
    pub quiet_exit_seconds: u64,
    /// L2: Minimum required outside capital inflow in ETH (default 0.1 ETH)
    pub quiet_min_inflow_eth: f64,
    /// L2: Idle timeout without external buy in seconds (default 25s)
    pub idle_timeout_seconds: u64,

    /// L3: Pre-graduation guard toggle
    pub grad_guard_enabled: bool,
    /// L3: Curve capacity threshold to trigger pre-graduation liquidation (default 90.0%)
    pub grad_guard_capacity_percent: f64,
    /// L3: Secondary pool trailing toggle
    pub pool_trail_enabled: bool,

    /// L4: Threshold of concurrent early holder approvals to trigger emergency exit (default 3)
    pub approval_anomaly_threshold: usize,
    /// L4: Emergency pre-emption slippage tolerance (default 90.0%)
    pub emergency_slippage_percent: f64,

    /// L5: Watchdog extreme loss threshold (default 30.0% -> 0.30)
    pub watchdog_loss_percent: f64,
    /// L5: Maximum hold duration in seconds (default 600s / 10 minutes)
    pub max_hold_seconds: u64,
    /// L5: Maximum duration to tolerate quoter failure in seconds (default 30s)
    pub pool_quote_timeout_seconds: u64,

    /// Prysm Target mode: take-profit market cap in USD (default $10,000)
    pub prysm_target_mcap_usd: f64,
    /// Prysm Target mode: strict stop-loss in percent (default 30.0 -> -30%)
    pub prysm_target_stop_loss_percent: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            trail_activate_percent: 20.0,
            trail_distance_percent: 15.0,
            stop_loss_percent: 20.0,

            quiet_exit_seconds: 30,
            quiet_min_inflow_eth: 0.1,
            idle_timeout_seconds: 25,

            grad_guard_enabled: true,
            grad_guard_capacity_percent: 90.0,
            pool_trail_enabled: true,

            approval_anomaly_threshold: 3,
            emergency_slippage_percent: 90.0,

            watchdog_loss_percent: 30.0,
            max_hold_seconds: 600,
            pool_quote_timeout_seconds: 30,

            prysm_target_mcap_usd: 10_000.0,
            prysm_target_stop_loss_percent: 30.0,
        }
    }
}

/// Where a position is valued and sold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitVenue {
    /// Pons bonding curve (`position.pool_address`): value from `getReserves()`, exit via `sell()`.
    PonsCurve,
    /// Uniswap-V2-style router (`position.pool_address`): value from `getAmountsOut`, exit via
    /// `swapExactTokensForETHSupportingFeeOnTransferTokens` along `[token, weth]`.
    V2Router { weth: Address },
}

impl ExitVenue {
    /// Venue implied by the entry calldata (V2 `swapExactETHForTokens*` or Pons `buy`).
    pub fn from_entry_calldata(calldata: &[u8]) -> Self {
        if crate::stream::fast_selectors::is_copyable_eth_swap(calldata)
            && calldata.first_chunk::<4>() != Some(&crate::payload::PONS_BUY_SELECTOR)
            && let Some((weth, _)) = crate::payload::CalldataPayloadBuilder::decode_v2_path_ends(calldata)
        {
            return ExitVenue::V2Router { weth };
        }
        ExitVenue::PonsCurve
    }
}

/// Internal high-resolution state tracker for an open position
#[derive(Debug, Clone)]
pub struct PositionTracker {
    pub position: Position,
    pub entry_instant: Instant,
    pub entry_price_eth: f64,
    pub current_price_eth: f64,
    pub highest_price_eth: f64,
    pub high_watermark_activated: bool,

    // Lapis 2 Flow tracking
    pub outside_inflow_eth: f64,
    pub last_outside_inflow_instant: Instant,
    pub quiet_evaluated: bool,

    // Lapis 3 Migration tracking
    pub curve_capacity_percent: f64,
    pub is_migrated_to_pool: bool,
    pub last_pool_quote_instant: Instant,

    // Lapis 4 Anomaly tracking
    pub approval_accounts: HashSet<Address>,

    // Prysm Series Strategy tracking
    pub is_prysm_strategy: bool,
    pub prysm_exit_mode: Option<crate::types::PrysmExitMode>,
    pub prysm_entry_mcap_usd: f64,
    pub current_mcap_usd: f64,

    // On-chain valuation & exit
    pub venue: ExitVenue,
    /// Resolved ERC-20 (a Pons copy-trade may only know the curve until `pairToken()` answers).
    pub token: Option<Address>,
    /// Master-account token balance observed after the entry filled (zero until observed).
    /// Valuation sells exactly this amount on the current curve state.
    pub tokens_held: U256,
    /// `current_mcap_usd` comes from live reserves; Target mode must not re-derive it from PnL.
    pub mcap_from_chain: bool,
    /// Exit allowance pre-approval already sent (so the exit is a single transaction).
    pub exit_preapproved: bool,
    /// Last exit that failed on-chain; the watchdog re-issues it with wider slippage.
    pub pending_exit: Option<ExitSignal>,
    pub exit_attempts: u32,
}

impl PositionTracker {
    pub fn new(position: Position, entry_price_eth: f64) -> Self {
        let now = Instant::now();
        Self {
            position,
            entry_instant: now,
            entry_price_eth,
            current_price_eth: entry_price_eth,
            highest_price_eth: entry_price_eth,
            high_watermark_activated: false,

            outside_inflow_eth: 0.0,
            last_outside_inflow_instant: now,
            quiet_evaluated: false,

            curve_capacity_percent: 0.0,
            is_migrated_to_pool: false,
            last_pool_quote_instant: now,

            approval_accounts: HashSet::new(),

            is_prysm_strategy: false,
            prysm_exit_mode: None,
            prysm_entry_mcap_usd: 0.0,
            current_mcap_usd: 0.0,

            venue: ExitVenue::PonsCurve,
            token: None,
            tokens_held: U256::ZERO,
            mcap_from_chain: false,
            exit_preapproved: false,
            pending_exit: None,
            exit_attempts: 0,
        }
    }
}

/// End-of-block market confirmation event
#[derive(Debug, Clone)]
pub struct BlockEvent {
    pub block_number: u64,
    pub timestamp_secs: u64,
    pub spot_price_eth: f64,
    pub curve_capacity_percent: f64,
    pub token_address: Address,
}

/// Capital inflow transaction event on curve
#[derive(Debug, Clone)]
pub struct InflowEvent {
    pub buyer: Address,
    pub amount_eth: f64,
    pub timestamp: Instant,
    pub is_deployer_or_initial: bool,
}

/// ERC-20 approval event on token
#[derive(Debug, Clone)]
pub struct ApprovalEvent {
    pub token: Address,
    pub owner: Address,
    pub spender: Address,
    pub timestamp: Instant,
}

/// Exit signal emitted by any defensive layer
#[derive(Debug, Clone)]
pub struct ExitSignal {
    pub position_id: String,
    pub token_address: Address,
    pub pool_address: Address,
    pub reason: ExitReason,
    pub reason_description: String,
    pub is_emergency: bool,
    pub slippage_percent: f64,
}
