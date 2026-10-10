use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Execution mode for broadcasting transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// Single execution: Master Account only
    Single,
    /// Multi-account sequence: Master Account + Satellite Sub-Accounts
    MultiAccount,
}

impl std::fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionMode::Single => write!(f, "Single (Master Only)"),
            ExecutionMode::MultiAccount => write!(f, "Multi-Account Sequence"),
        }
    }
}

/// Position exit mode specifically configured for Prysm Series Strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrysmExitMode {
    /// Opsi 1: Multi-Layer Default (Trailing +20% / -15% peak, Quiet 30s, Idle 25s, Hard SL -20%)
    System,
    /// Opsi 2: Valuation Target (Take-Profit at Market Cap >= $10,000 USD, Hard SL at -30%)
    Target,
}

impl std::fmt::Display for PrysmExitMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrysmExitMode::System => write!(f, "SISTEM (Trailing / Flow)"),
            PrysmExitMode::Target => write!(f, "TARGET ($10k MCap / -30% SL)"),
        }
    }
}

/// Recognized heuristic pattern variants in Prysm Series Strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrysmPattern {
    /// Pola A: Host Gambar Penyimpanan Prysmcloud (vercel-storage/IPFS, Dev Buy 0.005-0.06 ETH, Tax <= 3%, Exempt <= 1)
    PatternA,
    /// Pola B: Domain Ekstensi .lat & Nilai Dev Buy Khas (0.019-0.024 ETH, Tax <= 3%, Exempt <= 1)
    PatternB,
    /// Pola C: Sinyal Konfirmasi Akun Rekanan (Companion buy within 60s of launch)
    PatternC,
}

impl std::fmt::Display for PrysmPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrysmPattern::PatternA => write!(f, "Pola A (Prysmcloud Image Host)"),
            PrysmPattern::PatternB => write!(f, "Pola B (Domain .lat / 0.021 ETH Dev Buy)"),
            PrysmPattern::PatternC => write!(f, "Pola C (Companion Trade Signal)"),
        }
    }
}

/// On-chain detected trade signal from a monitored account or autonomous strategy
#[derive(Debug, Clone)]
pub struct TradeSignal {
    pub tx_hash: B256,
    pub monitored_account: Address,
    pub target_token: Address,
    pub pool_or_router: Address,
    pub entry_amount_wei: U256,
    pub swap_calldata: Vec<u8>,
    pub detected_at: Instant,
    pub block_number: u64,
    pub strategy_tag: Option<String>,
}

/// Report after transaction dispatch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchReport {
    pub tx_hashes: Vec<String>,
    pub account_roles: Vec<String>,
    pub block_number: u64,
    pub calldata_size_bytes: usize,
    pub execution_mode: ExecutionMode,
    pub gas_limit: u64,
    pub priority_fee_gwei: f64,
    pub latency_micros: u128,
    /// ETH actually sent by the master account (positions must be sized from this)
    pub trade_eth: f64,
}

/// Active or closed trading position
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub id: String,
    pub token_address: String,
    pub token_symbol: String,
    pub entry_eth_amount: f64,
    pub current_eth_value: f64,
    pub entry_timestamp_secs: u64,
    pub pool_address: String,
    pub is_open: bool,
    pub pnl_percent: f64,
    pub exit_reason: Option<String>,
    pub exit_tx_hash: Option<String>,
    pub strategy: Option<String>,
    pub exit_mode: Option<String>,
    pub entry_mcap_usd: Option<f64>,
    pub current_mcap_usd: Option<f64>,
}

/// Treasury wallet summary for portfolio reporting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletSummary {
    pub role: String,
    pub derivation_path: String,
    pub address: String,
    pub balance_eth: f64,
}

/// Gas rebalance transaction detail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GasRebalanceResult {
    pub sub_account_address: String,
    pub amount_eth: f64,
    pub tx_hash: String,
    pub success: bool,
}
