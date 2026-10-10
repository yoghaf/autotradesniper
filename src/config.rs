use crate::types::ExecutionMode;
use alloy::primitives::Address;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

fn default_prysm_enabled() -> bool { false }
fn default_prysm_size_eth() -> f64 { 0.01 }
fn default_prysm_exit_mode() -> crate::types::PrysmExitMode { crate::types::PrysmExitMode::System }
fn default_prysm_max_mcap_usd() -> f64 { 8000.0 }

/// Persistent runtime state stored on disk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeState {
    pub mnemonic_phrase: String,
    pub watchlist_accounts: Vec<String>,
    pub buffer_size: usize,
    pub gas_limit: u64,
    pub max_priority_fee_gwei: f64,
    pub max_fee_gwei: f64,
    pub default_trade_eth: f64,
    pub slippage_percent: f64,
    pub target_profit_percent: f64,
    pub time_decay_exit_seconds: u64,
    pub execution_mode: ExecutionMode,
    pub sub_account_count: usize,
    pub is_engine_active: bool,

    #[serde(default = "default_prysm_enabled")]
    pub prysm_enabled: bool,
    #[serde(default = "default_prysm_size_eth")]
    pub prysm_size_eth: f64,
    #[serde(default = "default_prysm_exit_mode")]
    pub prysm_exit_mode: crate::types::PrysmExitMode,
    #[serde(default = "default_prysm_max_mcap_usd")]
    pub prysm_max_mcap_usd: f64,
}

/// Global shared application configuration
#[derive(Clone)]
pub struct AppConfig {
    pub rpc_http_url: String,
    pub rpc_ws_url: String,
    pub chain_id: u64,
    pub telegram_bot_token: String,
    pub telegram_authorized_chat_id: i64,

    pub mnemonic_phrase: Arc<RwLock<String>>,
    pub watchlist_accounts: Arc<RwLock<HashSet<Address>>>,
    pub buffer_size: Arc<RwLock<usize>>,
    pub gas_limit: Arc<RwLock<u64>>,
    pub max_priority_fee_gwei: Arc<RwLock<f64>>,
    pub max_fee_gwei: Arc<RwLock<f64>>,
    pub default_trade_eth: Arc<RwLock<f64>>,
    pub slippage_percent: Arc<RwLock<f64>>,
    pub target_profit_percent: Arc<RwLock<f64>>,
    pub time_decay_exit_seconds: Arc<RwLock<u64>>,
    pub execution_mode: Arc<RwLock<ExecutionMode>>,
    pub sub_account_count: Arc<RwLock<usize>>,
    pub min_sub_account_eth: Arc<RwLock<f64>>,
    pub rebalance_target_eth: Arc<RwLock<f64>>,
    pub is_engine_active: Arc<AtomicBool>,

    // Prysm Series Strategy Dedicated Configuration
    pub prysm_enabled: Arc<AtomicBool>,
    pub prysm_size_eth: Arc<RwLock<f64>>,
    pub prysm_exit_mode: Arc<RwLock<crate::types::PrysmExitMode>>,
    pub prysm_max_mcap_usd: Arc<RwLock<f64>>,
    pub prysm_min_block_delay: Arc<RwLock<u64>>,
    pub prysm_max_signal_age_secs: Arc<RwLock<u64>>,
    pub prysm_slippage_percent: Arc<RwLock<f64>>,
    pub prysm_target_mcap_usd: Arc<RwLock<f64>>,
    pub prysm_target_stop_loss_percent: Arc<RwLock<f64>>,

    state_path: String,
}

impl AppConfig {
    pub fn load_from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();

        let rpc_http_url = std::env::var("RPC_HTTP_URL")
            .unwrap_or_else(|_| "https://rpc.robinhood.com".to_string());
        let rpc_ws_url = std::env::var("RPC_WS_URL")
            .unwrap_or_else(|_| "wss://rpc.robinhood.com/ws".to_string());
        let chain_id = std::env::var("CHAIN_ID")
            .unwrap_or_else(|_| "4663".to_string())
            .parse::<u64>()
            .unwrap_or(4663);

        let telegram_bot_token = std::env::var("TELEGRAM_BOT_TOKEN")
            .unwrap_or_default();
        let telegram_authorized_chat_id = std::env::var("TELEGRAM_AUTHORIZED_CHAT_ID")
            .unwrap_or_else(|_| "0".to_string())
            .parse::<i64>()
            .unwrap_or(0);

        let env_mnemonic = std::env::var("MNEMONIC_PHRASE").unwrap_or_default();

        let raw_watchlist = std::env::var("WATCHLIST_ACCOUNTS")
            .unwrap_or_else(|_| "0x742d35cc6634c0532925a3b844bc454e4438f44e".to_string());
        let mut watchlist_set = HashSet::new();
        for addr_str in raw_watchlist.split(',') {
            let trimmed = addr_str.trim();
            if !trimmed.is_empty() {
                if let Ok(addr) = Address::from_str(trimmed) {
                    watchlist_set.insert(addr);
                }
            }
        }

        let buffer_size = std::env::var("BUFFER_SIZE")
            .unwrap_or_else(|_| "94500".to_string())
            .parse::<usize>()
            .unwrap_or(94500);

        let gas_limit = std::env::var("GAS_LIMIT")
            .unwrap_or_else(|_| "600000".to_string())
            .parse::<u64>()
            .unwrap_or(600000);

        let max_priority_fee_gwei = std::env::var("MAX_PRIORITY_FEE_GWEI")
            .unwrap_or_else(|_| "2.5".to_string())
            .parse::<f64>()
            .unwrap_or(2.5);

        let max_fee_gwei = std::env::var("MAX_FEE_GWEI")
            .unwrap_or_else(|_| "15.0".to_string())
            .parse::<f64>()
            .unwrap_or(15.0);

        let default_trade_eth = std::env::var("DEFAULT_TRADE_ETH")
            .unwrap_or_else(|_| "0.05".to_string())
            .parse::<f64>()
            .unwrap_or(0.05);

        let slippage_percent = std::env::var("SLIPPAGE_PERCENT")
            .unwrap_or_else(|_| "1.0".to_string())
            .parse::<f64>()
            .unwrap_or(1.0);

        let target_profit_percent = std::env::var("TARGET_PROFIT_PERCENT")
            .unwrap_or_else(|_| "35.0".to_string())
            .parse::<f64>()
            .unwrap_or(35.0);

        let time_decay_exit_seconds = std::env::var("TIME_DECAY_EXIT_SECONDS")
            .unwrap_or_else(|_| "45".to_string())
            .parse::<u64>()
            .unwrap_or(45);

        let exec_mode_str = std::env::var("EXECUTION_MODE").unwrap_or_default();
        let execution_mode = if exec_mode_str.eq_ignore_ascii_case("Single") {
            ExecutionMode::Single
        } else {
            ExecutionMode::MultiAccount
        };

        let sub_account_count = std::env::var("SUB_ACCOUNT_COUNT")
            .unwrap_or_else(|_| "2".to_string())
            .parse::<usize>()
            .unwrap_or(2);

        let min_sub_account_eth = std::env::var("MIN_SUB_ACCOUNT_ETH")
            .unwrap_or_else(|_| "0.005".to_string())
            .parse::<f64>()
            .unwrap_or(0.005);

        let rebalance_target_eth = std::env::var("REBALANCE_TARGET_ETH")
            .unwrap_or_else(|_| "0.015".to_string())
            .parse::<f64>()
            .unwrap_or(0.015);

        let state_path = "runtime_config.json".to_string();

        let config = Self {
            rpc_http_url,
            rpc_ws_url,
            chain_id,
            telegram_bot_token,
            telegram_authorized_chat_id,
            mnemonic_phrase: Arc::new(RwLock::new(env_mnemonic)),
            watchlist_accounts: Arc::new(RwLock::new(watchlist_set)),
            buffer_size: Arc::new(RwLock::new(buffer_size)),
            gas_limit: Arc::new(RwLock::new(gas_limit)),
            max_priority_fee_gwei: Arc::new(RwLock::new(max_priority_fee_gwei)),
            max_fee_gwei: Arc::new(RwLock::new(max_fee_gwei)),
            default_trade_eth: Arc::new(RwLock::new(default_trade_eth)),
            slippage_percent: Arc::new(RwLock::new(slippage_percent)),
            target_profit_percent: Arc::new(RwLock::new(target_profit_percent)),
            time_decay_exit_seconds: Arc::new(RwLock::new(time_decay_exit_seconds)),
            execution_mode: Arc::new(RwLock::new(execution_mode)),
            sub_account_count: Arc::new(RwLock::new(sub_account_count)),
            min_sub_account_eth: Arc::new(RwLock::new(min_sub_account_eth)),
            rebalance_target_eth: Arc::new(RwLock::new(rebalance_target_eth)),
            is_engine_active: Arc::new(AtomicBool::new(true)),

            // Prysm Strategy defaults
            prysm_enabled: Arc::new(AtomicBool::new(false)),
            prysm_size_eth: Arc::new(RwLock::new(0.01)),
            prysm_exit_mode: Arc::new(RwLock::new(crate::types::PrysmExitMode::System)),
            prysm_max_mcap_usd: Arc::new(RwLock::new(8000.0)),
            prysm_min_block_delay: Arc::new(RwLock::new(3)),
            prysm_max_signal_age_secs: Arc::new(RwLock::new(15)),
            prysm_slippage_percent: Arc::new(RwLock::new(25.0)),
            prysm_target_mcap_usd: Arc::new(RwLock::new(10000.0)),
            prysm_target_stop_loss_percent: Arc::new(RwLock::new(30.0)),

            state_path,
        };

        // Try restoring persisted runtime state if present
        let _ = config.load_persisted_state();

        Ok(config)
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::load_from_env().expect("Default AppConfig should always load")
    }
}

impl AppConfig {

    /// Load persisted state from JSON file
    pub fn load_persisted_state(&self) -> Result<()> {
        if !Path::new(&self.state_path).exists() {
            return Ok(());
        }

        let data = std::fs::read_to_string(&self.state_path)
            .context("Failed to read runtime state file")?;
        let state: RuntimeState = serde_json::from_str(&data)
            .context("Failed to parse runtime state")?;

        if !state.mnemonic_phrase.is_empty() {
            if let Ok(mut lock) = self.mnemonic_phrase.try_write() {
                *lock = state.mnemonic_phrase;
            }
        }

        if let Ok(mut lock) = self.watchlist_accounts.try_write() {
            lock.clear();
            for addr_str in state.watchlist_accounts {
                if let Ok(addr) = Address::from_str(&addr_str) {
                    lock.insert(addr);
                }
            }
        }

        if let Ok(mut lock) = self.buffer_size.try_write() {
            *lock = state.buffer_size;
        }
        if let Ok(mut lock) = self.gas_limit.try_write() {
            *lock = state.gas_limit;
        }
        if let Ok(mut lock) = self.max_priority_fee_gwei.try_write() {
            *lock = state.max_priority_fee_gwei;
        }
        if let Ok(mut lock) = self.max_fee_gwei.try_write() {
            *lock = state.max_fee_gwei;
        }
        if let Ok(mut lock) = self.default_trade_eth.try_write() {
            *lock = state.default_trade_eth;
        }
        if let Ok(mut lock) = self.slippage_percent.try_write() {
            *lock = state.slippage_percent;
        }
        if let Ok(mut lock) = self.target_profit_percent.try_write() {
            *lock = state.target_profit_percent;
        }
        if let Ok(mut lock) = self.time_decay_exit_seconds.try_write() {
            *lock = state.time_decay_exit_seconds;
        }
        if let Ok(mut lock) = self.execution_mode.try_write() {
            *lock = state.execution_mode;
        }
        if let Ok(mut lock) = self.sub_account_count.try_write() {
            *lock = state.sub_account_count;
        }
        self.is_engine_active.store(state.is_engine_active, Ordering::SeqCst);

        // Restore Prysm Series Strategy configuration
        self.prysm_enabled.store(state.prysm_enabled, Ordering::SeqCst);
        if let Ok(mut lock) = self.prysm_size_eth.try_write() {
            *lock = state.prysm_size_eth;
        }
        if let Ok(mut lock) = self.prysm_exit_mode.try_write() {
            *lock = state.prysm_exit_mode;
        }
        if let Ok(mut lock) = self.prysm_max_mcap_usd.try_write() {
            *lock = state.prysm_max_mcap_usd;
        }

        Ok(())
    }

    /// Persist current runtime configuration to disk
    pub async fn save_persisted_state(&self) -> Result<()> {
        let watchlist_strs: Vec<String> = self
            .watchlist_accounts
            .read()
            .await
            .iter()
            .map(|a| format!("{:?}", a))
            .collect();

        let state = RuntimeState {
            mnemonic_phrase: self.mnemonic_phrase.read().await.clone(),
            watchlist_accounts: watchlist_strs,
            buffer_size: *self.buffer_size.read().await,
            gas_limit: *self.gas_limit.read().await,
            max_priority_fee_gwei: *self.max_priority_fee_gwei.read().await,
            max_fee_gwei: *self.max_fee_gwei.read().await,
            default_trade_eth: *self.default_trade_eth.read().await,
            slippage_percent: *self.slippage_percent.read().await,
            target_profit_percent: *self.target_profit_percent.read().await,
            time_decay_exit_seconds: *self.time_decay_exit_seconds.read().await,
            execution_mode: *self.execution_mode.read().await,
            sub_account_count: *self.sub_account_count.read().await,
            is_engine_active: self.is_engine_active.load(Ordering::SeqCst),

            prysm_enabled: self.prysm_enabled.load(Ordering::SeqCst),
            prysm_size_eth: *self.prysm_size_eth.read().await,
            prysm_exit_mode: *self.prysm_exit_mode.read().await,
            prysm_max_mcap_usd: *self.prysm_max_mcap_usd.read().await,
        };

        let json = serde_json::to_string_pretty(&state)?;
        std::fs::write(&self.state_path, json)?;
        Ok(())
    }

    pub async fn add_watchlist_account(&self, address: Address) -> bool {
        let mut list = self.watchlist_accounts.write().await;
        let inserted = list.insert(address);
        drop(list);
        let _ = self.save_persisted_state().await;
        inserted
    }

    pub async fn remove_watchlist_account(&self, address: &Address) -> bool {
        let mut list = self.watchlist_accounts.write().await;
        let removed = list.remove(address);
        drop(list);
        let _ = self.save_persisted_state().await;
        removed
    }

    pub async fn is_in_watchlist(&self, address: &Address) -> bool {
        self.watchlist_accounts.read().await.contains(address)
    }

    pub fn is_engine_running(&self) -> bool {
        self.is_engine_active.load(Ordering::SeqCst)
    }

    pub async fn toggle_engine(&self) -> bool {
        let current = self.is_engine_active.load(Ordering::SeqCst);
        let new_state = !current;
        self.is_engine_active.store(new_state, Ordering::SeqCst);
        let _ = self.save_persisted_state().await;
        new_state
    }

    // =========================================================================
    // PRYSM SERIES STRATEGY ACCESSORS
    // =========================================================================

    pub fn is_prysm_enabled(&self) -> bool {
        self.prysm_enabled.load(Ordering::SeqCst)
    }

    pub async fn toggle_prysm_strategy(&self) -> bool {
        let current = self.prysm_enabled.load(Ordering::SeqCst);
        let new_state = !current;
        self.prysm_enabled.store(new_state, Ordering::SeqCst);
        let _ = self.save_persisted_state().await;
        new_state
    }

    pub async fn set_prysm_size_eth(&self, size: f64) {
        let mut lock = self.prysm_size_eth.write().await;
        *lock = size.max(0.001);
        drop(lock);
        let _ = self.save_persisted_state().await;
    }

    pub async fn toggle_prysm_exit_mode(&self) -> crate::types::PrysmExitMode {
        let mut lock = self.prysm_exit_mode.write().await;
        let next = match *lock {
            crate::types::PrysmExitMode::System => crate::types::PrysmExitMode::Target,
            crate::types::PrysmExitMode::Target => crate::types::PrysmExitMode::System,
        };
        *lock = next;
        drop(lock);
        let _ = self.save_persisted_state().await;
        next
    }

    pub async fn set_prysm_max_mcap_usd(&self, mcap: f64) {
        let mut lock = self.prysm_max_mcap_usd.write().await;
        *lock = mcap.max(100.0);
        drop(lock);
        let _ = self.save_persisted_state().await;
    }
}
