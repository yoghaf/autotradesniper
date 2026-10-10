use crate::addr_set::AddressSet;
use crate::types::ExecutionMode;
use alloy::primitives::Address;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

fn default_prysm_enabled() -> bool { false }
fn default_prysm_size_eth() -> f64 { 0.01 }
fn default_prysm_exit_mode() -> crate::types::PrysmExitMode { crate::types::PrysmExitMode::System }
fn default_prysm_max_mcap_usd() -> f64 { 8000.0 }
fn default_prysm_min_block_delay() -> u64 { 1 }

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
    #[serde(default = "default_prysm_min_block_delay")]
    pub prysm_min_block_delay: u64,
}

/// The watched-wallet set.
///
/// `read`/`write` keep the async `HashSet` API used by Telegram and persistence. Every write
/// republishes an [`AddressSet`] snapshot, which the sequencer-feed hot path queries through
/// [`Watchlist::contains`]: a synchronous bitwise lookup that never awaits and never hashes.
pub struct Watchlist {
    set: RwLock<HashSet<Address>>,
    index: std::sync::RwLock<AddressSet>,
}

impl Watchlist {
    pub fn new(set: HashSet<Address>) -> Self {
        let index = AddressSet::new(set.iter());
        Self { set: RwLock::new(set), index: std::sync::RwLock::new(index) }
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, HashSet<Address>> {
        self.set.read().await
    }

    pub async fn write(&self) -> WatchlistWriteGuard<'_> {
        WatchlistWriteGuard { guard: self.set.write().await, index: &self.index }
    }

    pub fn try_write(&self) -> Result<WatchlistWriteGuard<'_>, tokio::sync::TryLockError> {
        Ok(WatchlistWriteGuard { guard: self.set.try_write()?, index: &self.index })
    }

    /// Hot-path membership test (see type docs).
    #[inline]
    pub fn contains(&self, address: &Address) -> bool {
        self.index.read().unwrap_or_else(|e| e.into_inner()).contains(address)
    }
}

/// Write access to the watchlist; the lookup index is rebuilt while the lock is still held,
/// so readers of [`Watchlist::contains`] never see a state older than the last released write.
pub struct WatchlistWriteGuard<'a> {
    guard: tokio::sync::RwLockWriteGuard<'a, HashSet<Address>>,
    index: &'a std::sync::RwLock<AddressSet>,
}

impl std::ops::Deref for WatchlistWriteGuard<'_> {
    type Target = HashSet<Address>;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for WatchlistWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for WatchlistWriteGuard<'_> {
    fn drop(&mut self) {
        let rebuilt = AddressSet::new(self.guard.iter());
        *self.index.write().unwrap_or_else(|e| e.into_inner()) = rebuilt;
    }
}

/// Global shared application configuration
#[derive(Clone)]
pub struct AppConfig {
    pub rpc_http_url: String,
    pub rpc_ws_url: String,
    /// Primary sequencer feed (`FEED_URL`); always the first entry of `feed_urls`.
    pub feed_url: String,
    /// Every feed the stream listener races (`FEED_URL` + `FEED_URLS`). A URL listed twice
    /// opens two connections, which can land on different edge servers.
    pub feed_urls: Vec<String>,
    /// Reconnect when no feed *message* (pings do not count) arrived for this long; 0 = off.
    /// Leave off on chains that can be legitimately idle.
    pub feed_data_timeout_secs: u64,
    pub send_rpcs: Vec<String>,
    pub chain_id: u64,
    /// L2 block number of feed message #0 (`FEED_GENESIS_BLOCK`, 0 for fresh Orbit chains).
    /// Keeps feed-derived block numbers consistent with the HTTP poller's real block numbers.
    pub feed_genesis_block: u64,
    pub telegram_bot_token: String,
    pub telegram_authorized_chat_id: i64,

    pub mnemonic_phrase: Arc<RwLock<String>>,
    pub watchlist_accounts: Arc<Watchlist>,
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
    /// Launchpad factory contracts whose launch calldata may arm Pola A/B (`PRYSM_LAUNCHPAD_FACTORIES`).
    /// Empty = Pola A/B fail closed, so arbitrary contracts cannot spoof a launch.
    pub prysm_launchpads: Arc<HashSet<Address>>,

    state_path: String,
    /// Serializes state file writes so concurrent saves cannot interleave.
    save_lock: Arc<tokio::sync::Mutex<()>>,
}

impl AppConfig {
    pub fn load_from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();

        let feed_url = std::env::var("FEED_URL")
            .or_else(|_| std::env::var("RPC_WS_URL"))
            .unwrap_or_else(|_| "wss://feed.mainnet.chain.robinhood.com".to_string());
        let mut feed_urls = vec![feed_url.clone()];
        feed_urls.extend(
            std::env::var("FEED_URLS")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
        );
        let feed_data_timeout_secs = std::env::var("FEED_DATA_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let rpc_http_url = std::env::var("RPC_HTTP_URL")
            .unwrap_or_else(|_| "https://rpc.mainnet.chain.robinhood.com".to_string());
        let rpc_ws_url = std::env::var("RPC_WS_URL")
            .unwrap_or_else(|_| "wss://feed.mainnet.chain.robinhood.com".to_string());

        let raw_send_rpcs = std::env::var("SEND_RPCS").unwrap_or_else(|_| {
            "https://sequencer.mainnet.chain.robinhood.com,https://rpc.mainnet.chain.robinhood.com,https://robinhood-rpc.publicnode.com,https://robinhood.drpc.org,https://robinhood-chain.gateway.tenderly.co".to_string()
        });
        let send_rpcs: Vec<String> = raw_send_rpcs
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let chain_id = std::env::var("CHAIN_ID")
            .unwrap_or_else(|_| "4663".to_string())
            .parse::<u64>()
            .unwrap_or(4663);

        let feed_genesis_block = std::env::var("FEED_GENESIS_BLOCK")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);

        // Pons Launchpad Factory addresses (evaluated by the Prysm series model).
        // Defaults to the official PonsV2LaunchFactory (0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e) from bottradepons.
        let raw_launchpads = std::env::var("PONS_FACTORY")
            .or_else(|_| std::env::var("PONS_LAUNCHPAD_FACTORIES"))
            .or_else(|_| std::env::var("PRYSM_LAUNCHPAD_FACTORIES"))
            .unwrap_or_else(|_| "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e".to_string());
        let prysm_launchpads: HashSet<Address> = raw_launchpads
            .split(',')
            .filter_map(|s| Address::from_str(s.trim()).ok())
            .collect();

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
            .unwrap_or_else(|_| "0".to_string())
            .parse::<usize>()
            .unwrap_or(0);

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
            feed_url,
            feed_urls,
            feed_data_timeout_secs,
            send_rpcs,
            chain_id,
            feed_genesis_block,
            telegram_bot_token,
            telegram_authorized_chat_id,
            mnemonic_phrase: Arc::new(RwLock::new(env_mnemonic)),
            watchlist_accounts: Arc::new(Watchlist::new(watchlist_set)),
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
            prysm_min_block_delay: Arc::new(RwLock::new(
                std::env::var("PRYSM_MIN_BLOCK_DELAY")
                    .ok()
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .unwrap_or(1),
            )),
            prysm_max_signal_age_secs: Arc::new(RwLock::new(15)),
            prysm_slippage_percent: Arc::new(RwLock::new(25.0)),
            prysm_target_mcap_usd: Arc::new(RwLock::new(10000.0)),
            prysm_target_stop_loss_percent: Arc::new(RwLock::new(30.0)),
            prysm_launchpads: Arc::new(prysm_launchpads),

            state_path,
            save_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        // Restore persisted runtime state. A state file that exists but cannot be parsed is fatal:
        // silently continuing would make WalletManager generate a fresh mnemonic and overwrite
        // the only copy of the old one.
        config
            .load_persisted_state()
            .with_context(|| format!("Refusing to start: '{}' exists but is unreadable", config.state_path))?;

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
        if let Ok(mut lock) = self.prysm_min_block_delay.try_write() {
            *lock = state.prysm_min_block_delay;
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
            prysm_min_block_delay: *self.prysm_min_block_delay.read().await,
        };

        let json = serde_json::to_vec_pretty(&state)?;
        let path = PathBuf::from(&self.state_path);
        let _guard = self.save_lock.lock().await;
        tokio::task::spawn_blocking(move || write_private_atomic(&path, &json))
            .await
            .context("State writer task failed")??;
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
        self.is_watched(address)
    }

    /// Synchronous, lock-free-for-readers form of [`Self::is_in_watchlist`] for the hot path.
    #[inline]
    pub fn is_watched(&self, address: &Address) -> bool {
        self.watchlist_accounts.contains(address)
    }

    pub fn is_engine_running(&self) -> bool {
        self.is_engine_active.load(Ordering::SeqCst)
    }

    /// Idempotent engine intake switch (use instead of `toggle_engine` when the target state matters)
    pub async fn set_engine_running(&self, running: bool) {
        self.is_engine_active.store(running, Ordering::SeqCst);
        let _ = self.save_persisted_state().await;
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

/// Write `data` to `path` atomically (temp file + fsync + rename) with owner-only permissions.
/// The state file holds the master mnemonic, so it must never be world-readable or half-written.
fn write_private_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(&tmp)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(data)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)
}
