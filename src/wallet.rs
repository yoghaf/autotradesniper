use crate::config::AppConfig;
use crate::types::{GasRebalanceResult, WalletSummary};
use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use bip32::{DerivationPath, XPrv};
use bip39::{Language, Mnemonic};
use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// How far below a failed nonce a "nonce too high" heal may reach back.
const MAX_NONCE_HEAL_SPAN: u64 = 16;

/// What is known about a nonce whose broadcast did not succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceState {
    /// Every endpoint turned the tx away: the nonce was never consumed.
    Unused,
    /// The node says the nonce is already used ("nonce too low"): never hand it out again.
    Consumed,
    /// Timeout / 5xx / mixed answers: the tx may or may not have been forwarded.
    Unknown,
}

/// Nonces returned while a later nonce is already in flight. The cursor cannot move backwards past
/// the later one without handing it out twice, so the returned nonce waits here and is the first one
/// given out by the next allocation. Invariant: every entry is below the cursor.
#[derive(Default)]
pub struct NonceHoles {
    set: Mutex<BTreeSet<u64>>,
    /// Mirror of `set.len()` so the allocation fast path stays lock-free while nothing is parked.
    len: AtomicUsize,
}

impl NonceHoles {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeSet<u64>> {
        // A poisoned lock only means another thread panicked mid-insert; the set is still usable.
        self.set.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn insert(&self, nonce: u64) {
        let mut set = self.lock();
        set.insert(nonce);
        self.len.store(set.len(), Ordering::Release);
    }

    fn pop_lowest(&self) -> Option<u64> {
        if self.len.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut set = self.lock();
        let nonce = set.pop_first();
        self.len.store(set.len(), Ordering::Release);
        nonce
    }

    fn purge_below(&self, floor: u64) {
        if self.len.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut set = self.lock();
        *set = set.split_off(&floor);
        self.len.store(set.len(), Ordering::Release);
    }

    /// Parked nonces, lowest first.
    pub fn snapshot(&self) -> Vec<u64> {
        self.lock().iter().copied().collect()
    }
}

/// Cursor + parked nonces of one address; shared by every `TreasuryAccount` clone of it.
#[derive(Clone, Default)]
struct NonceLane {
    cursor: Arc<AtomicU64>,
    holes: Arc<NonceHoles>,
}

/// Account definition in Treasury with cached in-memory Atomic Nonce Cursor
#[derive(Clone)]
pub struct TreasuryAccount {
    pub role: String,
    pub index: usize,
    pub derivation_path: String,
    pub address: Address,
    pub signer: PrivateKeySigner,
    /// Atomic Nonce Cursor: eliminates repeated RPC `eth_getTransactionCount` latency
    pub nonce_cursor: Arc<AtomicU64>,
    /// Returned-out-of-order nonces waiting to be reused (see [`NonceHoles`]).
    pub nonce_holes: Arc<NonceHoles>,
}

impl TreasuryAccount {
    /// Next nonce: the lowest parked one if any, else the cursor (one atomic add when nothing is parked).
    #[inline]
    pub fn allocate_nonce(&self) -> u64 {
        if let Some(nonce) = self.nonce_holes.pop_lowest() {
            return nonce;
        }
        self.nonce_cursor.fetch_add(1, Ordering::SeqCst)
    }

    /// Give back a nonce whose tx was not sent. With nothing allocated after it the cursor simply
    /// steps back (one compare-exchange); otherwise it is parked and reused first, so neither a gap
    /// nor a duplicate can result. Never touches the network.
    pub fn return_nonce(&self, nonce: u64) {
        if self
            .nonce_cursor
            .compare_exchange(nonce.wrapping_add(1), nonce, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return;
        }
        // Only a nonce this cursor has handed out can be parked (keeps "every hole < cursor").
        if nonce < self.nonce_cursor.load(Ordering::SeqCst) {
            self.nonce_holes.insert(nonce);
        }
    }

    /// Fold the node's `pending` nonce into the local state after an ambiguous failure of `nonce`.
    ///
    /// - The cursor only ever moves forward to the node's view, so a nonce in flight is never re-issued.
    /// - Parked nonces the chain already used are dropped.
    /// - If the node has not seen `nonce` (pending <= nonce), it and any unaccounted nonces between
    ///   the node's view and it are returned, closing a "nonce too high" gap.
    pub fn reconcile_nonce(&self, nonce: u64, on_chain_pending: u64) {
        self.nonce_cursor.fetch_max(on_chain_pending, Ordering::SeqCst);
        self.nonce_holes.purge_below(on_chain_pending);
        if nonce < on_chain_pending {
            return;
        }
        let lowest = on_chain_pending.max(nonce.saturating_sub(MAX_NONCE_HEAL_SPAN));
        for n in (lowest..=nonce).rev() {
            self.return_nonce(n);
        }
    }
}

/// Hierarchical Deterministic (HD) Treasury Manager
#[derive(Clone)]
pub struct WalletManager {
    config: AppConfig,
    mnemonic: Arc<RwLock<String>>,
    nonce_cache: Arc<RwLock<HashMap<Address, NonceLane>>>,
    /// Derived accounts (index 0 = master). BIP-39 seed stretching is 2048 rounds of
    /// PBKDF2-HMAC-SHA512, so it runs once per mnemonic / sub-account count, never per trade.
    accounts_cache: Arc<RwLock<Option<Arc<Vec<TreasuryAccount>>>>>,
}

impl WalletManager {
    pub async fn new(config: AppConfig) -> Result<Self> {
        let existing_phrase = config.mnemonic_phrase.read().await.clone();
        let mnemonic_phrase = if existing_phrase.trim().is_empty() {
            info!("No mnemonic provided in environment. Generating new BIP-39 12-word phrase...");
            let mut rng = rand::rngs::OsRng;
            let mnemonic = Mnemonic::generate_in_with(&mut rng, Language::English, 12)
                .context("Failed to generate BIP-39 mnemonic")?;
            let phrase = mnemonic.to_string();
            info!("Generated Master Mnemonic Phrase successfully");
            *config.mnemonic_phrase.write().await = phrase.clone();
            // Fatal on failure: a mnemonic that only lives in memory is lost at the next restart,
            // together with whatever was deposited to it.
            config
                .save_persisted_state()
                .await
                .context("Refusing to start: the newly generated mnemonic could not be persisted")?;
            phrase
        } else {
            existing_phrase.trim().to_string()
        };

        // Nonces are synced by the caller (see `sync_initial_nonces`) before any dispatch,
        // so the first trade never goes out with a stale cursor of 0.
        Ok(Self {
            config,
            mnemonic: Arc::new(RwLock::new(mnemonic_phrase)),
            nonce_cache: Arc::new(RwLock::new(HashMap::new())),
            accounts_cache: Arc::new(RwLock::new(None)),
        })
    }

    /// Update mnemonic phrase (e.g. from Telegram import or new wallet generation)
    pub async fn set_mnemonic(&self, phrase: &str) -> Result<()> {
        let normalized = phrase.trim();
        let _ = Mnemonic::parse_normalized(normalized)
            .context("Invalid 12-word BIP-39 mnemonic phrase")?;

        *self.mnemonic.write().await = normalized.to_string();
        *self.config.mnemonic_phrase.write().await = normalized.to_string();
        self.config.save_persisted_state().await?;
        self.nonce_cache.write().await.clear();
        *self.accounts_cache.write().await = None;
        info!("Updated Master Mnemonic Phrase in treasury");

        let _ = self.sync_initial_nonces().await;
        Ok(())
    }

    /// Generate a brand new 12-word mnemonic phrase
    pub async fn generate_new_mnemonic(&self) -> Result<String> {
        let mut rng = rand::rngs::OsRng;
        let mnemonic = Mnemonic::generate_in_with(&mut rng, Language::English, 12)
            .context("Failed to generate BIP-39 mnemonic")?;
        let phrase = mnemonic.to_string();
        self.set_mnemonic(&phrase).await?;
        Ok(phrase)
    }

    pub async fn get_mnemonic_phrase(&self) -> String {
        self.mnemonic.read().await.clone()
    }

    /// Retrieve or create the nonce lane (cursor + parked nonces) for an address
    async fn get_or_create_nonce_lane(&self, address: Address) -> NonceLane {
        if let Some(lane) = self.nonce_cache.read().await.get(&address) {
            return lane.clone();
        }
        self.nonce_cache.write().await.entry(address).or_default().clone()
    }

    /// The node's `pending` nonce for `address`.
    async fn pending_nonce(&self, address: Address) -> Result<u64> {
        let provider = ProviderBuilder::new()
            .connect(&self.config.rpc_http_url)
            .await
            .context("Nonce lookup: RPC connect failed")?;
        provider
            .get_transaction_count(address)
            .pending()
            .await
            .context("Nonce lookup: eth_getTransactionCount failed")
    }

    /// Synchronize on-chain nonces (`pending` tag) into the in-memory cursors.
    ///
    /// Uses `fetch_max` so a cursor that already advanced past the node's view is never
    /// rewound (rewinding would hand out a nonce that is already in flight).
    pub async fn sync_initial_nonces(&self) -> Result<()> {
        let accounts = self.get_all_accounts_shared().await?;
        let provider = ProviderBuilder::new()
            .connect(&self.config.rpc_http_url)
            .await
            .context("Nonce sync: RPC connect failed")?;

        for acc in accounts.iter() {
            match provider.get_transaction_count(acc.address).pending().await {
                Ok(on_chain_nonce) => {
                    acc.nonce_cursor.fetch_max(on_chain_nonce, Ordering::SeqCst);
                    info!("Synced in-memory nonce cursor for {:?}: {}", acc.address, on_chain_nonce);
                }
                Err(e) => warn!("Nonce sync failed for {:?}: {}", acc.address, e),
            }
        }
        Ok(())
    }

    /// Pull one account's cursor forward to the node's `pending` nonce.
    ///
    /// Never rewinds: a lower node view usually means a lagging endpoint or a nonce still in flight,
    /// and re-issuing an in-flight nonce is worse than a gap. Gaps are closed per failed nonce by
    /// [`Self::release_nonce`] / [`TreasuryAccount::reconcile_nonce`].
    pub async fn resync_nonce(&self, address: Address) -> Result<u64> {
        let on_chain = self.pending_nonce(address).await?;
        let lane = self.get_or_create_nonce_lane(address).await;
        lane.cursor.fetch_max(on_chain, Ordering::SeqCst);
        lane.holes.purge_below(on_chain);
        info!("Nonce cursor for {:?} synced forward to at least {}", address, on_chain);
        Ok(on_chain)
    }

    /// Hand back a nonce whose transaction no endpoint accepted (`nonce_unused`) or whose fate is unknown.
    pub async fn release_nonce(&self, acc: &TreasuryAccount, nonce: u64, nonce_unused: bool) {
        let state = if nonce_unused { NonceState::Unused } else { NonceState::Unknown };
        self.release_nonce_state(acc, nonce, state).await;
    }

    /// Settle a nonce after a failed broadcast.
    ///
    /// - `Unused`: returned locally (cursor step-back, or parked when a later nonce is in flight).
    ///   No RPC, so it works while the read RPC is rate-limiting us.
    /// - `Consumed` ("nonce too low"): never returned; the cursor is pulled forward from the node.
    /// - `Unknown` (timeout, 5xx, mixed): ask the node (2 s budget) and reconcile. If the node cannot
    ///   answer, the nonce is returned anyway: a re-used nonce can only be rejected as "nonce too low"
    ///   and heals on the next sync, while a leaked one blocks every later tx of the account.
    pub async fn release_nonce_state(&self, acc: &TreasuryAccount, nonce: u64, state: NonceState) {
        if state == NonceState::Unused {
            acc.return_nonce(nonce);
            warn!("Nonce {} of {:?} returned (tx rejected everywhere)", nonce, acc.address);
            return;
        }
        let lookup = tokio::time::timeout(std::time::Duration::from_secs(2), self.pending_nonce(acc.address)).await;
        match lookup {
            Ok(Ok(on_chain)) => acc.reconcile_nonce(nonce, on_chain),
            Ok(Err(e)) => {
                error!("Nonce lookup for {:?} failed: {:#}", acc.address, e);
                if state == NonceState::Unknown {
                    acc.return_nonce(nonce);
                }
            }
            Err(_) => {
                error!("Nonce lookup for {:?} timed out", acc.address);
                if state == NonceState::Unknown {
                    acc.return_nonce(nonce);
                }
            }
        }
    }

    /// Instant lock-free atomic nonce allocation (< 10 nanoseconds)
    pub async fn next_nonce_atomic(&self, address: Address) -> u64 {
        let lane = self.get_or_create_nonce_lane(address).await;
        if let Some(nonce) = lane.holes.pop_lowest() {
            return nonce;
        }
        lane.cursor.fetch_add(1, Ordering::SeqCst)
    }

    /// Derive an account at index using BIP-44: m/44'/60'/0'/0/{index}
    pub async fn derive_account(&self, index: usize) -> Result<TreasuryAccount> {
        let accounts = self.get_all_accounts_shared().await?;
        if let Some(acc) = accounts.get(index) {
            return Ok(acc.clone());
        }
        let phrase = self.mnemonic.read().await.clone();
        let mut derived = tokio::task::spawn_blocking(move || derive_signers(&phrase, index..=index))
            .await
            .context("Key derivation task failed")??;
        let (path, signer) = derived.pop().context("Derivation returned no key")?;
        Ok(self.make_account(index, path, signer).await)
    }

    async fn make_account(&self, index: usize, derivation_path: String, signer: PrivateKeySigner) -> TreasuryAccount {
        let address = signer.address();
        let role = if index == 0 {
            "Master Account (Treasury & Core Executor)".to_string()
        } else {
            format!("Sub-Account #{} (Satellite Dispatcher)", index)
        };
        let lane = self.get_or_create_nonce_lane(address).await;
        TreasuryAccount {
            role,
            index,
            derivation_path,
            address,
            signer,
            nonce_cursor: lane.cursor,
            nonce_holes: lane.holes,
        }
    }

    /// Cached master + sub-accounts. Re-derives only when the mnemonic or the configured
    /// sub-account count changes; the hot path clones an `Arc`.
    pub async fn get_all_accounts_shared(&self) -> Result<Arc<Vec<TreasuryAccount>>> {
        let count = *self.config.sub_account_count.read().await;
        if let Some(cached) = self.accounts_cache.read().await.as_ref().filter(|c| c.len() == count + 1) {
            return Ok(cached.clone());
        }

        let mut cache = self.accounts_cache.write().await;
        if let Some(cached) = cache.as_ref().filter(|c| c.len() == count + 1) {
            return Ok(cached.clone());
        }
        let phrase = self.mnemonic.read().await.clone();
        let derived = tokio::task::spawn_blocking(move || derive_signers(&phrase, 0..=count))
            .await
            .context("Key derivation task failed")??;
        let mut accounts = Vec::with_capacity(derived.len());
        for (index, (path, signer)) in derived.into_iter().enumerate() {
            accounts.push(self.make_account(index, path, signer).await);
        }
        let accounts = Arc::new(accounts);
        *cache = Some(accounts.clone());
        Ok(accounts)
    }

    /// Get the Master Account (Index 0)
    pub async fn get_master_account(&self) -> Result<TreasuryAccount> {
        let accounts = self.get_all_accounts_shared().await?;
        accounts.first().cloned().context("Master account missing")
    }

    /// Get all Satellite Sub-Accounts according to current configuration count (Index 1..=N)
    pub async fn get_sub_accounts(&self) -> Result<Vec<TreasuryAccount>> {
        Ok(self.get_all_accounts_shared().await?[1..].to_vec())
    }

    /// Get all accounts (Master + Sub-Accounts)
    pub async fn get_all_accounts(&self) -> Result<Vec<TreasuryAccount>> {
        Ok(self.get_all_accounts_shared().await?.as_ref().clone())
    }

    /// Fetch real-time ETH balances for all accounts
    pub async fn get_treasury_summary(&self) -> Result<Vec<WalletSummary>> {
        let accounts = self.get_all_accounts().await?;
        let rpc_http = self.config.rpc_http_url.clone();
        let provider = ProviderBuilder::new().connect(&rpc_http).await.ok();

        let mut summaries = Vec::new();
        for acc in accounts {
            let balance_eth = if let Some(ref p) = provider {
                match p.get_balance(acc.address).await {
                    Ok(wei) => wei_to_eth(wei),
                    Err(_) => 0.0,
                }
            } else {
                0.0
            };

            summaries.push(WalletSummary {
                role: acc.role,
                derivation_path: acc.derivation_path,
                address: format!("{:?}", acc.address),
                balance_eth,
            });
        }

        Ok(summaries)
    }

    /// Automated Gas Rebalancing: Master account funds sub-accounts whose balance is low
    pub async fn rebalance_gas(&self) -> Result<Vec<GasRebalanceResult>> {
        info!("Initiating Automated Gas Rebalancing for Satellite Sub-Accounts...");
        let master = self.get_master_account().await?;
        let sub_accounts = self.get_sub_accounts().await?;
        let min_balance_eth = *self.config.min_sub_account_eth.read().await;
        let target_balance_eth = *self.config.rebalance_target_eth.read().await;
        let rpc_http = self.config.rpc_http_url.clone();

        let master_wallet = EthereumWallet::from(master.signer.clone());
        let provider_res = ProviderBuilder::new()
            .wallet(master_wallet)
            .connect(&rpc_http)
            .await;

        let provider = match provider_res {
            Ok(p) => p,
            Err(e) => anyhow::bail!("Gas rebalance provider connect failed: {}", e),
        };

        let mut results = Vec::new();

        for sub in sub_accounts {
            // A failed balance read must not be treated as "0 ETH" (that would trigger a top-up).
            let current_balance_wei = match provider.get_balance(sub.address).await {
                Ok(b) => b,
                Err(e) => {
                    warn!("Skipping rebalance for {:?}: balance query failed: {}", sub.address, e);
                    continue;
                }
            };
            let current_balance_f = wei_to_eth(current_balance_wei);

            if current_balance_f < min_balance_eth {
                let deficit_eth = target_balance_eth - current_balance_f;
                if deficit_eth <= 0.0 {
                    continue;
                }

                let amount_wei = U256::from((deficit_eth * 1e18) as u128);
                info!(
                    "Sub-Account {:?} balance {:.4} ETH < {:.4} ETH. Dispatching {:.4} ETH gas allocation from Master...",
                    sub.address, current_balance_f, min_balance_eth, deficit_eth
                );

                let next_nonce = master.allocate_nonce();
                // No fixed 21000 gas limit: on Arbitrum the limit must also cover the L1 poster
                // component, so a bare transfer needs more. The provider's gas filler estimates it.
                let tx = TransactionRequest::default()
                    .with_to(sub.address)
                    .with_value(amount_wei)
                    .with_nonce(next_nonce);

                match provider.send_transaction(tx).await {
                    Ok(pending) => {
                        let tx_hash = format!("{:?}", pending.tx_hash());
                        info!("Rebalance dispatched for {:?}: {}", sub.address, tx_hash);
                        results.push(GasRebalanceResult {
                            sub_account_address: format!("{:?}", sub.address),
                            amount_eth: deficit_eth,
                            tx_hash,
                            success: true,
                        });
                    }
                    Err(e) => {
                        error!("Failed to rebalance gas for {:?}: {}", sub.address, e);
                        // The send may or may not have reached a node: settle the nonce against the
                        // node's view instead of rewinding the shared cursor blindly.
                        self.release_nonce(&master, next_nonce, false).await;
                        results.push(GasRebalanceResult {
                            sub_account_address: format!("{:?}", sub.address),
                            amount_eth: deficit_eth,
                            tx_hash: "Error".to_string(),
                            success: false,
                        });
                    }
                }
            } else {
                info!(
                    "Sub-Account {:?} has sufficient gas ({:.4} ETH >= {:.4} ETH). Skipping.",
                    sub.address, current_balance_f, min_balance_eth
                );
            }
        }

        Ok(results)
    }
}

/// Wei -> ETH as f64 without a String round-trip.
fn wei_to_eth(wei: U256) -> f64 {
    f64::from(wei) / 1e18
}

/// Derive `(path, signer)` for BIP-44 indices `m/44'/60'/0'/0/{i}`.
/// CPU-bound (PBKDF2 + secp256k1): call it from `spawn_blocking`, never on a Tokio worker.
fn derive_signers(
    phrase: &str,
    indices: std::ops::RangeInclusive<usize>,
) -> Result<Vec<(String, PrivateKeySigner)>> {
    let mnemonic = Mnemonic::parse_normalized(phrase)
        .context("Failed to parse BIP-39 mnemonic for derivation")?;
    let seed = mnemonic.to_seed("");
    let mut out = Vec::with_capacity(indices.clone().count());
    for index in indices {
        let path_str = format!("m/44'/60'/0'/0/{}", index);
        let derivation_path = DerivationPath::from_str(&path_str).context("Invalid derivation path")?;
        let xprv = XPrv::derive_from_path(&seed, &derivation_path)
            .context("Failed to derive private key from path")?;
        let signer = PrivateKeySigner::from_slice(&xprv.private_key().to_bytes())
            .context("Failed to create signer from private key bytes")?;
        out.push((path_str, signer));
    }
    Ok(out)
}
