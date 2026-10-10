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
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

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
}

/// Hierarchical Deterministic (HD) Treasury Manager
#[derive(Clone)]
pub struct WalletManager {
    config: AppConfig,
    mnemonic: Arc<RwLock<String>>,
    nonce_cache: Arc<RwLock<HashMap<Address, Arc<AtomicU64>>>>,
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

    /// Retrieve or create the atomic nonce cursor for an address
    async fn get_or_create_nonce_cursor(&self, address: Address) -> Arc<AtomicU64> {
        if let Some(cursor) = self.nonce_cache.read().await.get(&address) {
            return cursor.clone();
        }
        self.nonce_cache
            .write()
            .await
            .entry(address)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
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

    /// Re-anchor one account's cursor to the node's `pending` nonce.
    ///
    /// Called after a broadcast that no endpoint accepted: the allocated nonce was burned, and
    /// without a resync every later transaction from this account would sit behind a nonce gap.
    pub async fn resync_nonce(&self, address: Address) -> Result<u64> {
        let provider = ProviderBuilder::new()
            .connect(&self.config.rpc_http_url)
            .await
            .context("Nonce resync: RPC connect failed")?;
        let on_chain = provider
            .get_transaction_count(address)
            .pending()
            .await
            .context("Nonce resync: eth_getTransactionCount failed")?;
        self.get_or_create_nonce_cursor(address).await.store(on_chain, Ordering::SeqCst);
        warn!("Nonce cursor for {:?} re-synced to {}", address, on_chain);
        Ok(on_chain)
    }

    /// Hand back a nonce whose transaction no endpoint accepted.
    ///
    /// When the rejection is definitive (`nonce_unused`) and nothing was allocated after it,
    /// the cursor is rolled back with one compare-exchange: no RPC round trip, and it still
    /// works while the read RPC is rate-limiting us (a failed re-sync would otherwise leave a
    /// permanent gap until the next failure). In every other case (timeouts, "nonce too
    /// low/high", a later nonce already in flight) the cursor is re-anchored from the node.
    pub async fn release_nonce(&self, acc: &TreasuryAccount, nonce: u64, nonce_unused: bool) {
        if nonce_unused
            && acc
                .nonce_cursor
                .compare_exchange(nonce.wrapping_add(1), nonce, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            warn!("Nonce cursor for {:?} rolled back to {} (tx rejected everywhere)", acc.address, nonce);
            return;
        }
        match tokio::time::timeout(std::time::Duration::from_secs(2), self.resync_nonce(acc.address)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => error!("Nonce resync for {:?} failed: {:#}", acc.address, e),
            Err(_) => error!("Nonce resync for {:?} timed out", acc.address),
        }
    }

    /// Instant lock-free atomic nonce allocation (< 10 nanoseconds)
    pub async fn next_nonce_atomic(&self, address: Address) -> u64 {
        let cursor = self.get_or_create_nonce_cursor(address).await;
        cursor.fetch_add(1, Ordering::SeqCst)
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
        TreasuryAccount {
            role,
            index,
            derivation_path,
            address,
            signer,
            nonce_cursor: self.get_or_create_nonce_cursor(address).await,
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

                let next_nonce = self.next_nonce_atomic(master.address).await;
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
                        // The allocated nonce was not consumed on-chain; re-anchor the cursor.
                        let _ = self.resync_nonce(master.address).await;
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
