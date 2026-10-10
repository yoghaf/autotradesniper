use crate::config::AppConfig;
use crate::models::prysm::is_prysm_tag;
use crate::payload::{
    CalldataPayloadBuilder, DynamicFeeAdjuster, EXECUTION_GAS_RESERVE, TARGET_CALLDATA_BUFFER_SIZE,
};
use crate::types::{DispatchReport, ExecutionMode, TradeSignal};
use crate::wallet::WalletManager;
use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::Encodable2718;
use alloy::primitives::{Address, Bytes, TxKind, U256};
use alloy::signers::Signer;
use anyhow::{Context, Result};
use reqwest::header::CONTENT_TYPE;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Pre-warmed Persistent Connection Multi-Endpoint RPC & Direct Sequencer Broadcaster Pool
#[derive(Clone)]
pub struct BroadcasterPool {
    client: reqwest::Client,
    send_rpcs: Arc<[String]>,
}

impl BroadcasterPool {
    pub fn new(send_rpcs: Vec<String>) -> Self {
        let client = reqwest::Client::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(15)))
            // Signals can be minutes apart; an idle timeout would hand the next trade a cold
            // TCP + TLS handshake. `spawn_keep_warm` keeps the sessions alive instead.
            .pool_idle_timeout(None)
            .pool_max_idle_per_host(16)
            .connect_timeout(Duration::from_millis(1500))
            .timeout(Duration::from_millis(2500))
            .build()
            .unwrap_or_default();
        Self { client, send_rpcs: send_rpcs.into() }
    }

    /// Periodic cheap `eth_chainId` to every endpoint so TLS sessions stay warm.
    pub fn spawn_keep_warm(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        let pool = self.clone();
        tokio::spawn(async move {
            let body = bytes::Bytes::from_static(br#"{"jsonrpc":"2.0","id":0,"method":"eth_chainId","params":[]}"#);
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let pings = pool.send_rpcs.iter().map(|rpc| {
                    pool.client.post(rpc.as_str()).header(CONTENT_TYPE, "application/json").body(body.clone()).send()
                });
                futures_util::future::join_all(pings).await;
            }
        })
    }

    /// Broadcast raw signed transaction concurrently across all endpoints.
    /// The instant ANY endpoint accepts (or reports 'already known'), returns immediately!
    ///
    /// Each endpoint request runs in its own task: slower endpoints are not cancelled when the
    /// first one answers, so their pooled connections survive and the transaction keeps
    /// propagating redundantly. The JSON body is serialized once and shared (refcounted).
    pub async fn broadcast_raw_tx(&self, raw_hex: &str, expected_hash: &str) -> Result<String, String> {
        if self.send_rpcs.is_empty() {
            return Err("no SEND_RPCS endpoints configured".to_string());
        }
        let body = serde_json::to_vec(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_sendRawTransaction",
            "params": [raw_hex]
        }))
        .map(bytes::Bytes::from)
        .map_err(|e| e.to_string())?;

        let (tx, mut rx) = mpsc::channel(self.send_rpcs.len());
        for rpc in self.send_rpcs.iter().cloned() {
            let client = self.client.clone();
            let body = body.clone();
            let tx = tx.clone();
            let expected = expected_hash.to_owned();
            tokio::spawn(async move {
                let res = Self::send_one(&client, &rpc, body, &expected).await;
                let _ = tx.send((rpc, res)).await;
            });
        }
        drop(tx);

        let mut all_errors = Vec::new();
        while let Some((rpc, res)) = rx.recv().await {
            match res {
                Ok(hash) => {
                    info!("🚀 Direct Sequencer/RPC Broadcast Accepted by [{}] -> {}", rpc, hash);
                    return Ok(hash);
                }
                Err(err) => all_errors.push(format!("{}: {}", rpc, err)),
            }
        }
        Err(all_errors.join(" | "))
    }

    async fn send_one(client: &reqwest::Client, rpc: &str, body: bytes::Bytes, expected: &str) -> Result<String, String> {
        let res = client
            .post(rpc)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = res.status();
        let json: serde_json::Value = res
            .json()
            .await
            .map_err(|e| format!("HTTP {}: non JSON-RPC body ({})", status, e))?;
        if let Some(err) = json.get("error") {
            let err_msg = err.to_string().to_lowercase();
            if err_msg.contains("already known") || err_msg.contains("known transaction") {
                return Ok(expected.to_owned());
            }
            return Err(err_msg);
        }
        match json.get("result").and_then(|r| r.as_str()) {
            Some(h) if h.eq_ignore_ascii_case(expected) => Ok(h.to_owned()),
            Some(h) => Err(format!("endpoint returned hash {} != locally computed {}", h, expected)),
            None => Err(format!("HTTP {}: response without result", status)),
        }
    }

    /// Query JSON-RPC method across endpoints with automatic fallback
    pub async fn call_rpc(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value, String> {
        if self.send_rpcs.is_empty() {
            return Err("no RPC endpoints configured".to_string());
        }
        let body = serde_json::to_vec(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }))
        .map(bytes::Bytes::from)
        .map_err(|e| e.to_string())?;

        let mut last_err = String::new();
        for rpc in self.send_rpcs.iter() {
            match self.client.post(rpc.as_str())
                .header(CONTENT_TYPE, "application/json")
                .body(body.clone())
                .send()
                .await
            {
                Ok(res) => {
                    let status = res.status();
                    if let Ok(json) = res.json::<serde_json::Value>().await {
                        if let Some(err) = json.get("error") {
                            last_err = err.to_string();
                            continue;
                        }
                        if let Some(result) = json.get("result") {
                            return Ok(result.clone());
                        }
                    } else {
                        last_err = format!("HTTP {}: non-JSON response", status);
                    }
                }
                Err(e) => {
                    last_err = e.to_string();
                }
            }
        }
        Err(if last_err.is_empty() { "all RPC queries failed".to_string() } else { last_err })
    }

    /// Perform eth_call on a target contract
    pub async fn eth_call(&self, to: Address, data: &[u8]) -> Result<Vec<u8>, String> {
        let params = serde_json::json!([
            {
                "to": format!("{:?}", to),
                "data": alloy::hex::encode_prefixed(data)
            },
            "latest"
        ]);
        let res = self.call_rpc("eth_call", params).await?;
        let hex_str = res.as_str().ok_or_else(|| "eth_call result not a string".to_string())?;
        alloy::hex::decode(hex_str.trim_start_matches("0x")).map_err(|e| e.to_string())
    }

    /// Query eth_getTransactionReceipt for a given transaction hash
    pub async fn get_transaction_receipt(&self, tx_hash: alloy::primitives::B256) -> Result<Option<serde_json::Value>, String> {
        let params = serde_json::json!([format!("{:?}", tx_hash)]);
        let res = self.call_rpc("eth_getTransactionReceipt", params).await?;
        if res.is_null() {
            Ok(None)
        } else {
            Ok(Some(res))
        }
    }
}

/// High-Frequency Calldata Buffer Formatting & Concurrent Dispatcher Engine
pub struct Dispatcher {
    config: AppConfig,
    wallet_manager: WalletManager,
    fee_adjuster: Arc<DynamicFeeAdjuster>,
    broadcaster: Arc<BroadcasterPool>,
    report_tx: mpsc::Sender<DispatchReport>,
}

impl Dispatcher {
    pub fn new(
        config: AppConfig,
        wallet_manager: WalletManager,
        fee_adjuster: Arc<DynamicFeeAdjuster>,
        report_tx: mpsc::Sender<DispatchReport>,
    ) -> Self {
        let broadcaster = Arc::new(BroadcasterPool::new(config.send_rpcs.clone()));
        Self {
            config,
            wallet_manager,
            fee_adjuster,
            broadcaster,
            report_tx,
        }
    }

    pub fn broadcaster(&self) -> Arc<BroadcasterPool> {
        self.broadcaster.clone()
    }

    /// Dispatch transactions with In-Memory Nonce Caching, EMA Fee Calibration,
    /// and Pipelined Parallel Signing directly to the Sequencer.
    ///
    /// Returns `Err` unless at least one transaction was accepted by an endpoint, so callers
    /// never register a position for a buy that was never broadcast.
    pub async fn dispatch_signal(&self, signal: &TradeSignal) -> Result<DispatchReport> {
        let start_time = Instant::now();
        let is_strategy = is_prysm_tag(signal.strategy_tag.as_deref());

        // A strategy entry must name a resolved token. Prysm currently sets token == router ==
        // the launch tx's `to`; replaying the launch calldata there would create a duplicate
        // token, not buy the target.
        if is_strategy && signal.target_token == signal.pool_or_router {
            anyhow::bail!(
                "Refusing {:?} entry: token address unresolved (target == router {:?}); launch calldata replay disabled",
                signal.strategy_tag, signal.pool_or_router
            );
        }
        // Copy-trades are re-targeted per account; reject anything we cannot re-target before
        // a single nonce is consumed.
        if !is_strategy && CalldataPayloadBuilder::retarget_swap_recipient(&signal.swap_calldata, Address::ZERO).is_none() {
            anyhow::bail!("Refusing copy-trade: selector cannot be re-targeted to our recipient");
        }

        let buffer_size = *self.config.buffer_size.read().await;
        let exec_mode = *self.config.execution_mode.read().await;
        let configured_gas_limit = *self.config.gas_limit.read().await;
        let max_fee_cap_gwei = *self.config.max_fee_gwei.read().await;
        // Strategy signals carry their own size (e.g. prysm_size_eth); copy-trades use the default.
        let trade_eth = if is_strategy {
            f64::from(signal.entry_amount_wei) / 1e18
        } else {
            *self.config.default_trade_eth.read().await
        };
        anyhow::ensure!(trade_eth.is_finite() && trade_eth > 0.0, "Invalid trade size {} ETH", trade_eth);

        // Adaptive fee (EMA), with user-configured priority fee floor, capped by MAX_FEE_GWEI.
        let (ema_priority_wei, ema_max_fee_wei) = self.fee_adjuster.calculate_optimal_fees();
        let cap_wei = (max_fee_cap_gwei.max(0.0) * 1e9) as u128;
        let max_fee_wei = if cap_wei > 0 { ema_max_fee_wei.min(cap_wei) } else { ema_max_fee_wei };
        let configured_prio_gwei = *self.config.max_priority_fee_gwei.read().await;
        let configured_prio_wei = (configured_prio_gwei.max(0.0) * 1e9) as u128;
        let priority_fee_wei = ema_priority_wei.max(configured_prio_wei).min(max_fee_wei);
        let priority_fee_gwei = priority_fee_wei as f64 / 1e9;

        let value_wei = U256::from((trade_eth * 1e18) as u128);

        let accounts = self.wallet_manager.get_all_accounts_shared().await?;
        let account_count = match exec_mode {
            ExecutionMode::Single => 1,
            ExecutionMode::MultiAccount => accounts.len(),
        };

        let mut handles = Vec::with_capacity(account_count);
        let mut calldata_len = 0;
        let mut gas_limit_used = configured_gas_limit;
        for acc in accounts.iter().take(account_count).cloned() {
            let is_master = acc.index == 0;
            let (target_dest, acc_value_wei, payload) = if is_master {
                // Master account carries the real token purchase
                let swap = if is_strategy {
                    if let Some(retargeted) = CalldataPayloadBuilder::retarget_pons_buy_recipient(&signal.swap_calldata, acc.address) {
                        retargeted
                    } else {
                        signal.swap_calldata.clone()
                    }
                } else {
                    CalldataPayloadBuilder::retarget_swap_recipient(&signal.swap_calldata, acc.address)
                        .context("copy-trade re-targeting failed")?
                };
                let padding = CalldataPayloadBuilder::clamp_padding(swap.len(), buffer_size);
                let p = CalldataPayloadBuilder::build_calibrated_payload(&swap, padding);
                (signal.pool_or_router, value_wei, p)
            } else {
                // Satellite child accounts act purely as carrier dispatchers carrying the 95 KB block-booking buffer:
                // They do NOT buy tokens (value = 0 ETH), preserving 100% of token capital.
                // Their only duty is transmitting the calibrated 94.5 KB buffer to reserve sequencer block capacity!
                let carrier_size = if buffer_size > 0 { buffer_size } else { TARGET_CALLDATA_BUFFER_SIZE };
                let padding = CalldataPayloadBuilder::clamp_padding(0, carrier_size);
                let p = CalldataPayloadBuilder::build_calibrated_payload(&[], padding);
                (acc.address, U256::ZERO, p)
            };

            let gas_reserve = if is_master { EXECUTION_GAS_RESERVE } else { 21_000 };
            let gas_limit = configured_gas_limit
                .max(CalldataPayloadBuilder::intrinsic_gas(&payload, 0) + gas_reserve);
            calldata_len = payload.len();
            gas_limit_used = gas_limit;

            let broadcaster = self.broadcaster.clone();
            let chain_id = self.config.chain_id;

            // Atomic in-memory nonce allocation (< 10 ns)
            let nonce = acc.nonce_cursor.fetch_add(1, Ordering::SeqCst);

            // Pipelined parallel dispatch worker
            handles.push(tokio::spawn(async move {
                let tx = TxEip1559 {
                    chain_id,
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_wei,
                    max_priority_fee_per_gas: priority_fee_wei,
                    to: TxKind::Call(target_dest),
                    value: acc_value_wei,
                    input: Bytes::from(payload),
                    access_list: Default::default(),
                };

                let result = match acc.signer.sign_hash(&tx.signature_hash()).await {
                    Ok(sig) => {
                        let envelope = TxEnvelope::Eip1559(tx.into_signed(sig));
                        let hash = format!("{:?}", envelope.tx_hash());
                        let raw_hex = alloy::hex::encode_prefixed(envelope.encoded_2718());
                        broadcaster.broadcast_raw_tx(&raw_hex, &hash).await
                    }
                    Err(e) => Err(e.to_string()),
                };
                (acc.index, acc.address, acc.role, nonce, result)
            }));
        }

        let mut tx_hashes = Vec::new();
        let mut account_roles = Vec::new();
        let mut failures = Vec::new();
        let mut master_accepted = false;
        for handle in handles {
            let (index, address, role, nonce, result) = handle.await.context("dispatch worker panicked")?;
            match result {
                Ok(hash) => {
                    info!("✅ [{}] TX Broadcast: {} (Nonce: {})", role, hash, nonce);
                    master_accepted |= index == 0;
                    tx_hashes.push(hash);
                    account_roles.push(role);
                }
                Err(err_msg) => {
                    warn!("Broadcast failed for [{}] (nonce {}): {}", role, nonce, err_msg);
                    // The nonce was allocated but (most likely) never used: re-anchor it, or
                    // every later tx from this account would wait behind a nonce gap.
                    match tokio::time::timeout(Duration::from_secs(2), self.wallet_manager.resync_nonce(address)).await {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => error!("Nonce resync for {:?} failed: {:#}", address, e),
                        Err(_) => error!("Nonce resync for {:?} timed out", address),
                    }
                    failures.push(format!("[{}] {}", role, err_msg));
                }
            }
        }

        let latency_micros = start_time.elapsed().as_micros();
        info!(
            "⚡ Pipelined Dispatch completed in {} µs ({} accepted, {} failed)",
            latency_micros,
            tx_hashes.len(),
            failures.len()
        );
        let report = DispatchReport {
            tx_hashes,
            account_roles,
            block_number: signal.block_number,
            calldata_size_bytes: calldata_len,
            execution_mode: exec_mode,
            gas_limit: gas_limit_used,
            priority_fee_gwei,
            latency_micros,
            trade_eth,
        };

        if !report.tx_hashes.is_empty()
            && let Err(e) = self.report_tx.try_send(report.clone())
        {
            error!("Failed to send dispatch report: {}", e);
        }

        // The master (index 0) carries the position; satellites alone do not open one.
        anyhow::ensure!(
            master_accepted,
            "Master entry was not accepted by any endpoint: {}",
            failures.join(" || ")
        );
        Ok(report)
    }
}
