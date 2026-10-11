use crate::config::AppConfig;
use crate::models::prysm::is_prysm_tag;
use crate::payload::{CalldataPayloadBuilder, DynamicFeeAdjuster, TARGET_CALLDATA_BUFFER_SIZE};
use crate::types::{DispatchReport, ExecutionMode, TradeSignal};
use crate::wallet::{NonceState, TreasuryAccount, WalletManager};
use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::Encodable2718;
use alloy::primitives::{Address, Bytes, TxKind, U256};
use alloy::signers::Signer;
use anyhow::{Context, Result};
use reqwest::header::CONTENT_TYPE;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Why a broadcast was not accepted.
#[derive(Debug, Clone)]
pub struct BroadcastError {
    pub detail: String,
    /// Every endpoint turned the request away (JSON-RPC error, HTTP 4xx, refused connection),
    /// so none of them forwarded the tx. A timeout or 5xx makes this false: the tx may still land.
    pub rejected_everywhere: bool,
}

impl BroadcastError {
    /// The nonce is provably unused: safe to hand it back without asking a node.
    /// "nonce too low/high" means our cursor is wrong, so it must be re-synced instead.
    pub fn nonce_unused(&self) -> bool {
        self.nonce_state() == NonceState::Unused
    }

    /// What the failure proves about the nonce.
    /// - any endpoint reporting the nonce (or a same-nonce replacement) as taken: `Consumed`;
    /// - every endpoint turned the tx away for a reason unrelated to the nonce: `Unused`;
    /// - anything else (timeouts, 5xx, mixed answers, "nonce too high"): `Unknown`.
    pub fn nonce_state(&self) -> NonceState {
        let detail = self.detail.to_lowercase();
        if detail.contains("nonce too low")
            || detail.contains("replacement transaction underpriced")
            || detail.contains("nonce has already been used")
        {
            NonceState::Consumed
        } else if self.rejected_everywhere && !detail.contains("nonce") {
            NonceState::Unused
        } else {
            NonceState::Unknown
        }
    }

    /// The sequencer refused the gas limit (intrinsic / poster / floor gas). The nonce is untouched,
    /// so re-signing the same nonce with a higher limit is safe: only one tx per nonce can execute.
    pub fn gas_too_low(&self) -> bool {
        let detail = self.detail.to_lowercase();
        detail.contains("intrinsic gas") || detail.contains("gas too low") || detail.contains("floor data gas")
    }

    /// Turned away by a rate limiter (HTTP 429 or JSON-RPC -32005).
    pub fn rate_limited(&self) -> bool {
        let detail = self.detail.to_lowercase();
        detail.contains("http 429") || detail.contains("too many requests") || detail.contains("rate limit") || detail.contains("-32005")
    }
}

impl std::fmt::Display for BroadcastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

struct EndpointError {
    definitive: bool,
    message: String,
}

/// `eth_sendRawTransaction` request body, hex-encoded straight into one buffer.
pub fn send_raw_tx_body(raw_tx: &[u8]) -> bytes::Bytes {
    const HEAD: &[u8] = br#"{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x"#;
    const TAIL: &[u8] = br#""]}"#;
    let mut body = Vec::with_capacity(HEAD.len() + raw_tx.len() * 2 + TAIL.len());
    body.extend_from_slice(HEAD);
    let start = body.len();
    body.resize(start + raw_tx.len() * 2, 0);
    // Hex output length is exact, so this cannot fail.
    let _ = alloy::hex::encode_to_slice(raw_tx, &mut body[start..]);
    body.extend_from_slice(TAIL);
    bytes::Bytes::from(body)
}

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

    /// Broadcast a signed transaction (EIP-2718 bytes) concurrently across all endpoints.
    /// The instant ANY endpoint accepts (or reports 'already known'), returns immediately!
    ///
    /// Each endpoint request runs in its own task: slower endpoints are not cancelled when the
    /// first one answers, so their pooled connections survive and the transaction keeps
    /// propagating redundantly. The JSON body is built once, without a `serde_json::Value`
    /// tree, and shared (refcounted).
    pub async fn broadcast_raw_tx(&self, raw_tx: &[u8], expected_hash: &str) -> Result<String, BroadcastError> {
        if self.send_rpcs.is_empty() {
            return Err(BroadcastError {
                detail: "no SEND_RPCS endpoints configured".to_string(),
                rejected_everywhere: true,
            });
        }
        let body = send_raw_tx_body(raw_tx);

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
        let mut rejected_everywhere = true;
        while let Some((rpc, res)) = rx.recv().await {
            match res {
                Ok(hash) => {
                    info!("🚀 Direct Sequencer/RPC Broadcast Accepted by [{}] -> {}", rpc, hash);
                    return Ok(hash);
                }
                Err(err) => {
                    rejected_everywhere &= err.definitive;
                    all_errors.push(format!("{}: {}", rpc, err.message));
                }
            }
        }
        Err(BroadcastError { detail: all_errors.join(" | "), rejected_everywhere })
    }

    async fn send_one(client: &reqwest::Client, rpc: &str, body: bytes::Bytes, expected: &str) -> Result<String, EndpointError> {
        let res = client
            .post(rpc)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            // A refused connection never carried the tx; a timeout may have.
            .map_err(|e| EndpointError { definitive: e.is_connect(), message: e.to_string() })?;
        let status = res.status();
        let json: serde_json::Value = res.json().await.map_err(|e| EndpointError {
            // 4xx (e.g. 429 behind a CDN) means the request was turned away; a 5xx gateway
            // error may hide an upstream that already forwarded the tx.
            definitive: status.is_client_error(),
            message: format!("HTTP {}: non JSON-RPC body ({})", status, e),
        })?;
        if let Some(err) = json.get("error") {
            let err_msg = err.to_string().to_lowercase();
            if err_msg.contains("already known") || err_msg.contains("known transaction") {
                return Ok(expected.to_owned());
            }
            // Keep the status: a 429 with a JSON-RPC error body must stay recognisable as rate limiting.
            let message = if status.is_success() { err_msg } else { format!("HTTP {}: {}", status, err_msg) };
            return Err(EndpointError { definitive: true, message });
        }
        match json.get("result").and_then(|r| r.as_str()) {
            Some(h) if h.eq_ignore_ascii_case(expected) => Ok(h.to_owned()),
            Some(h) => Err(EndpointError {
                definitive: false,
                message: format!("endpoint returned hash {} != locally computed {}", h, expected),
            }),
            None => Err(EndpointError { definitive: false, message: format!("HTTP {}: response without result", status) }),
        }
    }

    /// Ask every endpoint at once and return the first non-null `result`.
    ///
    /// For data that appears on different nodes at different times (receipts right after
    /// sequencing), the fastest indexer wins instead of the first one in the list. Returns
    /// `Ok(Null)` when every endpoint answered but none had the data yet.
    pub async fn call_rpc_race(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value, String> {
        if self.send_rpcs.is_empty() {
            return Err("no RPC endpoints configured".to_string());
        }
        let body = bytes::Bytes::from(
            serde_json::to_vec(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
                .map_err(|e| e.to_string())?,
        );
        // Spawned (not merely polled) so the losers are not cancelled when one endpoint wins:
        // a cancelled request discards its pooled connection and the next poll pays a handshake.
        let (tx, mut rx) = mpsc::channel(self.send_rpcs.len());
        for rpc in self.send_rpcs.iter() {
            let req = self.client.post(rpc.as_str()).header(CONTENT_TYPE, "application/json").body(body.clone());
            let tx = tx.clone();
            tokio::spawn(async move {
                let res: Result<serde_json::Value, String> = async {
                    let json: serde_json::Value = req.send().await.map_err(|e| e.to_string())?.json().await.map_err(|e| e.to_string())?;
                    if let Some(err) = json.get("error") {
                        return Err(err.to_string());
                    }
                    Ok(json.get("result").cloned().unwrap_or(serde_json::Value::Null))
                }
                .await;
                let _ = tx.send(res).await;
            });
        }
        drop(tx);

        let mut answered = false;
        let mut last_err = String::new();
        while let Some(res) = rx.recv().await {
            match res {
                Ok(v) if !v.is_null() => return Ok(v),
                Ok(_) => answered = true,
                Err(e) => last_err = e,
            }
        }
        if answered { Ok(serde_json::Value::Null) } else { Err(last_err) }
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

    /// Query eth_getTransactionReceipt for a given transaction hash (first endpoint that has it)
    pub async fn get_transaction_receipt(&self, tx_hash: alloy::primitives::B256) -> Result<Option<serde_json::Value>, String> {
        let params = serde_json::json!([format!("{:?}", tx_hash)]);
        let res = self.call_rpc_race("eth_getTransactionReceipt", params).await?;
        if res.is_null() {
            Ok(None)
        } else {
            Ok(Some(res))
        }
    }
}

/// Pause before the single re-broadcast of a rate-limited master tx.
const RATE_LIMIT_RETRY_DELAY: Duration = Duration::from_millis(60);

/// What one dispatch worker may do after its first broadcast is refused.
#[derive(Clone, Copy)]
struct RetryPlan {
    /// Lower bound for an escalated gas limit: calldata floor + 1.5x L1 component.
    floor_with_l1: u64,
    /// Re-send the identical tx once after a 429 (master only: carriers must not add load to a
    /// limiter that is already pushing back).
    retry_rate_limited: bool,
}

/// Sign `tx` and broadcast it; at most one retry, either
/// - "gas too low": same nonce, escalated limit (the rejected tx can never execute, so there is no
///   double-spend risk), or
/// - rate limited: the identical bytes again after a short pause.
async fn sign_and_broadcast(
    acc: &TreasuryAccount,
    mut tx: TxEip1559,
    broadcaster: &BroadcasterPool,
    plan: RetryPlan,
) -> Result<String, BroadcastError> {
    let mut gas_retry_left = true;
    let mut rate_retry_left = plan.retry_rate_limited;
    loop {
        let sig = match acc.signer.sign_hash(&tx.signature_hash()).await {
            Ok(sig) => sig,
            // Never signed, so never sent.
            Err(e) => return Err(BroadcastError { detail: format!("signing failed: {}", e), rejected_everywhere: true }),
        };
        let envelope = TxEnvelope::Eip1559(tx.clone().into_signed(sig));
        let hash = format!("{:?}", envelope.tx_hash());
        match broadcaster.broadcast_raw_tx(&envelope.encoded_2718(), &hash).await {
            Err(e) if gas_retry_left && e.gas_too_low() => {
                gas_retry_left = false;
                let bumped = CalldataPayloadBuilder::escalated_gas_limit(tx.gas_limit, plan.floor_with_l1);
                if bumped <= tx.gas_limit {
                    return Err(e);
                }
                warn!("[{}] gas limit {} refused ({}); retrying with {}", acc.role, tx.gas_limit, e, bumped);
                tx.gas_limit = bumped;
            }
            Err(e) if rate_retry_left && e.rate_limited() => {
                rate_retry_left = false;
                warn!("[{}] rate limited ({}); retrying once", acc.role, e);
                tokio::time::sleep(RATE_LIMIT_RETRY_DELAY).await;
            }
            other => return other,
        }
    }
}

/// High-Frequency Calldata Buffer Formatting & Concurrent Dispatcher Engine
pub struct Dispatcher {
    config: AppConfig,
    wallet_manager: Arc<WalletManager>,
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
            wallet_manager: Arc::new(wallet_manager),
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

        // A strategy entry must name a resolved token. Replaying a launch tx's calldata into
        // the launchpad would create a duplicate token, not buy the target.
        if is_strategy && signal.target_token == signal.pool_or_router {
            anyhow::bail!(
                "Refusing {:?} entry: token address unresolved (target == router {:?}); launch calldata replay disabled",
                signal.strategy_tag, signal.pool_or_router
            );
        }

        let buffer_size = *self.config.buffer_size.read().await;
        let exec_mode = *self.config.execution_mode.read().await;
        let configured_gas_limit = *self.config.gas_limit.read().await;
        let max_fee_cap_gwei = *self.config.max_fee_gwei.read().await;

        // Master size in wei. Strategy signals carry their exact size: a wei -> f64 -> wei round
        // trip can land 1 wei off the `ethIn` encoded in the Pons calldata (e.g. 0.0021 ETH).
        let value_wei = if is_strategy {
            signal.entry_amount_wei
        } else {
            let eth = *self.config.default_trade_eth.read().await;
            anyhow::ensure!(eth.is_finite() && eth > 0.0, "Invalid DEFAULT_TRADE_ETH {}", eth);
            U256::from((eth * 1e18) as u128)
        };
        anyhow::ensure!(!value_wei.is_zero(), "Invalid trade size 0 wei");
        let trade_eth = f64::from(value_wei) / 1e18;
        // What the source calldata was quoted for: the watched wallet's msg.value for a
        // copy-trade, our own size for a strategy entry.
        let reference_value = signal.entry_amount_wei;

        // Reject anything we cannot re-target before a single nonce is consumed.
        if CalldataPayloadBuilder::adapt_buy_calldata(&signal.swap_calldata, Address::ZERO, value_wei, reference_value).is_none() {
            anyhow::bail!("Refusing signal: calldata cannot be re-targeted to our recipient and size");
        }

        // Adaptive fee (EMA), with the user-configured priority fee floor, capped by MAX_FEE_GWEI.
        // The priority fee never exceeds max fee (invalid tx otherwise).
        let (ema_priority_wei, max_fee_wei) = self.fee_adjuster.capped_fees(max_fee_cap_gwei);
        let configured_prio_gwei = *self.config.max_priority_fee_gwei.read().await;
        let configured_prio_wei = if configured_prio_gwei.is_finite() { (configured_prio_gwei.max(0.0) * 1e9) as u128 } else { 0 };
        let priority_fee_wei = ema_priority_wei.max(configured_prio_wei).min(max_fee_wei);
        let priority_fee_gwei = priority_fee_wei as f64 / 1e9;
        let l1_component_gas = self.fee_adjuster.l1_component_gas();

        let accounts = self.wallet_manager.get_all_accounts_shared().await?;
        let account_count = match exec_mode {
            ExecutionMode::Single => 1,
            ExecutionMode::MultiAccount => accounts.len(),
        };

        if l1_component_gas == 0 {
            warn!(
                "L1 poster-gas estimate unavailable (NodeInterface not answering?): gas limits use the {} fallback",
                crate::payload::L1_COMPONENT_FALLBACK_GAS
            );
        }

        let mut handles = Vec::with_capacity(account_count);
        let mut nonces = Vec::with_capacity(account_count);
        // Reported figures describe the master tx (the one that carries the position), not the last carrier.
        let mut calldata_len = 0;
        let mut gas_limit_used = configured_gas_limit;
        for acc in accounts.iter().take(account_count).cloned() {
            let is_master = acc.index == 0;
            let (target_dest, acc_value_wei, input, gas_limit, floor_gas) = if is_master {
                // Master account carries the real token purchase. Recipient, `ethIn` and the
                // minimum output follow our own size (a copied trade was quoted for theirs).
                let swap = CalldataPayloadBuilder::adapt_buy_calldata(&signal.swap_calldata, acc.address, value_wei, reference_value)
                    .context("calldata re-targeting failed")?;
                // Padding never pushes the signed tx over the sequencer's size cap.
                let padding = CalldataPayloadBuilder::clamp_padding(swap.len(), buffer_size);
                let gas_limit = CalldataPayloadBuilder::gas_limit(&swap, padding, configured_gas_limit, l1_component_gas);
                let floor_gas = CalldataPayloadBuilder::calldata_floor_gas(&swap, padding);
                // One calloc'd buffer (zeroed pages, no memset) holding calldata + padding.
                let input = Bytes::from(CalldataPayloadBuilder::build_calibrated_payload(&swap, padding));
                (signal.pool_or_router, value_wei, input, gas_limit, floor_gas)
            } else {
                // Satellite child accounts act purely as carrier dispatchers carrying the 95 KB block-booking buffer:
                // They do NOT buy tokens (value = 0 ETH), preserving 100% of token capital.
                // Their only duty is transmitting the calibrated 94.5 KB buffer to reserve sequencer block capacity!
                // A zero-value call to the account itself (an EOA) runs no code, so it cannot revert.
                let carrier_size = if buffer_size > 0 { buffer_size } else { TARGET_CALLDATA_BUFFER_SIZE };
                let padding = CalldataPayloadBuilder::clamp_padding(0, carrier_size);
                let gas_limit = CalldataPayloadBuilder::carrier_gas_limit(padding, configured_gas_limit, l1_component_gas);
                let floor_gas = CalldataPayloadBuilder::calldata_floor_gas(&[], padding);
                // Zero-copy: a refcounted view of the static zero page, shared by every carrier.
                (acc.address, U256::ZERO, CalldataPayloadBuilder::zero_padding(padding), gas_limit, floor_gas)
            };
            if is_master {
                calldata_len = input.len();
                gas_limit_used = gas_limit;
            }

            let plan = RetryPlan {
                floor_with_l1: floor_gas.saturating_add(CalldataPayloadBuilder::l1_margin(l1_component_gas)),
                retry_rate_limited: is_master,
            };
            let broadcaster = self.broadcaster.clone();
            let wallet_manager = self.wallet_manager.clone();
            let chain_id = self.config.chain_id;

            // Atomic in-memory nonce allocation (< 10 ns); a parked nonce is reused first.
            let nonce = acc.allocate_nonce();
            nonces.push(nonce);

            // Pipelined parallel dispatch worker. It settles its own nonce on failure, so the
            // rollbacks of several failed accounts run in parallel (not one 2 s lookup after the other),
            // never delay the report, and still happen if the caller of `dispatch_signal` is dropped.
            handles.push(tokio::spawn(async move {
                let tx = TxEip1559 {
                    chain_id,
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_wei,
                    max_priority_fee_per_gas: priority_fee_wei,
                    to: TxKind::Call(target_dest),
                    value: acc_value_wei,
                    input,
                    access_list: Default::default(),
                };
                let result = sign_and_broadcast(&acc, tx, &broadcaster, plan).await;
                if let Err(err) = &result {
                    wallet_manager.release_nonce_state(&acc, nonce, err.nonce_state()).await;
                }
                (acc, nonce, result)
            }));
        }

        let mut tx_hashes = Vec::new();
        let mut account_roles = Vec::new();
        let mut failures = Vec::new();
        let mut master_accepted = false;
        for (i, handle) in handles.into_iter().enumerate() {
            let (acc, nonce, result) = match handle.await {
                Ok(done) => done,
                Err(join_err) => {
                    // A panicking worker never reached its own release: settle the nonce here, and keep
                    // collecting the other workers instead of abandoning their results.
                    let acc = &accounts[i];
                    error!("dispatch worker for [{}] died: {}", acc.role, join_err);
                    self.wallet_manager.release_nonce_state(acc, nonces[i], NonceState::Unknown).await;
                    failures.push(format!("[{}] worker died: {}", acc.role, join_err));
                    continue;
                }
            };
            match result {
                Ok(hash) => {
                    info!("✅ [{}] TX Broadcast: {} (Nonce: {})", acc.role, hash, nonce);
                    master_accepted |= acc.index == 0;
                    tx_hashes.push(hash);
                    account_roles.push(acc.role);
                }
                Err(err) => {
                    warn!("Broadcast failed for [{}] (nonce {}): {}", acc.role, nonce, err);
                    failures.push(format!("[{}] {}", acc.role, err));
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
