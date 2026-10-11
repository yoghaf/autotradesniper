use crate::config::AppConfig;
use crate::models::prysm::is_prysm_tag;
use crate::payload::{self, CalldataPayloadBuilder, DynamicFeeAdjuster, TARGET_CALLDATA_BUFFER_SIZE};
use crate::types::{DispatchReport, ExecutionMode, TradeSignal};
use crate::wallet::{NonceState, TreasuryAccount, WalletManager};
use alloy::consensus::TxEip1559;
use alloy::primitives::{Address, Bytes, B256, U256};
use anyhow::{Context, Result};
use reqwest::header::CONTENT_TYPE;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

mod presign;

pub use presign::{
    sign_sync, Armed, BatchState, Claim, ClaimedSlot, EntryCtx, Invalidation, PresignPolicy, PresignSnapshot, PresignStats,
    SignedBytes, SlotTicket, StagedBatch, StagingBook, Teardown, TxSpec,
};

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

/// JSON-RPC requests issued by one pool, by method (shared by its clones).
#[derive(Debug, Default)]
pub struct RpcCallCounters {
    pub eth_call: std::sync::atomic::AtomicU64,
    pub receipts: std::sync::atomic::AtomicU64,
    pub block_number: std::sync::atomic::AtomicU64,
    pub other: std::sync::atomic::AtomicU64,
}

/// Point-in-time copy of [`RpcCallCounters`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RpcCallCounts {
    pub eth_call: u64,
    pub receipts: u64,
    pub block_number: u64,
    pub other: u64,
}

impl RpcCallCounters {
    fn bump(&self, method: &str) {
        use std::sync::atomic::Ordering::Relaxed;
        match method {
            "eth_call" => self.eth_call.fetch_add(1, Relaxed),
            "eth_getTransactionReceipt" => self.receipts.fetch_add(1, Relaxed),
            "eth_blockNumber" => self.block_number.fetch_add(1, Relaxed),
            _ => self.other.fetch_add(1, Relaxed),
        };
    }

    pub fn snapshot(&self) -> RpcCallCounts {
        use std::sync::atomic::Ordering::Relaxed;
        RpcCallCounts {
            eth_call: self.eth_call.load(Relaxed),
            receipts: self.receipts.load(Relaxed),
            block_number: self.block_number.load(Relaxed),
            other: self.other.load(Relaxed),
        }
    }
}

/// Pre-warmed Persistent Connection Multi-Endpoint RPC & Direct Sequencer Broadcaster Pool
#[derive(Clone)]
pub struct BroadcasterPool {
    client: reqwest::Client,
    send_rpcs: Arc<[String]>,
    calls: Arc<RpcCallCounters>,
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
        Self { client, send_rpcs: send_rpcs.into(), calls: Arc::new(RpcCallCounters::default()) }
    }

    /// Read requests issued so far (one per logical call, not per endpoint raced).
    pub fn call_counts(&self) -> RpcCallCounts {
        self.calls.snapshot()
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
        self.broadcast_body(send_raw_tx_body(raw_tx), expected_hash).await
    }

    /// Fan out an already built `eth_sendRawTransaction` request body (see [`send_raw_tx_body`]).
    ///
    /// This is the zero-compute half of [`Self::broadcast_raw_tx`]: a pre-signed packet carries its
    /// body, so the hot path is only the socket writes.
    pub async fn broadcast_body(&self, body: bytes::Bytes, expected_hash: &str) -> Result<String, BroadcastError> {
        if self.send_rpcs.is_empty() {
            return Err(BroadcastError {
                detail: "no SEND_RPCS endpoints configured".to_string(),
                rejected_everywhere: true,
            });
        }

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
        self.calls.bump(method);
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
        self.calls.bump(method);
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
        self.eth_call_at(to, data, None).await
    }

    /// `eth_call` against the state *after* block `block` (`None` = `latest`).
    ///
    /// Pinning the block is what lets a reading be merged with locally replayed trades: the caller
    /// knows exactly which trades the result already contains. A node that has not imported the
    /// block yet answers with an error (callers retry).
    pub async fn eth_call_at(&self, to: Address, data: &[u8], block: Option<u64>) -> Result<Vec<u8>, String> {
        let tag = match block {
            Some(n) => serde_json::Value::String(format!("0x{:x}", n)),
            None => serde_json::Value::String("latest".to_string()),
        };
        let params = serde_json::json!([
            {
                "to": format!("{:?}", to),
                "data": alloy::hex::encode_prefixed(data)
            },
            tag
        ]);
        let res = self.call_rpc("eth_call", params).await?;
        let hex_str = res.as_str().ok_or_else(|| "eth_call result not a string".to_string())?;
        alloy::hex::decode(hex_str.trim_start_matches("0x")).map_err(|e| e.to_string())
    }

    /// `eth_blockNumber` of the first endpoint that answers.
    pub async fn block_number(&self) -> Result<u64, String> {
        let res = self.call_rpc("eth_blockNumber", serde_json::json!([])).await?;
        let hex_str = res.as_str().ok_or_else(|| "eth_blockNumber result not a string".to_string())?;
        u64::from_str_radix(hex_str.trim_start_matches("0x"), 16).map_err(|e| e.to_string())
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

/// Broadcast `tx`, signing it first unless `presigned` already holds its bytes; at most one retry,
/// either
/// - "gas too low": same nonce, escalated limit, signed again (the rejected tx can never execute,
///   so there is no double-spend risk), or
/// - rate limited: the identical bytes again after a short pause (no second signature).
///
/// With a pre-signed packet the first attempt does no cryptography at all: the stored JSON body goes
/// straight to the sockets.
async fn sign_and_broadcast(
    acc: &TreasuryAccount,
    mut tx: TxEip1559,
    broadcaster: &BroadcasterPool,
    plan: RetryPlan,
    presigned: Option<SignedBytes>,
) -> Result<String, BroadcastError> {
    let mut gas_retry_left = true;
    let mut rate_retry_left = plan.retry_rate_limited;
    let mut ready = presigned;
    loop {
        let signed = match ready.take() {
            Some(signed) => signed,
            // Never signed, so never sent.
            None => sign_sync(&acc.signer, tx.clone())
                .map_err(|e| BroadcastError { detail: format!("signing failed: {}", e), rejected_everywhere: true })?,
        };
        match broadcaster.broadcast_body(signed.body.clone(), &signed.hash_hex).await {
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
                ready = Some(signed);
            }
            other => return other,
        }
    }
}

/// What staging an entry's primary tx achieved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageOutcome {
    /// Master nonce leased and the primary signed ahead of time.
    Staged { nonce: u64 },
    /// A staged primary no longer matched the fresh quote/fees: signed again under the same nonce.
    Restaged { nonce: u64 },
    /// The staged primary is still valid against the fresh numbers; nothing to do.
    Unchanged,
    /// No batch was armed for this launch (or it was already dispatched / cancelled).
    NotArmed,
    /// The batch left `Staged` while this call was working; nothing was leased.
    Closed,
    /// Pre-signing is switched off.
    Disabled,
    /// The signal cannot be pre-signed (unresolved target, zero size, calldata not re-targetable).
    Refused(String),
}

/// The `TxSpec` of a satellite carrier: a zero-value call to its own EOA carrying the block-booking
/// buffer. Independent of any launch, which is why carriers can be pre-signed before the curve is known.
fn carrier_spec(ctx: &EntryCtx, acc: &TreasuryAccount) -> TxSpec {
    let carrier_size = if ctx.buffer_size > 0 { ctx.buffer_size } else { TARGET_CALLDATA_BUFFER_SIZE };
    let padding = CalldataPayloadBuilder::clamp_padding(0, carrier_size);
    let floor_gas = CalldataPayloadBuilder::calldata_floor_gas(&[], padding);
    TxSpec {
        chain_id: ctx.chain_id,
        // A zero-value call to the account itself (an EOA) runs no code, so it cannot revert.
        to: acc.address,
        value: U256::ZERO,
        // Zero-copy: a refcounted view of the static zero page, shared by every carrier.
        input: CalldataPayloadBuilder::zero_padding(padding),
        gas_limit: CalldataPayloadBuilder::carrier_gas_limit(padding, ctx.configured_gas_limit, ctx.l1_component_gas),
        max_fee_per_gas: ctx.max_fee_wei,
        max_priority_fee_per_gas: ctx.priority_fee_wei,
        floor_with_l1: floor_gas.saturating_add(CalldataPayloadBuilder::l1_margin(ctx.l1_component_gas)),
        retry_rate_limited: false,
        gas_floor_without_l1: CalldataPayloadBuilder::gas_needed_without_l1(&[], padding, payload::CARRIER_GAS_RESERVE),
        configured_gas_limit: ctx.configured_gas_limit,
        l1_component_gas: ctx.l1_component_gas,
        swap: Vec::new(),
    }
}

/// The `TxSpec` of the master tx that carries the real token purchase. Recipient, `ethIn` and the
/// minimum output follow our own size (a copied trade was quoted for theirs).
fn primary_spec(ctx: &EntryCtx, acc: &TreasuryAccount, signal: &TradeSignal, value_wei: U256, reference_value: U256) -> Result<TxSpec> {
    let swap = CalldataPayloadBuilder::adapt_buy_calldata(&signal.swap_calldata, acc.address, value_wei, reference_value)
        .context("calldata re-targeting failed")?;
    Ok(primary_spec_for(ctx, signal.pool_or_router, value_wei, swap))
}

/// The primary's `TxSpec` from its already re-targeted swap calldata (also used to rebuild a staged
/// primary for new padding / fees without the original signal).
fn primary_spec_for(ctx: &EntryCtx, to: Address, value_wei: U256, swap: Vec<u8>) -> TxSpec {
    // Padding never pushes the signed tx over the sequencer's size cap.
    let padding = CalldataPayloadBuilder::clamp_padding(swap.len(), ctx.buffer_size);
    let floor_gas = CalldataPayloadBuilder::calldata_floor_gas(&swap, padding);
    // One calloc'd buffer (zeroed pages, no memset) holding calldata + padding.
    let input = Bytes::from(CalldataPayloadBuilder::build_calibrated_payload(&swap, padding));
    TxSpec {
        chain_id: ctx.chain_id,
        to,
        value: value_wei,
        input,
        gas_limit: CalldataPayloadBuilder::gas_limit(&swap, padding, ctx.configured_gas_limit, ctx.l1_component_gas),
        max_fee_per_gas: ctx.max_fee_wei,
        max_priority_fee_per_gas: ctx.priority_fee_wei,
        floor_with_l1: floor_gas.saturating_add(CalldataPayloadBuilder::l1_margin(ctx.l1_component_gas)),
        retry_rate_limited: true,
        gas_floor_without_l1: CalldataPayloadBuilder::gas_needed_without_l1(&swap, padding, payload::EXECUTION_GAS_RESERVE),
        configured_gas_limit: ctx.configured_gas_limit,
        l1_component_gas: ctx.l1_component_gas,
        swap,
    }
}

/// Accounts taking part in an entry: the master alone in single mode or with the padding switch
/// off (carriers exist only to carry the padding), master + every carrier otherwise.
fn active_accounts(mode: ExecutionMode, carriers_enabled: bool, derived: usize) -> usize {
    match mode {
        ExecutionMode::MultiAccount if carriers_enabled => derived,
        _ => 1.min(derived),
    }
}

/// What [`Dispatcher::reshape_presigned`] changed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReshapeReport {
    /// Live batches looked at.
    pub batches: usize,
    /// Carrier slots dropped (their nonces handed back): fewer carriers, single mode, padding off.
    pub slots_released: usize,
    /// Carrier slots added (nonce leased, signed in the background): more carriers.
    pub slots_added: usize,
    /// Existing slots signed again under their nonce: new padding or fee parameters.
    pub slots_resigned: usize,
}

/// High-Frequency Calldata Buffer Formatting & Concurrent Dispatcher Engine
pub struct Dispatcher {
    config: AppConfig,
    wallet_manager: Arc<WalletManager>,
    fee_adjuster: Arc<DynamicFeeAdjuster>,
    broadcaster: Arc<BroadcasterPool>,
    report_tx: mpsc::Sender<DispatchReport>,
    policy: PresignPolicy,
    staging: Arc<StagingBook>,
    /// Entries between the intake check and the end of their dispatch (plus whatever the caller
    /// covers with an [`EntryGuard`], e.g. the position registration that follows).
    in_flight: Arc<AtomicUsize>,
}

/// Marks an entry as in flight while alive (see [`Dispatcher::entry_guard`]).
#[must_use]
pub struct EntryGuard(Arc<AtomicUsize>);

impl Drop for EntryGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
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
            policy: PresignPolicy::default(),
            staging: Arc::new(StagingBook::default()),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Count an entry as in flight until the guard is dropped. `dispatch_signal` holds one; the
    /// caller holds another across the dispatch *and* the position registration, so the circuit
    /// breaker and the wallet guard can see an entry that has left but is not registered yet.
    pub fn entry_guard(&self) -> EntryGuard {
        self.in_flight.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        EntryGuard(self.in_flight.clone())
    }

    /// Entries currently in flight (see [`Self::entry_guard`]).
    pub fn entries_in_flight(&self) -> usize {
        self.in_flight.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Replace the pre-signing policy (see [`PresignPolicy::from_env`]).
    pub fn with_presign_policy(mut self, policy: PresignPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn broadcaster(&self) -> Arc<BroadcasterPool> {
        self.broadcaster.clone()
    }

    pub fn presign_policy(&self) -> PresignPolicy {
        self.policy
    }

    /// Lifetime counters of the pre-signing pipeline.
    pub fn presign_snapshot(&self) -> PresignSnapshot {
        self.staging.stats_ref().snapshot()
    }

    /// Batches currently staged.
    pub fn staged_count(&self) -> usize {
        self.staging.live_count()
    }

    /// True while at least one batch is staged (lock-free; the per-block hooks use it to stay free).
    pub fn has_staged(&self) -> bool {
        self.staging.live_count() > 0
    }

    /// The live batch of a launch (inspection / tests).
    pub fn staged_batch(&self, launch_tx: B256) -> Option<Arc<StagedBatch>> {
        self.staging.live_batch(launch_tx)
    }

    // ── AOT pre-signing ─────────────────────────────────────────────────────

    /// Fees and gas sizing of this very moment, read exactly as a dispatch would read them.
    pub async fn entry_ctx(&self) -> EntryCtx {
        // The padding switch gates every byte of padding (the operator's "0 KB" mode).
        let carriers_enabled = self.config.carrier_padding_enabled();
        let buffer_size = if carriers_enabled { *self.config.buffer_size.read().await } else { 0 };
        let configured_gas_limit = *self.config.gas_limit.read().await;
        // One consistent pair, even while a fee preset is being applied.
        let (configured_prio_gwei, max_fee_cap_gwei) = self.config.fee_caps().await;
        // Adaptive fee (EMA), with the user-configured priority fee floor, capped by MAX_FEE_GWEI.
        // The priority fee never exceeds max fee (invalid tx otherwise).
        let (ema_priority_wei, max_fee_wei) = self.fee_adjuster.capped_fees(max_fee_cap_gwei);
        let configured_prio_wei = if configured_prio_gwei.is_finite() { (configured_prio_gwei.max(0.0) * 1e9) as u128 } else { 0 };
        EntryCtx {
            chain_id: self.config.chain_id,
            buffer_size,
            carriers_enabled,
            configured_gas_limit,
            l1_component_gas: self.fee_adjuster.l1_component_gas(),
            priority_fee_wei: ema_priority_wei.max(configured_prio_wei).min(max_fee_wei),
            max_fee_wei,
        }
    }

    /// Phase 1 of the AOT pipeline, called when an entry is armed (a Pola A/B match whose launch
    /// receipt or target block is still outstanding).
    ///
    /// Creates the batch of `launch_tx`, leases a nonce for every satellite carrier and signs the
    /// carriers on one blocking thread: a carrier does not depend on the launched token, so its bytes
    /// can be ready long before `T_LAUNCH` resolves. The batch is torn down by the latest of
    /// `hold_until` / the policy's `max_hold`.
    ///
    /// Returns false when pre-signing is off or the launch was already dispatched/cancelled.
    pub async fn arm_presign(&self, launch_tx: B256, hold_until: Instant) -> bool {
        if !self.policy.enabled {
            return false;
        }
        let now = Instant::now();
        let expires_at = (now + self.policy.max_hold).min(hold_until);
        if expires_at <= now {
            return false;
        }
        if !self.config.is_engine_running() {
            return false; // intake paused: nothing new is staged
        }
        let exec_mode = *self.config.execution_mode.read().await;
        let Ok(accounts) = self.wallet_manager.get_all_accounts_shared().await else { return false };
        let account_count = active_accounts(exec_mode, self.config.carrier_padding_enabled(), accounts.len());
        let batch = match self.staging.arm(Arc::new(StagedBatch::new(launch_tx, account_count, now, expires_at))) {
            Armed::New(batch) => batch,
            Armed::Existing(_) => return true,
            Armed::Refused => return false,
        };
        // Paused (circuit breaker) while we were awaiting above: its teardown may have run before
        // this batch was registered, so take it down ourselves rather than hold nonces for nothing.
        if !self.config.is_engine_running() {
            self.staging.teardown(launch_tx, Teardown::Cancelled);
            return false;
        }
        self.spawn_janitor(&batch);

        let ctx = self.entry_ctx().await;
        let tickets: Vec<_> = accounts
            .iter()
            .take(account_count)
            .skip(1)
            .filter_map(|acc| batch.lease_slot(acc.index, acc, carrier_spec(&ctx, acc)).map(|t| (acc.clone(), t)))
            .collect();
        debug!("[presign] armed {:?}: {} carrier packet(s), hold {:?}", launch_tx, tickets.len(), expires_at - now);
        self.sign_tickets(&batch, tickets).await;
        true
    }

    /// Phase 2: the launch resolved and the order still waits for its target block. Leases the master
    /// nonce and signs the primary buy now, so the target block only needs a socket write.
    ///
    /// Idempotent and also the *re-quote* entry point: when called again with a fresher quote the
    /// staged primary is kept if the Revert Guard moved within the policy's tolerance, and otherwise
    /// signed again under the same leased nonce (no allocator churn, never two live txs per nonce
    /// because the replaced bytes were never sent).
    pub async fn stage_primary(&self, signal: &TradeSignal) -> StageOutcome {
        if !self.policy.enabled {
            return StageOutcome::Disabled;
        }
        if !is_prysm_tag(signal.strategy_tag.as_deref()) {
            return StageOutcome::Refused("not a strategy entry".to_string());
        }
        if signal.target_token == signal.pool_or_router {
            return StageOutcome::Refused("token address unresolved".to_string());
        }
        if signal.entry_amount_wei.is_zero() {
            return StageOutcome::Refused("zero size".to_string());
        }
        let Some(batch) = self.staging.live_batch(signal.tx_hash) else { return StageOutcome::NotArmed };
        let Ok(accounts) = self.wallet_manager.get_all_accounts_shared().await else {
            return StageOutcome::Refused("accounts unavailable".to_string());
        };
        let Some(master) = accounts.first() else { return StageOutcome::Refused("no master account".to_string()) };
        let ctx = self.entry_ctx().await;
        let size = signal.entry_amount_wei;
        let spec = match primary_spec(&ctx, master, signal, size, size) {
            Ok(spec) => spec,
            Err(e) => return StageOutcome::Refused(format!("{:#}", e)),
        };

        let existing = batch.slot_spec(0);
        if let Some((staged, _)) = &existing
            && staged.valid_against(&spec, &self.policy).is_ok()
        {
            return StageOutcome::Unchanged;
        }
        let Some(ticket) = batch.lease_slot(0, master, spec) else { return StageOutcome::Closed };
        let nonce = ticket.nonce;
        self.sign_tickets(&batch, vec![(master.clone(), ticket)]).await;
        if existing.is_some() {
            PresignStats::bump(&self.staging.stats_ref().primary_restaged);
            StageOutcome::Restaged { nonce }
        } else {
            PresignStats::bump(&self.staging.stats_ref().primary_staged);
            StageOutcome::Staged { nonce }
        }
    }

    /// Proactive fee/gas re-validation, meant to run once per new block while anything is staged:
    /// a staged packet whose fee cap fell below the live value (beyond tolerance) or whose gas limit
    /// no longer covers the L1 poster component is signed again *now*, in the background, so the
    /// target block still finds a valid packet. Returns how many packets were re-signed.
    pub async fn revalidate_presigned(&self) -> usize {
        if !self.policy.enabled || self.staging.live_count() == 0 {
            return 0;
        }
        let ctx = self.entry_ctx().await;
        let Ok(accounts) = self.wallet_manager.get_all_accounts_shared().await else { return 0 };
        let mut resigned = 0;
        for batch in self.staging.live_batches() {
            let tickets: Vec<_> = batch
                .slot_specs()
                .into_iter()
                .filter(|(_, spec, _)| {
                    spec.fee_gas_check(ctx.max_fee_wei, ctx.priority_fee_wei, ctx.l1_component_gas, &self.policy).is_err()
                })
                .filter_map(|(idx, spec, _)| {
                    let acc = accounts.get(idx)?;
                    batch.lease_slot(idx, acc, spec.refreshed(&ctx)).map(|t| (acc.clone(), t))
                })
                .collect();
            resigned += tickets.len();
            self.sign_tickets(&batch, tickets).await;
        }
        if resigned > 0 {
            self.staging.stats_ref().proactive_resigns.fetch_add(resigned as u64, std::sync::atomic::Ordering::Relaxed);
            info!("[presign] re-signed {} staged packet(s) after a fee/gas change", resigned);
        }
        resigned
    }

    /// Bring every staged batch in line with runtime parameters the operator just changed (carrier
    /// count, single/multi mode, padding switch / size, fee preset), *before* the target block:
    ///
    /// * slots of accounts no longer in the layout are dropped and their nonces handed back;
    /// * carriers that joined the layout get a nonce leased and are signed in the background;
    /// * slots whose padding or fees no longer match the current parameters (beyond the fee
    ///   tolerance, in either direction: an operator lowering the cap wants a lower cap) are signed
    ///   again under the nonce they hold.
    ///
    /// Without this a change only reached the next batch, and the claiming dispatch had to sign
    /// the mismatching slots inline on the hot path.
    pub async fn reshape_presigned(&self) -> ReshapeReport {
        let mut report = ReshapeReport::default();
        if !self.policy.enabled || self.staging.live_count() == 0 {
            return report;
        }
        let exec_mode = *self.config.execution_mode.read().await;
        // May derive new carriers (PBKDF2, blocking pool): here, not in the next dispatch.
        let Ok(accounts) = self.wallet_manager.get_all_accounts_shared().await else { return report };
        let ctx = self.entry_ctx().await;
        let count = active_accounts(exec_mode, ctx.carriers_enabled, accounts.len());
        let tol = self.policy.fee_tolerance_bps as u128;
        let fee_off = |staged: u128, now: u128| staged.abs_diff(now) * 10_000 > now * tol;
        for batch in self.staging.live_batches() {
            report.batches += 1;
            report.slots_released += batch.resize(count);
            let mut tickets = Vec::new();
            for (idx, acc) in accounts.iter().enumerate().take(count) {
                let fresh = if idx == 0 {
                    // The primary is rebuilt from its own (already re-targeted) swap.
                    match batch.slot_spec(0) {
                        Some((spec, _)) => primary_spec_for(&ctx, spec.to, spec.value, spec.swap.clone()),
                        None => continue, // staged at resolution, not yet: nothing to reshape
                    }
                } else {
                    carrier_spec(&ctx, acc)
                };
                let needs = match batch.slot_spec(idx) {
                    None => {
                        report.slots_added += 1;
                        true
                    }
                    Some((staged, _)) => {
                        let stale = staged.valid_against(&fresh, &self.policy).is_err()
                            || fee_off(staged.max_fee_per_gas, fresh.max_fee_per_gas)
                            || fee_off(staged.max_priority_fee_per_gas, fresh.max_priority_fee_per_gas);
                        report.slots_resigned += usize::from(stale);
                        stale
                    }
                };
                if needs && let Some(ticket) = batch.lease_slot(idx, acc, fresh) {
                    tickets.push((acc.clone(), ticket));
                }
            }
            self.sign_tickets(&batch, tickets).await;
        }
        if report.slots_released + report.slots_added + report.slots_resigned > 0 {
            info!(
                "[presign] reshaped {} batch(es) for new runtime parameters: -{} slot(s), +{} slot(s), {} re-signed",
                report.batches, report.slots_released, report.slots_added, report.slots_resigned
            );
        }
        report
    }

    /// Tear down the staged batch of `launch_tx`: its order was rejected, cancelled or failed. Every
    /// leased nonce goes back to the allocator. False if nothing live was staged (or a dispatch had
    /// already claimed it: cancel and dispatch can never both win).
    pub fn cancel_presigned(&self, launch_tx: B256) -> bool {
        self.staging.teardown(launch_tx, Teardown::Cancelled)
    }

    /// Tear down every staged batch (engine switched off, shutdown).
    pub fn cancel_all_presigned(&self) -> usize {
        self.staging.teardown_all(Teardown::Cancelled)
    }

    /// Sign the given slots one after the other on a single blocking thread. Signing is CPU-bound and
    /// runs next to the feed reader: one thread keeps the interference bounded, and the whole batch
    /// is still ready within a few ms, far inside the receipt wait it hides behind.
    async fn sign_tickets(&self, batch: &Arc<StagedBatch>, tickets: Vec<(TreasuryAccount, SlotTicket)>) {
        if tickets.is_empty() {
            return;
        }
        let batch = batch.clone();
        let book = self.staging.clone();
        let worker = tokio::task::spawn_blocking(move || {
            for (acc, ticket) in tickets {
                match sign_sync(&acc.signer, ticket.spec.to_tx(ticket.nonce)) {
                    Ok(signed) => {
                        if batch.install_signed(&ticket, signed) {
                            PresignStats::bump(&book.stats_ref().packets_signed_ahead);
                        } else {
                            PresignStats::bump(&book.stats_ref().packets_discarded);
                            if batch.state() != BatchState::Staged {
                                // Claimed or torn down: the rest would be thrown away too.
                                break;
                            }
                        }
                    }
                    Err(e) => warn!("[presign] signing slot {} failed: {}", ticket.index, e),
                }
            }
        });
        if let Err(e) = worker.await {
            // Leases stay with the batch; dispatch signs those slots inline under the same nonces.
            warn!("[presign] signer thread died: {}", e);
        }
    }

    /// Per-batch deadline, off the hot path: tears the batch down at `expires_at` unless it was
    /// claimed or cancelled first.
    fn spawn_janitor(&self, batch: &Arc<StagedBatch>) {
        let book = self.staging.clone();
        let batch = batch.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(batch.expires_at())) => {
                    // The sleep elapsed by construction; pass the deadline itself so the test of
                    // "due" does not depend on which clock the runtime is using.
                    if book.expire_if_due(batch.key(), batch.expires_at()) {
                        warn!("[presign] batch {:?} expired unclaimed; nonces handed back", batch.key());
                    }
                }
                _ = batch.closed() => {}
            }
        });
    }

    /// Settle one account's claimed slot against the tx a dispatch would build right now.
    /// Returns the nonce to use and the bytes to send unchanged, if they are still valid.
    fn settle_slot(&self, acc: &TreasuryAccount, slot: Option<ClaimedSlot>, spec_now: &TxSpec) -> (u64, Option<SignedBytes>) {
        let stats = self.staging.stats_ref();
        let Some(slot) = slot else {
            PresignStats::bump(&stats.signed_inline_fresh);
            return (acc.allocate_nonce(), None);
        };
        // A lease of another address (mnemonic swapped since staging), or one the chain has already
        // consumed, is worthless: give it up and allocate normally.
        if slot.lease.account().address != acc.address {
            slot.lease.release();
            PresignStats::bump(&stats.signed_inline_fresh);
            return (acc.allocate_nonce(), None);
        }
        if slot.lease.is_overtaken() {
            PresignStats::bump(&stats.leases_overtaken);
            slot.lease.release();
            return (acc.allocate_nonce(), None);
        }
        let ClaimedSlot { lease, spec, signed } = slot;
        let nonce = lease.commit();
        match signed {
            Some(bytes) => match spec.valid_against(spec_now, &self.policy) {
                Ok(()) => {
                    PresignStats::bump(&stats.prebuilt_sent);
                    (nonce, Some(bytes))
                }
                Err(why) => {
                    debug!("[presign] [{}] staged packet unusable ({}); re-signing under nonce {}", acc.role, why, nonce);
                    PresignStats::bump(&stats.resigned_stale);
                    (nonce, None)
                }
            },
            None => {
                PresignStats::bump(&stats.signed_inline_leased);
                (nonce, None)
            }
        }
    }

    // ── Dispatch ────────────────────────────────────────────────────────────

    /// Dispatch transactions with In-Memory Nonce Caching, EMA Fee Calibration,
    /// and Pipelined Parallel Signing directly to the Sequencer.
    ///
    /// A strategy entry first claims its staged batch (see [`Self::arm_presign`]): pre-signed packets
    /// that are still valid are broadcast byte for byte, everything else is signed here exactly as
    /// before, under the nonce the batch already holds.
    ///
    /// Returns `Err` unless at least one transaction was accepted by an endpoint, so callers
    /// never register a position for a buy that was never broadcast.
    pub async fn dispatch_signal(&self, signal: &TradeSignal) -> Result<DispatchReport> {
        let _in_flight = self.entry_guard();
        let is_strategy = is_prysm_tag(signal.strategy_tag.as_deref());
        let result = self.dispatch_inner(signal, is_strategy).await;
        if is_strategy && result.is_err() {
            // A refusal before the claim must not leave leased nonces waiting for the TTL
            // (no-op when the batch was claimed).
            self.staging.teardown(signal.tx_hash, Teardown::Cancelled);
        }
        result
    }

    async fn dispatch_inner(&self, signal: &TradeSignal, is_strategy: bool) -> Result<DispatchReport> {
        let start_time = Instant::now();

        // A strategy entry must name a resolved token. Replaying a launch tx's calldata into
        // the launchpad would create a duplicate token, not buy the target.
        if is_strategy && signal.target_token == signal.pool_or_router {
            anyhow::bail!(
                "Refusing {:?} entry: token address unresolved (target == router {:?}); launch calldata replay disabled",
                signal.strategy_tag, signal.pool_or_router
            );
        }

        // Paused intake (operator /pause or panic): no new entry leaves, whatever was already queued
        // (a copied signal in the channel, a Prysm order released by its receipt).
        anyhow::ensure!(self.config.is_engine_running(), "engine intake paused: entry refused");

        let exec_mode = *self.config.execution_mode.read().await;
        let ctx = self.entry_ctx().await;

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

        let priority_fee_gwei = ctx.priority_fee_wei as f64 / 1e9;

        let accounts = self.wallet_manager.get_all_accounts_shared().await?;
        let account_count = active_accounts(exec_mode, ctx.carriers_enabled, accounts.len());

        if ctx.l1_component_gas == 0 {
            warn!(
                "L1 poster-gas estimate unavailable (NodeInterface not answering?): gas limits use the {} fallback",
                crate::payload::L1_COMPONENT_FALLBACK_GAS
            );
        }

        // Intake again, after the awaits above: a circuit breaker that fired meanwhile stops the
        // entry here. From this point to the broadcast there is no await left.
        anyhow::ensure!(self.config.is_engine_running(), "engine intake paused: entry refused");

        // Exactly one dispatch can own a launch's staged batch (compare-exchange); later or racing
        // cancels and expiries find it gone.
        let (claimed_batch, mut claimed): (Option<Arc<StagedBatch>>, Vec<Option<ClaimedSlot>>) = if is_strategy && self.policy.enabled {
            match self.staging.claim(signal.tx_hash) {
                Some((batch, claim)) => (Some(batch), claim.slots),
                None => (None, Vec::new()),
            }
        } else {
            (None, Vec::new())
        };

        let mut handles = Vec::with_capacity(account_count);
        let mut nonces = Vec::with_capacity(account_count);
        // Reported figures describe the master tx (the one that carries the position), not the last carrier.
        let mut calldata_len = 0;
        let mut gas_limit_used = ctx.configured_gas_limit;
        for acc in accounts.iter().take(account_count).cloned() {
            let is_master = acc.index == 0;
            let spec = if is_master {
                primary_spec(&ctx, &acc, signal, value_wei, reference_value)?
            } else {
                // Satellite child accounts act purely as carrier dispatchers carrying the 95 KB block-booking buffer:
                // They do NOT buy tokens (value = 0 ETH), preserving 100% of token capital.
                // Their only duty is transmitting the calibrated 94.5 KB buffer to reserve sequencer block capacity!
                carrier_spec(&ctx, &acc)
            };
            if is_master {
                calldata_len = spec.input.len();
                gas_limit_used = spec.gas_limit;
            }

            let plan = RetryPlan { floor_with_l1: spec.floor_with_l1, retry_rate_limited: spec.retry_rate_limited };
            let broadcaster = self.broadcaster.clone();
            let wallet_manager = self.wallet_manager.clone();

            // Atomic in-memory nonce allocation (< 10 ns), or the nonce the staged slot already holds.
            // A parked nonce is reused first.
            let slot = claimed.get_mut(acc.index).and_then(Option::take);
            let (nonce, presigned) = self.settle_slot(&acc, slot, &spec);
            nonces.push(nonce);

            // Pipelined parallel dispatch worker. It settles its own nonce on failure, so the
            // rollbacks of several failed accounts run in parallel (not one 2 s lookup after the other),
            // never delay the report, and still happen if the caller of `dispatch_signal` is dropped.
            handles.push(tokio::spawn(async move {
                let tx = spec.to_tx(nonce);
                let result = sign_and_broadcast(&acc, tx, &broadcaster, plan, presigned).await;
                if let Err(err) = &result {
                    wallet_manager.release_nonce_state(&acc, nonce, err.nonce_state()).await;
                }
                (acc, nonce, result)
            }));
        }
        // Slots of accounts that are not part of this dispatch (mode or count changed since staging).
        let unused = claimed.iter().flatten().count();
        if unused > 0 {
            self.staging.stats_ref().leases_released_unused.fetch_add(unused as u64, std::sync::atomic::Ordering::Relaxed);
        }
        drop(claimed); // dropping a lease hands its nonce back
        if let Some(batch) = &claimed_batch {
            // Every worker is spawned: Claimed -> Dispatched (the batch has left the book already).
            batch.mark_dispatched();
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
