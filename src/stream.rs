use crate::config::AppConfig;
use crate::payload::DynamicFeeAdjuster;
use crate::types::TradeSignal;
use alloy::consensus::transaction::SignerRecoverable;
use alloy::consensus::{Transaction as TxTrait, TxEnvelope};
use alloy::eips::eip2718::Decodable2718;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::BlockTransactions;
use anyhow::{Context, Result};
use base64::Engine;
use futures_util::StreamExt;
use serde::Deserialize;
use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{self, Message};
use tracing::{error, info, trace, warn};

/// Nitro's broadcaster pings every ~5s; Nitro's own feed client gives up after 20s of silence.
const FEED_IDLE_TIMEOUT: Duration = Duration::from_secs(20);
const MIN_RECONNECT_BACKOFF: Duration = Duration::from_millis(500);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);
/// A feed session that stayed up this long counts as healthy and resets the backoff.
const HEALTHY_SESSION: Duration = Duration::from_secs(60);
/// HTTP fallback never replays more than this many blocks after a stall (stale signals).
const MAX_HTTP_CATCHUP_BLOCKS: u64 = 32;

/// `arbostypes.L1MessageType_L2Message`: only these feed messages carry an L2 message payload.
pub const L1_MESSAGE_TYPE_L2_MESSAGE: u8 = 3;
const L2_KIND_BATCH: u8 = 3;
const L2_KIND_SIGNED_TX: u8 = 4;
/// Nitro rejects L2 batches nested deeper than this (`parseL2Message`, depth >= 16).
pub const MAX_L2_BATCH_DEPTH: usize = 16;

/// Pre-computed 4-byte DEX Swap Method Signatures as u32 integers
/// Enables single CPU clock-cycle bitwise matching (Zero Heap Allocation)
pub mod fast_selectors {
    /// Pons bonding curve buy(uint256,uint256,address) -> 0x59a87bc1
    pub const PONS_BUY: u32 = 0x59a87bc1;
    /// swapExactETHForTokens(uint256,address[],address,uint256) -> 0x7ff36ab5
    pub const SWAP_EXACT_ETH: u32 = 0x7ff36ab5;
    /// swapExactETHForTokensSupportingFeeOnTransferTokens -> 0xb6f9de95
    pub const SWAP_EXACT_ETH_FEE: u32 = 0xb6f9de95;
    /// swapExactTokensForTokens -> 0x38ed1739
    pub const SWAP_EXACT_TOKENS: u32 = 0x38ed1739;
    /// exactInputSingle((address,address,uint24,address,uint256,uint256,uint160)) -> 0x414bf382
    pub const EXACT_INPUT_SINGLE: u32 = 0x414bf382;
    /// multicall(bytes[]) -> 0xac9650d8
    pub const MULTICALL: u32 = 0xac9650d8;
    /// execute(bytes,bytes[],uint256) -> 0x3593564c (Universal Router)
    pub const UNIVERSAL_ROUTER_EXECUTE: u32 = 0x3593564c;

    /// Instantaneous O(1) integer comparison without memory allocation
    #[inline(always)]
    pub fn is_swap_selector(selector_bytes: &[u8]) -> bool {
        if selector_bytes.len() < 4 {
            return false;
        }
        let tag = u32::from_be_bytes([
            selector_bytes[0],
            selector_bytes[1],
            selector_bytes[2],
            selector_bytes[3],
        ]);
        matches!(
            tag,
            PONS_BUY
                | SWAP_EXACT_ETH
                | SWAP_EXACT_ETH_FEE
                | SWAP_EXACT_TOKENS
                | EXACT_INPUT_SINGLE
                | MULTICALL
                | UNIVERSAL_ROUTER_EXECUTE
        )
    }

    /// Swaps that can be copied with our own `msg.value` and recipient:
    /// Pons curve `buy(uint256,uint256,address)` or V2 `swapExactETHForTokens*(...)`.
    #[inline(always)]
    pub fn is_copyable_eth_swap(calldata: &[u8]) -> bool {
        match calldata.first_chunk::<4>() {
            Some(sel) => matches!(u32::from_be_bytes(*sel), PONS_BUY | SWAP_EXACT_ETH | SWAP_EXACT_ETH_FEE),
            None => false,
        }
    }
}

/// Zero-Copy Raw Byte-Slice Transaction Parser
///
/// Bypasses full JSON struct deserialization by scanning raw byte offsets
/// directly for `from`, `to`, `data`/`input`, `value`, and `hash`.
pub struct ZeroCopyTxParser;

impl ZeroCopyTxParser {
    /// Fast hex ASCII to byte conversion
    #[inline(always)]
    fn hex_val(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }

    /// Decode 40 hex ASCII characters directly into a 20-byte Address
    #[inline(always)]
    pub fn parse_address_from_hex(hex_bytes: &[u8]) -> Option<Address> {
        if hex_bytes.len() < 40 {
            return None;
        }
        let mut out = [0u8; 20];
        for i in 0..20 {
            let h = Self::hex_val(hex_bytes[i * 2])?;
            let l = Self::hex_val(hex_bytes[i * 2 + 1])?;
            out[i] = (h << 4) | l;
        }
        Some(Address::from(out))
    }

    /// Extract key byte offsets from raw JSON bytes without heap allocation
    /// Scans for `"key":"0x..."` patterns in the payload slice
    #[inline]
    pub fn find_field_hex_slice<'a>(json: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
        let mut i = 0;
        let key_len = key.len();
        let json_len = json.len();

        while i + key_len + 4 < json_len {
            if &json[i..i + key_len] == key {
                let mut start = i + key_len;
                // Skip quotes and colons
                while start < json_len && (json[start] == b'"' || json[start] == b':' || json[start] == b' ') {
                    start += 1;
                }
                // Check for "0x"
                if start + 2 < json_len && json[start] == b'0' && (json[start + 1] == b'x' || json[start + 1] == b'X') {
                    start += 2;
                    let mut end = start;
                    while end < json_len && json[end] != b'"' && json[end] != b',' && json[end] != b'}' {
                        end += 1;
                    }
                    return Some(&json[start..end]);
                }
            }
            i += 1;
        }
        None
    }

    /// Zero-copy extraction of target token address from calldata payload
    #[inline(always)]
    pub fn extract_target_token_fast(calldata: &[u8]) -> Option<Address> {
        let len = calldata.len();
        if len >= 32 {
            let last_word = &calldata[len - 32..len];
            let addr_bytes: [u8; 20] = last_word[12..32].try_into().ok()?;
            return Some(Address::from(addr_bytes));
        }
        None
    }
}

/// L2 message unpacker for Arbitrum Nitro sequencer feeds
/// Kind 4 = Single signed tx; Kind 3 = Batch of (8-byte big-endian length + message)
///
/// The bytes are untrusted: delayed-inbox messages (`sendL2Message` on L1) are user supplied
/// and reach the feed verbatim. Lengths are bounds-checked without arithmetic overflow, and a
/// batch nested deeper than Nitro allows invalidates the whole message, exactly as Nitro does,
/// so nothing is acted on that the chain would not execute.
pub fn extract_l2_txs<'a>(bytes: &'a [u8], out: &mut Vec<&'a [u8]>) {
    let initial_len = out.len();
    if !extract_l2_txs_at_depth(bytes, out, 0) {
        out.truncate(initial_len);
    }
}

/// Returns `false` when the message is invalid as a whole.
fn extract_l2_txs_at_depth<'a>(bytes: &'a [u8], out: &mut Vec<&'a [u8]>, depth: usize) -> bool {
    match bytes.split_first() {
        Some((&L2_KIND_SIGNED_TX, tx)) => out.push(tx),
        Some((&L2_KIND_BATCH, mut rest)) => {
            if depth >= MAX_L2_BATCH_DEPTH {
                return false;
            }
            while let Some((len_bytes, tail)) = rest.split_first_chunk::<8>() {
                // Truncated or oversized segment: Nitro treats it as end-of-batch.
                let Ok(n) = usize::try_from(u64::from_be_bytes(*len_bytes)) else { break };
                let Some((segment, next)) = tail.split_at_checked(n) else { break };
                if !extract_l2_txs_at_depth(segment, out, depth + 1) {
                    return false;
                }
                rest = next;
            }
        }
        _ => {}
    }
    true
}

/// Borrowed view of one broadcaster frame: `{"version":1,"messages":[...]}`.
/// Deserializing into these structs avoids building a `serde_json::Value` tree per frame;
/// `l2Msg` (base64, never JSON-escaped) is borrowed straight from the frame buffer.
#[derive(Deserialize)]
struct FeedFrame<'a> {
    #[serde(borrow, default)]
    messages: Vec<FeedMessage<'a>>,
}

#[derive(Deserialize)]
struct FeedMessage<'a> {
    #[serde(rename = "sequenceNumber")]
    sequence_number: u64,
    #[serde(borrow)]
    message: FeedMessageWithMetadata<'a>,
}

#[derive(Deserialize)]
struct FeedMessageWithMetadata<'a> {
    #[serde(borrow)]
    message: L1IncomingMessage<'a>,
}

#[derive(Deserialize)]
struct L1IncomingMessage<'a> {
    #[serde(default)]
    header: Option<L1Header>,
    #[serde(rename = "l2Msg", borrow, default)]
    l2_msg: Cow<'a, str>,
}

#[derive(Deserialize)]
struct L1Header {
    kind: u8,
}

/// Real-Time Sequencer Feed & Signal Replication Stream Listener
pub struct StreamListener {
    config: AppConfig,
    fee_adjuster: Arc<DynamicFeeAdjuster>,
    prysm_engine: Arc<crate::models::PrysmEngine>,
    signal_tx: mpsc::Sender<TradeSignal>,
    /// Highest feed sequence number processed + 1 (0 = none); drops duplicates after reconnects.
    next_feed_seq: AtomicU64,
    fee_refresher_started: AtomicBool,
}

impl StreamListener {
    pub fn new(
        config: AppConfig,
        fee_adjuster: Arc<DynamicFeeAdjuster>,
        prysm_engine: Arc<crate::models::PrysmEngine>,
        signal_tx: mpsc::Sender<TradeSignal>,
    ) -> Self {
        Self {
            config,
            fee_adjuster,
            prysm_engine,
            signal_tx,
            next_feed_seq: AtomicU64::new(0),
            fee_refresher_started: AtomicBool::new(false),
        }
    }

    /// Background fee refresher to poll gas prices from RPC without adding latency to the sequencer feed
    fn spawn_fee_refresher(&self) {
        // `run` may be restarted by a supervisor; never stack a second refresher.
        if self.fee_refresher_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let fee_adjuster = self.fee_adjuster.clone();
        let rpc_http = self.config.rpc_http_url.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_millis(1500))
                .build()
                .unwrap_or_default();
            let payload = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_gasPrice",
                "params": []
            });

            let mut tick = tokio::time::interval(Duration::from_secs(2));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if let Ok(resp) = client.post(&rpc_http).json(&payload).send().await {
                    if let Ok(json) = resp.json::<serde_json::Value>().await {
                        if let Some(res_str) = json.get("result").and_then(|r| r.as_str()) {
                            let hex_clean = res_str.trim_start_matches("0x");
                            if let Ok(val) = u128::from_str_radix(hex_clean, 16) {
                                let gwei = val as f64 / 1e9;
                                fee_adjuster.update_block_fees(gwei, 2.5);
                            }
                        }
                    }
                }
            }
        });
    }

    /// Run the stream listener loop
    ///
    /// The WebSocket feed is always the primary source. While it is down, the HTTP poller
    /// bridges the reconnect backoff window only, then the feed is retried, so one failed
    /// handshake (HTTP 429, Cloudflare reset, DNS blip) never pins the engine to polling.
    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting High-Frequency Sequencer Stream Listener for Chain ID: {} (Feed URL: {})",
            self.config.chain_id, self.config.feed_url
        );

        self.spawn_fee_refresher();

        let feed_url = self.config.feed_url.clone();
        let http_url = self.config.rpc_http_url.clone();

        let mut backoff = MIN_RECONNECT_BACKOFF;

        loop {
            if !self.config.is_engine_running() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            let session_start = Instant::now();
            let outcome = self.listen_sequencer_feed(&feed_url).await;
            if session_start.elapsed() >= HEALTHY_SESSION {
                backoff = MIN_RECONNECT_BACKOFF;
            }

            let wait = match &outcome {
                Ok(()) => backoff,
                Err(e) => retry_after_hint(e).unwrap_or(backoff).min(Duration::from_secs(300)),
            };
            let wait = jitter(wait);
            match &outcome {
                Ok(()) => warn!("Sequencer feed closed. Reconnecting in {:?} (HTTP poller bridging)...", wait),
                Err(e) => warn!("Sequencer feed error: {:#}. Reconnecting in {:?} (HTTP poller bridging)...", e, wait),
            }

            let bridge_start = Instant::now();
            match tokio::time::timeout(wait, self.listen_http_poll(&http_url)).await {
                Err(_) => {} // backoff window elapsed: retry the feed
                Ok(Err(http_err)) => {
                    error!("HTTP Sequencer poller error: {:#}", http_err);
                    tokio::time::sleep(wait.saturating_sub(bridge_start.elapsed())).await;
                }
                Ok(Ok(())) => {}
            }
            backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
        }
    }

    /// Real-time Arbitrum Nitro Sequencer Feed connection
    ///
    /// Returns `Ok(())` on an orderly close and `Err` on handshake failure, transport error
    /// or a silent (half-open) connection.
    async fn listen_sequencer_feed(&self, feed_url: &str) -> Result<()> {
        let (mut ws_stream, _) = connect_async(feed_url)
            .await
            .context("Failed to connect to Arbitrum Sequencer Feed WebSocket")?;

        info!("⚡ Successfully connected to Real-Time Sequencer Feed ({})", feed_url);
        let mut b64_buf = Vec::with_capacity(64 * 1024);

        loop {
            // Without a deadline a half-open TCP connection would block here forever while
            // the engine looks healthy. Server pings count as traffic.
            let msg = match tokio::time::timeout(FEED_IDLE_TIMEOUT, ws_stream.next()).await {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => return Err(e).context("Sequencer feed read error"),
                Ok(None) => return Ok(()),
                Err(_) => anyhow::bail!("Sequencer feed silent for {:?}", FEED_IDLE_TIMEOUT),
            };

            match msg {
                Message::Text(text) => {
                    if self.config.is_engine_running() {
                        self.handle_feed_frame(text.as_bytes(), &mut b64_buf).await;
                    }
                }
                Message::Binary(bin) => {
                    if self.config.is_engine_running() {
                        self.handle_feed_frame(&bin, &mut b64_buf).await;
                    }
                }
                Message::Close(frame) => {
                    info!("Sequencer Feed received close frame from server: {:?}", frame);
                    return Ok(());
                }
                // Ping: tungstenite queues the Pong and flushes it on the next read.
                _ => {}
            }
        }
    }

    /// Decode one broadcaster frame and process each contained message in order.
    async fn handle_feed_frame(&self, frame: &[u8], b64_buf: &mut Vec<u8>) {
        let parsed: FeedFrame<'_> = match serde_json::from_slice(frame) {
            Ok(f) => f,
            Err(e) => {
                trace!("Ignoring unparseable feed frame: {}", e);
                return;
            }
        };

        for m in parsed.messages {
            let seq = m.sequence_number;
            if self.next_feed_seq.fetch_max(seq.saturating_add(1), Ordering::Relaxed) > seq {
                continue; // duplicate delivered after a reconnect
            }
            // Every feed message produces exactly one L2 block.
            let block_number = self.config.feed_genesis_block.saturating_add(seq);
            let incoming = m.message.message;
            let carries_l2 = incoming.header.as_ref().is_none_or(|h| h.kind == L1_MESSAGE_TYPE_L2_MESSAGE);

            b64_buf.clear();
            let l2_bytes: &[u8] = if carries_l2
                && base64::engine::general_purpose::STANDARD
                    .decode_vec(incoming.l2_msg.as_bytes(), b64_buf)
                    .is_ok()
            {
                b64_buf
            } else {
                &[]
            };
            self.process_l2_message(block_number, l2_bytes).await;
        }
    }

    /// Process raw L2 sequencer bytes: release matured block queues & inspect transactions
    async fn process_l2_message(&self, block_number: u64, l2_bytes: &[u8]) {
        let start = Instant::now();

        // 1. Notify Prysm Series Strategy of block progression (releases matured +3 block orders)
        self.prysm_engine.process_new_block(block_number).await;

        if l2_bytes.is_empty() {
            return;
        }

        // 2. Extract signed transactions from Arbitrum Nitro L2 payload
        let mut tx_slices = Vec::with_capacity(16);
        extract_l2_txs(l2_bytes, &mut tx_slices);

        let prysm_enabled = self.config.is_prysm_enabled();
        for tx_raw in tx_slices {
            // `_exact`: trailing bytes make Nitro reject the tx, so we must not act on it either.
            let Ok(envelope) = TxEnvelope::decode_2718_exact(tx_raw) else { continue };
            let input = envelope.input();

            // ecrecover costs tens of µs; skip it for txs no pipeline can act on.
            if !prysm_enabled && !fast_selectors::is_copyable_eth_swap(input) {
                continue;
            }
            let Ok(from) = envelope.recover_signer() else { continue };
            let to = envelope.to();
            let value = envelope.value();
            let tx_hash = *envelope.tx_hash();

            // Pipeline 1: Watchlist Target Wallets
            self.inspect_transaction_fast(from, to, value, input, tx_hash, block_number, start)
                .await;

            // Pipeline 2: Autonomous Prysm Pattern Evaluator
            self.prysm_engine
                .inspect_transaction(from, to, value, input, tx_hash, block_number, start)
                .await;
        }
    }

    /// HTTP fallback polling in case WS feed is not reachable or during dev
    async fn listen_http_poll(&self, http_url: &str) -> Result<()> {
        let provider = ProviderBuilder::new()
            .connect(http_url)
            .await
            .context("Failed to connect to HTTP RPC")?;

        // Never fall back to block 0: that would replay the entire chain as fresh signals.
        let mut last_block = provider
            .get_block_number()
            .await
            .context("HTTP poller: initial eth_blockNumber failed")?;
        info!("Sequencer HTTP Poller active from block #{}", last_block);

        loop {
            if !self.config.is_engine_running() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            match provider.get_block_number().await {
                Ok(current_block) if current_block > last_block => {
                    let from_block = (last_block + 1)
                        .max(current_block.saturating_sub(MAX_HTTP_CATCHUP_BLOCKS - 1));
                    for block_num in from_block..=current_block {
                        // `.full()`: the default request returns only tx hashes.
                        let block = match provider.get_block_by_number(block_num.into()).full().await {
                            Ok(Some(b)) => b,
                            // Retry this block on the next tick instead of silently skipping it.
                            Ok(None) | Err(_) => break,
                        };
                        let start = Instant::now();
                        self.prysm_engine.process_new_block(block_num).await;

                        if let BlockTransactions::Full(txs) = block.transactions {
                            for tx in txs {
                                let from = tx.inner.signer();
                                let to = tx.inner.to();
                                let value = tx.inner.value();
                                let input = tx.inner.input();
                                let tx_hash = *tx.inner.tx_hash();

                                self.inspect_transaction_fast(from, to, value, input, tx_hash, block_num, start)
                                    .await;
                                self.prysm_engine
                                    .inspect_transaction(from, to, value, input, tx_hash, block_num, start)
                                    .await;
                            }
                        }
                        last_block = block_num;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    trace!("HTTP block poll tick: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Microsecond-optimized inspection of incoming transaction against watchlist
    ///
    /// Utilizes O(1) HashSet lookup + 1-cycle integer bitwise selector matching.
    /// Internal compute latency: < 5 microseconds.
    #[inline]
    pub async fn inspect_transaction_fast(
        &self,
        from: Address,
        to: Option<Address>,
        value: U256,
        input: &[u8],
        tx_hash: B256,
        block_number: u64,
        start_time: Instant,
    ) {
        // Step 1: Only swaps we can replay for our own account. Plain ETH transfers from a
        // watched wallet (CEX deposit, wallet shuffle) must never be "copied".
        if !fast_selectors::is_copyable_eth_swap(input) {
            return;
        }

        let to_address = match to {
            Some(addr) => addr,
            None => return, // Contract creation, not a swap
        };

        // Step 2: Fast watchlist lookup (O(1) in HashSet)
        if !self.config.is_in_watchlist(&from).await {
            return;
        }

        let elapsed = start_time.elapsed().as_micros();
        info!(
            "⚡ ZERO-COPY ON-CHAIN SIGNAL DETECTED in {} µs! Account: {:?} -> Router: {:?}, Value: {} wei",
            elapsed, from, to_address, value
        );

        // Step 3: Zero-copy parameter extraction (Pons curve address or last element of V2 `path`)
        let is_pons = input
            .first_chunk::<4>()
            .map(|sel| u32::from_be_bytes(*sel) == fast_selectors::PONS_BUY)
            .unwrap_or(false);

        let target_token = if is_pons {
            to_address
        } else {
            ZeroCopyTxParser::extract_target_token_fast(input).unwrap_or(to_address)
        };

        let signal = TradeSignal {
            tx_hash,
            monitored_account: from,
            target_token,
            pool_or_router: to_address,
            entry_amount_wei: value,
            swap_calldata: input.to_vec(),
            detected_at: start_time,
            block_number,
            strategy_tag: None,
        };

        // Never block the feed reader on a full queue: a backlog of 1000 signals is stale anyway.
        if let Err(e) = self.signal_tx.try_send(signal) {
            error!("Dropping trade signal (queue unavailable): {}", e);
        }
    }
}

/// Server-requested delay from an HTTP 429/503 handshake response (`Retry-After` seconds).
fn retry_after_hint(err: &anyhow::Error) -> Option<Duration> {
    let tungstenite::Error::Http(resp) = err.downcast_ref::<tungstenite::Error>()? else {
        return None;
    };
    let status = resp.status().as_u16();
    if status != 429 && status != 503 {
        return None;
    }
    let secs = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(10);
    Some(Duration::from_secs(secs.max(1)))
}

/// ±20% jitter so many clients do not reconnect in lockstep after a feed restart.
fn jitter(d: Duration) -> Duration {
    d.mul_f64(0.8 + rand::random::<f64>() * 0.4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_frame_decodes_with_borrowed_l2_msg() {
        let frame = br#"{"version":1,"messages":[{"sequenceNumber":42,"message":{"message":{"header":{"kind":3,"sender":"0xa4b000000000000000000073657175656e636572","blockNumber":1,"timestamp":2,"requestId":null,"baseFeeL1":null},"l2Msg":"BAE="},"delayedMessagesRead":7},"signature":null}]}"#;
        let parsed: FeedFrame<'_> = serde_json::from_slice(frame).unwrap();
        let m = &parsed.messages[0];
        assert_eq!(m.sequence_number, 42);
        assert_eq!(m.message.message.header.as_ref().map(|h| h.kind), Some(L1_MESSAGE_TYPE_L2_MESSAGE));
        assert!(matches!(m.message.message.l2_msg, Cow::Borrowed("BAE=")));

        // Confirmation-only frames carry no messages.
        let parsed: FeedFrame<'_> = serde_json::from_slice(br#"{"version":1,"confirmedSequenceNumberMessage":{"sequenceNumber":41}}"#).unwrap();
        assert!(parsed.messages.is_empty());
    }
}
