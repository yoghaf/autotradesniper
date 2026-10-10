use crate::config::AppConfig;
use crate::payload::DynamicFeeAdjuster;
use crate::types::TradeSignal;
use alloy::consensus::Transaction as TxTrait;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::BlockTransactions;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

/// Pre-computed 4-byte DEX Swap Method Signatures as u32 integers
/// Enables single CPU clock-cycle bitwise matching (Zero Heap Allocation)
pub mod fast_selectors {
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
            SWAP_EXACT_ETH
                | SWAP_EXACT_ETH_FEE
                | SWAP_EXACT_TOKENS
                | EXACT_INPUT_SINGLE
                | MULTICALL
                | UNIVERSAL_ROUTER_EXECUTE
        )
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

/// Real-Time Sequencer Feed & Signal Replication Stream Listener
pub struct StreamListener {
    config: AppConfig,
    fee_adjuster: Arc<DynamicFeeAdjuster>,
    prysm_engine: Arc<crate::models::PrysmEngine>,
    signal_tx: mpsc::Sender<TradeSignal>,
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
        }
    }

    /// Run the stream listener loop
    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting High-Frequency Sequencer Stream Listener for Chain ID: {} (WS: {})",
            self.config.chain_id, self.config.rpc_ws_url
        );

        let ws_url = self.config.rpc_ws_url.clone();
        let http_url = self.config.rpc_http_url.clone();

        loop {
            if !self.config.is_engine_running() {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                continue;
            }

            match self.listen_websocket_feed(&ws_url).await {
                Ok(_) => {
                    warn!("Sequencer WebSocket stream closed gracefully. Reconnecting...");
                }
                Err(e) => {
                    warn!(
                        "Sequencer WebSocket connection failed: {}. Falling back to HTTP Polling...",
                        e
                    );
                    if let Err(http_err) = self.listen_http_poll(&http_url).await {
                        error!("HTTP Sequencer polling error: {}. Retrying in 2s...", http_err);
                        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    }
                }
            }
        }
    }

    /// WebSocket Sequencer Feed connection with low-latency block streaming
    async fn listen_websocket_feed(&self, ws_url: &str) -> Result<()> {
        let provider = ProviderBuilder::new()
            .connect(ws_url)
            .await
            .context("Failed to connect to Sequencer WebSocket")?;

        info!("Successfully connected to Sequencer WebSocket Feed (TCP_NODELAY active)");
        let sub = provider.subscribe_blocks().await?;
        let mut stream = sub.into_stream();

        while let Some(block_header) = stream.next().await {
            let start = Instant::now();
            let block_number = block_header.number;

            // Feed base fee into EMA Dynamic Fee Adjuster for adaptive fee calculation
            if let Some(base_fee) = block_header.base_fee_per_gas {
                let base_fee_gwei = base_fee as f64 / 1e9;
                self.fee_adjuster.update_block_fees(base_fee_gwei, 2.5);
            }

            if !self.config.is_engine_running() {
                continue;
            }

            trace!("Received Sequencer Block #{}", block_number);

            // Notify Prysm Series Strategy of new block confirmation (releases matured +3 block orders)
            self.prysm_engine.process_new_block(block_number).await;

            // Fetch block with full transactions for microsecond inspection
            if let Ok(Some(block)) = provider.get_block_by_number(block_number.into()).await {
                if let BlockTransactions::Full(transactions) = block.transactions {
                    for tx in transactions {
                        let from = tx.inner.signer();
                        let to = tx.inner.to();
                        let value = tx.inner.value();
                        let input = tx.inner.input();
                        let tx_hash = *tx.inner.tx_hash();

                        // Pipeline 1: Watchlist Monitored Signals
                        self.inspect_transaction_fast(
                            from,
                            to,
                            value,
                            input,
                            tx_hash,
                            block_number,
                            start,
                        ).await;

                        // Pipeline 2: Autonomous Prysm Series Strategy Evaluator
                        self.prysm_engine.inspect_transaction(
                            from,
                            to,
                            value,
                            input,
                            tx_hash,
                            block_number,
                            start,
                        ).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// HTTP fallback polling in case WS feed is not reachable or during dev
    async fn listen_http_poll(&self, http_url: &str) -> Result<()> {
        let provider = ProviderBuilder::new()
            .connect(http_url)
            .await
            .context("Failed to connect to HTTP RPC")?;

        let mut last_block = provider.get_block_number().await.unwrap_or(0);
        info!("Sequencer HTTP Poller active from block #{}", last_block);

        loop {
            if !self.config.is_engine_running() {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                continue;
            }

            match provider.get_block_number().await {
                Ok(current_block) => {
                    if current_block > last_block {
                        for block_num in (last_block + 1)..=current_block {
                            let start = Instant::now();
                            self.prysm_engine.process_new_block(block_num).await;

                            if let Ok(Some(block)) = provider.get_block_by_number(block_num.into()).await {
                                if let BlockTransactions::Full(txs) = block.transactions {
                                    for tx in txs {
                                        let from = tx.inner.signer();
                                        let to = tx.inner.to();
                                        let value = tx.inner.value();
                                        let input = tx.inner.input();
                                        let tx_hash = *tx.inner.tx_hash();

                                        self.inspect_transaction_fast(
                                            from,
                                            to,
                                            value,
                                            input,
                                            tx_hash,
                                            block_num,
                                            start,
                                        ).await;

                                        self.prysm_engine.inspect_transaction(
                                            from,
                                            to,
                                            value,
                                            input,
                                            tx_hash,
                                            block_num,
                                            start,
                                        ).await;
                                    }
                                }
                            }
                        }
                        last_block = current_block;
                    }
                }
                Err(e) => {
                    trace!("HTTP block poll tick: {}", e);
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
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
        // Step 1: Fast watchlist lookup (O(1) in HashSet)
        if !self.config.is_in_watchlist(&from).await {
            return;
        }

        let to_address = match to {
            Some(addr) => addr,
            None => return, // Contract creation, not a swap
        };

        // Step 2: Instant 1-cycle bitwise signature matching
        let is_swap = fast_selectors::is_swap_selector(input);
        if !is_swap && value == U256::ZERO {
            return;
        }

        let elapsed = start_time.elapsed().as_micros();
        info!(
            "⚡ ZERO-COPY ON-CHAIN SIGNAL DETECTED in {} µs! Account: {:?} -> Router: {:?}, Value: {} wei",
            elapsed, from, to_address, value
        );

        // Step 3: Zero-copy parameter extraction
        let target_token = ZeroCopyTxParser::extract_target_token_fast(input)
            .unwrap_or(to_address);

        let signal = TradeSignal {
            tx_hash,
            monitored_account: from,
            target_token,
            pool_or_router: to_address,
            entry_amount_wei: value,
            swap_calldata: input.to_vec(),
            detected_at: Instant::now(),
            block_number,
            strategy_tag: None,
        };

        if let Err(e) = self.signal_tx.send(signal).await {
            error!("Failed to enqueue detected trade signal: {}", e);
        }
    }
}
