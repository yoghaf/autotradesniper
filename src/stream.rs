use crate::config::AppConfig;
use crate::types::TradeSignal;
use alloy::consensus::Transaction as TxTrait;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::BlockTransactions;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

/// Known DEX swap function selectors on Arbitrum Nitro / Robinhood Chain
pub mod selectors {
    // swapExactETHForTokens(uint256,address[],address,uint256) -> 0x7ff36ab5
    pub const SWAP_EXACT_ETH_FOR_TOKENS: [u8; 4] = [0x7f, 0xf3, 0x6a, 0xb5];
    // swapExactETHForTokensSupportingFeeOnTransferTokens -> 0xb6f9de95
    pub const SWAP_EXACT_ETH_SUPPORTING_FEE: [u8; 4] = [0xb6, 0xf9, 0xde, 0x95];
    // swapExactTokensForTokens -> 0x38ed1739
    pub const SWAP_EXACT_TOKENS_FOR_TOKENS: [u8; 4] = [0x38, 0xed, 0x17, 0x39];
    // exactInputSingle((address,address,uint24,address,uint256,uint256,uint160)) -> 0x414bf382
    pub const EXACT_INPUT_SINGLE: [u8; 4] = [0x41, 0x4b, 0xf3, 0x82];
    // multicall(bytes[]) -> 0xac9650d8
    pub const MULTICALL: [u8; 4] = [0xac, 0x96, 0x50, 0xd8];
    // execute(bytes,bytes[],uint256) -> 0x3593564c (Universal Router)
    pub const UNIVERSAL_ROUTER_EXECUTE: [u8; 4] = [0x35, 0x93, 0x56, 0x4c];
}

/// Real-Time Sequencer Feed & Signal Replication Stream Listener
pub struct StreamListener {
    config: AppConfig,
    signal_tx: mpsc::Sender<TradeSignal>,
}

impl StreamListener {
    pub fn new(config: AppConfig, signal_tx: mpsc::Sender<TradeSignal>) -> Self {
        Self { config, signal_tx }
    }

    /// Run the stream listener loop
    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting Sequencer Stream Listener for Chain ID: {} (WS: {})",
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

    /// WebSocket Sequencer Feed connection
    async fn listen_websocket_feed(&self, ws_url: &str) -> Result<()> {
        let provider = ProviderBuilder::new()
            .connect(ws_url)
            .await
            .context("Failed to connect to Sequencer WebSocket")?;

        info!("Successfully connected to Sequencer WebSocket Feed!");
        let sub = provider.subscribe_blocks().await?;
        let mut stream = sub.into_stream();

        while let Some(block_header) = stream.next().await {
            let start = Instant::now();
            let block_number = block_header.number;

            if !self.config.is_engine_running() {
                continue;
            }

            trace!("Received Sequencer Block #{}", block_number);

            // Fetch block with full transactions for microsecond inspection
            if let Ok(Some(block)) = provider.get_block_by_number(block_number.into()).await {
                if let BlockTransactions::Full(transactions) = block.transactions {
                    for tx in transactions {
                        let from = tx.inner.signer();
                        let to = tx.inner.to();
                        let value = tx.inner.value();
                        let input = tx.inner.input();
                        let tx_hash = *tx.inner.tx_hash();

                        self.inspect_transaction(
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
                            if let Ok(Some(block)) = provider.get_block_by_number(block_num.into()).await {
                                if let BlockTransactions::Full(txs) = block.transactions {
                                    for tx in txs {
                                        let from = tx.inner.signer();
                                        let to = tx.inner.to();
                                        let value = tx.inner.value();
                                        let input = tx.inner.input();
                                        let tx_hash = *tx.inner.tx_hash();

                                        self.inspect_transaction(
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

    /// High-frequency inspection of incoming transaction against watchlist
    pub async fn inspect_transaction(
        &self,
        from: Address,
        to: Option<Address>,
        value: U256,
        input: &[u8],
        tx_hash: B256,
        block_number: u64,
        start_time: Instant,
    ) {
        // Fast watchlist lookup (O(1) in HashSet)
        if !self.config.is_in_watchlist(&from).await {
            return;
        }

        let to_address = match to {
            Some(addr) => addr,
            None => return, // Contract creation, not a swap
        };

        // Microsecond inspection of calldata payload
        if input.len() < 4 {
            return;
        }

        let selector = &input[0..4];
        let is_swap = selector == selectors::SWAP_EXACT_ETH_FOR_TOKENS
            || selector == selectors::SWAP_EXACT_ETH_SUPPORTING_FEE
            || selector == selectors::SWAP_EXACT_TOKENS_FOR_TOKENS
            || selector == selectors::EXACT_INPUT_SINGLE
            || selector == selectors::MULTICALL
            || selector == selectors::UNIVERSAL_ROUTER_EXECUTE;

        if !is_swap && value == U256::ZERO {
            return;
        }

        let elapsed = start_time.elapsed().as_micros();
        info!(
            "⚡ ON-CHAIN SIGNAL DETECTED in {} µs! Monitored Account: {:?} -> Target: {:?}, Value: {} wei",
            elapsed, from, to_address, value
        );

        // Extract target token parameter from swap calldata
        let target_token = Self::extract_target_token(input).unwrap_or(to_address);

        let signal = TradeSignal {
            tx_hash,
            monitored_account: from,
            target_token,
            pool_or_router: to_address,
            entry_amount_wei: value,
            swap_calldata: input.to_vec(),
            detected_at: Instant::now(),
            block_number,
        };

        if let Err(e) = self.signal_tx.send(signal).await {
            error!("Failed to enqueue detected trade signal: {}", e);
        }
    }

    /// Fast extraction of target token address from calldata payload
    fn extract_target_token(calldata: &[u8]) -> Option<Address> {
        if calldata.len() < 36 {
            return None;
        }

        // Standard Uniswap V2 path extraction: last 32 bytes of path array or tokenOut in V3
        let len = calldata.len();
        if len >= 32 {
            let last_word = &calldata[len - 32..len];
            let addr_bytes: [u8; 20] = last_word[12..32].try_into().ok()?;
            return Some(Address::from(addr_bytes));
        }

        None
    }
}
