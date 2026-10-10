use alloy::primitives::{Address, U256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Target buffer size in bytes for Robinhood Chain L2 sequencer batch calibration
pub const TARGET_CALLDATA_BUFFER_SIZE: usize = 94_500;
/// Gas Limit static allocation for Arbitrum Nitro transactions
pub const STATIC_GAS_LIMIT: u64 = 600_000;
/// Nitro sequencer default `--execution.sequencer.max-tx-data-size`: larger encoded txs are
/// rejected with "oversized data", so padding is clamped to stay below it.
pub const MAX_SEQUENCER_TX_BYTES: usize = 95_000;
/// Worst-case EIP-1559 envelope bytes around the calldata (type, RLP headers, fees, to, value, sig).
pub const TX_ENVELOPE_OVERHEAD: usize = 256;
/// Gas kept for swap execution on top of intrinsic calldata gas (Arbitrum also adds L1 poster gas).
pub const EXECUTION_GAS_RESERVE: u64 = 300_000;
/// Fast exit payload size for a 2-hop path: selector + 5 head words + path length + 2 addresses
pub const FAST_EXIT_CALLDATA_SIZE: usize = 4 + 32 * 5 + 32 + 32 * 2;
/// swapExactTokensForETH(uint256,uint256,address[],address,uint256)
pub const SWAP_EXACT_TOKENS_FOR_ETH_SELECTOR: [u8; 4] = [0x18, 0xcb, 0xaf, 0xe5];

/// Low-level optimized calldata payload assembler
pub struct CalldataPayloadBuilder;

impl CalldataPayloadBuilder {
    /// Zero-fill optimized calldata buffer construction
    ///
    /// `vec![0; n]` maps to `alloc_zeroed` (calloc), which for large buffers hands out
    /// pre-zeroed pages, so the padding costs no explicit memset.
    #[inline]
    pub fn build_calibrated_payload(swap_calldata: &[u8], buffer_size: usize) -> Vec<u8> {
        let mut buffer = vec![0u8; swap_calldata.len() + buffer_size];
        buffer[..swap_calldata.len()].copy_from_slice(swap_calldata);
        buffer
    }

    /// Pre-allocated reusable buffer pool variant
    /// Writes into a caller-provided reusable scratch buffer to achieve ZERO heap allocations
    #[inline]
    pub fn write_into_scratch(dest: &mut Vec<u8>, swap_calldata: &[u8], buffer_size: usize) {
        dest.clear();
        // `reserve` is relative to len (0 after clear), so this guarantees one allocation at most.
        dest.reserve(swap_calldata.len() + buffer_size);
        dest.extend_from_slice(swap_calldata);
        dest.resize(swap_calldata.len() + buffer_size, 0);
    }

    /// Largest padding that keeps the signed tx under the sequencer's size cap.
    #[inline]
    pub fn clamp_padding(calldata_len: usize, requested: usize) -> usize {
        requested.min(MAX_SEQUENCER_TX_BYTES.saturating_sub(calldata_len + TX_ENVELOPE_OVERHEAD))
    }

    /// EIP-2028 intrinsic gas for `calldata` followed by `padding` zero bytes.
    #[inline]
    pub fn intrinsic_gas(calldata: &[u8], padding: usize) -> u64 {
        let data_gas: u64 = calldata.iter().map(|&b| if b == 0 { 4u64 } else { 16 }).sum();
        21_000 + data_gas + 4 * padding as u64
    }

    /// Rewrite the recipient of a copied V2 `swapExactETHForTokens*` call to `recipient`.
    ///
    /// Replaying the watched wallet's calldata verbatim would buy tokens *for that wallet*:
    /// `to` is an explicit argument. Returns `None` for any selector whose amounts or recipients
    /// cannot be safely re-targeted (V3 `exactInputSingle` and Universal Router encode `amountIn`
    /// in calldata, which would no longer match our `msg.value`).
    pub fn retarget_swap_recipient(calldata: &[u8], recipient: Address) -> Option<Vec<u8>> {
        // (uint256 amountOutMin, address[] path, address to, uint256 deadline)
        const TO_WORD: usize = 4 + 32 * 2;
        if calldata.len() < 4 + 32 * 4 || !crate::stream::fast_selectors::is_copyable_eth_swap(calldata) {
            return None;
        }
        let mut out = calldata.to_vec();
        out[TO_WORD..TO_WORD + 12].fill(0);
        out[TO_WORD + 12..TO_WORD + 32].copy_from_slice(recipient.as_slice());
        Some(out)
    }

    /// ABI-encode `swapExactTokensForETH(amountIn, amountOutMin, path, to, deadline)`.
    pub fn build_fast_exit_payload(
        amount_in: U256,
        amount_out_min: U256,
        path: &[Address],
        recipient: &Address,
        deadline: u64,
    ) -> Vec<u8> {
        let mut payload = Vec::with_capacity(4 + 32 * (6 + path.len()));
        payload.extend_from_slice(&SWAP_EXACT_TOKENS_FOR_ETH_SELECTOR);
        payload.extend_from_slice(&amount_in.to_be_bytes::<32>());
        payload.extend_from_slice(&amount_out_min.to_be_bytes::<32>());
        // Offset of the dynamic `path` tail, measured from the start of the arguments.
        payload.extend_from_slice(&U256::from(32 * 5).to_be_bytes::<32>());
        payload.extend_from_slice(&[0u8; 12]);
        payload.extend_from_slice(recipient.as_slice());
        payload.extend_from_slice(&U256::from(deadline).to_be_bytes::<32>());
        payload.extend_from_slice(&U256::from(path.len()).to_be_bytes::<32>());
        for hop in path {
            payload.extend_from_slice(&[0u8; 12]);
            payload.extend_from_slice(hop.as_slice());
        }
        payload
    }
}

/// Exponential Moving Average (EMA) Dynamic Gas & Priority Fee Adjuster
///
/// Tracks fee volatility across L2 sequencer blocks to compute deterministic,
/// competitive fees that win top-of-batch ordering without overpaying.
#[derive(Debug, Clone)]
pub struct DynamicFeeAdjuster {
    /// Smoothing factor for EMA (alpha = 0.2 corresponds to a 10-block window)
    alpha: f64,
    /// Multiplier to ensure transaction outbids competing submissions
    priority_multiplier: f64,
    /// Minimum priority fee in Gwei floor
    min_priority_fee_gwei: f64,
    /// Atomic representation of current EMA Base Fee (stored as micro-Gwei integer, 1e-6 Gwei)
    ema_base_fee_atomic: Arc<AtomicU64>,
    /// Atomic representation of current EMA Priority Fee (stored as micro-Gwei integer, 1e-6 Gwei)
    ema_priority_fee_atomic: Arc<AtomicU64>,
}

impl DynamicFeeAdjuster {
    pub fn new(min_priority_gwei: f64, priority_multiplier: f64) -> Self {
        let initial_base_nano = (1.0 * 1_000_000.0) as u64; // 1 Gwei base
        let initial_prio_nano = (min_priority_gwei * 1_000_000.0) as u64;

        Self {
            alpha: 0.20,
            priority_multiplier,
            min_priority_fee_gwei: min_priority_gwei,
            ema_base_fee_atomic: Arc::new(AtomicU64::new(initial_base_nano)),
            ema_priority_fee_atomic: Arc::new(AtomicU64::new(initial_prio_nano)),
        }
    }

    /// Feed a newly received block's fee metrics into the EMA estimator
    #[inline]
    pub fn update_block_fees(&self, base_fee_gwei: f64, priority_fee_gwei: f64) {
        // Read previous base fee
        let prev_base_nano = self.ema_base_fee_atomic.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let new_base = (self.alpha * base_fee_gwei) + ((1.0 - self.alpha) * prev_base_nano);
        self.ema_base_fee_atomic.store((new_base * 1_000_000.0) as u64, Ordering::Relaxed);

        // Read previous priority fee
        let prev_prio_nano = self.ema_priority_fee_atomic.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let effective_prio = priority_fee_gwei.max(self.min_priority_fee_gwei);
        let new_prio = (self.alpha * effective_prio) + ((1.0 - self.alpha) * prev_prio_nano);
        self.ema_priority_fee_atomic.store((new_prio * 1_000_000.0) as u64, Ordering::Relaxed);
    }

    /// Compute optimal (Max Priority Fee Per Gas, Max Fee Per Gas) in Wei
    ///
    /// Executes in < 20 nanoseconds (atomic lock-free arithmetic).
    #[inline(always)]
    pub fn calculate_optimal_fees(&self) -> (u128, u128) {
        let prio_gwei = (self.ema_priority_fee_atomic.load(Ordering::Relaxed) as f64 / 1_000_000.0)
            * self.priority_multiplier;
        let base_gwei = self.ema_base_fee_atomic.load(Ordering::Relaxed) as f64 / 1_000_000.0;

        let effective_prio_gwei = prio_gwei.max(self.min_priority_fee_gwei);
        // Max fee = (2.0 * BaseFee) + PriorityFee to withstand block fee spikes
        let max_fee_gwei = (base_gwei * 2.0) + effective_prio_gwei;

        let priority_fee_wei = (effective_prio_gwei * 1e9) as u128;
        let max_fee_wei = (max_fee_gwei * 1e9) as u128;

        (priority_fee_wei, max_fee_wei)
    }

    pub fn get_current_metrics(&self) -> (f64, f64) {
        let base = self.ema_base_fee_atomic.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let prio = self.ema_priority_fee_atomic.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        (base, prio)
    }
}
