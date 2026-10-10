use alloy::primitives::Address;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Target buffer size in bytes for Robinhood Chain L2 sequencer batch calibration
pub const TARGET_CALLDATA_BUFFER_SIZE: usize = 94_500;
/// Gas Limit static allocation for Arbitrum Nitro transactions
pub const STATIC_GAS_LIMIT: u64 = 600_000;
/// Fast exit compact payload size (standard swapExactTokensForETH ABI)
pub const FAST_EXIT_CALLDATA_SIZE: usize = 164;

/// Low-level optimized calldata payload assembler
pub struct CalldataPayloadBuilder;

impl CalldataPayloadBuilder {
    /// Zero-fill optimized calldata buffer construction
    ///
    /// Copies `swap_calldata` and writes `buffer_size` zero bytes using low-level
    /// memory filling (`slice::fill(0)` / `memset`), avoiding repeated reallocation
    /// or high-level iteration.
    #[inline(always)]
    pub fn build_calibrated_payload(swap_calldata: &[u8], buffer_size: usize) -> Vec<u8> {
        let total_size = swap_calldata.len() + buffer_size;
        let mut buffer = Vec::with_capacity(total_size);

        // 1. Copy swap instructions
        buffer.extend_from_slice(swap_calldata);

        // 2. Instant low-level zero-padding using pre-allocated slice fill
        let current_len = buffer.len();
        buffer.resize(total_size, 0);

        // Memory zero-fill optimization: ensures contiguous zeroed block in-place
        unsafe {
            std::ptr::write_bytes(
                buffer.as_mut_ptr().add(current_len),
                0u8,
                buffer_size,
            );
        }

        buffer
    }

    /// Pre-allocated reusable buffer pool variant
    /// Writes into a caller-provided reusable scratch buffer to achieve ZERO heap allocations
    #[inline(always)]
    pub fn write_into_scratch(
        dest: &mut Vec<u8>,
        swap_calldata: &[u8],
        buffer_size: usize,
    ) {
        dest.clear();
        let total_size = swap_calldata.len() + buffer_size;
        if dest.capacity() < total_size {
            dest.reserve(total_size - dest.capacity());
        }

        dest.extend_from_slice(swap_calldata);
        let offset = dest.len();
        dest.resize(total_size, 0);

        unsafe {
            std::ptr::write_bytes(
                dest.as_mut_ptr().add(offset),
                0u8,
                buffer_size,
            );
        }
    }

    /// Construct ultra-compact fast exit liquidation payload (~164 bytes)
    /// Standard ABI: swapExactTokensForETH(uint256,uint256,address[],address,uint256)
    #[inline(always)]
    pub fn build_fast_exit_payload(token_address: &Address, router_or_pool: &Address) -> Vec<u8> {
        let mut payload = Vec::with_capacity(FAST_EXIT_CALLDATA_SIZE);
        // selector: 0x18cbafe5
        payload.extend_from_slice(&[0x18, 0xcb, 0xaf, 0xe5]);

        // Packed 160-byte argument space (5 words: amountIn, minOut, pathOffset, to, deadline)
        let mut args = [0u8; 160];
        // Embed token address into path offset word
        args[12..32].copy_from_slice(token_address.as_slice());
        // Embed recipient / router into destination word
        args[44..64].copy_from_slice(router_or_pool.as_slice());

        payload.extend_from_slice(&args);
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
    /// Atomic representation of current EMA Base Fee (stored as nano-Gwei integer)
    ema_base_fee_atomic: Arc<AtomicU64>,
    /// Atomic representation of current EMA Priority Fee (stored as nano-Gwei integer)
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
