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

// Pons Bonding Curve & ERC-20 Function Selectors
/// buy(uint256,uint256,address) on Pons bonding curve (payable)
pub const PONS_BUY_SELECTOR: [u8; 4] = [0x59, 0xa8, 0x7b, 0xc1];
/// sell(uint256,uint256,address) on Pons bonding curve
pub const PONS_SELL_SELECTOR: [u8; 4] = [0xd0, 0x4c, 0x69, 0x83];
/// approve(address,uint256) on ERC-20
pub const ERC20_APPROVE_SELECTOR: [u8; 4] = [0x09, 0x5e, 0xa7, 0xb3];
/// balanceOf(address) on ERC-20
pub const ERC20_BALANCE_OF_SELECTOR: [u8; 4] = [0x70, 0xa0, 0x82, 0x31];
/// allowance(address,address) on ERC-20
pub const ERC20_ALLOWANCE_SELECTOR: [u8; 4] = [0xdd, 0x62, 0xed, 0x3e];
/// getReserves() on Pons bonding curve
pub const PONS_GET_RESERVES_SELECTOR: [u8; 4] = [0x09, 0x02, 0xf1, 0xac];
/// pairToken() on Pons bonding curve to query paired ERC-20 token
pub const PONS_PAIR_TOKEN_SELECTOR: [u8; 4] = [0x3d, 0xe3, 0x5b, 0x79];
/// PonsV2LaunchFactory Launched event topic hash
pub const PONS_LAUNCH_TOPIC_HEX: &str = "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607";
/// Snipe tax applied on Pons bonding curve during Block +1 and Block +2 (6.18% = 618 bps)
pub const PONS_SNIPE_TAX_BPS: u32 = 618;
/// Base protocol fee on Pons bonding curve (1.00% = 100 bps)
pub const PONS_BASE_FEE_BPS: u32 = 100;
/// Initial virtual ETH reserve on Pons bonding curve: 1.68 ETH (in wei)
pub const PONS_INITIAL_ETH_RESERVE_WEI: u128 = 1_680_000_000_000_000_000;

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

    /// Rewrite the recipient of a copied trade calldata (Pons curve `buy` or V2 `swapExactETHForTokens*`) to `recipient`.
    ///
    /// Replaying the watched wallet's calldata verbatim would buy tokens *for that wallet*:
    /// `to` or `recipient` is an explicit argument. Returns `None` for any selector whose amounts or recipients
    /// cannot be safely re-targeted.
    pub fn retarget_swap_recipient(calldata: &[u8], recipient: Address) -> Option<Vec<u8>> {
        if calldata.len() < 4 {
            return None;
        }
        // Case 1: Pons bonding curve buy(uint256 ethIn, uint256 minTokensOut, address recipient)
        if calldata[..4] == PONS_BUY_SELECTOR {
            return Self::retarget_pons_buy_recipient(calldata, recipient);
        }
        // Case 2: Uniswap V2 swapExactETHForTokens*(uint256 amountOutMin, address[] path, address to, uint256 deadline)
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

    /// ABI-encode `buy(uint256 ethIn, uint256 minTokensOut, address recipient)` for Pons bonding curve (payable)
    pub fn build_pons_buy_payload(eth_in: U256, min_tokens_out: U256, recipient: Address) -> Vec<u8> {
        let mut payload = Vec::with_capacity(4 + 32 * 3);
        payload.extend_from_slice(&PONS_BUY_SELECTOR);
        payload.extend_from_slice(&eth_in.to_be_bytes::<32>());
        payload.extend_from_slice(&min_tokens_out.to_be_bytes::<32>());
        payload.extend_from_slice(&[0u8; 12]);
        payload.extend_from_slice(recipient.as_slice());
        payload
    }

    /// Rewrite recipient of Pons curve buy calldata if selector matches PONS_BUY_SELECTOR
    pub fn retarget_pons_buy_recipient(calldata: &[u8], recipient: Address) -> Option<Vec<u8>> {
        if calldata.len() < 4 + 32 * 3 || calldata[..4] != PONS_BUY_SELECTOR {
            return None;
        }
        let mut out = calldata.to_vec();
        out[4 + 32 * 2..4 + 32 * 2 + 12].fill(0);
        out[4 + 32 * 2 + 12..4 + 32 * 3].copy_from_slice(recipient.as_slice());
        Some(out)
    }

    /// ABI-encode `sell(uint256 tokensIn, uint256 minEthOut, address recipient)` for Pons bonding curve
    pub fn build_pons_sell_payload(tokens_in: U256, min_eth_out: U256, recipient: Address) -> Vec<u8> {
        let mut payload = Vec::with_capacity(4 + 32 * 3);
        payload.extend_from_slice(&PONS_SELL_SELECTOR);
        payload.extend_from_slice(&tokens_in.to_be_bytes::<32>());
        payload.extend_from_slice(&min_eth_out.to_be_bytes::<32>());
        payload.extend_from_slice(&[0u8; 12]);
        payload.extend_from_slice(recipient.as_slice());
        payload
    }

    /// ABI-encode `approve(address spender, uint256 amount)` for ERC-20
    pub fn build_erc20_approve_payload(spender: Address, amount: U256) -> Vec<u8> {
        let mut payload = Vec::with_capacity(4 + 32 * 2);
        payload.extend_from_slice(&ERC20_APPROVE_SELECTOR);
        payload.extend_from_slice(&[0u8; 12]);
        payload.extend_from_slice(spender.as_slice());
        payload.extend_from_slice(&amount.to_be_bytes::<32>());
        payload
    }

    /// ABI-encode `balanceOf(address account)` for ERC-20
    pub fn build_erc20_balance_of_payload(account: Address) -> Vec<u8> {
        let mut payload = Vec::with_capacity(4 + 32);
        payload.extend_from_slice(&ERC20_BALANCE_OF_SELECTOR);
        payload.extend_from_slice(&[0u8; 12]);
        payload.extend_from_slice(account.as_slice());
        payload
    }

    /// ABI-encode `allowance(address owner, address spender)` for ERC-20
    pub fn build_erc20_allowance_payload(owner: Address, spender: Address) -> Vec<u8> {
        let mut payload = Vec::with_capacity(4 + 32 * 2);
        payload.extend_from_slice(&ERC20_ALLOWANCE_SELECTOR);
        payload.extend_from_slice(&[0u8; 12]);
        payload.extend_from_slice(owner.as_slice());
        payload.extend_from_slice(&[0u8; 12]);
        payload.extend_from_slice(spender.as_slice());
        payload
    }

    /// ABI-encode `getReserves()` for Pons bonding curve
    pub fn build_pons_get_reserves_payload() -> Vec<u8> {
        PONS_GET_RESERVES_SELECTOR.to_vec()
    }

    /// Decode single U256 output from `balanceOf`, `allowance`, etc.
    pub fn decode_u256_output(data: &[u8]) -> Option<U256> {
        if data.len() < 32 {
            return None;
        }
        Some(U256::from_be_slice(&data[..32]))
    }

    /// Decode `(uint256 ethReserve, uint256 tokenReserve)` from Pons `getReserves()`
    pub fn decode_get_reserves_output(data: &[u8]) -> Option<(U256, U256)> {
        if data.len() < 64 {
            return None;
        }
        let eth_reserve = U256::from_be_slice(&data[..32]);
        let token_reserve = U256::from_be_slice(&data[32..64]);
        Some((eth_reserve, token_reserve))
    }

    /// Decode `address` from 32-byte ABI return data (e.g. `pairToken()`)
    pub fn decode_address_output(data: &[u8]) -> Option<Address> {
        if data.len() < 32 {
            return None;
        }
        let addr_bytes: [u8; 20] = data[12..32].try_into().ok()?;
        Some(Address::from(addr_bytes))
    }

    /// Pons curve output for buying tokens with `eth_in`
    pub fn calculate_pons_tokens_out(
        eth_reserve: U256,
        token_reserve: U256,
        eth_in: U256,
        fee_bps: u32,
        ctax_bps: u32,
    ) -> U256 {
        let total_tax_bps = U256::from((fee_bps + ctax_bps) as u128);
        let tax = eth_in * total_tax_bps / U256::from(10_000);
        let e = eth_in.saturating_sub(tax);
        let denom = eth_reserve + e;
        if denom.is_zero() {
            return U256::ZERO;
        }
        token_reserve * e / denom
    }

    /// Calculate minimum tokens out for Pons curve buy, assuming Block +1 snipe tax (6.18%)
    /// and slippage tolerance.
    ///
    /// REVERT GUARD:
    /// In Block 0 (the launch block), Pons applies a 99% buy tax.
    /// In Block +1, Pons applies only 6.18% snipe tax.
    /// If an order is targeted for Block +1 but is accidentally sequenced in Block 0,
    /// actual tokens received would be ~1% of normal quote.
    /// Because min_tokens_out is quoted for 6.18% tax with slippage tolerance (e.g. 25%),
    /// a 99% fill violates `require(tokensOut >= minTokensOut)` and REVERTS safely on-chain,
    /// protecting 100% of the input ETH from being taxed away!
    pub fn calculate_pons_buy_revert_guard(
        eth_in: U256,
        eth_reserve: Option<U256>,
        token_reserve: Option<U256>,
        slippage_percent: f64,
    ) -> U256 {
        let er = eth_reserve.unwrap_or_else(|| U256::from(PONS_INITIAL_ETH_RESERVE_WEI));
        let tr = token_reserve.unwrap_or_else(|| {
            U256::from(1_000_000_000u64) * U256::from(1_000_000_000_000_000_000u64)
        });
        let quote = Self::calculate_pons_tokens_out(er, tr, eth_in, PONS_BASE_FEE_BPS, PONS_SNIPE_TAX_BPS);
        if quote.is_zero() {
            return U256::from(1);
        }
        let slip_factor = ((100.0 - slippage_percent.clamp(0.0, 90.0)) / 100.0 * 10_000.0) as u128;
        (quote * U256::from(slip_factor)) / U256::from(10_000)
    }

    /// Pons curve net ETH out for selling `tokens_in`
    pub fn calculate_pons_eth_out(
        eth_reserve: U256,
        token_reserve: U256,
        tokens_in: U256,
        fee_bps: u32,
        ctax_bps: u32,
    ) -> U256 {
        let denom = token_reserve + tokens_in;
        if denom.is_zero() {
            return U256::ZERO;
        }
        let gross_eth = eth_reserve * tokens_in / denom;
        let total_tax_bps = U256::from((fee_bps + ctax_bps) as u128);
        let tax = gross_eth * total_tax_bps / U256::from(10_000);
        gross_eth.saturating_sub(tax)
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
