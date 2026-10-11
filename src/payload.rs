use alloy::primitives::{address, Address, U256};
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
/// Gas kept by a carrier: a zero-value self-call to an EOA runs no code, so this is only slack.
pub const CARRIER_GAS_RESERVE: u64 = 21_000;
/// L1 poster-gas assumption while no `NodeInterface` estimate exists yet (0 = never measured, e.g. the
/// first seconds after start, or a read RPC without the NodeInterface precompile). Deliberately generous:
/// unused gas is refunded, so a high guess only raises the balance a tx needs in order to be submitted.
pub const L1_COMPONENT_FALLBACK_GAS: u64 = 500_000;
/// Hard ceiling for an escalated retry limit (a retry that needs more than this is not a rounding error).
pub const MAX_ESCALATED_GAS_LIMIT: u64 = 8_000_000;
/// EIP-7623 floor price per calldata token (zero byte = 1 token, non-zero byte = 4 tokens).
pub const CALLDATA_FLOOR_GAS_PER_TOKEN: u64 = 10;

/// Pre-zeroed backing store for padding-only payloads. A zero-initialised static lives in `.bss`:
/// no binary size, no allocation, no memset; every carrier borrows a slice of it.
static ZERO_PAD: [u8; MAX_SEQUENCER_TX_BYTES] = [0; MAX_SEQUENCER_TX_BYTES];
/// Fast exit payload size for a 2-hop path: selector + 5 head words + path length + 2 addresses
pub const FAST_EXIT_CALLDATA_SIZE: usize = 4 + 32 * 5 + 32 + 32 * 2;
/// swapExactTokensForETH(uint256,uint256,address[],address,uint256)
pub const SWAP_EXACT_TOKENS_FOR_ETH_SELECTOR: [u8; 4] = [0x18, 0xcb, 0xaf, 0xe5];
/// swapExactTokensForETHSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)
pub const SWAP_EXACT_TOKENS_FOR_ETH_FOT_SELECTOR: [u8; 4] = [0x79, 0x1a, 0xc9, 0x47];
/// getAmountsOut(uint256,address[]) on a V2 router
pub const V2_GET_AMOUNTS_OUT_SELECTOR: [u8; 4] = [0xd0, 0x6c, 0xa6, 0x1f];

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
/// Real ETH raised (`ethReserve - initial virtual reserve`) at which the curve graduates
/// (4.2 ETH). Verify against the deployed contract: Layer 3 exits at 90% of it.
pub const PONS_GRADUATION_ETH_WEI: u128 = 4_200_000_000_000_000_000;
/// Initial virtual token reserve on Pons bonding curve: 1,000,000,000 tokens (18 decimals)
pub const PONS_INITIAL_TOKEN_RESERVE_WEI: u128 = 1_000_000_000 * 1_000_000_000_000_000_000;
/// Total token supply used for market-cap math (1,000,000,000 tokens)
pub const PONS_TOTAL_SUPPLY_TOKENS: f64 = 1_000_000_000.0;
/// Creator tax assumed when quoting sells (the launch parser cannot read the real one yet).
pub const PONS_ASSUMED_CREATOR_TAX_BPS: u32 = 200;
/// Fallback ETH/USD reference for market-cap guards (`ETH_USD_PRICE` overrides it).
pub const DEFAULT_ETH_USD_PRICE: f64 = 2600.0;

/// Arbitrum Nitro `NodeInterface` virtual contract (answers `eth_call` only).
pub const NODE_INTERFACE_ADDRESS: Address = address!("00000000000000000000000000000000000000c8");
/// `gasEstimateL1Component(address,bool,bytes)` on `NodeInterface`
pub const GAS_ESTIMATE_L1_COMPONENT_SELECTOR: [u8; 4] = [0x77, 0xd4, 0x88, 0xa2];

/// ETH/USD reference for market-cap guards, read once from `ETH_USD_PRICE`.
pub fn eth_usd_reference() -> f64 {
    static PRICE: std::sync::LazyLock<f64> = std::sync::LazyLock::new(|| {
        std::env::var("ETH_USD_PRICE")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|p| p.is_finite() && *p > 0.0)
            .unwrap_or(DEFAULT_ETH_USD_PRICE)
    });
    *PRICE
}

/// Fully diluted market cap in ETH from curve reserves (`price = ethReserve / tokenReserve`).
pub fn pons_mcap_eth(eth_reserve: U256, token_reserve: U256) -> Option<f64> {
    if token_reserve.is_zero() {
        return None;
    }
    let mcap = f64::from(eth_reserve) / f64::from(token_reserve) * PONS_TOTAL_SUPPLY_TOKENS;
    mcap.is_finite().then_some(mcap)
}

/// Share of the graduation target already raised, in percent.
pub fn pons_curve_capacity_percent(eth_reserve: U256) -> f64 {
    let raised = eth_reserve.saturating_sub(U256::from(PONS_INITIAL_ETH_RESERVE_WEI));
    (f64::from(raised) / PONS_GRADUATION_ETH_WEI as f64 * 100.0).clamp(0.0, 100.0)
}

/// Curve reserves right after the launch tx's dev buy, from the initial virtual reserves.
/// Used when live `getReserves()` is unavailable: quoting against the *initial* reserves
/// overstates our fill by roughly `(E0 / (E0 + devBuy))^2` and tightens the Revert Guard.
pub fn pons_reserves_after_buy(eth_in: U256) -> (U256, U256) {
    let e0 = U256::from(PONS_INITIAL_ETH_RESERVE_WEI);
    let t0 = U256::from(PONS_INITIAL_TOKEN_RESERVE_WEI);
    let out = CalldataPayloadBuilder::calculate_pons_tokens_out(e0, t0, eth_in, 0, 0);
    (e0 + eth_in, t0 - out)
}

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

    /// Calldata followed by `padding` zero bytes. With no padding the input is moved, not copied.
    #[inline]
    pub fn with_padding(mut calldata: Vec<u8>, padding: usize) -> Vec<u8> {
        if padding > 0 {
            calldata.resize(calldata.len() + padding, 0);
        }
        calldata
    }

    /// Gas limit for a tx carrying `calldata` + `padding`.
    ///
    /// On Arbitrum the gas limit must also cover the L1 data-poster component (charged in L2 gas
    /// units, it rises with L1 fees and falls with the L2 base fee). A tx whose limit is below
    /// `intrinsic + poster gas` is rejected by the sequencer ("intrinsic gas too low"). Unused gas
    /// is refunded, so the 1.5x margin only raises the balance needed to submit, not the cost.
    #[inline]
    pub fn gas_limit(calldata: &[u8], padding: usize, configured: u64, l1_component_gas: u64) -> u64 {
        Self::gas_limit_with_reserve(calldata, padding, configured, l1_component_gas, EXECUTION_GAS_RESERVE)
    }

    /// Gas limit of a zero-value carrier: `padding` zero bytes sent to the sender itself.
    #[inline]
    pub fn carrier_gas_limit(padding: usize, configured: u64, l1_component_gas: u64) -> u64 {
        Self::gas_limit_with_reserve(&[], padding, configured, l1_component_gas, CARRIER_GAS_RESERVE)
    }

    /// `intrinsic + reserve + 1.5 x L1 poster gas`, never below `configured`.
    ///
    /// An L1 estimate of 0 means "never measured", not "free": it is replaced by
    /// [`L1_COMPONENT_FALLBACK_GAS`] so a missing estimate cannot silently under-size the limit.
    #[inline]
    pub fn gas_limit_with_reserve(calldata: &[u8], padding: usize, configured: u64, l1_component_gas: u64, reserve: u64) -> u64 {
        configured.max(
            Self::intrinsic_gas(calldata, padding)
                .saturating_add(reserve)
                .saturating_add(Self::l1_margin(l1_component_gas)),
        )
    }

    /// 1.5x the L1 poster component (fallback when unmeasured).
    #[inline]
    pub fn l1_margin(l1_component_gas: u64) -> u64 {
        let l1 = if l1_component_gas == 0 { L1_COMPONENT_FALLBACK_GAS } else { l1_component_gas };
        l1.saturating_add(l1 / 2)
    }

    /// EIP-7623 floor for `calldata` followed by `padding` zero bytes:
    /// `21000 + 10 x (zero bytes + 4 x non-zero bytes)`.
    ///
    /// Whether the chain enforces it on L2 user gas is NOT established (Arbitrum documents a
    /// calldata floor as a batch-posting price parameter). It is only used as the lower bound of an
    /// escalated retry after the sequencer rejected the first limit, never to size the first attempt.
    #[inline]
    pub fn calldata_floor_gas(calldata: &[u8], padding: usize) -> u64 {
        let tokens: u64 = calldata.iter().map(|&b| if b == 0 { 1u64 } else { 4 }).sum::<u64>() + padding as u64;
        21_000u64.saturating_add(CALLDATA_FLOOR_GAS_PER_TOKEN.saturating_mul(tokens))
    }

    /// Limit for the single retry after a "gas too low" rejection: at least double the rejected limit,
    /// and at least the calldata floor plus the L1 margin; capped at [`MAX_ESCALATED_GAS_LIMIT`].
    /// Doubling is cheap: unused gas is refunded.
    pub fn escalated_gas_limit(rejected_limit: u64, floor_with_l1: u64) -> u64 {
        rejected_limit.saturating_mul(2).max(floor_with_l1).min(MAX_ESCALATED_GAS_LIMIT)
    }

    /// Zero-copy, allocation-free carrier payload: a refcounted view over the static zero page.
    /// `len` above the static page (never produced by `clamp_padding`) falls back to one allocation.
    #[inline]
    pub fn zero_padding(len: usize) -> alloy::primitives::Bytes {
        match ZERO_PAD.get(..len) {
            Some(pad) => alloy::primitives::Bytes::from_static(pad),
            None => alloy::primitives::Bytes::from(vec![0u8; len]),
        }
    }

    /// Re-target a copied or strategy buy for one of our accounts sending `value` wei.
    ///
    /// `reference_value` is the ETH the calldata was originally quoted for (the watched
    /// wallet's `msg.value`, or the master size for satellites). Besides the recipient, the
    /// amounts must follow our own size:
    /// - Pons `buy(ethIn, minTokensOut, recipient)`: `ethIn` is set to exactly `value`, so it
    ///   always matches `msg.value`.
    /// - Both selectors: the minimum output is scaled down linearly when we buy less than the
    ///   reference. The curve output is concave in the input, so the scaled minimum never exceeds
    ///   what our smaller order can fill. When we buy more, the reference minimum is kept (loose
    ///   but never a false revert).
    ///
    /// Returns `None` for selectors whose amounts cannot be safely re-targeted, or when the
    /// reference size is unknown (0).
    pub fn adapt_buy_calldata(
        calldata: &[u8],
        recipient: Address,
        value: U256,
        reference_value: U256,
    ) -> Option<Vec<u8>> {
        let selector = *calldata.first_chunk::<4>()?;
        let (min_word, recipient_word) = if selector == PONS_BUY_SELECTOR {
            if calldata.len() < 4 + 32 * 3 {
                return None;
            }
            (4 + 32, 4 + 32 * 2)
        } else if crate::stream::fast_selectors::is_copyable_eth_swap(calldata) {
            // swapExactETHForTokens*(amountOutMin, path, to, deadline)
            if calldata.len() < 4 + 32 * 4 {
                return None;
            }
            (4, 4 + 32 * 2)
        } else {
            return None;
        };

        let mut out = calldata.to_vec();
        let reference = if selector == PONS_BUY_SELECTOR {
            let quoted_eth_in = U256::from_be_slice(&calldata[4..36]);
            out[4..36].copy_from_slice(&value.to_be_bytes::<32>());
            if quoted_eth_in.is_zero() { reference_value } else { quoted_eth_in }
        } else {
            reference_value
        };
        if reference.is_zero() {
            return None;
        }
        if value < reference {
            let min_out = U256::from_be_slice(&calldata[min_word..min_word + 32]);
            let scaled = min_out.saturating_mul(value) / reference;
            out[min_word..min_word + 32].copy_from_slice(&scaled.to_be_bytes::<32>());
        }
        out[recipient_word..recipient_word + 12].fill(0);
        out[recipient_word + 12..recipient_word + 32].copy_from_slice(recipient.as_slice());
        Some(out)
    }

    /// `(first, last)` hop of a V2 `swapExactETHForTokens*` path (WETH, output token),
    /// ABI-decoded with bounds checks. Reading "the last 32 bytes" instead breaks on calldata
    /// with trailing bytes (some wallets and aggregators append tracking data) and silently
    /// picks a wrong token.
    pub fn decode_v2_path_ends(calldata: &[u8]) -> Option<(Address, Address)> {
        let args = calldata.get(4..)?;
        let word = |at: usize| -> Option<&[u8]> { args.get(at..at.checked_add(32)?) };
        let as_usize = |w: &[u8]| -> Option<usize> {
            if w[..24].iter().any(|&b| b != 0) {
                return None;
            }
            usize::try_from(u64::from_be_bytes(w[24..32].try_into().ok()?)).ok()
        };
        let as_address = |w: &[u8]| -> Option<Address> {
            w[..12].iter().all(|&b| b == 0).then(|| Address::from_slice(&w[12..32]))
        };
        let offset = as_usize(word(32)?)?;
        let len = as_usize(word(offset)?)?;
        if len < 2 {
            return None;
        }
        let first_at = offset.checked_add(32)?;
        let last_at = first_at.checked_add((len - 1).checked_mul(32)?)?;
        Some((as_address(word(first_at)?)?, as_address(word(last_at)?)?))
    }

    /// ABI-encode `getAmountsOut(uint256 amountIn, address[] path)` on a V2 router.
    pub fn build_v2_get_amounts_out_payload(amount_in: U256, path: &[Address]) -> Vec<u8> {
        let mut payload = Vec::with_capacity(4 + 32 * (3 + path.len()));
        payload.extend_from_slice(&V2_GET_AMOUNTS_OUT_SELECTOR);
        payload.extend_from_slice(&amount_in.to_be_bytes::<32>());
        payload.extend_from_slice(&U256::from(32 * 2).to_be_bytes::<32>());
        payload.extend_from_slice(&U256::from(path.len()).to_be_bytes::<32>());
        for hop in path {
            payload.extend_from_slice(&[0u8; 12]);
            payload.extend_from_slice(hop.as_slice());
        }
        payload
    }

    /// Last element of the `uint256[]` returned by `getAmountsOut`.
    pub fn decode_v2_amounts_out_last(data: &[u8]) -> Option<U256> {
        let word = |at: usize| -> Option<U256> { data.get(at..at.checked_add(32)?).map(U256::from_be_slice) };
        let offset = usize::try_from(word(0)?).ok()?;
        let len = usize::try_from(word(offset)?).ok()?;
        if len == 0 {
            return None;
        }
        word(offset.checked_add(32)?.checked_add((len - 1).checked_mul(32)?)?)
    }

    /// ABI-encode `NodeInterface.gasEstimateL1Component(to, false, data)`.
    pub fn build_l1_component_estimate_payload(to: Address, data: &[u8]) -> Vec<u8> {
        let padded = data.len().div_ceil(32) * 32;
        let mut payload = Vec::with_capacity(4 + 32 * 4 + padded);
        payload.extend_from_slice(&GAS_ESTIMATE_L1_COMPONENT_SELECTOR);
        payload.extend_from_slice(&[0u8; 12]);
        payload.extend_from_slice(to.as_slice());
        payload.extend_from_slice(&[0u8; 32]); // contractCreation = false
        payload.extend_from_slice(&U256::from(32 * 3).to_be_bytes::<32>());
        payload.extend_from_slice(&U256::from(data.len()).to_be_bytes::<32>());
        payload.extend_from_slice(data);
        payload.resize(4 + 32 * 4 + padded, 0);
        payload
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
        Self::encode_v2_exit(SWAP_EXACT_TOKENS_FOR_ETH_SELECTOR, amount_in, amount_out_min, path, recipient, deadline)
    }

    /// ABI-encode `swapExactTokensForETHSupportingFeeOnTransferTokens(...)`: same arguments as
    /// `swapExactTokensForETH`, but also works for tokens that tax transfers.
    pub fn build_v2_exit_fot_payload(
        amount_in: U256,
        amount_out_min: U256,
        path: &[Address],
        recipient: &Address,
        deadline: u64,
    ) -> Vec<u8> {
        Self::encode_v2_exit(SWAP_EXACT_TOKENS_FOR_ETH_FOT_SELECTOR, amount_in, amount_out_min, path, recipient, deadline)
    }

    fn encode_v2_exit(
        selector: [u8; 4],
        amount_in: U256,
        amount_out_min: U256,
        path: &[Address],
        recipient: &Address,
        deadline: u64,
    ) -> Vec<u8> {
        let mut payload = Vec::with_capacity(4 + 32 * (6 + path.len()));
        payload.extend_from_slice(&selector);
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
        // NaN would cast to a zero factor, i.e. minTokensOut = 0 and no guard at all: fail closed.
        let slippage = if slippage_percent.is_finite() { slippage_percent.clamp(0.0, 90.0) } else { 0.0 };
        let slip_factor = ((100.0 - slippage) / 100.0 * 10_000.0) as u128;
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
    /// Latest `NodeInterface.gasEstimateL1Component` for a ~100 byte buy (L2 gas units).
    l1_component_gas: Arc<AtomicU64>,
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
            l1_component_gas: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record the latest L1 poster-gas estimate (from the background refresher).
    #[inline]
    pub fn set_l1_component_gas(&self, gas: u64) {
        self.l1_component_gas.store(gas, Ordering::Relaxed);
    }

    /// Latest L1 poster-gas estimate in L2 gas units (0 until the first successful query).
    #[inline]
    pub fn l1_component_gas(&self) -> u64 {
        self.l1_component_gas.load(Ordering::Relaxed)
    }

    /// `(priority, max fee)` in wei with `MAX_FEE_GWEI` applied (`cap_gwei <= 0` = no cap).
    /// The priority fee never exceeds the max fee (the tx would be invalid otherwise).
    #[inline]
    pub fn capped_fees(&self, cap_gwei: f64) -> (u128, u128) {
        let (ema_priority_wei, ema_max_fee_wei) = self.calculate_optimal_fees();
        let cap_wei = if cap_gwei.is_finite() { (cap_gwei.max(0.0) * 1e9) as u128 } else { 0 };
        let max_fee_wei = if cap_wei > 0 { ema_max_fee_wei.min(cap_wei) } else { ema_max_fee_wei };
        (ema_priority_wei.min(max_fee_wei), max_fee_wei)
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
