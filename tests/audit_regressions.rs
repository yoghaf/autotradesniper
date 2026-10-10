//! Regression tests for the findings in AUDIT.md. Each test pins the fixed behavior of one
//! finding; the pre-fix code failed (or panicked on) every one of them.

use alloy::primitives::{address, Address, B256, U256};
use alloy::sol_types::SolCall;
use autotradesniper::config::AppConfig;
use autotradesniper::models::prysm::{is_prysm_tag, LaunchMetadata, PrysmEngine, PRYSMCLOUD_SUBSTRING};
use autotradesniper::payload::{
    CalldataPayloadBuilder, DynamicFeeAdjuster, MAX_SEQUENCER_TX_BYTES, TX_ENVELOPE_OVERHEAD,
};
use autotradesniper::risk::evaluator::RiskEvaluator;
use autotradesniper::risk::types::{BlockEvent, PositionTracker, RiskConfig};
use autotradesniper::stream::{extract_l2_txs, StreamListener, MAX_L2_BATCH_DEPTH};
use autotradesniper::telegram::html_escape;
use autotradesniper::types::{Position, PrysmExitMode};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

alloy::sol! {
    function swapExactTokensForETH(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline);
    function swapExactETHForTokens(uint256 amountOutMin, address[] path, address to, uint256 deadline);
}

const LAUNCHPAD: Address = address!("00000000000000000000000000000000000fac70");
const ATTACKER: Address = address!("00000000000000000000000000000000deadbeef");

fn wrap_batch(inner: &[u8]) -> Vec<u8> {
    let mut out = vec![3u8];
    out.extend_from_slice(&(inner.len() as u64).to_be_bytes());
    out.extend_from_slice(inner);
    out
}

// ── L2 unpacker ─────────────────────────────────────────────────────────────

#[test]
fn l2_unpacker_survives_hostile_length_prefix() {
    // u64::MAX length: `i + n` overflowed and sliced `bytes[9..8]` -> panic (debug and release).
    let mut msg = vec![3u8];
    msg.extend_from_slice(&u64::MAX.to_be_bytes());
    msg.extend_from_slice(&[4, 1, 2, 3]);
    let mut out = Vec::new();
    extract_l2_txs(&msg, &mut out);
    assert!(out.is_empty());
}

#[test]
fn l2_unpacker_keeps_txs_before_truncated_segment() {
    let mut msg = wrap_batch(&[4, 0xaa]);
    msg.extend_from_slice(&100u64.to_be_bytes()); // claims 100 bytes, only 2 follow
    msg.extend_from_slice(&[4, 0xbb]);
    let mut out = Vec::new();
    extract_l2_txs(&msg, &mut out);
    assert_eq!(out, vec![&[0xaa][..]]);
}

#[test]
fn l2_unpacker_enforces_nitro_batch_depth() {
    let mut ok = vec![4u8, 0xcc];
    for _ in 0..MAX_L2_BATCH_DEPTH {
        ok = wrap_batch(&ok);
    }
    let mut out = Vec::new();
    extract_l2_txs(&ok, &mut out);
    assert_eq!(out.len(), 1, "16 nested batches are valid in Nitro");

    // One level deeper invalidates the whole message (Nitro executes none of its txs).
    let too_deep = wrap_batch(&ok);
    let mut out = Vec::new();
    extract_l2_txs(&too_deep, &mut out);
    assert!(out.is_empty());
}

// ── Prysm guardrails ─────────────────────────────────────────────────────────

fn spoof_calldata() -> Vec<u8> {
    format!("\0\0https://x.{}/a.png\0", PRYSMCLOUD_SUBSTRING).into_bytes()
}

async fn prysm_engine(launchpads: &[Address]) -> (PrysmEngine, mpsc::Receiver<autotradesniper::types::TradeSignal>) {
    let mut config = AppConfig::default();
    config.prysm_enabled.store(true, std::sync::atomic::Ordering::SeqCst);
    *config.prysm_min_block_delay.write().await = 3;
    config.prysm_launchpads = Arc::new(launchpads.iter().copied().collect());
    let (tx, rx) = mpsc::channel(16);
    (PrysmEngine::new(config, tx), rx)
}

#[tokio::test]
async fn prysm_ignores_marker_calldata_sent_to_unlisted_contract() {
    // Pre-fix: a zero-value tx to ANY address with the marker string armed Pola A and, three
    // blocks later, sent our entry ETH to that address.
    let (engine, mut rx) = prysm_engine(&[]).await;
    engine
        .inspect_transaction(address!("0000000000000000000000000000000000000abc"), Some(ATTACKER), U256::ZERO, &spoof_calldata(), B256::ZERO, 100, Instant::now())
        .await;
    engine.process_new_block(103).await;
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn prysm_zero_value_launch_fails_dev_buy_check() {
    // Pre-fix: dev buy fell back to a hard-coded 0.021 ETH, so value 0 always passed.
    let (engine, mut rx) = prysm_engine(&[LAUNCHPAD]).await;
    engine
        .inspect_transaction(address!("0000000000000000000000000000000000000abc"), Some(LAUNCHPAD), U256::ZERO, &spoof_calldata(), B256::ZERO, 100, Instant::now())
        .await;
    engine.process_new_block(103).await;
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn prysm_dedups_same_launch_but_not_other_launches() {
    let (engine, mut rx) = prysm_engine(&[LAUNCHPAD]).await;
    let creator = address!("0000000000000000000000000000000000000abc");
    let value = U256::from(20_000_000_000_000_000u64); // 0.02 ETH
    let launch_a = B256::repeat_byte(0xa1);
    let launch_b = B256::repeat_byte(0xb2);
    // Launch A observed twice (feed + HTTP bridge), then an unrelated launch B.
    for (tx_hash, block) in [(launch_a, 100), (launch_a, 100), (launch_b, 101)] {
        engine
            .inspect_transaction(creator, Some(LAUNCHPAD), value, &spoof_calldata(), tx_hash, block, Instant::now())
            .await;
    }
    engine.process_new_block(104).await;
    let first = rx.try_recv().expect("launch A released");
    assert!(is_prysm_tag(first.strategy_tag.as_deref()));
    rx.try_recv().expect("launch B released (other launches on the same launchpad still count)");
    assert!(rx.try_recv().is_err(), "the duplicate of launch A must not create a second entry");
}

#[tokio::test]
async fn prysm_staleness_uses_subsecond_precision() {
    let (engine, _rx) = prysm_engine(&[]).await;
    let now = Instant::now();
    let meta = LaunchMetadata {
        token_address: Address::repeat_byte(1),
        pool_address: Address::repeat_byte(2),
        creator_address: Address::repeat_byte(3),
        image_url: None,
        website_url: None,
        dev_buy_eth: 0.02,
        creator_tax_bps: 100,
        tax_exempt_wallets: vec![],
        launch_block: 100,
        launch_instant: now - Duration::from_millis(15_500),
        market_cap_usd: 5000.0,
    };
    // 15.5s truncated to 15s used to pass a 15s limit.
    assert!(engine.validate_entry_guardrails(&meta, 103, now).await.is_err());
}

#[test]
fn strategy_tag_helper_matches_emitted_tags() {
    assert!(is_prysm_tag(Some("Prysm: Pola A (Prysmcloud Image Host)")));
    assert!(!is_prysm_tag(None));
    assert!(!is_prysm_tag(Some("Watchlist")));
}

// ── Watchlist copy-trading ───────────────────────────────────────────────────

fn listener(config: AppConfig) -> (StreamListener, mpsc::Receiver<autotradesniper::types::TradeSignal>) {
    let (tx, rx) = mpsc::channel(16);
    let prysm = Arc::new(PrysmEngine::new(config.clone(), tx.clone()));
    (StreamListener::new(config, Arc::new(DynamicFeeAdjuster::new(2.5, 1.25)), prysm, tx), rx)
}

fn v2_buy(recipient: Address) -> Vec<u8> {
    swapExactETHForTokensCall {
        amountOutMin: U256::from(1u64),
        path: vec![Address::repeat_byte(0xee), Address::repeat_byte(0x7a)],
        to: recipient,
        deadline: U256::from(1_900_000_000u64),
    }
    .abi_encode()
}

#[tokio::test]
async fn watchlist_plain_eth_transfer_is_not_copied() {
    let config = AppConfig::default();
    let watched = address!("742d35cc6634c0532925a3b844bc454e4438f44e");
    config.watchlist_accounts.write().await.insert(watched);
    let (listener, mut rx) = listener(config);
    let cex_deposit = address!("00000000000000000000000000000000000c0ffe");
    listener
        .inspect_transaction_fast(watched, Some(cex_deposit), U256::from(10u64).pow(U256::from(18u64)), &[], B256::ZERO, 1, Instant::now())
        .await;
    assert!(rx.try_recv().is_err(), "a plain transfer must not trigger a copy-trade");

    listener
        .inspect_transaction_fast(watched, Some(Address::repeat_byte(0x55)), U256::from(1u64), &v2_buy(watched), B256::ZERO, 1, Instant::now())
        .await;
    let sig = rx.try_recv().expect("V2 ETH->token swap is copied");
    assert_eq!(sig.target_token, Address::repeat_byte(0x7a));
}

#[test]
fn copy_trade_calldata_pays_out_to_our_account() {
    let theirs = Address::repeat_byte(0x99);
    let ours = Address::repeat_byte(0x42);
    let rewritten = CalldataPayloadBuilder::retarget_swap_recipient(&v2_buy(theirs), ours).unwrap();
    let decoded = swapExactETHForTokensCall::abi_decode(&rewritten).unwrap();
    assert_eq!(decoded.to, ours);
    assert_eq!(decoded.path.len(), 2);

    // Selectors that hard-code amounts/recipients elsewhere are refused, not replayed.
    let universal_router = [0x35, 0x93, 0x56, 0x4c, 0, 0, 0, 0];
    assert!(CalldataPayloadBuilder::retarget_swap_recipient(&universal_router, ours).is_none());
}

// ── Payload construction ─────────────────────────────────────────────────────

#[test]
fn fast_exit_payload_is_valid_abi() {
    let token = Address::repeat_byte(0x11);
    let weth = Address::repeat_byte(0x22);
    let me = Address::repeat_byte(0x33);
    let payload = CalldataPayloadBuilder::build_fast_exit_payload(U256::from(1234u64), U256::from(5u64), &[token, weth], &me, 1_900_000_000);
    let call = swapExactTokensForETHCall::abi_decode(&payload).expect("decodes as swapExactTokensForETH");
    assert_eq!(call.amountIn, U256::from(1234u64));
    assert_eq!(call.amountOutMin, U256::from(5u64));
    assert_eq!(call.path, vec![token, weth]);
    assert_eq!(call.to, me);
    assert_eq!(call.deadline, U256::from(1_900_000_000u64));
}

#[test]
fn padding_keeps_tx_under_sequencer_size_cap() {
    let big_calldata = vec![1u8; 2_000];
    let padding = CalldataPayloadBuilder::clamp_padding(big_calldata.len(), 94_500);
    assert!(big_calldata.len() + padding + TX_ENVELOPE_OVERHEAD <= MAX_SEQUENCER_TX_BYTES);
    assert_eq!(CalldataPayloadBuilder::clamp_padding(100, 1_000), 1_000);

    let payload = CalldataPayloadBuilder::build_calibrated_payload(&[7, 8], 3);
    assert_eq!(payload, vec![7, 8, 0, 0, 0]);
    let mut scratch = Vec::new();
    CalldataPayloadBuilder::write_into_scratch(&mut scratch, &[7, 8], 3);
    assert_eq!(scratch, payload);
    assert_eq!(CalldataPayloadBuilder::intrinsic_gas(&[0, 1], 10), 21_000 + 4 + 16 + 40);
}

// ── Risk rules ───────────────────────────────────────────────────────────────

#[test]
fn prysm_target_mode_is_not_cut_by_flow_rules() {
    let pos = Position {
        id: "POS-1".into(),
        token_address: format!("{:?}", Address::repeat_byte(0x11)),
        token_symbol: "TKN".into(),
        entry_eth_amount: 0.01,
        current_eth_value: 0.01,
        entry_timestamp_secs: 0,
        pool_address: format!("{:?}", Address::repeat_byte(0x22)),
        is_open: true,
        pnl_percent: 0.0,
        exit_reason: None,
        exit_tx_hash: None,
        strategy: Some("Prysm: Pola A".into()),
        exit_mode: None,
        entry_mcap_usd: Some(5000.0),
        current_mcap_usd: Some(5000.0),
    };
    let mut tracker = PositionTracker::new(pos, 0.01);
    tracker.is_prysm_strategy = true;
    tracker.prysm_exit_mode = Some(PrysmExitMode::Target);
    tracker.prysm_entry_mcap_usd = 5000.0;
    // No outside inflow for 26s: System mode would exit on Inactivity Decay.
    let now = Instant::now();
    tracker.entry_instant = now - Duration::from_secs(26);
    tracker.last_outside_inflow_instant = now - Duration::from_secs(26);

    let event = BlockEvent { block_number: 1, timestamp_secs: 0, spot_price_eth: 0.0105, curve_capacity_percent: 95.0, token_address: Address::repeat_byte(0x11) };
    let config = RiskConfig::default();
    assert!(RiskEvaluator::evaluate_block_event(&mut tracker, &event, None, now, &config).is_none());

    // Target thresholds come from config, not literals.
    let config = RiskConfig { prysm_target_mcap_usd: 5_200.0, ..RiskConfig::default() };
    assert!(RiskEvaluator::evaluate_block_event(&mut tracker, &event, None, now, &config).is_some());
}

#[test]
fn telegram_html_escape() {
    assert_eq!(html_escape("L2: Quiet (< 0.1 ETH) & >= 90%"), "L2: Quiet (&lt; 0.1 ETH) &amp; &gt;= 90%");
}

// ── Pons Curve & ERC-20 Encoders ──────────────────────────────────────────

#[test]
fn pons_buy_payload_matches_curve_abi() {
    let eth_in = U256::from(50_000_000_000_000_000u64); // 0.05 ETH
    let min_tokens_out = U256::from(1_000_000);
    let recipient = address!("1111111111111111111111111111111111111111");

    let payload = CalldataPayloadBuilder::build_pons_buy_payload(eth_in, min_tokens_out, recipient);
    assert_eq!(&payload[..4], &autotradesniper::payload::PONS_BUY_SELECTOR);
    assert_eq!(&payload[..4], &[0x59, 0xa8, 0x7b, 0xc1]);
    assert_eq!(payload.len(), 4 + 32 * 3);

    // Retarget recipient
    let other_recipient = address!("2222222222222222222222222222222222222222");
    let retargeted = CalldataPayloadBuilder::retarget_pons_buy_recipient(&payload, other_recipient);
    assert!(retargeted.is_some());
    let retargeted = retargeted.unwrap();
    assert_eq!(&retargeted[4 + 32 * 2 + 12..], other_recipient.as_slice());
}

#[test]
fn pons_sell_payload_matches_curve_abi() {
    let tokens_in = U256::from(500_000_000);
    let min_eth_out = U256::from(10_000_000_000_000_000u64); // 0.01 ETH
    let recipient = address!("3333333333333333333333333333333333333333");

    let payload = CalldataPayloadBuilder::build_pons_sell_payload(tokens_in, min_eth_out, recipient);
    assert_eq!(&payload[..4], &autotradesniper::payload::PONS_SELL_SELECTOR);
    assert_eq!(&payload[..4], &[0xd0, 0x4c, 0x69, 0x83]);
    assert_eq!(payload.len(), 4 + 32 * 3);
}

#[test]
fn erc20_approve_and_balance_payloads() {
    let spender = address!("4444444444444444444444444444444444444444");
    let approve_payload = CalldataPayloadBuilder::build_erc20_approve_payload(spender, U256::MAX);
    assert_eq!(&approve_payload[..4], &autotradesniper::payload::ERC20_APPROVE_SELECTOR);
    assert_eq!(&approve_payload[..4], &[0x09, 0x5e, 0xa7, 0xb3]);
    assert_eq!(approve_payload.len(), 4 + 32 * 2);

    let balance_payload = CalldataPayloadBuilder::build_erc20_balance_of_payload(spender);
    assert_eq!(&balance_payload[..4], &autotradesniper::payload::ERC20_BALANCE_OF_SELECTOR);
    assert_eq!(&balance_payload[..4], &[0x70, 0xa0, 0x82, 0x31]);
    assert_eq!(balance_payload.len(), 4 + 32);
}

#[test]
fn topic_address_parsing_and_reserve_math() {
    let topic_hex = "0x00000000000000000000000016d69c46f20965157584371788cdf557dfde2c53";
    let addr = autotradesniper::models::prysm::parse_address_from_topic(topic_hex);
    assert_eq!(addr, Some(address!("16d69c46f20965157584371788cdf557dfde2c53")));

    // Curve reserves decode test
    let mut raw = vec![0u8; 64];
    // 1.7584 ETH = 1758400000000000000 wei
    let eth_res = U256::from(1758400000000000000u64);
    // 1,000,000,000 tokens
    let tok_res = U256::from(1_000_000_000u64);
    raw[..32].copy_from_slice(&eth_res.to_be_bytes::<32>());
    raw[32..64].copy_from_slice(&tok_res.to_be_bytes::<32>());

    let decoded = CalldataPayloadBuilder::decode_get_reserves_output(&raw);
    assert_eq!(decoded, Some((eth_res, tok_res)));

    // Net ETH calculation for selling 10,000,000 tokens
    let eth_out = CalldataPayloadBuilder::calculate_pons_eth_out(eth_res, tok_res, U256::from(10_000_000), 100, 200);
    assert!(eth_out > U256::ZERO);
}

// ── Block +1, Zero-Delay Companion & Revert Guard Tests ─────────────────────

#[tokio::test]
async fn block_plus_one_delay_scheduling() {
    let mut config = AppConfig::default();
    config.prysm_enabled.store(true, std::sync::atomic::Ordering::SeqCst);
    *config.prysm_min_block_delay.write().await = 1; // Block +1
    *config.prysm_size_eth.write().await = 0.05;
    config.prysm_launchpads = Arc::new([LAUNCHPAD].into_iter().collect());

    let (tx, mut rx) = mpsc::channel(16);
    let engine = PrysmEngine::new(config, tx);

    let calldata = spoof_calldata();
    // Launch at block 100
    engine
        .inspect_transaction(
            address!("1111111111111111111111111111111111111111"),
            Some(LAUNCHPAD),
            U256::from(21_000_000_000_000_000u64), // 0.021 ETH
            &calldata,
            B256::from([1u8; 32]),
            100,
            Instant::now(),
        )
        .await;

    // At block 100, must NOT be dispatched
    assert!(rx.try_recv().is_err());

    // At block 101 (Block +1), must be released and dispatched!
    engine.process_new_block(101).await;
    let signal = rx.try_recv().expect("Should dispatch at Block +1");
    assert_eq!(signal.block_number, 101);
}

#[tokio::test]
async fn companion_pattern_c_zero_delay_execution() {
    let mut config = AppConfig::default();
    config.prysm_enabled.store(true, std::sync::atomic::Ordering::SeqCst);
    *config.prysm_min_block_delay.write().await = 3; // Even if config delay is 3, Pattern C must have 0 delay!
    let launchpad = LAUNCHPAD;
    config.prysm_launchpads = Arc::new([launchpad].into_iter().collect());

    let (tx, mut rx) = mpsc::channel(16);
    let engine = PrysmEngine::new(config, tx);

    // 1. Register launch
    let calldata = spoof_calldata();
    engine
        .inspect_transaction(
            address!("1111111111111111111111111111111111111111"),
            Some(launchpad),
            U256::from(21_000_000_000_000_000u64),
            &calldata,
            B256::from([1u8; 32]),
            100,
            Instant::now(),
        )
        .await;

    // 2. Companion account buys immediately
    let companion = autotradesniper::models::prysm::COMPANION_ACCOUNTS[0];
    let buy_calldata = vec![0x59, 0xa8, 0x7b, 0xc1]; // Pons buy
    engine
        .inspect_transaction(
            companion,
            Some(launchpad),
            U256::from(50_000_000_000_000_000u64),
            &buy_calldata,
            B256::from([2u8; 32]),
            100,
            Instant::now(),
        )
        .await;

    // Must be dispatched IMMEDIATELY without waiting for block advance!
    let signal = rx.try_recv().expect("Pattern C should execute with 0 block delay");
    assert!(signal.strategy_tag.unwrap().contains("Pola C"));
}

#[test]
fn pons_buy_copy_trade_recognition_and_retargeting() {
    let buy_payload = CalldataPayloadBuilder::build_pons_buy_payload(
        U256::from(50_000_000_000_000_000u64),
        U256::from(10_000_000),
        address!("1111111111111111111111111111111111111111"),
    );

    // 1. is_copyable_eth_swap recognizes Pons buy selector
    assert!(autotradesniper::stream::fast_selectors::is_copyable_eth_swap(&buy_payload));

    // 2. retarget_swap_recipient retargets Pons buy recipient to our account
    let our_account = address!("9999999999999999999999999999999999999999");
    let retargeted = CalldataPayloadBuilder::retarget_swap_recipient(&buy_payload, our_account);
    assert!(retargeted.is_some());
    let retargeted = retargeted.unwrap();
    assert_eq!(&retargeted[4 + 32 * 2 + 12..], our_account.as_slice());
}

#[test]
fn pons_buy_revert_guard_protects_capital() {
    let eth_in = U256::from(50_000_000_000_000_000u64); // 0.05 ETH
    let er = U256::from(autotradesniper::payload::PONS_INITIAL_ETH_RESERVE_WEI); // 1.68 ETH
    let tr = U256::from(1_000_000_000u64) * U256::from(1_000_000_000_000_000_000u64); // 1e9 tokens
    let slippage = 25.0; // 25%

    // Calculate revert guard min_tokens_out (quoted for 6.18% snipe tax + 25% slippage)
    let min_tokens_out = CalldataPayloadBuilder::calculate_pons_buy_revert_guard(eth_in, Some(er), Some(tr), slippage);
    assert!(min_tokens_out > U256::ZERO);

    // In Block +1 (6.18% snipe tax): actual tokens out must exceed min_tokens_out
    let actual_tokens_block1 = CalldataPayloadBuilder::calculate_pons_tokens_out(
        er,
        tr,
        eth_in,
        autotradesniper::payload::PONS_BASE_FEE_BPS,
        autotradesniper::payload::PONS_SNIPE_TAX_BPS,
    );
    assert!(actual_tokens_block1 >= min_tokens_out, "Block +1 fill must succeed");

    // In Block 0 (99% tax): actual tokens out is ~99% lower, far below min_tokens_out!
    let actual_tokens_block0 = CalldataPayloadBuilder::calculate_pons_tokens_out(
        er,
        tr,
        eth_in,
        100,
        9900, // 99% tax
    );
    assert!(
        actual_tokens_block0 < min_tokens_out,
        "Block 0 with 99% tax must violate min_tokens_out and REVERT on-chain, protecting capital"
    );
}

#[test]
fn lightweight_zero_buffer_calldata_size() {
    let buy_payload = CalldataPayloadBuilder::build_pons_buy_payload(
        U256::from(50_000_000_000_000_000u64),
        U256::from(10_000_000),
        address!("1111111111111111111111111111111111111111"),
    );

    let calibrated = CalldataPayloadBuilder::build_calibrated_payload(&buy_payload, 0);
    assert_eq!(calibrated.len(), buy_payload.len());
    assert_eq!(calibrated.len(), 4 + 32 * 3); // exactly 100 bytes!
}

