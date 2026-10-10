use autotradesniper::config::AppConfig;
use autotradesniper::models::prysm::{
    LaunchMetadata, PrysmEngine, COMPANION_ACCOUNTS, PRYSMCLOUD_SUBSTRING,
};
use autotradesniper::risk::price_rules::PriceRules;
use autotradesniper::risk::types::{ExitReason, PositionTracker, RiskConfig};
use autotradesniper::types::{Position, PrysmExitMode, TradeSignal};
use alloy::primitives::{address, Address, U256};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

fn create_sample_launch_meta(
    image_url: Option<String>,
    website_url: Option<String>,
    dev_buy_eth: f64,
    creator_tax_bps: u32,
    tax_exempt_wallets: Vec<Address>,
    launch_block: u64,
    market_cap_usd: f64,
) -> LaunchMetadata {
    let token_address = address!("1111111111111111111111111111111111111111");
    let pool_address = address!("2222222222222222222222222222222222222222");
    let creator_address = address!("3333333333333333333333333333333333333333");

    LaunchMetadata {
        token_address,
        pool_address,
        creator_address,
        image_url,
        website_url,
        dev_buy_eth,
        creator_tax_bps,
        tax_exempt_wallets,
        launch_block,
        launch_instant: Instant::now(),
        market_cap_usd,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. POLA A UNIT TESTS: Prysmcloud Storage Image URL
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_pattern_a_valid_matches() {
    // Standard valid Prysmcloud launch
    let meta = create_sample_launch_meta(
        Some("https://vercel-storage.com/prysmcloud/token-images/img1.png".to_string()),
        Some("https://project.xyz".to_string()),
        0.02,
        200,
        vec![],
        100,
        5000.0,
    );
    assert!(PrysmEngine::evaluate_pattern_a(&meta));

    // Lower boundary dev buy (0.005 ETH)
    let meta_min = create_sample_launch_meta(
        Some("https://ipfs.io/prysmcloud/token-images/avatar.jpg".to_string()),
        None,
        0.005,
        300,
        vec![],
        100,
        5000.0,
    );
    assert!(PrysmEngine::evaluate_pattern_a(&meta_min));

    // Upper boundary dev buy (0.06 ETH)
    let meta_max = create_sample_launch_meta(
        Some(format!("https://cdn.xyz/{}", PRYSMCLOUD_SUBSTRING)),
        None,
        0.06,
        100,
        vec![address!("4444444444444444444444444444444444444444")], // 1 non-dev exempt wallet allowed
        100,
        5000.0,
    );
    assert!(PrysmEngine::evaluate_pattern_a(&meta_max));
}

#[test]
fn test_pattern_a_rejections() {
    // Missing prysmcloud substring
    let meta_wrong_url = create_sample_launch_meta(
        Some("https://aws.s3.com/images/token.png".to_string()),
        None,
        0.02,
        200,
        vec![],
        100,
        5000.0,
    );
    assert!(!PrysmEngine::evaluate_pattern_a(&meta_wrong_url));

    // Dev buy below minimum (< 0.005 ETH)
    let meta_low_dev = create_sample_launch_meta(
        Some("https://cdn/prysmcloud/token-images/x.png".to_string()),
        None,
        0.0049,
        200,
        vec![],
        100,
        5000.0,
    );
    assert!(!PrysmEngine::evaluate_pattern_a(&meta_low_dev));

    // Dev buy above maximum (> 0.06 ETH)
    let meta_high_dev = create_sample_launch_meta(
        Some("https://cdn/prysmcloud/token-images/x.png".to_string()),
        None,
        0.061,
        200,
        vec![],
        100,
        5000.0,
    );
    assert!(!PrysmEngine::evaluate_pattern_a(&meta_high_dev));

    // Creator tax exceeds 300 bps (> 3%)
    let meta_high_tax = create_sample_launch_meta(
        Some("https://cdn/prysmcloud/token-images/x.png".to_string()),
        None,
        0.02,
        350,
        vec![],
        100,
        5000.0,
    );
    assert!(!PrysmEngine::evaluate_pattern_a(&meta_high_tax));

    // Tax-exempt wallets count > 1 beyond developer
    let meta_multi_exempt = create_sample_launch_meta(
        Some("https://cdn/prysmcloud/token-images/x.png".to_string()),
        None,
        0.02,
        200,
        vec![
            address!("5555555555555555555555555555555555555555"),
            address!("6666666666666666666666666666666666666666"),
        ],
        100,
        5000.0,
    );
    assert!(!PrysmEngine::evaluate_pattern_a(&meta_multi_exempt));
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. POLA B UNIT TESTS: .lat Domain & Characteristic Dev Buy (~0.021 ETH)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_pattern_b_valid_matches() {
    // Characteristic ~0.021 ETH and .lat domain
    let meta = create_sample_launch_meta(
        None,
        Some("https://sol-swap.lat".to_string()),
        0.021,
        250,
        vec![],
        100,
        6000.0,
    );
    assert!(PrysmEngine::evaluate_pattern_b(&meta));

    // Lower boundary dev buy (0.019 ETH)
    let meta_min = create_sample_launch_meta(
        None,
        Some("http://token-123.lat/".to_string()),
        0.019,
        300,
        vec![],
        100,
        6000.0,
    );
    assert!(PrysmEngine::evaluate_pattern_b(&meta_min));

    // Upper boundary dev buy (0.024 ETH)
    let meta_max = create_sample_launch_meta(
        None,
        Some("https://alpha99.lat".to_string()),
        0.024,
        150,
        vec![],
        100,
        6000.0,
    );
    assert!(PrysmEngine::evaluate_pattern_b(&meta_max));
}

#[test]
fn test_pattern_b_rejections() {
    // Domain not .lat (e.g. .com or .io)
    let meta_wrong_tld = create_sample_launch_meta(
        None,
        Some("https://sol-swap.com".to_string()),
        0.021,
        200,
        vec![],
        100,
        6000.0,
    );
    assert!(!PrysmEngine::evaluate_pattern_b(&meta_wrong_tld));

    // Dev buy below 0.019 ETH (e.g. 0.018 ETH)
    let meta_low_dev = create_sample_launch_meta(
        None,
        Some("https://project.lat".to_string()),
        0.018,
        200,
        vec![],
        100,
        6000.0,
    );
    assert!(!PrysmEngine::evaluate_pattern_b(&meta_low_dev));

    // Dev buy above 0.024 ETH (e.g. 0.025 ETH)
    let meta_high_dev = create_sample_launch_meta(
        None,
        Some("https://project.lat".to_string()),
        0.025,
        200,
        vec![],
        100,
        6000.0,
    );
    assert!(!PrysmEngine::evaluate_pattern_b(&meta_high_dev));

    // Creator tax > 300 bps
    let meta_high_tax = create_sample_launch_meta(
        None,
        Some("https://project.lat".to_string()),
        0.021,
        350,
        vec![],
        100,
        6000.0,
    );
    assert!(!PrysmEngine::evaluate_pattern_b(&meta_high_tax));
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. POLA C UNIT TESTS: Companion Accounts Trade Signals
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_pattern_c_companion_signals() {
    let token = address!("1111111111111111111111111111111111111111");
    let mut meta = create_sample_launch_meta(None, None, 0.01, 100, vec![], 100, 5000.0);
    meta.token_address = token;

    let companion1 = COMPANION_ACCOUNTS[0]; // 0xe89c3d903e84aab1c6327c794ea8e749b60a347c
    let companion2 = COMPANION_ACCOUNTS[1]; // 0x791ef6e64a3fb267502ceb950b57a81671974065
    let random_buyer = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    let now = Instant::now();

    // 1. Companion 1 within 15 seconds -> MATCH
    meta.launch_instant = now - Duration::from_secs(15);
    assert!(PrysmEngine::evaluate_pattern_c(companion1, token, Some(&meta), now));

    // 2. Companion 2 at exactly 60 seconds -> MATCH
    meta.launch_instant = now - Duration::from_secs(60);
    assert!(PrysmEngine::evaluate_pattern_c(companion2, token, Some(&meta), now));

    // 3. Companion 1 at 61 seconds (> 60s window) -> REJECT
    meta.launch_instant = now - Duration::from_secs(61);
    assert!(!PrysmEngine::evaluate_pattern_c(companion1, token, Some(&meta), now));

    // 4. Random non-companion address within 10 seconds -> REJECT
    meta.launch_instant = now - Duration::from_secs(10);
    assert!(!PrysmEngine::evaluate_pattern_c(random_buyer, token, Some(&meta), now));
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. ENTRY GUARDRAILS TESTS (Staleness, Mcap Ceiling, Min Block Guard)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_entry_guardrails_staleness_and_mcap() {
    let config = AppConfig::default();
    config.prysm_enabled.store(true, std::sync::atomic::Ordering::SeqCst);
    *config.prysm_max_signal_age_secs.write().await = 15;
    *config.prysm_max_mcap_usd.write().await = 8000.0;
    *config.prysm_min_block_delay.write().await = 3;

    let (tx, _rx) = mpsc::channel(100);
    let engine = PrysmEngine::new(config, tx);

    let now = Instant::now();
    let mut meta = create_sample_launch_meta(None, None, 0.02, 100, vec![], 100, 5000.0);

    // Case 1: All guardrails valid (age 5s, mcap $5,000, current block 103)
    meta.launch_instant = now - Duration::from_secs(5);
    meta.market_cap_usd = 5000.0;
    assert!(engine.validate_entry_guardrails(&meta, 103, now).await.is_ok());

    // Case 2: Staleness violation (age 16s > 15s)
    meta.launch_instant = now - Duration::from_secs(16);
    let res = engine.validate_entry_guardrails(&meta, 103, now).await;
    assert!(res.is_err());
    assert!(res.unwrap_err().contains("Staleness"));

    // Case 3: Market cap ceiling violation ($8,500 > $8,000 limit)
    meta.launch_instant = now - Duration::from_secs(5);
    meta.market_cap_usd = 8500.0;
    let res = engine.validate_entry_guardrails(&meta, 103, now).await;
    assert!(res.is_err());
    assert!(res.unwrap_err().contains("Market Cap"));

    // Case 4: Min block guard violation (current block 101 < 100 + 3)
    meta.market_cap_usd = 5000.0;
    let res = engine.validate_entry_guardrails(&meta, 101, now).await;
    assert!(res.is_err());
    assert!(res.unwrap_err().contains("Min Block Guard"));
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. QUEUE & RELEASE: MIN BLOCK GUARD (+3 BLOCKS)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_min_block_guard_delayed_release() {
    let config = AppConfig::default();
    config.prysm_enabled.store(true, std::sync::atomic::Ordering::SeqCst);
    *config.prysm_min_block_delay.write().await = 3;
    *config.prysm_size_eth.write().await = 0.01;

    let (tx, mut rx) = mpsc::channel(100);
    let engine = PrysmEngine::new(config, tx);

    let launch_block = 100;
    let meta = create_sample_launch_meta(
        Some("https://vercel/prysmcloud/token-images/pic.png".to_string()),
        None,
        0.02,
        200,
        vec![],
        launch_block,
        5000.0,
    );

    // Inspect launch calldata at Block #100
    // Calldata with prysmcloud image substring
    let calldata = b"launchToken(prysmcloud/token-images/pic.png)";
    engine.inspect_transaction(
        meta.creator_address,
        Some(meta.token_address),
        U256::from(20000000000000000u64), // 0.02 ETH
        calldata,
        alloy::primitives::B256::ZERO,
        launch_block,
        Instant::now(),
    ).await;

    // Verify order is queued and NOT dispatched yet at Block #100, #101, #102
    assert!(rx.try_recv().is_err());

    engine.process_new_block(101).await;
    assert!(rx.try_recv().is_err());

    engine.process_new_block(102).await;
    assert!(rx.try_recv().is_err());

    // Advance to Block #103 (Launch #100 + 3) -> Order must be released and dispatched!
    engine.process_new_block(103).await;

    let dispatched = rx.try_recv();
    assert!(dispatched.is_ok(), "Expected dispatched signal at Block #103");
    let sig: TradeSignal = dispatched.unwrap();
    assert_eq!(sig.block_number, 103);
    assert!(sig.strategy_tag.as_ref().map(|s| s.contains("Prysm")).unwrap_or(false));
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. PRYSM EXIT MODES: Target vs System Mode
// ─────────────────────────────────────────────────────────────────────────────

fn create_sample_prysm_tracker(exit_mode: PrysmExitMode, entry_mcap: f64) -> PositionTracker {
    let pos = Position {
        id: "POS-PRYSM-1".to_string(),
        token_address: "0x1111111111111111111111111111111111111111".to_string(),
        token_symbol: "TKN-PRYSM".to_string(),
        entry_eth_amount: 0.01,
        current_eth_value: 0.01,
        entry_timestamp_secs: 1000,
        pool_address: "0x2222222222222222222222222222222222222222".to_string(),
        is_open: true,
        pnl_percent: 0.0,
        exit_reason: None,
        exit_tx_hash: None,
        strategy: Some("PrysmSeries".to_string()),
        exit_mode: Some(format!("{}", exit_mode)),
        entry_mcap_usd: Some(entry_mcap),
        current_mcap_usd: Some(entry_mcap),
    };

    let mut tracker = PositionTracker::new(pos, 0.01);
    tracker.is_prysm_strategy = true;
    tracker.prysm_exit_mode = Some(exit_mode);
    tracker.prysm_entry_mcap_usd = entry_mcap;
    tracker.current_mcap_usd = entry_mcap;
    tracker
}

#[test]
fn test_prysm_target_mode_take_profit_mcap_10k() {
    let config = RiskConfig::default();
    // Entry at $5,000 USD market cap
    let mut tracker = create_sample_prysm_tracker(PrysmExitMode::Target, 5000.0);

    // 1. Price increases by +50% -> Mcap = $7,500 USD (Not reaching $10,000 yet)
    let sig1 = PriceRules::evaluate(&mut tracker, 0.015, &config);
    assert!(sig1.is_none());
    assert!((tracker.current_mcap_usd - 7500.0).abs() < 0.01);

    // 2. Price reaches +100% -> Mcap = $10,000 USD -> MUST trigger Take-Profit!
    let sig2 = PriceRules::evaluate(&mut tracker, 0.020, &config);
    assert!(sig2.is_some());
    let exit = sig2.unwrap();
    assert_eq!(exit.reason, ExitReason::PrysmTargetMcap);
    assert!(exit.reason_description.contains("$10,000.00 USD"));
}

#[test]
fn test_prysm_target_mode_strict_stop_loss_minus_30() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_prysm_tracker(PrysmExitMode::Target, 5000.0);

    // 1. Price drops by -20% -> Target mode should NOT exit at -20% (Target mode has -30% SL)
    let sig1 = PriceRules::evaluate(&mut tracker, 0.008, &config);
    assert!(sig1.is_none());

    // 2. Price drops by -31% (0.0069 ETH) -> MUST trigger strict -30% SL!
    let sig2 = PriceRules::evaluate(&mut tracker, 0.0069, &config);
    assert!(sig2.is_some());
    let exit = sig2.unwrap();
    assert_eq!(exit.reason, ExitReason::PrysmTargetStopLoss);
    assert!(exit.reason_description.contains("-30.00% Deficit"));
}

#[test]
fn test_prysm_system_mode_falls_back_to_6layer_rules() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_prysm_tracker(PrysmExitMode::System, 5000.0);

    // In System mode, Hard SL is -20% (not -30%)
    let sig = PriceRules::evaluate(&mut tracker, 0.0079, &config); // -21%
    assert!(sig.is_some());
    let exit = sig.unwrap();
    assert_eq!(exit.reason, ExitReason::StopLoss);
}

#[tokio::test]
async fn test_tx_envelope_sign_and_encode() {
    use alloy::consensus::transaction::SignerRecoverable;
    use alloy::consensus::{SignableTransaction, Transaction, TxEip1559, TxEnvelope};
    use alloy::eips::eip2718::{Decodable2718, Encodable2718};
    use alloy::primitives::{Address, Bytes, TxKind, U256};
    use alloy::signers::local::PrivateKeySigner;
    use alloy::signers::Signer;

    let signer = PrivateKeySigner::random();
    let tx = TxEip1559 {
        chain_id: 4663,
        nonce: 0,
        gas_limit: 150_000,
        max_fee_per_gas: 100_000_000,
        max_priority_fee_per_gas: 100_000_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Bytes::from(vec![1, 2, 3, 4]),
        access_list: Default::default(),
    };
    let sig = signer.sign_hash(&tx.signature_hash()).await.unwrap();
    let signed_tx = tx.into_signed(sig);
    let envelope = TxEnvelope::Eip1559(signed_tx);
    let mut raw_bytes = Vec::new();
    envelope.encode_2718(&mut raw_bytes);
    let raw_hex = format!("0x{}", hex::encode(&raw_bytes));
    assert!(raw_hex.starts_with("0x02")); // EIP-1559 type 2 prefix!

    // Verify roundtrip decode
    let mut slice = raw_bytes.as_slice();
    let decoded = TxEnvelope::decode_2718(&mut slice).unwrap();
    assert_eq!(decoded.recover_signer().unwrap(), signer.address());
    assert_eq!(decoded.value(), U256::ZERO);
}

#[test]
fn test_l2_message_unpacking_kind3_and_kind4() {
    fn extract_l2_txs<'a>(bytes: &'a [u8], out: &mut Vec<&'a [u8]>) {
        if bytes.is_empty() {
            return;
        }
        let kind = bytes[0];
        if kind == 4 {
            out.push(&bytes[1..]);
        } else if kind == 3 {
            let mut i = 1;
            while i + 8 <= bytes.len() {
                let n = u64::from_be_bytes(bytes[i..i + 8].try_into().unwrap_or_default()) as usize;
                i += 8;
                if i + n <= bytes.len() {
                    extract_l2_txs(&bytes[i..i + n], out);
                }
                i += n;
            }
        }
    }

    // Construct a mock kind 4 message (single tx)
    let mock_tx1 = vec![0x02, 0xaa, 0xbb];
    let mut msg_kind4 = vec![4u8];
    msg_kind4.extend_from_slice(&mock_tx1);

    let mut out1 = Vec::new();
    extract_l2_txs(&msg_kind4, &mut out1);
    assert_eq!(out1.len(), 1);
    assert_eq!(out1[0], &mock_tx1[..]);

    // Construct a mock kind 3 message (batch of 2 messages)
    let mock_tx2 = vec![0x02, 0xcc, 0xdd, 0xee];
    let mut sub_msg2 = vec![4u8];
    sub_msg2.extend_from_slice(&mock_tx2);

    let mut batch_msg = vec![3u8];
    // Entry 1: length of msg_kind4 (8 bytes) + msg_kind4
    batch_msg.extend_from_slice(&(msg_kind4.len() as u64).to_be_bytes());
    batch_msg.extend_from_slice(&msg_kind4);
    // Entry 2: length of sub_msg2 (8 bytes) + sub_msg2
    batch_msg.extend_from_slice(&(sub_msg2.len() as u64).to_be_bytes());
    batch_msg.extend_from_slice(&sub_msg2);

    let mut out2 = Vec::new();
    extract_l2_txs(&batch_msg, &mut out2);
    assert_eq!(out2.len(), 2);
    assert_eq!(out2[0], &mock_tx1[..]);
    assert_eq!(out2[1], &mock_tx2[..]);
}
