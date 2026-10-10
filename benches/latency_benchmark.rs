use autotradesniper::payload::{CalldataPayloadBuilder, DynamicFeeAdjuster, TARGET_CALLDATA_BUFFER_SIZE};
use autotradesniper::stream::{fast_selectors, ZeroCopyTxParser};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use alloy::primitives::Address;
use std::sync::atomic::{AtomicU64, Ordering};

fn bench_zero_copy_parsing(c: &mut Criterion) {
    let raw_sample_json = br#"{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x1","result":{"hash":"0x88df0147ff268f886da7171e1b669d145d910f138800b0b868b6a030f8d9cdf6","from":"0x742d35cc6634c0532925a3b844bc454e4438f44e","to":"0x1b44f3514812d835eb1bdb0acb32253ddb043d42","input":"0x7ff36ab50000000000000000000000000000000000000000000000000de0b6b3a7640000","value":"0x112210f47de98115"}}}"#;

    c.bench_function("zero_copy_from_address_extraction", |b| {
        b.iter(|| {
            let slice = ZeroCopyTxParser::find_field_hex_slice(black_box(raw_sample_json), b"\"from\"");
            if let Some(s) = slice {
                let addr = ZeroCopyTxParser::parse_address_from_hex(s);
                black_box(addr);
            }
        });
    });

    c.bench_function("zero_copy_to_address_extraction", |b| {
        b.iter(|| {
            let slice = ZeroCopyTxParser::find_field_hex_slice(black_box(raw_sample_json), b"\"to\"");
            if let Some(s) = slice {
                let addr = ZeroCopyTxParser::parse_address_from_hex(s);
                black_box(addr);
            }
        });
    });
}

fn bench_fast_method_signatures(c: &mut Criterion) {
    let selector = [0x7f, 0xf3, 0x6a, 0xb5]; // swapExactETHForTokens

    c.bench_function("fast_signature_1cycle_match", |b| {
        b.iter(|| {
            let is_swap = fast_selectors::is_swap_selector(black_box(&selector));
            black_box(is_swap);
        });
    });
}

fn bench_calldata_buffer_assembly(c: &mut Criterion) {
    let sample_calldata = vec![0x7f, 0xf3, 0x6a, 0xb5, 0x01, 0x02, 0x03, 0x04];

    c.bench_function("zero_fill_calldata_buffer_94500b", |b| {
        b.iter(|| {
            let payload = CalldataPayloadBuilder::build_calibrated_payload(
                black_box(&sample_calldata),
                black_box(TARGET_CALLDATA_BUFFER_SIZE),
            );
            black_box(payload);
        });
    });

    let mut scratch = Vec::with_capacity(100_000);
    c.bench_function("reusable_scratch_calldata_buffer_94500b", |b| {
        b.iter(|| {
            CalldataPayloadBuilder::write_into_scratch(
                black_box(&mut scratch),
                black_box(&sample_calldata),
                black_box(TARGET_CALLDATA_BUFFER_SIZE),
            );
            black_box(&scratch);
        });
    });
}

fn bench_fast_exit_payload(c: &mut Criterion) {
    let token = Address::repeat_byte(0x11);
    let router = Address::repeat_byte(0x22);

    c.bench_function("fast_exit_164b_payload_assembly", |b| {
        b.iter(|| {
            let payload = CalldataPayloadBuilder::build_fast_exit_payload(
                black_box(&token),
                black_box(&router),
            );
            black_box(payload);
        });
    });
}

fn bench_ema_fee_calculator(c: &mut Criterion) {
    let fee_adjuster = DynamicFeeAdjuster::new(2.5, 1.25);

    c.bench_function("ema_dynamic_fee_calculation", |b| {
        b.iter(|| {
            let (prio, max) = fee_adjuster.calculate_optimal_fees();
            black_box((prio, max));
        });
    });
}

fn bench_atomic_nonce_cursor(c: &mut Criterion) {
    let nonce = AtomicU64::new(100);

    c.bench_function("atomic_in_memory_nonce_fetch_add", |b| {
        b.iter(|| {
            let n = nonce.fetch_add(1, Ordering::SeqCst);
            black_box(n);
        });
    });
}

fn bench_risk_evaluator_latency(c: &mut Criterion) {
    use autotradesniper::risk::evaluator::RiskEvaluator;
    use autotradesniper::risk::types::{BlockEvent, PositionTracker, RiskConfig};
    use autotradesniper::types::Position;
    use std::time::Instant;

    let config = RiskConfig::default();
    let pos = Position {
        id: "POS-BENCH-1".to_string(),
        token_address: "0x1111111111111111111111111111111111111111".to_string(),
        token_symbol: "TKN".to_string(),
        entry_eth_amount: 1.0,
        current_eth_value: 1.0,
        entry_timestamp_secs: 1000,
        pool_address: "0x2222222222222222222222222222222222222222".to_string(),
        is_open: true,
        pnl_percent: 0.0,
        exit_reason: None,
        exit_tx_hash: None,
        strategy: None,
        exit_mode: None,
        entry_mcap_usd: None,
        current_mcap_usd: None,
    };
    let mut tracker = PositionTracker::new(pos, 1.0);
    let block = BlockEvent {
        block_number: 999999,
        timestamp_secs: 1005,
        spot_price_eth: 1.05,
        curve_capacity_percent: 60.0,
        token_address: Address::repeat_byte(0x11),
    };
    let now = Instant::now();

    c.bench_function("risk_multi_layer_evaluator_block_decision", |b| {
        b.iter(|| {
            let signal = RiskEvaluator::evaluate_block_event(
                black_box(&mut tracker),
                black_box(&block),
                black_box(Some(1.05)),
                black_box(now),
                black_box(&config),
            );
            black_box(signal);
        });
    });
}

fn bench_prysm_pattern_matching(c: &mut Criterion) {
    use autotradesniper::models::prysm::{LaunchMetadata, PrysmEngine, COMPANION_ACCOUNTS};
    use alloy::primitives::address;
    use std::time::Instant;

    let meta_a = LaunchMetadata {
        token_address: address!("1111111111111111111111111111111111111111"),
        pool_address: address!("2222222222222222222222222222222222222222"),
        creator_address: address!("3333333333333333333333333333333333333333"),
        image_url: Some("https://vercel-storage.com/prysmcloud/token-images/tok.png".to_string()),
        website_url: None,
        dev_buy_eth: 0.02,
        creator_tax_bps: 200,
        tax_exempt_wallets: vec![],
        launch_block: 100,
        launch_instant: Instant::now(),
        market_cap_usd: 5000.0,
    };

    let meta_b = LaunchMetadata {
        token_address: address!("1111111111111111111111111111111111111111"),
        pool_address: address!("2222222222222222222222222222222222222222"),
        creator_address: address!("3333333333333333333333333333333333333333"),
        image_url: None,
        website_url: Some("https://sol-swap.lat".to_string()),
        dev_buy_eth: 0.021,
        creator_tax_bps: 200,
        tax_exempt_wallets: vec![],
        launch_block: 100,
        launch_instant: Instant::now(),
        market_cap_usd: 5000.0,
    };

    c.bench_function("prysm_pattern_a_evaluation_latency", |b| {
        b.iter(|| {
            let res = PrysmEngine::evaluate_pattern_a(black_box(&meta_a));
            black_box(res);
        });
    });

    c.bench_function("prysm_pattern_b_evaluation_latency", |b| {
        b.iter(|| {
            let res = PrysmEngine::evaluate_pattern_b(black_box(&meta_b));
            black_box(res);
        });
    });

    let companion = COMPANION_ACCOUNTS[0];
    let token = meta_a.token_address;
    let now = Instant::now();

    c.bench_function("prysm_pattern_c_evaluation_latency", |b| {
        b.iter(|| {
            let res = PrysmEngine::evaluate_pattern_c(
                black_box(companion),
                black_box(token),
                black_box(Some(&meta_a)),
                black_box(now),
            );
            black_box(res);
        });
    });
}

criterion_group!(
    benches,
    bench_zero_copy_parsing,
    bench_fast_method_signatures,
    bench_calldata_buffer_assembly,
    bench_fast_exit_payload,
    bench_ema_fee_calculator,
    bench_atomic_nonce_cursor,
    bench_risk_evaluator_latency,
    bench_prysm_pattern_matching
);
criterion_main!(benches);

