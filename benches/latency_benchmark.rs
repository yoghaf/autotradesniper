use autotradesniper::addr_set::AddressSet;
use autotradesniper::config::Watchlist;
use autotradesniper::payload::{CalldataPayloadBuilder, DynamicFeeAdjuster, TARGET_CALLDATA_BUFFER_SIZE};
use autotradesniper::stream::feed::ClaimRing;
use autotradesniper::stream::{fast_selectors, L2Txs, RawTxView, l2_message_is_valid};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::{Decodable2718, Encodable2718};
use alloy::primitives::{Address, Bytes, TxKind, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

/// A signed EIP-1559 tx, as it appears (RLP, no sender) inside an L2 feed message.
fn raw_tx(to: Address, input: Vec<u8>) -> Vec<u8> {
    let signer = PrivateKeySigner::random();
    let tx = TxEip1559 {
        chain_id: 4663,
        nonce: 7,
        gas_limit: 300_000,
        max_fee_per_gas: 100_000_000,
        max_priority_fee_per_gas: 1_000_000,
        to: TxKind::Call(to),
        value: U256::from(10u64).pow(U256::from(16u64)),
        input: Bytes::from(input),
        access_list: Default::default(),
    };
    let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    TxEnvelope::Eip1559(tx.into_signed(sig)).encoded_2718()
}

fn bench_zero_copy_parsing(c: &mut Criterion) {
    // Pons-style buy (selector + a few words) and an unrelated ERC-20 transfer.
    let mut buy = vec![0x59, 0xa8, 0x7b, 0xc1];
    buy.extend_from_slice(&[0x11; 96]);
    let mut transfer = vec![0xa9, 0x05, 0x9c, 0xbb];
    transfer.extend_from_slice(&[0x22; 64]);
    let relevant = raw_tx(Address::repeat_byte(0x42), buy);
    let irrelevant = raw_tx(Address::repeat_byte(0x43), transfer);

    // What every tx of every block used to pay: a full decode (copies calldata, builds Vecs).
    c.bench_function("tx_full_decode_2718_exact_irrelevant", |b| {
        b.iter(|| black_box(TxEnvelope::decode_2718_exact(black_box(irrelevant.as_slice()))).is_ok());
    });
    // What it costs now: RLP headers walked in place, calldata borrowed, nothing allocated.
    c.bench_function("tx_zero_copy_peek_irrelevant", |b| {
        b.iter(|| {
            let v = RawTxView::parse(black_box(irrelevant.as_slice())).unwrap();
            black_box(fast_selectors::is_copyable_eth_swap(v.input))
        });
    });
    c.bench_function("tx_zero_copy_peek_relevant", |b| {
        b.iter(|| {
            let v = RawTxView::parse(black_box(relevant.as_slice())).unwrap();
            black_box((v.to.is_some(), fast_selectors::is_copyable_eth_swap(v.input)))
        });
    });

    // Sender recovery is not in the payload: this is the cost every *candidate* tx still pays.
    // To compare backends, add the "secp256k1" feature to the alloy dependency (libsecp256k1).
    {
        use alloy::consensus::transaction::SignerRecoverable;
        let envelope = TxEnvelope::decode_2718_exact(relevant.as_slice()).unwrap();
        c.bench_function("tx_ecrecover_sender", |b| {
            b.iter(|| black_box(black_box(&envelope).recover_signer().unwrap()));
        });
    }

    // A block message: batch of 50 txs, 2 of them relevant.
    let mut batch = vec![3u8];
    for i in 0..50 {
        let tx = if i % 25 == 0 { &relevant } else { &irrelevant };
        batch.extend_from_slice(&((tx.len() + 1) as u64).to_be_bytes());
        batch.push(4);
        batch.extend_from_slice(tx);
    }
    c.bench_function("l2_batch_50tx_validate_and_prefilter", |b| {
        b.iter(|| {
            let msg = black_box(batch.as_slice());
            assert!(l2_message_is_valid(msg));
            let mut candidates = 0u32;
            for raw in L2Txs::new(msg) {
                if let Some(v) = RawTxView::parse(raw) {
                    candidates += fast_selectors::is_copyable_eth_swap(v.input) as u32;
                }
            }
            black_box(candidates)
        });
    });
}

fn bench_address_matching(c: &mut Criterion) {
    let watched: Vec<Address> = (1u8..=8).map(Address::repeat_byte).collect();
    let hash_set: HashSet<Address> = watched.iter().copied().collect();
    let set = AddressSet::new(watched.iter());
    let watchlist = Watchlist::new(hash_set.clone());
    let hit = Address::repeat_byte(5);
    let miss = Address::repeat_byte(0xee);

    c.bench_function("watch_hashset_contains_miss", |b| b.iter(|| hash_set.contains(black_box(&miss))));
    c.bench_function("watch_address_set_contains_miss", |b| b.iter(|| set.contains(black_box(&miss))));
    c.bench_function("watch_address_set_contains_hit", |b| b.iter(|| set.contains(black_box(&hit))));
    c.bench_function("watch_watchlist_snapshot_miss", |b| b.iter(|| watchlist.contains(black_box(&miss))));

    // The previous hot-path lookup: async tokio::RwLock read + SipHash probe of a HashSet.
    let old_path = tokio::sync::RwLock::new(hash_set.clone());
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    c.bench_function("watch_OLD_tokio_rwlock_hashset_miss", |b| {
        // One block_on around the whole timed loop, so only `read().await` + probe is measured.
        b.iter_custom(|iters| {
            rt.block_on(async {
                let t = std::time::Instant::now();
                for _ in 0..iters {
                    black_box(old_path.read().await.contains(black_box(&miss)));
                }
                t.elapsed()
            })
        })
    });

    let ring = ClaimRing::default();
    let mut block = 0u64;
    c.bench_function("feed_claim_ring_win_then_duplicate", |b| {
        b.iter(|| {
            block += 1;
            let won = ring.try_claim(black_box(block));
            let dup = ring.try_claim(black_box(block));
            black_box((won, dup))
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
    let weth = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);
    let path = [token, weth];

    c.bench_function("fast_exit_260b_payload_assembly", |b| {
        b.iter(|| {
            let payload = CalldataPayloadBuilder::build_fast_exit_payload(
                black_box(alloy::primitives::U256::from(1_000_000u64)),
                black_box(alloy::primitives::U256::from(1u64)),
                black_box(&path),
                black_box(&recipient),
                black_box(1_900_000_000),
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
        launch_tx_hash: alloy::primitives::B256::ZERO,
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
        launch_tx_hash: alloy::primitives::B256::ZERO,
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
    bench_address_matching,
    bench_fast_method_signatures,
    bench_calldata_buffer_assembly,
    bench_fast_exit_payload,
    bench_ema_fee_calculator,
    bench_atomic_nonce_cursor,
    bench_risk_evaluator_latency,
    bench_prysm_pattern_matching
);
criterion_main!(benches);

