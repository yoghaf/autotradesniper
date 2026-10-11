//! Cost of the in-memory curve replica, in release mode:
//!
//! ```text
//! cargo bench --bench mirror_latency
//! ```
//!
//! * read path: one `read()` + valuation of a position (what the risk loop does per position),
//! * feed path: what every transaction of every block pays in `observe_raw_tx`, for the cases that
//!   matter (nothing tracked, a trade on a tracked curve, an unrelated tx scanned for the curve's
//!   address, a 94.5 KB carrier tx).

use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::Encodable2718;
use alloy::primitives::{Address, Bytes, TxKind, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use autotradesniper::payload::{self, CalldataPayloadBuilder, TARGET_CALLDATA_BUFFER_SIZE};
use autotradesniper::risk::curve_mirror::{CurveMirror, MirrorPolicy};
use std::hint::black_box;
use std::time::{Duration, Instant};

const CURVE: Address = Address::new([0x0c; 20]);

fn raw_tx(signer: &PrivateKeySigner, nonce: u64, to: Address, value: U256, input: Vec<u8>) -> Vec<u8> {
    let tx = TxEip1559 {
        chain_id: 4663,
        nonce,
        gas_limit: 300_000,
        max_fee_per_gas: 100_000_000,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(to),
        value,
        input: Bytes::from(input),
        access_list: Default::default(),
    };
    let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    TxEnvelope::Eip1559(tx.into_signed(sig)).encoded_2718()
}

/// Median and p99 of `iters` timed runs of `f`, in nanoseconds.
fn ns<F: FnMut()>(iters: usize, mut f: F) -> (f64, f64) {
    for _ in 0..iters / 10 {
        f();
    }
    // Batches of 64 calls per sample: Instant itself costs ~20 ns.
    let mut samples: Vec<f64> = (0..iters / 64)
        .map(|_| {
            let t = Instant::now();
            for _ in 0..64 {
                f();
            }
            t.elapsed().as_nanos() as f64 / 64.0
        })
        .collect();
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (samples[samples.len() / 2], samples[samples.len() * 99 / 100])
}

fn main() {
    let signer = PrivateKeySigner::random();
    let policy = MirrorPolicy { stale_after: Duration::from_secs(3_600), ..MirrorPolicy::default() };
    let m = CurveMirror::new(policy);
    let (e, t) = payload::pons_reserves_after_buy(U256::from(300_000_000_000_000_000u64));
    let mut block = 1_000u64;
    m.begin_block(block, 1, true);
    m.end_block(block);
    m.track(CURVE);
    assert!(m.apply_anchor(CURVE, block, e, t));
    // Some history so the volatility window is full.
    for i in 0..32u64 {
        block += 1;
        m.begin_block(block, 1, true);
        let q = U256::from(1_000_000_000_000_000u64);
        let buy = raw_tx(&signer, i, CURVE, q, CalldataPayloadBuilder::build_pons_buy_payload(q, U256::ZERO, signer.address()));
        m.observe_raw_tx(block, 0, &buy);
        m.end_block(block);
    }
    let held = U256::from(5_000_000u64) * U256::from(10u64).pow(U256::from(18u64));

    println!("── read path (per position, per evaluation) ──");
    let (med, p99) = ns(2_000_000, || {
        let snap = m.read(black_box(&CURVE)).fresh().unwrap();
        black_box(snap.value_of(black_box(held)));
    });
    println!("read() + value_of(held)                       median {:>7.1} ns   p99 {:>7.1} ns", med, p99);
    let (med, _) = ns(2_000_000, || {
        black_box(CalldataPayloadBuilder::calculate_pons_eth_out(black_box(e), black_box(t), black_box(held), 300, 0));
    });
    println!("  of which the U256 curve math                median {:>7.1} ns", med);

    println!("\n── feed path (per transaction in a block) ──");
    let q = U256::from(1_000_000_000_000_000u64);
    let curve_buy = raw_tx(&signer, 99, CURVE, q, CalldataPayloadBuilder::build_pons_buy_payload(q, U256::ZERO, signer.address()));
    let mut small = vec![0xa9, 0x05, 0x9c, 0xbb];
    small.extend_from_slice(&[0x11; 64]);
    let unrelated = raw_tx(&signer, 100, Address::repeat_byte(0x42), U256::ZERO, small);
    let mut routed = vec![0x12, 0x34, 0x56, 0x78];
    routed.extend_from_slice(&[0x33; 4_000]);
    let router_4k = raw_tx(&signer, 101, Address::repeat_byte(0x43), U256::ZERO, routed);
    let carrier = raw_tx(&signer, 102, signer.address(), U256::ZERO, vec![0u8; TARGET_CALLDATA_BUFFER_SIZE]);

    let idle = CurveMirror::new(policy);
    let (med, _) = ns(2_000_000, || idle.observe_raw_tx(black_box(block), 0, black_box(&unrelated)));
    println!("nothing tracked (any tx)                      median {:>7.1} ns", med);
    let (med, _) = ns(2_000_000, || m.observe_raw_tx(black_box(block), 1, black_box(&unrelated)));
    println!("tracked curve, unrelated 68-byte call         median {:>7.1} ns", med);
    let (med, _) = ns(400_000, || m.observe_raw_tx(black_box(block), 2, black_box(&router_4k)));
    println!("tracked curve, 4 KB call scanned for it       median {:>7.1} ns", med);
    let (med, _) = ns(200_000, || m.observe_raw_tx(black_box(block), 3, black_box(&carrier)));
    println!("tracked curve, 94.5 KB carrier tx             median {:>7.1} ns", med);
    // A trade: each call appends one trade to the journal (bounded by max_journal), so time fresh
    // blocks of one trade each and re-anchor in between.
    let mut samples = Vec::new();
    for i in 0..2_000u64 {
        block += 1;
        m.begin_block(block, 1, true);
        let t0 = Instant::now();
        m.observe_raw_tx(block, 0, black_box(&curve_buy));
        samples.push(t0.elapsed().as_nanos() as f64);
        m.end_block(block);
        if i % 500 == 499 {
            let s = m.snapshot_any(&CURVE).unwrap();
            m.apply_anchor(CURVE, block, s.eth_reserve, s.token_reserve);
        }
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!("tracked curve, a buy applied (in order)       median {:>7.1} ns", samples[samples.len() / 2]);

    let s = m.stats();
    println!("\n{} trades applied, {} fresh reads", s.trades_applied, s.reads_fresh);
}
