//! Pins the "zero heap allocation on the hot path" property with a counting allocator.
//!
//! The counter is thread-local, so concurrently running tests cannot pollute each other's
//! measurement: only allocations made by the measuring thread inside the measured closure count.

use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::{Decodable2718, Encodable2718};
use alloy::primitives::{Address, Bytes, TxKind, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use autotradesniper::addr_set::AddressSet;
use autotradesniper::config::AppConfig;
use autotradesniper::models::PrysmEngine;
use autotradesniper::payload::DynamicFeeAdjuster;
use autotradesniper::stream::feed::{ClaimRing, WinLedger};
use autotradesniper::stream::{l2_message_is_valid, L2Txs, RawTxView, StreamListener};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;

struct CountingAlloc;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.with(|c| c.set(c.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.with(|c| c.set(c.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// Heap allocations performed by `f` on this thread.
fn allocations_in<R>(f: impl FnOnce() -> R) -> (R, u64) {
    let before = ALLOCS.with(|c| c.get());
    let out = f();
    (out, ALLOCS.with(|c| c.get()) - before)
}

fn raw_tx(to: Address, input: &[u8]) -> Vec<u8> {
    let signer = PrivateKeySigner::random();
    let tx = TxEip1559 {
        chain_id: 4663,
        nonce: 1,
        gas_limit: 300_000,
        max_fee_per_gas: 100_000_000,
        max_priority_fee_per_gas: 1,
        to: TxKind::Call(to),
        value: U256::from(10u64).pow(U256::from(16u64)),
        input: Bytes::copy_from_slice(input),
        access_list: Default::default(),
    };
    let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    TxEnvelope::Eip1559(tx.into_signed(sig)).encoded_2718()
}

/// A batch message (kind 3) wrapping kind-4 transactions.
fn batch_of(txs: &[Vec<u8>]) -> Vec<u8> {
    let mut msg = vec![3u8];
    for tx in txs {
        msg.extend_from_slice(&((tx.len() + 1) as u64).to_be_bytes());
        msg.push(4);
        msg.extend_from_slice(tx);
    }
    msg
}

const PONS_BUY: [u8; 4] = [0x59, 0xa8, 0x7b, 0xc1];

fn listener() -> StreamListener {
    let config = AppConfig::default();
    let (tx, _rx) = mpsc::channel(4);
    let prysm = Arc::new(PrysmEngine::new(config.clone(), tx.clone()));
    StreamListener::new(config, Arc::new(DynamicFeeAdjuster::new(2.5, 1.25)), prysm, tx)
}

#[test]
fn scanning_a_block_for_candidate_txs_allocates_nothing() {
    let listener = listener();
    let mut txs = Vec::new();
    for i in 0..48u8 {
        let mut transfer = vec![0xa9, 0x05, 0x9c, 0xbb];
        transfer.extend_from_slice(&[i; 64]);
        txs.push(raw_tx(Address::repeat_byte(0x40 + i % 8), &transfer));
    }
    let mut buy = PONS_BUY.to_vec();
    buy.extend_from_slice(&[0x11; 96]);
    txs.insert(7, raw_tx(Address::repeat_byte(0x99), &buy));
    txs.insert(30, raw_tx(Address::repeat_byte(0x98), &buy));
    let block = batch_of(&txs);

    let (candidates, allocs) = allocations_in(|| {
        assert!(l2_message_is_valid(&block));
        L2Txs::new(&block).filter(|raw| listener.is_candidate_tx(raw, true)).count()
    });
    assert_eq!(candidates, 2, "only the two Pons buys are worth an ecrecover");
    assert_eq!(allocs, 0, "validate + walk + prefilter 50 txs must not touch the heap");

    // Contrast: the full decoder the prefilter replaces allocates for every single tx.
    let (_, full_decode_allocs) = allocations_in(|| {
        for raw in L2Txs::new(&block) {
            let _ = TxEnvelope::decode_2718_exact(raw);
        }
    });
    assert!(full_decode_allocs >= 50, "decode_2718_exact allocated only {full_decode_allocs} times for 50 txs");
}

#[test]
fn raw_view_borrows_from_and_to_and_calldata() {
    let mut buy = PONS_BUY.to_vec();
    buy.extend_from_slice(&[0x33; 32]);
    let raw = raw_tx(Address::repeat_byte(0x42), &buy);
    let (view, allocs) = allocations_in(|| RawTxView::parse(&raw).unwrap());
    assert_eq!(allocs, 0);
    assert_eq!(view.to, Some(&[0x42; 20]));
    assert_eq!(view.input, buy.as_slice());
    let wire = raw.as_ptr_range();
    assert!(wire.contains(&view.input.as_ptr()), "calldata must be a slice of the wire bytes, not a copy");
    assert!(wire.contains(&(view.to.unwrap().as_ptr())), "recipient must be borrowed too");
}

#[test]
fn address_matching_allocates_nothing_and_agrees_with_hashset() {
    let watched: Vec<Address> = (1u8..=16).map(Address::repeat_byte).collect();
    let hash_set: HashSet<Address> = watched.iter().copied().collect();
    let set = AddressSet::new(watched.iter());
    let watchlist = autotradesniper::config::Watchlist::new(hash_set.clone());
    let probes: Vec<Address> = (0u8..=255).map(Address::repeat_byte).collect();

    let ((a, b), allocs) = allocations_in(|| {
        let mut set_hits = 0;
        let mut list_hits = 0;
        for p in &probes {
            set_hits += set.contains(p) as u32;
            list_hits += watchlist.contains(p) as u32;
            // The raw-bytes form used straight on a borrowed `to` field.
            assert_eq!(set.contains_bytes(&p.0 .0), set.contains(p));
        }
        (set_hits, list_hits)
    });
    assert_eq!(allocs, 0);
    assert_eq!((a, b), (16, 16));
    assert_eq!(probes.iter().filter(|p| hash_set.contains(*p)).count(), 16);
}

#[test]
fn first_packet_wins_claim_is_allocation_free() {
    let ring = ClaimRing::default();
    let ledger = WinLedger::default();
    let ((won, dup), allocs) = allocations_in(|| {
        let mut won = 0;
        let mut dup = 0;
        for block in 1_000..1_500u64 {
            if ring.try_claim(block) {
                ledger.record_win(block);
                won += 1;
            }
            if !ring.try_claim(block) {
                let _ = ledger.lateness_us(block);
                dup += 1;
            }
        }
        (won, dup)
    });
    assert_eq!((won, dup, allocs), (500, 500, 0));
}

#[tokio::test(flavor = "current_thread")]
async fn watchlist_edits_are_visible_to_the_lock_free_index_immediately() {
    let config = AppConfig::default();
    let who = Address::repeat_byte(0x5a);
    assert!(!config.is_watched(&who));
    config.watchlist_accounts.write().await.insert(who);
    assert!(config.is_watched(&who), "write guard drop must republish the index");
    {
        // Several edits under one guard publish once, on release.
        let mut list = config.watchlist_accounts.write().await;
        list.insert(Address::repeat_byte(0x5b));
        list.remove(&who);
    }
    assert!(!config.is_watched(&who));
    assert!(config.is_watched(&Address::repeat_byte(0x5b)));
}
