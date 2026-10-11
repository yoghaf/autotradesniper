//! Regression tests for the AOT pre-signing pipeline (`dispatcher::presign`, `Dispatcher::arm_presign`
//! / `stage_primary` / `dispatch_signal`, the nonce lease in `wallet`, and the staging hooks and
//! event-driven scheduling in `models::prysm`).
//!
//! Everything runs against the real `Dispatcher`, `WalletManager` and `PrysmEngine` with a
//! programmable mock JSON-RPC node, so a pre-signed packet is judged by the bytes the node receives.

use alloy::consensus::transaction::SignerRecoverable;
use alloy::consensus::{Transaction as _, TxEnvelope};
use alloy::eips::eip2718::Decodable2718;
use alloy::primitives::{address, keccak256, Address, B256, U256};
use autotradesniper::config::AppConfig;
use autotradesniper::dispatcher::{
    send_raw_tx_body, sign_sync, Armed, BatchState, BroadcasterPool, Dispatcher, PresignPolicy, StageOutcome, StagedBatch, StagingBook,
    Teardown, TxSpec,
};
use autotradesniper::models::prysm::{LaunchResolution, PrysmEngine, PRYSMCLOUD_SUBSTRING};
use autotradesniper::payload::{self, CalldataPayloadBuilder, DynamicFeeAdjuster};
use autotradesniper::types::{ExecutionMode, TradeSignal};
use autotradesniper::wallet::{TreasuryAccount, WalletManager};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

const MNEMONIC: &str = "test test test test test test test test test test test junk";
const LAUNCHPAD: Address = address!("00000000000000000000000000000000000fac70");
const TOKEN: Address = address!("00000000000000000000000000000000000070c3");
const CURVE: Address = address!("00000000000000000000000000000000000c0e7e");
const OTHER_CURVE: Address = address!("00000000000000000000000000000000000c0e7f");
/// Small padding keeps debug-build signing fast; the mechanics do not depend on the size.
const BUFFER: usize = 2_000;

// ── Mock JSON-RPC node ──────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Seen {
    from: Address,
    to: Option<Address>,
    nonce: u64,
    value: U256,
    max_fee: u128,
    gas: u64,
    input_head: Vec<u8>,
    raw_len: usize,
    hash: B256,
}

struct NodeState {
    seen: Mutex<Vec<Seen>>,
    /// Accept `eth_sendRawTransaction`; false = JSON-RPC error "execution reverted".
    accept: AtomicBool,
    /// Receipt returned by `eth_getTransactionReceipt` (null while `None`).
    receipt: Mutex<Option<serde_json::Value>>,
    receipt_polls: AtomicUsize,
}

impl NodeState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            accept: AtomicBool::new(true),
            receipt: Mutex::new(None),
            receipt_polls: AtomicUsize::new(0),
        })
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    fn by(&self, who: Address) -> Vec<Seen> {
        self.seen().into_iter().filter(|s| s.from == who).collect()
    }
}

fn decode_raw(raw: &[u8]) -> Seen {
    let tx = TxEnvelope::decode_2718_exact(raw).expect("valid EIP-2718 tx");
    Seen {
        from: tx.recover_signer().expect("signature"),
        to: tx.to(),
        nonce: tx.nonce(),
        value: tx.value(),
        max_fee: tx.max_fee_per_gas(),
        gas: tx.gas_limit(),
        input_head: tx.input()[..tx.input().len().min(100)].to_vec(),
        raw_len: raw.len(),
        hash: keccak256(raw),
    }
}

fn answer(state: &NodeState, req: &serde_json::Value) -> serde_json::Value {
    let error = |msg: &str| serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":msg}});
    match req["method"].as_str().unwrap_or_default() {
        "eth_sendRawTransaction" => {
            let raw = alloy::hex::decode(req["params"][0].as_str().unwrap()).unwrap();
            let seen = decode_raw(&raw);
            let hash = seen.hash;
            state.seen.lock().unwrap().push(seen);
            if state.accept.load(Ordering::SeqCst) {
                serde_json::json!({"jsonrpc":"2.0","id":1,"result":format!("{:?}", hash)})
            } else {
                error("execution reverted (mock)")
            }
        }
        "eth_getTransactionReceipt" => {
            state.receipt_polls.fetch_add(1, Ordering::SeqCst);
            let receipt = state.receipt.lock().unwrap().clone().unwrap_or(serde_json::Value::Null);
            serde_json::json!({"jsonrpc":"2.0","id":1,"result":receipt})
        }
        // getReserves() is not served: the engine then quotes on the post-dev-buy curve.
        _ => error("unsupported"),
    }
}

/// Minimal HTTP/1.1 keep-alive JSON-RPC server.
async fn mock_node(state: Arc<NodeState>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let state = state.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                loop {
                    let header_end = loop {
                        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break p + 4;
                        }
                        let mut chunk = [0u8; 65536];
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..header_end]).to_lowercase();
                    let len: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                    while buf.len() < header_end + len {
                        let mut chunk = [0u8; 65536];
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let req: serde_json::Value = serde_json::from_slice(&buf[header_end..header_end + len]).unwrap_or_default();
                    buf.drain(..header_end + len);
                    let body = answer(&state, &req).to_string();
                    let resp = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}", body.len(), body);
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    url
}

// ── Harness ─────────────────────────────────────────────────────────────────

struct Rig {
    dispatcher: Arc<Dispatcher>,
    config: AppConfig,
    fee: Arc<DynamicFeeAdjuster>,
    accounts: Vec<TreasuryAccount>,
    node: Arc<NodeState>,
    url: String,
}

async fn base_config(url: &str, carriers: usize) -> AppConfig {
    let mut config = AppConfig::default();
    config.send_rpcs = vec![url.to_string()];
    config.rpc_http_url = "http://127.0.0.1:1".to_string(); // read RPC down: only local settlement can heal
    *config.mnemonic_phrase.write().await = MNEMONIC.to_string();
    *config.execution_mode.write().await = ExecutionMode::MultiAccount;
    *config.sub_account_count.write().await = carriers;
    *config.buffer_size.write().await = BUFFER;
    *config.gas_limit.write().await = payload::STATIC_GAS_LIMIT;
    *config.max_priority_fee_gwei.write().await = 0.0;
    *config.max_fee_gwei.write().await = 1_000.0;
    config
}

async fn rig_with(carriers: usize, policy: PresignPolicy) -> Rig {
    let node = NodeState::new();
    let url = mock_node(node.clone()).await;
    let config = base_config(&url, carriers).await;
    let wm = WalletManager::new(config.clone()).await.unwrap();
    let accounts = wm.get_all_accounts().await.unwrap();
    let fee = Arc::new(DynamicFeeAdjuster::new(0.0, 1.0));
    let (report_tx, rx) = mpsc::channel(256);
    std::mem::forget(rx); // keep the channel open: reports are not under test
    let dispatcher =
        Arc::new(Dispatcher::new(config.clone(), wm.clone(), fee.clone(), report_tx).with_presign_policy(policy));
    Rig { dispatcher, config, fee, accounts, node, url }
}

async fn rig(carriers: usize) -> Rig {
    rig_with(carriers, PresignPolicy::default()).await
}

const MIN_OUT: u64 = 1_000_000_000_000;

fn signal_for(launch: B256, curve: Address, min_out: u64) -> TradeSignal {
    let size = U256::from(10_000_000_000_000_000u64); // 0.01 ETH
    TradeSignal {
        tx_hash: launch,
        monitored_account: Address::ZERO,
        target_token: TOKEN,
        pool_or_router: curve,
        entry_amount_wei: size,
        swap_calldata: CalldataPayloadBuilder::build_pons_buy_payload(size, U256::from(min_out), Address::ZERO),
        detected_at: Instant::now(),
        block_number: 100,
        strategy_tag: Some("Prysm: Pola A".to_string()),
    }
}

fn launch(n: u8) -> B256 {
    B256::repeat_byte(n)
}

fn cursor(acc: &TreasuryAccount) -> u64 {
    acc.nonce_cursor.load(Ordering::SeqCst)
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Number of staged slots that already hold signed bytes.
fn signed_slots(batch: &Option<Arc<StagedBatch>>) -> usize {
    batch.as_ref().map_or(0, |b| b.slot_specs().iter().filter(|(_, _, signed)| *signed).count())
}

/// Every nonce of `acc` is either seen on the node or parked, none twice, and together they are exactly `[0, cursor)`.
fn assert_nonce_invariant(rig: &Rig, acc: &TreasuryAccount) {
    let sent: Vec<u64> = rig.node.by(acc.address).iter().map(|s| s.nonce).collect();
    let unique: BTreeSet<u64> = sent.iter().copied().collect();
    assert_eq!(unique.len(), sent.len(), "{}: a nonce was sent twice: {:?}", acc.role, sent);
    let parked = acc.nonce_holes.snapshot();
    let mut all: BTreeSet<u64> = unique;
    for h in &parked {
        assert!(all.insert(*h), "{}: nonce {} is both sent and parked", acc.role, h);
    }
    let want: BTreeSet<u64> = (0..cursor(acc)).collect();
    assert_eq!(all, want, "{}: sent {:?} + parked {:?} must cover exactly [0, cursor={})", acc.role, sent, parked, cursor(acc));
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Pre-signed packets are valid and are sent untouched
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_packets_are_sent_untouched_with_zero_signing_on_the_hot_path() {
    let r = rig(3).await;
    let l = launch(1);

    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    let batch = r.dispatcher.staged_batch(l);
    assert_eq!(signed_slots(&batch), 3, "all three carriers are signed ahead of time");
    // Carriers need no launch data: the primary slot (index 0) is not leased before the curve is known.
    let leased: Vec<(usize, u64)> = batch.as_ref().unwrap().leased_nonces();
    assert_eq!(leased, vec![(1, 0), (2, 0), (3, 0)]);
    assert_eq!(cursor(&r.accounts[0]), 0, "the master nonce is not held before the primary exists");

    let sig = signal_for(l, CURVE, MIN_OUT);
    assert_eq!(r.dispatcher.stage_primary(&sig).await, StageOutcome::Staged { nonce: 0 });
    assert_eq!(signed_slots(&r.dispatcher.staged_batch(l)), 4);
    assert!(r.accounts[0].nonce_holes.is_leased(0), "the staged primary holds the master nonce");

    let report = r.dispatcher.dispatch_signal(&sig).await.expect("dispatch");
    assert_eq!(report.tx_hashes.len(), 4);

    let s = r.dispatcher.presign_snapshot();
    assert_eq!(s.claimed, 1);
    assert_eq!(s.prebuilt_sent, 4, "every account broadcast its stored bytes");
    assert_eq!((s.resigned_stale, s.signed_inline_leased, s.signed_inline_fresh), (0, 0, 0), "no signature on the hot path");

    // What the node received is a valid, correctly attributed tx per account, at the staged nonce.
    let seen = r.node.seen();
    assert_eq!(seen.len(), 4);
    for acc in &r.accounts {
        let mine = r.node.by(acc.address);
        assert_eq!(mine.len(), 1, "{}", acc.role);
        assert_eq!(mine[0].nonce, 0);
        assert_eq!(cursor(acc), 1);
        assert!(acc.nonce_holes.leased_snapshot().is_empty(), "committed leases are released from protection");
    }
    let master = &r.node.by(r.accounts[0].address)[0];
    assert_eq!(master.to, Some(CURVE));
    assert_eq!(master.value, U256::from(10_000_000_000_000_000u64));
    assert_eq!(U256::from_be_slice(&master.input_head[36..68]), U256::from(MIN_OUT));
    assert!(master.raw_len <= payload::MAX_SEQUENCER_TX_BYTES);
    assert!(master.gas >= 21_000, "a real gas limit was signed ({})", master.gas);
    assert!(r.dispatcher.staged_batch(l).is_none(), "a claimed batch leaves the book");
    for acc in &r.accounts {
        assert_nonce_invariant(&r, acc);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_bytes_are_a_complete_ready_to_write_request() {
    // `sign_sync` is what pre-signing stores: raw envelope, hash and the full JSON body.
    let r = rig(1).await;
    let acc = &r.accounts[0];
    let spec: TxSpec = {
        let ctx = r.dispatcher.entry_ctx().await;
        // A carrier-shaped spec is enough to exercise the encoder.
        let s = signal_for(launch(9), CURVE, MIN_OUT);
        let swap = CalldataPayloadBuilder::adapt_buy_calldata(&s.swap_calldata, acc.address, s.entry_amount_wei, s.entry_amount_wei).unwrap();
        TxSpec {
            chain_id: ctx.chain_id,
            to: CURVE,
            value: s.entry_amount_wei,
            input: swap.clone().into(),
            gas_limit: 700_000,
            max_fee_per_gas: ctx.max_fee_wei,
            max_priority_fee_per_gas: ctx.priority_fee_wei,
            floor_with_l1: 0,
            retry_rate_limited: true,
            gas_floor_without_l1: 0,
            configured_gas_limit: 0,
            l1_component_gas: 0,
            swap,
        }
    };
    let signed = sign_sync(&acc.signer, spec.to_tx(7)).unwrap();
    assert_eq!(signed.hash, keccak256(&signed.raw), "the envelope hash is the tx hash");
    assert_eq!(signed.hash_hex, format!("{:?}", signed.hash));
    assert_eq!(signed.body, send_raw_tx_body(&signed.raw), "the stored body is exactly what the broadcaster would build");
    let tx = TxEnvelope::decode_2718_exact(&signed.raw[..]).unwrap();
    assert_eq!(tx.recover_signer().unwrap(), acc.address);
    assert_eq!(tx.nonce(), 7);

    // And it goes out through the zero-compute path.
    let pool = BroadcasterPool::new(vec![r.url.clone()]);
    let hash = pool.broadcast_body(signed.body.clone(), &signed.hash_hex).await.expect("accepted");
    assert_eq!(hash, signed.hash_hex);
    assert_eq!(r.node.seen().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_order_that_resolves_before_its_block_signs_only_the_primary_inline() {
    // Default profile (Block +1): the order is due the moment T_LAUNCH resolves, so the primary has
    // no waiting window. Carriers were signed during the receipt wait; the primary is signed inline.
    let r = rig(2).await;
    let l = launch(2);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    assert_eq!(signed_slots(&r.dispatcher.staged_batch(l)), 2);

    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.expect("dispatch");
    let s = r.dispatcher.presign_snapshot();
    assert_eq!(s.prebuilt_sent, 2, "both carriers: stored bytes");
    assert_eq!(s.signed_inline_fresh, 1, "the primary: classic allocate + sign");
    assert_eq!(r.node.seen().len(), 3);
    for acc in &r.accounts {
        assert_nonce_invariant(&r, acc);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Revert Guard / fee changes: keep or re-sign (invalidation thresholds)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quote_drift_within_tolerance_keeps_the_primary_beyond_it_resigns_under_the_same_nonce() {
    let r = rig(1).await;
    let l = launch(3);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    assert_eq!(r.dispatcher.stage_primary(&signal_for(l, CURVE, MIN_OUT)).await, StageOutcome::Staged { nonce: 0 });

    // +1% (tolerance 2%): the staged guard stands; asking again changes nothing.
    let near = MIN_OUT + MIN_OUT / 100;
    assert_eq!(r.dispatcher.stage_primary(&signal_for(l, CURVE, near)).await, StageOutcome::Unchanged);
    assert_eq!(cursor(&r.accounts[0]), 1, "no nonce churn");

    // +5%: the guard is stale. Re-signed in the background, SAME nonce, nothing allocated.
    let far = MIN_OUT + MIN_OUT / 20;
    assert_eq!(r.dispatcher.stage_primary(&signal_for(l, CURVE, far)).await, StageOutcome::Restaged { nonce: 0 });
    assert_eq!(cursor(&r.accounts[0]), 1);
    assert_eq!(r.accounts[0].nonce_holes.leased_snapshot(), vec![0]);

    // The claiming dispatch (fresh quote `far`) now finds the re-signed packet valid: no inline signing.
    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, far)).await.expect("dispatch");
    let s = r.dispatcher.presign_snapshot();
    assert_eq!((s.prebuilt_sent, s.resigned_stale, s.primary_restaged), (2, 0, 1));
    let master = &r.node.by(r.accounts[0].address)[0];
    assert_eq!((master.nonce, U256::from_be_slice(&master.input_head[36..68])), (0, U256::from(far)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_primary_found_at_dispatch_is_resigned_inline_under_its_own_nonce() {
    let r = rig(1).await;
    let l = launch(4);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    assert_eq!(r.dispatcher.stage_primary(&signal_for(l, CURVE, MIN_OUT)).await, StageOutcome::Staged { nonce: 0 });

    // The reserves moved after staging and nobody re-staged: dispatch gets a signal quoted 10% lower.
    let fresh = MIN_OUT - MIN_OUT / 10;
    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, fresh)).await.expect("dispatch");

    let s = r.dispatcher.presign_snapshot();
    assert_eq!(s.prebuilt_sent, 1, "the carrier is unaffected");
    assert_eq!(s.resigned_stale, 1, "the primary is signed again, on the hot path, because the guard drifted");
    let master = &r.node.by(r.accounts[0].address)[0];
    assert_eq!(master.nonce, 0, "re-signed under the staged nonce: never a hole between staged and fresh");
    assert_eq!(U256::from_be_slice(&master.input_head[36..68]), U256::from(fresh), "the node got the FRESH guard");
    assert_nonce_invariant(&r, &r.accounts[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_different_target_is_never_filled_with_the_staged_bytes() {
    let r = rig(1).await;
    let l = launch(5);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    assert_eq!(r.dispatcher.stage_primary(&signal_for(l, CURVE, MIN_OUT)).await, StageOutcome::Staged { nonce: 0 });

    // Same launch tx, other curve: the staged packet would buy the wrong token.
    r.dispatcher.dispatch_signal(&signal_for(l, OTHER_CURVE, MIN_OUT)).await.expect("dispatch");
    assert_eq!(r.dispatcher.presign_snapshot().resigned_stale, 1);
    assert_eq!(r.node.by(r.accounts[0].address)[0].to, Some(OTHER_CURVE));
    assert_nonce_invariant(&r, &r.accounts[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fee_rise_inside_tolerance_keeps_packets_beyond_it_triggers_a_proactive_resign() {
    let r = rig(2).await;
    let l = launch(6);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    let before = r.dispatcher.entry_ctx().await;
    let first_max_fee = before.max_fee_wei;

    // One small EMA step (+10% of the cap): inside the 10% tolerance -> nothing to do.
    r.fee.update_block_fees(1.5, 0.0);
    assert_eq!(r.dispatcher.revalidate_presigned().await, 0);

    // The base fee keeps climbing: the staged cap is now far below what a fresh tx would carry.
    for _ in 0..40 {
        r.fee.update_block_fees(50.0, 0.0);
    }
    let live = r.dispatcher.entry_ctx().await;
    assert!(live.max_fee_wei > first_max_fee * 10);
    assert_eq!(r.dispatcher.revalidate_presigned().await, 2, "both staged carriers are signed again, ahead of the block");
    assert_eq!(r.dispatcher.presign_snapshot().proactive_resigns, 2);
    // Same nonces, new fee.
    assert_eq!(r.dispatcher.staged_batch(l).unwrap().leased_nonces(), vec![(1, 0), (2, 0)]);
    assert_eq!(signed_slots(&r.dispatcher.staged_batch(l)), 2);

    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.expect("dispatch");
    let s = r.dispatcher.presign_snapshot();
    assert_eq!((s.prebuilt_sent, s.resigned_stale), (2, 0), "the re-signed carriers are valid at the target block");
    for c in &r.accounts[1..] {
        let tx = &r.node.by(c.address)[0];
        assert!(tx.max_fee >= live.max_fee_wei * 9 / 10, "carrier fee {} follows the live fee {}", tx.max_fee, live.max_fee_wei);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_the_proactive_pass_dispatch_resigns_stale_fee_packets_under_their_nonces() {
    let r = rig(2).await;
    let l = launch(7);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    for _ in 0..40 {
        r.fee.update_block_fees(50.0, 0.0);
    }
    let live = r.dispatcher.entry_ctx().await;

    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.expect("dispatch");
    let s = r.dispatcher.presign_snapshot();
    assert_eq!(s.resigned_stale, 2, "both carriers fail the fee check");
    assert_eq!(s.prebuilt_sent, 0);
    for c in &r.accounts[1..] {
        let tx = &r.node.by(c.address)[0];
        assert_eq!(tx.nonce, 0);
        assert!(tx.max_fee >= live.max_fee_wei * 9 / 10);
    }
    for acc in &r.accounts {
        assert_nonce_invariant(&r, acc);
    }
}

#[test]
fn quote_tolerance_boundary_is_exact_and_other_differences_never_match() {
    let size = U256::from(10_000_000_000_000_000u64);
    let buy = |min: u64, to: Address| CalldataPayloadBuilder::build_pons_buy_payload(size, U256::from(min), to);
    let base = buy(10_000, Address::ZERO);
    // |a - b| * 10_000 <= tolerance * fresh
    assert!(CalldataPayloadBuilder::pons_buy_quote_compatible(&base, &buy(10_200, Address::ZERO), 200), "stale 10_000 vs fresh 10_200: 196 bps");
    assert!(CalldataPayloadBuilder::pons_buy_quote_compatible(&base, &buy(10_204, Address::ZERO), 200), "204/10204 = 199.9 bps");
    assert!(!CalldataPayloadBuilder::pons_buy_quote_compatible(&base, &buy(10_205, Address::ZERO), 200), "205/10205 = 200.9 bps");
    assert!(CalldataPayloadBuilder::pons_buy_quote_compatible(&base, &base, 0));
    // A different recipient or size is a different trade, whatever the tolerance.
    assert!(!CalldataPayloadBuilder::pons_buy_quote_compatible(&base, &buy(10_000, Address::repeat_byte(1)), 10_000));
    assert!(!CalldataPayloadBuilder::pons_buy_quote_compatible(&base, &base[..50], 10_000));
    let mut sell = base.clone();
    sell[..4].copy_from_slice(&payload::PONS_SELL_SELECTOR);
    assert!(CalldataPayloadBuilder::pons_buy_quote_compatible(&sell, &sell.clone(), 0), "byte-identical calldata is trivially the same tx, whatever the selector");
    let mut other_sell = sell.clone();
    other_sell[4 + 32] ^= 1;
    assert!(!CalldataPayloadBuilder::pons_buy_quote_compatible(&sell, &other_sell, 10_000), "only a Pons buy's guard word may differ");
    assert!(!CalldataPayloadBuilder::pons_buy_same_but_guard(&sell, &sell.clone()), "not a Pons buy");
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Cancellation, expiry and tombstones: safe release of leased nonces
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_releases_every_leased_nonce_and_the_next_dispatch_starts_clean() {
    let r = rig(3).await;
    let l = launch(10);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    assert_eq!(r.dispatcher.stage_primary(&signal_for(l, CURVE, MIN_OUT)).await, StageOutcome::Staged { nonce: 0 });
    for acc in &r.accounts {
        assert!(acc.nonce_holes.is_leased(0));
        assert_eq!(cursor(acc), 1);
    }

    assert!(r.dispatcher.cancel_presigned(l), "the live batch was torn down by this call");
    assert!(!r.dispatcher.cancel_presigned(l), "a second cancel finds nothing live");
    assert!(r.dispatcher.staged_batch(l).is_none());
    for acc in &r.accounts {
        assert!(acc.nonce_holes.leased_snapshot().is_empty());
        assert_eq!(cursor(acc), 0, "{}: the cursor stepped back, no hole", acc.role);
        assert!(acc.nonce_holes.snapshot().is_empty());
    }
    assert_eq!(r.dispatcher.presign_snapshot().cancelled, 1);

    // A later dispatch for the same launch is a plain on-the-fly one and reuses nonce 0.
    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.expect("dispatch");
    let s = r.dispatcher.presign_snapshot();
    assert_eq!((s.prebuilt_sent, s.signed_inline_fresh), (0, 4));
    for acc in &r.accounts {
        assert_eq!(r.node.by(acc.address)[0].nonce, 0);
        assert_nonce_invariant(&r, acc);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unclaimed_batch_expires_on_its_own_and_hands_the_nonces_back() {
    let policy = PresignPolicy { max_hold: Duration::from_millis(200), ..PresignPolicy::default() };
    let r = rig_with(2, policy).await;
    let l = launch(11);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(60)).await);
    assert!(r.accounts[1].nonce_holes.is_leased(0));

    wait_until("the janitor to expire the batch", || r.dispatcher.presign_snapshot().expired == 1).await;
    assert_eq!(r.dispatcher.presign_snapshot().expired, 1);
    for acc in &r.accounts {
        assert!(acc.nonce_holes.leased_snapshot().is_empty());
        assert_eq!(cursor(acc), 0);
    }
    assert!(!r.dispatcher.has_staged());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_deadline_in_the_past_stages_nothing() {
    let r = rig(2).await;
    assert!(!r.dispatcher.arm_presign(launch(12), Instant::now()).await);
    assert!(!r.dispatcher.has_staged());
    assert_eq!(cursor(&r.accounts[1]), 0, "no nonce was leased");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_arm_after_dispatch_or_cancel_is_refused_and_leases_nothing() {
    let r = rig(2).await;

    // Dispatch won first (the arm task was slow): the launch is tombstoned.
    let l = launch(13);
    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.expect("dispatch");
    assert!(!r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);

    // Cancel won first.
    let l2 = launch(14);
    assert!(!r.dispatcher.cancel_presigned(l2), "nothing live yet, but the launch is now closed");
    assert!(!r.dispatcher.arm_presign(l2, Instant::now() + Duration::from_secs(10)).await);

    assert_eq!(r.dispatcher.presign_snapshot().late_arm_refused, 2);
    assert!(!r.dispatcher.has_staged());
    for acc in &r.accounts {
        assert!(acc.nonce_holes.leased_snapshot().is_empty());
        assert_nonce_invariant(&r, acc);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_for_another_launch_or_a_copy_trade_does_not_consume_the_batch() {
    let r = rig(1).await;
    let l = launch(15);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);

    r.dispatcher.dispatch_signal(&signal_for(launch(99), CURVE, MIN_OUT)).await.expect("other launch");
    assert!(r.dispatcher.staged_batch(l).is_some(), "launch 15 is untouched");
    assert!(r.accounts[1].nonce_holes.is_leased(0));
    // The other launch's carrier took the NEXT nonce, not the leased one.
    assert_eq!(r.node.by(r.accounts[1].address)[0].nonce, 1);
    assert_eq!(r.dispatcher.presign_snapshot().claimed, 0);

    // Our launch still dispatches from its batch.
    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.expect("dispatch");
    assert_eq!(r.dispatcher.presign_snapshot().claimed, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn carriers_staged_for_accounts_no_longer_used_are_released_at_dispatch() {
    let r = rig(3).await;
    let l = launch(16);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    // Telegram switched to master-only after staging.
    *r.config.execution_mode.write().await = ExecutionMode::Single;

    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.expect("dispatch");
    assert_eq!(r.node.seen().len(), 1);
    assert_eq!(r.dispatcher.presign_snapshot().leases_released_unused, 3);
    for acc in &r.accounts[1..] {
        assert_eq!(cursor(acc), 0, "the unused carrier nonces went back");
        assert!(acc.nonce_holes.leased_snapshot().is_empty());
    }
    assert_nonce_invariant(&r, &r.accounts[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_prebuilt_broadcast_settles_its_nonce_like_any_other_failure() {
    let r = rig(2).await;
    let l = launch(17);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    assert_eq!(r.dispatcher.stage_primary(&signal_for(l, CURVE, MIN_OUT)).await, StageOutcome::Staged { nonce: 0 });

    r.node.accept.store(false, Ordering::SeqCst); // every endpoint turns the txs away
    let err = r.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.expect_err("master rejected");
    assert!(err.to_string().contains("Master entry was not accepted"), "{err}");
    assert_eq!(r.dispatcher.presign_snapshot().prebuilt_sent, 3, "the stored bytes did go out");
    // Rejected everywhere = provably unused: every nonce is back, nothing is leased, nothing leaked.
    for acc in &r.accounts {
        assert_eq!(cursor(acc), 0, "{}", acc.role);
        assert!(acc.nonce_holes.leased_snapshot().is_empty());
        assert!(acc.nonce_holes.snapshot().is_empty());
    }
    assert!(r.dispatcher.staged_batch(l).is_none());
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Nonce allocator: a staged nonce survives gap healing; no race can duplicate or lose one
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gap_healing_cannot_hand_out_a_nonce_that_a_staged_packet_holds() {
    let r = rig(1).await;
    let (protected, plain) = (&r.accounts[0], &r.accounts[1]);
    for acc in [protected, plain] {
        acc.nonce_cursor.store(5, Ordering::SeqCst);
    }

    // `protected` has a staged (signed, unsent) tx on nonce 5, and a later tx 6 in flight.
    let lease = protected.lease_nonce();
    assert_eq!(lease.nonce(), 5);
    assert_eq!(protected.allocate_nonce(), 6);
    // `plain` has the same shape but nothing protects nonce 5.
    assert_eq!(plain.allocate_nonce(), 5);
    assert_eq!(plain.allocate_nonce(), 6);

    // Tx 6 fails ambiguously; the node's pending nonce is 5 (it never saw 5): the healing walks
    // [5, 6] and returns both.
    protected.reconcile_nonce(6, 5);
    plain.reconcile_nonce(6, 5);

    assert_eq!(plain.allocate_nonce(), 5, "unprotected: the nonce of a not-yet-sent tx is re-issued (the hazard)");
    assert_eq!(protected.allocate_nonce(), 6, "protected: 5 is skipped, only 6 comes back");
    assert!(protected.nonce_holes.is_leased(5));
    assert!(!protected.nonce_holes.snapshot().contains(&5));

    // Releasing the lease parks 5 (7 is already out), and it is the very next nonce.
    lease.release();
    assert_eq!(protected.allocate_nonce(), 5);
}

#[tokio::test]
async fn commit_keeps_the_nonce_consumed_and_release_or_drop_hands_it_back() {
    let r = rig(0).await;
    let acc = &r.accounts[0];

    let committed = acc.lease_nonce();
    assert_eq!(committed.commit(), 0);
    assert_eq!((cursor(acc), acc.nonce_holes.leased_snapshot().len()), (1, 0), "committed: consumed, no longer protected, not returned");

    let released = acc.lease_nonce();
    assert_eq!(released.nonce(), 1);
    released.release();
    assert_eq!(cursor(acc), 1, "released: the cursor stepped back");

    {
        let dropped = acc.lease_nonce();
        assert_eq!(dropped.nonce(), 1);
        assert!(acc.nonce_holes.is_leased(1));
    } // a dropped lease (panic, shutdown, bug) must not leak the nonce
    assert_eq!(cursor(acc), 1);
    assert!(acc.nonce_holes.leased_snapshot().is_empty());
}

#[tokio::test]
async fn a_lease_the_chain_already_used_is_not_returned() {
    let r = rig(0).await;
    let acc = &r.accounts[0];
    acc.nonce_cursor.store(5, Ordering::SeqCst);
    let lease = acc.lease_nonce();
    assert_eq!(lease.nonce(), 5);
    assert!(!lease.is_overtaken());

    // A node reports pending nonce 8: someone else used 5.
    acc.reconcile_nonce(2, 8);
    assert!(lease.is_overtaken());
    lease.release();
    assert!(acc.nonce_holes.snapshot().is_empty(), "a consumed nonce must not re-enter the pool");
    assert_eq!(acc.allocate_nonce(), 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_overtaken_staged_nonce_is_replaced_by_a_fresh_one_at_dispatch() {
    let r = rig(1).await;
    let l = launch(18);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    // Something else (a manual tx from the same key) consumed nonce 0 and 1 meanwhile.
    r.accounts[1].reconcile_nonce(0, 2);

    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.expect("dispatch");
    let s = r.dispatcher.presign_snapshot();
    assert_eq!(s.leases_overtaken, 1);
    assert_eq!(s.prebuilt_sent, 0, "bytes signed for a used nonce can never be included; not sent");
    let carrier = &r.node.by(r.accounts[1].address)[0];
    assert!(carrier.nonce >= 2, "fresh nonce {} is beyond what the chain used", carrier.nonce);
}

#[test]
fn the_batch_state_machine_lets_exactly_one_party_take_the_slots() {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    rt.block_on(async {
        let r = rig(1).await;
        let acc = &r.accounts[1];
        let spec = {
            let ctx = r.dispatcher.entry_ctx().await;
            let mut s = signal_for(launch(1), CURVE, MIN_OUT);
            s.swap_calldata = CalldataPayloadBuilder::build_pons_buy_payload(s.entry_amount_wei, U256::from(MIN_OUT), acc.address);
            let swap = s.swap_calldata.clone();
            TxSpec {
                chain_id: ctx.chain_id,
                to: acc.address,
                value: U256::ZERO,
                input: swap.clone().into(),
                gas_limit: 100_000,
                max_fee_per_gas: ctx.max_fee_wei,
                max_priority_fee_per_gas: ctx.priority_fee_wei,
                floor_with_l1: 0,
                retry_rate_limited: false,
                gas_floor_without_l1: 0,
                configured_gas_limit: 0,
                l1_component_gas: 0,
                swap,
            }
        };
        let book = StagingBook::default();
        let key = launch(1);
        let now = Instant::now();
        let batch = Arc::new(StagedBatch::new(key, 2, now, now + Duration::from_secs(30)));
        let Armed::New(batch) = book.arm(batch) else { panic!("first arm is new") };
        assert!(matches!(book.arm(Arc::new(StagedBatch::new(key, 2, now, now))), Armed::Existing(_)), "duplicate arm reuses the live batch");

        // Slot creation leases the nonce; re-spec'ing keeps it and invalidates the old ticket.
        let t1 = batch.lease_slot(1, acc, spec.clone()).expect("leased");
        assert_eq!((t1.nonce, cursor(acc)), (0, 1));
        let t2 = batch.lease_slot(1, acc, spec.clone()).expect("re-spec");
        assert_eq!((t2.nonce, cursor(acc)), (0, 1), "same nonce, no churn");
        let signed = sign_sync(&acc.signer, spec.to_tx(0)).unwrap();
        assert!(!batch.install_signed(&t1, signed.clone()), "a result for a replaced spec is discarded");
        assert!(batch.install_signed(&t2, signed.clone()));

        // Claim and teardown race for the same batch: exactly one wins.
        assert!(book.teardown(key, Teardown::Cancelled), "cancel won");
        assert_eq!(batch.state(), BatchState::Cancelled);
        assert!(book.claim(key).is_none(), "claim after cancel finds nothing");
        assert!(batch.claim().is_none(), "even with the Arc in hand");
        assert!(batch.lease_slot(1, acc, spec.clone()).is_none(), "no new lease on a closed batch");
        assert!(!batch.install_signed(&t2, signed), "no install on a closed batch");
        assert_eq!(cursor(acc), 0, "cancel returned the leased nonce");
        assert!(acc.nonce_holes.leased_snapshot().is_empty());

        // The reverse order: claim first, cancel finds a tombstone.
        let key2 = launch(2);
        let b2 = Arc::new(StagedBatch::new(key2, 2, now, now + Duration::from_secs(30)));
        let Armed::New(b2) = book.arm(b2) else { panic!() };
        let t = b2.lease_slot(1, acc, spec.clone()).unwrap();
        assert_eq!(t.nonce, 0);
        let (_, claim) = book.claim(key2).expect("claim wins");
        assert!(!book.teardown(key2, Teardown::Cancelled), "cancel after claim: too late, nothing torn down");
        assert!(b2.lease_slot(1, acc, spec).is_none());
        let slot = claim.slots.into_iter().nth(1).flatten().expect("the claimed slot");
        assert_eq!(slot.lease.nonce(), 0);
        assert!(acc.nonce_holes.is_leased(0), "the claimer now owns the lease");
        assert_eq!(slot.lease.commit(), 0);
        assert!(acc.nonce_holes.leased_snapshot().is_empty());
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_cancel_and_expiry_race_with_exactly_one_winner_and_a_gapless_nonce_lane() {
    const ROUNDS: usize = 60;
    let policy = PresignPolicy { max_hold: Duration::from_millis(2), ..PresignPolicy::default() };
    let r = Arc::new(rig_with(2, policy).await);
    let mut claim_wins = 0u64;
    let mut cancel_wins = 0u64;

    for round in 0..ROUNDS {
        let l = B256::from(U256::from(1_000 + round as u64));
        let before = r.dispatcher.presign_snapshot();
        let hold = if round % 3 == 0 { Duration::from_millis(1) } else { Duration::from_secs(10) };
        // `arm_presign` may even lose to the 1 ms hold: then there is simply nothing to race.
        r.dispatcher.arm_presign(l, Instant::now() + hold).await;

        let d1 = r.clone();
        let d2 = r.clone();
        let d3 = r.clone();
        let dispatch = tokio::spawn(async move { d1.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.is_ok() });
        let cancel_a = tokio::spawn(async move { d2.dispatcher.cancel_presigned(l) });
        let cancel_b = tokio::spawn(async move { d3.dispatcher.cancel_presigned(l) });
        assert!(dispatch.await.unwrap(), "round {round}: dispatch must succeed whoever wins the batch");
        let (a, b) = (cancel_a.await.unwrap(), cancel_b.await.unwrap());
        assert!(!(a && b), "round {round}: two cancels both claim to have torn the batch down");

        let after = r.dispatcher.presign_snapshot();
        let claimed = after.claimed - before.claimed;
        let torn = (after.cancelled - before.cancelled) + (after.expired - before.expired);
        assert!(claimed + torn <= 1, "round {round}: claimed {claimed} + torn down {torn}: more than one winner");
        claim_wins += claimed;
        cancel_wins += torn;
        assert!(r.dispatcher.staged_batch(l).is_none(), "round {round}: nothing stays live");
    }

    // Whoever won, the lanes are intact: no duplicate, no hole, nothing left leased.
    for acc in &r.accounts {
        assert!(acc.nonce_holes.leased_snapshot().is_empty(), "{}: leaked lease", acc.role);
        assert_nonce_invariant(&r, acc);
        assert_eq!(r.node.by(acc.address).len(), ROUNDS, "{}: exactly one tx per round", acc.role);
    }
    // Rounds where arming lost to its own 1 ms hold have no batch at all, so this is an upper bound.
    assert!(claim_wins + cancel_wins <= ROUNDS as u64, "claimed {claim_wins} + torn down {cancel_wins} over {ROUNDS} rounds");
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. PrysmEngine: staging hooks, scheduling, exactly-once release
// ═══════════════════════════════════════════════════════════════════════════

fn spoof_calldata() -> Vec<u8> {
    format!("\0\0https://x.{}/a.png\0", PRYSMCLOUD_SUBSTRING).into_bytes()
}

fn resolved(token: Address, curve: Address) -> LaunchResolution {
    LaunchResolution::Resolved { token, curve, reserves: None }
}

struct EngineRig {
    r: Rig,
    engine: Arc<PrysmEngine>,
    rx: mpsc::Receiver<TradeSignal>,
}

async fn engine_rig(delay: u64, carriers: usize) -> EngineRig {
    let node = NodeState::new();
    let url = mock_node(node.clone()).await;
    let mut config = base_config(&url, carriers).await;
    config.prysm_enabled.store(true, Ordering::SeqCst);
    *config.prysm_min_block_delay.write().await = delay;
    *config.prysm_size_eth.write().await = 0.01;
    *config.prysm_max_signal_age_secs.write().await = 15;
    *config.prysm_slippage_percent.write().await = 25.0;
    *config.prysm_max_mcap_usd.write().await = 1.0e9;
    config.prysm_launchpads = Arc::new([LAUNCHPAD].into_iter().collect());
    let wm = WalletManager::new(config.clone()).await.unwrap();
    let accounts = wm.get_all_accounts().await.unwrap();
    let fee = Arc::new(DynamicFeeAdjuster::new(0.0, 1.0));
    let (report_tx, report_rx) = mpsc::channel(256);
    std::mem::forget(report_rx);
    let dispatcher = Arc::new(Dispatcher::new(config.clone(), wm.clone(), fee.clone(), report_tx));
    let (signal_tx, rx) = mpsc::channel(256);
    let engine = Arc::new(PrysmEngine::new(config.clone(), signal_tx).with_presigner(dispatcher.clone()));
    engine.set_master_address(accounts[0].address).await;
    let r = Rig { dispatcher, config, fee, accounts, node, url };
    EngineRig { r, engine, rx }
}

async fn see_launch(e: &EngineRig, tx: B256, block: u64) {
    e.engine
        .inspect_transaction(
            Address::repeat_byte(0x11),
            Some(LAUNCHPAD),
            U256::from(21_000_000_000_000_000u64), // 0.021 ETH dev buy: Pola A
            &spoof_calldata(),
            tx,
            block,
            Instant::now(),
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_order_is_staged_in_two_phases_and_dispatched_from_stored_bytes() {
    // Block +2: the order waits for block 101 even after T_LAUNCH resolves, which is the window in
    // which the primary can be signed ahead.
    let mut e = engine_rig(2, 2).await;
    let l = launch(30);

    see_launch(&e, l, 100).await;
    // Phase 1 (match time): carriers are signed in the background; the master nonce is NOT held yet.
    wait_until("carriers pre-signed", || signed_slots(&e.r.dispatcher.staged_batch(l)) == 2).await;
    assert!(e.rx.try_recv().is_err());
    assert_eq!(cursor(&e.r.accounts[0]), 0);

    // Phase 2 (resolution): the order still waits for block 101, so the primary is signed now.
    e.engine.record_resolution(l, resolved(TOKEN, CURVE)).await;
    wait_until("primary pre-signed", || signed_slots(&e.r.dispatcher.staged_batch(l)) == 3).await;
    assert!(e.r.accounts[0].nonce_holes.is_leased(0), "the staged primary protects the master nonce while it waits");
    assert!(e.rx.try_recv().is_err(), "block 101 has not been seen: nothing fires");

    // Target block: the engine fires, the dispatcher blasts the stored bytes.
    e.engine.process_new_block(101).await;
    let signal = e.rx.try_recv().expect("fires at 101");
    assert_eq!((signal.target_token, signal.pool_or_router), (TOKEN, CURVE));
    e.r.dispatcher.dispatch_signal(&signal).await.expect("dispatch");

    let s = e.r.dispatcher.presign_snapshot();
    assert_eq!((s.prebuilt_sent, s.resigned_stale, s.signed_inline_leased, s.signed_inline_fresh), (3, 0, 0, 0));
    let master = &e.r.node.by(e.r.accounts[0].address)[0];
    assert_eq!((master.to, master.nonce), (Some(CURVE), 0));
    for acc in &e.r.accounts {
        assert_nonce_invariant(&e.r, acc);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_a_waiting_launch_drops_the_order_and_releases_the_staged_nonces() {
    let mut e = engine_rig(2, 2).await;
    let l = launch(31);
    see_launch(&e, l, 100).await;
    e.engine.record_resolution(l, resolved(TOKEN, CURVE)).await;
    wait_until("everything pre-signed", || signed_slots(&e.r.dispatcher.staged_batch(l)) == 3).await;

    assert!(e.engine.cancel_launch(l).await);
    assert!(e.r.dispatcher.staged_batch(l).is_none());
    for acc in &e.r.accounts {
        assert!(acc.nonce_holes.leased_snapshot().is_empty());
        assert_eq!(cursor(acc), 0, "{}: nonce handed back", acc.role);
    }
    e.engine.process_new_block(101).await;
    assert!(e.rx.try_recv().is_err(), "the cancelled order never fires");
    assert!(!e.engine.cancel_launch(l).await, "nothing left to cancel");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reverted_launch_releases_the_staged_carriers_immediately() {
    let e = engine_rig(2, 2).await;
    let l = launch(32);
    see_launch(&e, l, 100).await;
    wait_until("carriers pre-signed", || signed_slots(&e.r.dispatcher.staged_batch(l)) == 2).await;

    e.engine.record_resolution(l, LaunchResolution::Failed).await;
    assert!(e.r.dispatcher.staged_batch(l).is_none(), "no waiting for the TTL");
    for acc in &e.r.accounts {
        assert_eq!(cursor(acc), 0);
        assert!(acc.nonce_holes.leased_snapshot().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_quote_while_waiting_restages_the_primary_under_the_same_nonce() {
    let mut e = engine_rig(2, 1).await;
    let l = launch(33);
    see_launch(&e, l, 100).await;
    e.engine
        .record_resolution(
            l,
            LaunchResolution::Resolved { token: TOKEN, curve: CURVE, reserves: Some(payload::pons_reserves_after_buy(U256::from(21_000_000_000_000_000u64))) },
        )
        .await;
    wait_until("primary pre-signed", || signed_slots(&e.r.dispatcher.staged_batch(l)) == 2).await;
    let guard = |sig: &TradeSignal| U256::from_be_slice(&sig.swap_calldata[36..68]);

    // Other buyers pushed the price up: much less ETH-for-token headroom. The curve now holds more ETH.
    let (eth, tok) = payload::pons_reserves_after_buy(U256::from(21_000_000_000_000_000u64));
    assert!(e.engine.update_reserves(l, eth * U256::from(110u64) / U256::from(100u64), tok * U256::from(90u64) / U256::from(100u64)));
    wait_until("primary re-signed", || e.r.dispatcher.presign_snapshot().primary_restaged == 1).await;
    wait_until("re-signed bytes installed", || signed_slots(&e.r.dispatcher.staged_batch(l)) == 2).await;
    assert_eq!(cursor(&e.r.accounts[0]), 1, "same nonce: the allocator was not touched");

    e.engine.process_new_block(101).await;
    let signal = e.rx.try_recv().expect("fires at 101");
    e.r.dispatcher.dispatch_signal(&signal).await.expect("dispatch");
    let s = e.r.dispatcher.presign_snapshot();
    assert_eq!((s.prebuilt_sent, s.resigned_stale), (2, 0), "the engine's last quote matches the re-signed packet");
    let master = &e.r.node.by(e.r.accounts[0].address)[0];
    assert_eq!(U256::from_be_slice(&master.input_head[36..68]), guard(&signal), "the node got the guard of the NEWEST quote");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resolution_that_overtakes_the_arming_task_still_gets_its_primary_staged() {
    // `inspect_transaction` only spawns the arming; resolving right away races it. The primary
    // staging must wait for the batch instead of giving up (NotArmed).
    let e = engine_rig(2, 1).await;
    let launches: Vec<B256> = (0..12u8).map(|n| launch(60 + n)).collect();
    for &l in &launches {
        see_launch(&e, l, 100).await;
        e.engine.record_resolution(l, resolved(TOKEN, CURVE)).await;
    }
    for &l in &launches {
        wait_until("carrier and primary pre-signed", || signed_slots(&e.r.dispatcher.staged_batch(l)) == 2).await;
    }
    // The counter is bumped after the signer thread has installed the bytes: wait for it too.
    wait_until("every primary counted once", || e.r.dispatcher.presign_snapshot().primary_staged >= launches.len() as u64).await;
    assert_eq!(e.r.dispatcher.presign_snapshot().primary_staged, launches.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dispatched_batch_reaches_its_terminal_state() {
    let r = rig(1).await;
    let l = launch(70);
    assert!(r.dispatcher.arm_presign(l, Instant::now() + Duration::from_secs(10)).await);
    let batch = r.dispatcher.staged_batch(l).expect("live");
    assert_eq!(batch.state(), BatchState::Staged);
    r.dispatcher.dispatch_signal(&signal_for(l, CURVE, MIN_OUT)).await.expect("dispatch");
    assert_eq!(batch.state(), BatchState::Dispatched);
    assert!(batch.claim().is_none(), "a dispatched batch cannot be claimed again");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_block_plus_one_profile_stages_carriers_only_and_pola_c_stages_nothing() {
    // Block +1: due at the launch block, so as soon as T_LAUNCH resolves the order fires.
    let mut e = engine_rig(1, 2).await;
    let l = launch(34);
    see_launch(&e, l, 100).await;
    wait_until("carriers pre-signed", || signed_slots(&e.r.dispatcher.staged_batch(l)) == 2).await;
    e.engine.record_resolution(l, resolved(TOKEN, CURVE)).await;
    let signal = e.rx.try_recv().expect("fires on resolution");
    assert_eq!(e.r.dispatcher.presign_snapshot().primary_staged, 0, "no waiting window: the primary is not staged");
    e.r.dispatcher.dispatch_signal(&signal).await.expect("dispatch");
    let s = e.r.dispatcher.presign_snapshot();
    assert_eq!((s.prebuilt_sent, s.signed_inline_fresh), (2, 1));

    // Pola C on an already resolved curve is the 0-delay profile: no arming at all.
    let companion = autotradesniper::models::prysm::COMPANION_ACCOUNTS[0];
    let armed_before = e.r.dispatcher.presign_snapshot().armed;
    let buy = {
        let mut v = Vec::from(payload::PONS_BUY_SELECTOR);
        v.extend_from_slice(&[0u8; 96]);
        v
    };
    e.engine.inspect_transaction(companion, Some(CURVE), U256::from(1u64), &buy, B256::from([0xcc; 32]), 100, Instant::now()).await;
    // (The launch above already took the entry slot, so this companion buy is a duplicate; what matters
    // is that no batch is armed for it.)
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(e.r.dispatcher.presign_snapshot().armed, armed_before);
    assert!(e.rx.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn switching_prysm_off_releases_every_staged_batch() {
    let e = engine_rig(2, 2).await;
    see_launch(&e, launch(35), 100).await;
    wait_until("carriers pre-signed", || signed_slots(&e.r.dispatcher.staged_batch(launch(35))) == 2).await;

    e.r.config.prysm_enabled.store(false, Ordering::SeqCst);
    e.engine.process_new_block(101).await;
    assert!(!e.r.dispatcher.has_staged());
    for acc in &e.r.accounts {
        assert_eq!(cursor(acc), 0);
        assert!(acc.nonce_holes.leased_snapshot().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn pending_to_dispatched_is_exactly_once_under_concurrent_blocks_and_resolutions() {
    // 40 launches, 8 tasks hammering `process_new_block` / `record_resolution` / `cancel_launch` at the
    // same time. Each launch must produce exactly one signal (or none if cancelled), never two.
    const LAUNCHES: u8 = 40;
    let mut e = engine_rig(2, 0).await;
    for n in 0..LAUNCHES {
        see_launch(&e, B256::from([n + 1; 32]), 100).await;
    }
    let mut tasks = Vec::new();
    for t in 0..8u8 {
        let engine = e.engine.clone();
        tasks.push(tokio::spawn(async move {
            for n in 0..LAUNCHES {
                let l = B256::from([n + 1; 32]);
                match (n + t) % 4 {
                    0 => engine.record_resolution(l, resolved(Address::repeat_byte(n + 1), Address::repeat_byte(n + 101))).await,
                    1 => engine.process_new_block(101 + (n as u64 % 3)).await,
                    2 => engine.process_new_block(101).await,
                    _ => {
                        if n % 10 == 9 {
                            engine.cancel_launch(l).await;
                        } else {
                            engine.process_new_block(102).await
                        }
                    }
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    // Make sure every non-cancelled launch is resolved and every block has been seen.
    for n in 0..LAUNCHES {
        e.engine.record_resolution(B256::from([n + 1; 32]), resolved(Address::repeat_byte(n + 1), Address::repeat_byte(n + 101))).await;
    }
    e.engine.process_new_block(110).await;

    let mut seen = BTreeSet::new();
    while let Ok(sig) = e.rx.try_recv() {
        assert!(seen.insert(sig.tx_hash), "launch {:?} was released twice", sig.tx_hash);
    }
    let cancelled = (0..LAUNCHES).filter(|n| n % 10 == 9).count();
    assert!(seen.len() >= LAUNCHES as usize - cancelled - 4, "released {} of {} (cancels race with releases)", seen.len(), LAUNCHES);
    assert!(seen.len() <= LAUNCHES as usize);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_block_wakes_the_receipt_resolver_instead_of_waiting_for_the_poll_tick() {
    let node = NodeState::new();
    let url = mock_node(node.clone()).await;
    let mut config = base_config(&url, 0).await;
    config.prysm_enabled.store(true, Ordering::SeqCst);
    *config.prysm_min_block_delay.write().await = 1;
    *config.prysm_max_signal_age_secs.write().await = 30;
    *config.prysm_max_mcap_usd.write().await = 1.0e9;
    config.prysm_launchpads = Arc::new([LAUNCHPAD].into_iter().collect());
    let (signal_tx, mut rx) = mpsc::channel(8);
    // A deliberately slow tick: only the block event can make the resolver react in time.
    let engine = PrysmEngine::new(config, signal_tx)
        .with_broadcaster(Arc::new(BroadcasterPool::new(vec![url])))
        .with_receipt_poll_interval(Duration::from_secs(10));

    let l = launch(40);
    engine
        .inspect_transaction(Address::repeat_byte(0x11), Some(LAUNCHPAD), U256::from(21_000_000_000_000_000u64), &spoof_calldata(), l, 100, Instant::now())
        .await;
    wait_until("the first poll", || node.receipt_polls.load(Ordering::SeqCst) >= 1).await;

    // The receipt appears on the node; the resolver is now asleep until the next 10 s tick.
    let topic = |a: Address| format!("0x{:0>64}", alloy::hex::encode(a.as_slice()));
    *node.receipt.lock().unwrap() = Some(serde_json::json!({
        "status": "0x1",
        "logs": [{"address": format!("{:?}", LAUNCHPAD), "topics": [payload::PONS_LAUNCH_TOPIC_HEX, topic(TOKEN), topic(CURVE)]}]
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(400), rx.recv()).await.is_err(),
        "no new block yet: the resolver must still be waiting for its tick"
    );

    let polls_before = node.receipt_polls.load(Ordering::SeqCst);
    let t0 = Instant::now();
    engine.process_new_block(101).await; // block L+1 is sealed: the receipt of L is surely indexed
    let signal = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await.expect("the block event wakes the resolver").expect("signal");
    assert_eq!((signal.target_token, signal.pool_or_router), (TOKEN, CURVE));
    assert!(t0.elapsed() < Duration::from_secs(3), "released {:?} after the block event, not after the 10 s tick", t0.elapsed());
    assert!(node.receipt_polls.load(Ordering::SeqCst) > polls_before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_block_hook_is_free_while_nothing_is_queued() {
    // `process_new_block` runs for every feed message. With an empty queue it must not wait on
    // anything: 100k calls complete in well under a second even with a config writer holding the lock.
    let e = engine_rig(1, 0).await;
    let _writer = e.r.config.prysm_max_signal_age_secs.write().await; // would stall an awaiting `release_due`
    let t0 = Instant::now();
    for i in 0..100_000u64 {
        e.engine.process_new_block(100 + i / 1_000).await;
    }
    assert!(t0.elapsed() < Duration::from_secs(5), "100k idle block hooks took {:?}", t0.elapsed());
}
