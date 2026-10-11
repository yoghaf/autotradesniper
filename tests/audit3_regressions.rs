//! Regression tests for the third audit round (dispatcher + payload: parallel fan-out, nonce lane,
//! gas limits). Every test drives the real `Dispatcher` against a programmable mock JSON-RPC node.

use alloy::consensus::transaction::SignerRecoverable;
use alloy::consensus::{Transaction as _, TxEnvelope};
use alloy::eips::eip2718::Decodable2718;
use alloy::primitives::{keccak256, Address, B256, U256};
use autotradesniper::config::AppConfig;
use autotradesniper::dispatcher::Dispatcher;
use autotradesniper::payload::{self, CalldataPayloadBuilder, DynamicFeeAdjuster};
use autotradesniper::types::{ExecutionMode, TradeSignal};
use autotradesniper::wallet::{TreasuryAccount, WalletManager};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

const MNEMONIC: &str = "test test test test test test test test test test test junk";
const TOKEN: Address = Address::new([0x70; 20]);
const CURVE: Address = Address::new([0xc0; 20]);

// ── Programmable mock node ──────────────────────────────────────────────────

struct Reply {
    status: u16,
    body: String,
    delay: Duration,
}

impl Reply {
    fn accept(raw: &[u8]) -> Self {
        let body = serde_json::json!({"jsonrpc":"2.0","id":1,"result":format!("{:?}", keccak256(raw))});
        Self { status: 200, body: body.to_string(), delay: Duration::ZERO }
    }
    fn rpc_error(message: &str) -> Self {
        let body = serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":message}});
        Self { status: 200, body: body.to_string(), delay: Duration::ZERO }
    }
    /// HTTP 429 carrying a JSON-RPC error body, as public RPC providers send it.
    fn too_many_requests() -> Self {
        let body = serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"rate limit exceeded"}});
        Self { status: 429, body: body.to_string(), delay: Duration::ZERO }
    }
    /// 5xx with a non-JSON body: the tx may have been forwarded upstream (ambiguous).
    fn bad_gateway() -> Self {
        Self { status: 502, body: "bad gateway".to_string(), delay: Duration::ZERO }
    }
    fn after(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

#[derive(Clone, Debug)]
struct Seen {
    from: Address,
    to: Option<Address>,
    nonce: u64,
    gas: u64,
    value: U256,
    input_len: usize,
    zero_input: bool,
    raw_len: usize,
    at: Instant,
}

fn decode_raw(raw: &[u8]) -> Seen {
    let tx = TxEnvelope::decode_2718_exact(raw).expect("valid EIP-2718 tx");
    Seen {
        from: tx.recover_signer().expect("signature"),
        to: tx.to(),
        nonce: tx.nonce(),
        gas: tx.gas_limit(),
        value: tx.value(),
        input_len: tx.input().len(),
        zero_input: tx.input().iter().all(|&b| b == 0),
        raw_len: raw.len(),
        at: Instant::now(),
    }
}

type Handler = Arc<dyn Fn(&Seen, &[u8]) -> Reply + Send + Sync>;

/// Minimal HTTP/1.1 keep-alive JSON-RPC server; `handler` decides every `eth_sendRawTransaction`.
async fn mock_node(handler: Handler) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let handler = handler.clone();
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
                    let reply = if req["method"] == "eth_sendRawTransaction" {
                        let raw = alloy::hex::decode(req["params"][0].as_str().unwrap()).unwrap();
                        let seen = decode_raw(&raw);
                        handler(&seen, &raw)
                    } else {
                        Reply::rpc_error("unsupported")
                    };
                    if !reply.delay.is_zero() {
                        tokio::time::sleep(reply.delay).await;
                    }
                    let resp = format!(
                        "HTTP/1.1 {} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                        reply.status,
                        reply.body.len(),
                        reply.body
                    );
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
    accounts: Vec<TreasuryAccount>,
}

async fn rig(url: &str, carriers: usize, start_nonces: &[u64]) -> Rig {
    let mut config = AppConfig::default();
    config.send_rpcs = vec![url.to_string()];
    config.rpc_http_url = "http://127.0.0.1:1".to_string(); // read RPC down: only local rollback can heal
    *config.mnemonic_phrase.write().await = MNEMONIC.to_string();
    *config.execution_mode.write().await = ExecutionMode::MultiAccount;
    *config.sub_account_count.write().await = carriers;
    *config.buffer_size.write().await = 94_500;
    let wm = WalletManager::new(config.clone()).await.unwrap();
    let accounts = wm.get_all_accounts().await.unwrap();
    for (acc, &n) in accounts.iter().zip(start_nonces) {
        acc.nonce_cursor.store(n, Ordering::SeqCst);
    }
    let (report_tx, rx) = mpsc::channel(64);
    std::mem::forget(rx); // keep the channel open: reports are not under test
    let dispatcher = Arc::new(Dispatcher::new(config, wm, Arc::new(DynamicFeeAdjuster::new(0.0, 1.0)), report_tx));
    Rig { dispatcher, accounts }
}

fn signal() -> TradeSignal {
    let size = U256::from(10_000_000_000_000_000u64);
    TradeSignal {
        tx_hash: B256::ZERO,
        monitored_account: Address::ZERO,
        target_token: TOKEN,
        pool_or_router: CURVE,
        entry_amount_wei: size,
        swap_calldata: CalldataPayloadBuilder::build_pons_buy_payload(size, U256::from(1u64), Address::ZERO),
        detected_at: Instant::now(),
        block_number: 1,
        strategy_tag: Some("Prysm: Pola A".to_string()),
    }
}

fn cursor(acc: &TreasuryAccount) -> u64 {
    acc.nonce_cursor.load(Ordering::SeqCst)
}

// ── Dispatcher: parallel fan-out ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_and_carriers_are_broadcast_in_parallel_not_serially() {
    const HOLD: Duration = Duration::from_millis(1_000);
    let seen = Arc::new(Mutex::new(Vec::<Seen>::new()));
    let log = seen.clone();
    let url = mock_node(Arc::new(move |s, raw| {
        log.lock().unwrap().push(s.clone());
        Reply::accept(raw).after(HOLD)
    }))
    .await;
    let rig = rig(&url, 4, &[0; 5]).await;

    let t0 = Instant::now();
    let report = rig.dispatcher.dispatch_signal(&signal()).await.expect("dispatch");
    let elapsed = t0.elapsed();
    assert_eq!(report.tx_hashes.len(), 5);

    // Five requests that each take 1 s: a serial queue needs >= 5 s, a parallel one ~1 s plus signing.
    // The bound is deliberately loose (4x) so a loaded CI box cannot fake a "serial" result.
    assert!(elapsed < HOLD * 4, "dispatch took {:?}: broadcasts are being serialised", elapsed);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 5);
    let first = seen.iter().map(|s| s.at).min().unwrap();
    let last = seen.iter().map(|s| s.at).max().unwrap();
    // If the last request reached the node before the first one was answered, all five were in flight
    // at once. (Measured unloaded: ~55 ms spread, dominated by signing + hex-encoding 5 x 190 KB.)
    assert!(last - first < HOLD, "arrival spread {:?} reaches the per-request hold: txs were queued", last - first);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn carriers_are_zero_value_self_calls_within_the_size_cap_and_report_describes_the_master() {
    let seen = Arc::new(Mutex::new(Vec::<Seen>::new()));
    let log = seen.clone();
    let url = mock_node(Arc::new(move |s, raw| {
        log.lock().unwrap().push(s.clone());
        Reply::accept(raw)
    }))
    .await;
    let rig = rig(&url, 3, &[0; 4]).await;
    let master = rig.accounts[0].address;

    let report = rig.dispatcher.dispatch_signal(&signal()).await.expect("dispatch");

    let seen = seen.lock().unwrap().clone();
    let (carriers, masters): (Vec<_>, Vec<_>) = seen.iter().partition(|s| s.from != master);
    assert_eq!(carriers.len(), 3);
    assert_eq!(masters.len(), 1);
    for c in &carriers {
        assert_eq!(c.value, U256::ZERO, "a carrier never moves ETH");
        assert_eq!(c.to, Some(c.from), "a carrier only calls its own EOA (no code runs, so it cannot revert)");
        assert!(c.zero_input && c.input_len == 94_500);
        assert!(c.raw_len <= payload::MAX_SEQUENCER_TX_BYTES, "signed tx {} exceeds the sequencer cap", c.raw_len);
        let l1 = CalldataPayloadBuilder::l1_margin(0);
        assert!(c.gas >= CalldataPayloadBuilder::intrinsic_gas(&[], 94_500) + l1, "carrier limit {} below intrinsic + L1", c.gas);
    }
    let m = masters[0];
    assert!(m.raw_len <= payload::MAX_SEQUENCER_TX_BYTES);
    // The report used to describe the LAST account (a carrier: 94 500 bytes, carrier gas).
    assert_eq!(report.calldata_size_bytes, m.input_len);
    assert_eq!(report.calldata_size_bytes, 100 + 94_500);
    assert_eq!(report.gas_limit, m.gas);
}

// ── Dispatcher: 429 / transient failures keep the nonce lane intact ─────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_limited_carriers_hand_their_nonces_back_and_the_primary_still_lands() {
    let master_addr = Arc::new(Mutex::new(Address::ZERO));
    let m = master_addr.clone();
    let url = mock_node(Arc::new(move |s, raw| {
        if s.from == *m.lock().unwrap() { Reply::accept(raw) } else { Reply::too_many_requests() }
    }))
    .await;
    let rig = rig(&url, 3, &[10, 20, 30, 40]).await;
    *master_addr.lock().unwrap() = rig.accounts[0].address;

    let report = rig.dispatcher.dispatch_signal(&signal()).await.expect("master must survive carrier 429s");
    assert_eq!(report.tx_hashes.len(), 1);

    assert_eq!(cursor(&rig.accounts[0]), 11, "master consumed one nonce");
    for (acc, start) in rig.accounts[1..].iter().zip([20, 30, 40]) {
        assert_eq!(cursor(acc), start, "429'd carrier must not leave a nonce gap");
        assert!(acc.nonce_holes.snapshot().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rate_limited_primary_is_re_sent_once() {
    let master_addr = Arc::new(Mutex::new(Address::ZERO));
    let attempts = Arc::new(AtomicUsize::new(0));
    let (m, a) = (master_addr.clone(), attempts.clone());
    let url = mock_node(Arc::new(move |s, raw| {
        if s.from != *m.lock().unwrap() {
            return Reply::accept(raw);
        }
        if a.fetch_add(1, Ordering::SeqCst) == 0 { Reply::too_many_requests() } else { Reply::accept(raw) }
    }))
    .await;
    let rig = rig(&url, 1, &[5, 5]).await;
    *master_addr.lock().unwrap() = rig.accounts[0].address;

    rig.dispatcher.dispatch_signal(&signal()).await.expect("second attempt is accepted");
    assert_eq!(attempts.load(Ordering::SeqCst), 2, "exactly one re-send, never a loop");
    assert_eq!(cursor(&rig.accounts[0]), 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ambiguous_failure_is_reused_by_the_next_dispatch_leaving_no_gap() {
    // The carrier's first broadcast dies with a 502 (the tx may have been forwarded) and the read RPC
    // is down, so nothing can confirm it. The nonce must come back instead of leaking.
    let master_addr = Arc::new(Mutex::new(Address::ZERO));
    let accepted = Arc::new(Mutex::new(Vec::<Seen>::new()));
    let failed_once = Arc::new(AtomicUsize::new(0));
    let (m, acc_log, f) = (master_addr.clone(), accepted.clone(), failed_once.clone());
    let url = mock_node(Arc::new(move |s, raw| {
        if s.from != *m.lock().unwrap() && f.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::bad_gateway();
        }
        acc_log.lock().unwrap().push(s.clone());
        Reply::accept(raw)
    }))
    .await;
    let rig = rig(&url, 1, &[0, 0]).await;
    *master_addr.lock().unwrap() = rig.accounts[0].address;
    let carrier = rig.accounts[1].address;

    for _ in 0..3 {
        let _ = rig.dispatcher.dispatch_signal(&signal()).await.expect("master accepted every time");
    }
    let nonces: Vec<u64> = accepted.lock().unwrap().iter().filter(|s| s.from == carrier).map(|s| s.nonce).collect();
    assert_eq!(nonces.len(), 2, "first carrier broadcast failed, the other two landed");
    assert_eq!(nonces, vec![0, 1], "accepted carrier nonces are gap-free");
    assert_eq!(cursor(&rig.accounts[1]), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_dispatches_with_random_failures_never_gap_or_duplicate_a_nonce() {
    // 12 dispatches, 4 at a time on the same accounts, while the node rejects (429), drops (502) or
    // accepts at random. Per account: accepted nonces are unique and, together with the nonces still
    // parked for reuse, cover [0, cursor) exactly. The old store()-based resync could rewind below
    // an in-flight nonce here.
    let accepted = Arc::new(Mutex::new(Vec::<Seen>::new()));
    let tick = Arc::new(AtomicUsize::new(0));
    let (log, t) = (accepted.clone(), tick.clone());
    let url = mock_node(Arc::new(move |s, raw| {
        let n = t.fetch_add(1, Ordering::SeqCst);
        match n % 4 {
            1 => Reply::too_many_requests(),
            2 => Reply::bad_gateway(),
            _ => {
                log.lock().unwrap().push(s.clone());
                Reply::accept(raw)
            }
        }
    }))
    .await;
    let rig = rig(&url, 3, &[0; 4]).await;

    for _round in 0..3 {
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let d = rig.dispatcher.clone();
            tasks.push(tokio::spawn(async move {
                let _ = d.dispatch_signal(&signal()).await; // master may be refused: that is fine
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    }

    let accepted = accepted.lock().unwrap().clone();
    for acc in &rig.accounts {
        let mut mine: Vec<u64> = accepted.iter().filter(|s| s.from == acc.address).map(|s| s.nonce).collect();
        mine.sort_unstable();
        let unique: BTreeSet<u64> = mine.iter().copied().collect();
        assert_eq!(unique.len(), mine.len(), "{}: a nonce was accepted twice: {:?}", acc.role, mine);
        let parked: BTreeSet<u64> = acc.nonce_holes.snapshot().into_iter().collect();
        assert!(unique.is_disjoint(&parked), "{}: nonce both accepted and parked", acc.role);
        let covered: BTreeSet<u64> = unique.union(&parked).copied().collect();
        let expected: BTreeSet<u64> = (0..cursor(acc)).collect();
        assert_eq!(covered, expected, "{}: gap in [0, {}): accepted {:?}, parked {:?}", acc.role, cursor(acc), mine, parked);
    }
}

// ── Dispatcher: gas-limit rejection is escalated exactly once ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn intrinsic_gas_too_low_is_retried_once_with_the_same_nonce_and_a_higher_limit() {
    const THRESHOLD: u64 = 2_000_000;
    let seen = Arc::new(Mutex::new(Vec::<Seen>::new()));
    let log = seen.clone();
    let url = mock_node(Arc::new(move |s, raw| {
        log.lock().unwrap().push(s.clone());
        if s.gas < THRESHOLD { Reply::rpc_error("intrinsic gas too low") } else { Reply::accept(raw) }
    }))
    .await;
    let rig = rig(&url, 2, &[7, 8, 9]).await;

    let report = rig.dispatcher.dispatch_signal(&signal()).await.expect("escalated retry is accepted");
    assert_eq!(report.tx_hashes.len(), 3);

    let seen = seen.lock().unwrap().clone();
    for (acc, start) in rig.accounts.iter().zip([7u64, 8, 9]) {
        let mine: Vec<&Seen> = seen.iter().filter(|s| s.from == acc.address).collect();
        assert_eq!(mine.len(), 2, "{}: one refusal + one retry", acc.role);
        assert_eq!(mine[0].nonce, start);
        assert_eq!(mine[1].nonce, start, "a gas retry reuses the nonce");
        assert!(mine[1].gas >= THRESHOLD && mine[1].gas >= mine[0].gas * 2 || mine[1].gas == payload::MAX_ESCALATED_GAS_LIMIT);
        assert_eq!(cursor(acc), start + 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_persistent_gas_rejection_stops_after_one_retry_and_returns_every_nonce() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let a = attempts.clone();
    let url = mock_node(Arc::new(move |_, _| {
        a.fetch_add(1, Ordering::SeqCst);
        Reply::rpc_error("intrinsic gas too low: have 1, want 2")
    }))
    .await;
    let rig = rig(&url, 2, &[3, 4, 5]).await;

    assert!(rig.dispatcher.dispatch_signal(&signal()).await.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 6, "3 accounts x (first try + one retry)");
    for (acc, start) in rig.accounts.iter().zip([3, 4, 5]) {
        assert_eq!(cursor(acc), start, "{}: nonce handed back", acc.role);
    }
}

// ── Nonce lane unit behaviour ───────────────────────────────────────────────

async fn master() -> TreasuryAccount {
    let config = AppConfig::default();
    *config.mnemonic_phrase.write().await = MNEMONIC.to_string();
    *config.sub_account_count.write().await = 0;
    WalletManager::new(config).await.unwrap().get_master_account().await.unwrap()
}

#[tokio::test]
async fn a_nonce_returned_under_an_in_flight_later_one_is_reused_first() {
    let acc = master().await;
    let (a, b, c) = (acc.allocate_nonce(), acc.allocate_nonce(), acc.allocate_nonce());
    assert_eq!((a, b, c), (0, 1, 2));

    acc.return_nonce(a); // 1 and 2 are in flight: cursor cannot step back
    assert_eq!(acc.nonce_holes.snapshot(), vec![0]);
    assert_eq!(cursor(&acc), 3, "in-flight nonces are never re-issued");
    assert_eq!(acc.allocate_nonce(), 0, "the parked nonce fills the gap first");
    assert_eq!(acc.allocate_nonce(), 3);

    acc.return_nonce(3); // top of the stack: plain step-back
    assert_eq!(cursor(&acc), 3);
    assert!(acc.nonce_holes.snapshot().is_empty());
}

#[tokio::test]
async fn reconcile_never_rewinds_below_the_node_and_closes_a_too_high_gap() {
    let acc = master().await;
    for _ in 0..6 {
        acc.allocate_nonce(); // 0..=5 in flight, cursor 6
    }
    // Node already sees nonces 0..=3 (pending = 4); nonce 5 failed ambiguously: 4 and 5 are unaccounted.
    acc.reconcile_nonce(5, 4);
    assert_eq!(cursor(&acc), 4, "gap [4, 5] closed by stepping the cursor back to the node's view");
    assert!(acc.nonce_holes.snapshot().is_empty());

    // "nonce too low": the node is ahead of us. Cursor jumps forward, stale parked nonces are dropped.
    let acc = master().await;
    for _ in 0..3 {
        acc.allocate_nonce();
    }
    acc.return_nonce(0); // parked: 1, 2 in flight
    acc.reconcile_nonce(2, 9);
    assert_eq!(cursor(&acc), 9);
    assert!(acc.nonce_holes.snapshot().is_empty(), "parked nonce 0 is below the chain's view");
    assert_eq!(acc.allocate_nonce(), 9);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn allocate_and_return_from_many_threads_never_duplicates_or_loses_a_nonce() {
    let acc = master().await;
    let kept = Arc::new(Mutex::new(Vec::<u64>::new()));
    let mut tasks = Vec::new();
    for t in 0..8u64 {
        let (acc, kept) = (acc.clone(), kept.clone());
        tasks.push(tokio::task::spawn_blocking(move || {
            let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ (t + 1);
            let mut mine = Vec::new();
            for _ in 0..4_000 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let n = acc.allocate_nonce();
                if x % 10 < 3 {
                    acc.return_nonce(n);
                } else {
                    mine.push(n);
                }
            }
            kept.lock().unwrap().extend(mine);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let kept = kept.lock().unwrap().clone();
    let unique: BTreeSet<u64> = kept.iter().copied().collect();
    assert_eq!(unique.len(), kept.len(), "a nonce was handed out twice");
    let parked: BTreeSet<u64> = acc.nonce_holes.snapshot().into_iter().collect();
    assert!(unique.is_disjoint(&parked));
    let covered: BTreeSet<u64> = unique.union(&parked).copied().collect();
    assert_eq!(covered, (0..cursor(&acc)).collect::<BTreeSet<u64>>(), "every nonce below the cursor is used or parked");
}

// ── payload.rs ──────────────────────────────────────────────────────────────

#[test]
fn carrier_padding_is_a_shared_zero_page_not_a_fresh_allocation() {
    let a = CalldataPayloadBuilder::zero_padding(94_500);
    let b = CalldataPayloadBuilder::zero_padding(94_500);
    assert_eq!(a.len(), 94_500);
    assert!(a.iter().all(|&x| x == 0));
    assert_eq!(a.as_ptr(), b.as_ptr(), "every carrier borrows the same static page: zero copies, zero allocations");
    let oversize = CalldataPayloadBuilder::zero_padding(payload::MAX_SEQUENCER_TX_BYTES + 1);
    assert_eq!(oversize.len(), payload::MAX_SEQUENCER_TX_BYTES + 1);
    assert!(oversize.iter().all(|&x| x == 0));
}

#[test]
fn a_missing_l1_estimate_is_never_treated_as_free() {
    let intrinsic = CalldataPayloadBuilder::intrinsic_gas(&[], 94_500);
    assert_eq!(intrinsic, 21_000 + 4 * 94_500);

    // A padded payload is sized for the EIP-7623 calldata floor (it dominates intrinsic + reserve).
    let floor = CalldataPayloadBuilder::calldata_floor_gas(&[], 94_500);
    assert_eq!(floor, 966_000);
    assert!(floor > intrinsic + payload::CARRIER_GAS_RESERVE);

    // Unmeasured (0): fall back to the generous assumption, not to "no poster gas".
    let unknown = CalldataPayloadBuilder::carrier_gas_limit(94_500, 0, 0);
    assert_eq!(unknown, floor + payload::L1_COMPONENT_FALLBACK_GAS * 3 / 2);

    // Measured: exactly 1.5x the measurement, and the configured floor still wins when higher.
    let measured = CalldataPayloadBuilder::carrier_gas_limit(94_500, 0, 200_000);
    assert_eq!(measured, floor + 300_000);
    assert_eq!(CalldataPayloadBuilder::carrier_gas_limit(94_500, 9_000_000, 200_000), 9_000_000);
    // 50k L1 (a typical reading): ~1.04M, the first attempt clears the floor.
    assert_eq!(CalldataPayloadBuilder::carrier_gas_limit(94_500, 600_000, 50_000), 1_041_000);

    // Master: the padded buy is also floor-bound; unpadded, intrinsic + execution reserve rules.
    let swap = CalldataPayloadBuilder::build_pons_buy_payload(U256::from(1u64), U256::from(1u64), Address::ZERO);
    let master = CalldataPayloadBuilder::gas_limit(&swap, 94_500, 0, 200_000);
    assert_eq!(master, CalldataPayloadBuilder::calldata_floor_gas(&swap, 94_500) + 300_000);
    let unpadded = CalldataPayloadBuilder::gas_limit(&swap, 0, 0, 200_000);
    assert_eq!(unpadded, CalldataPayloadBuilder::intrinsic_gas(&swap, 0) + payload::EXECUTION_GAS_RESERVE + 300_000);
}

#[test]
fn escalation_doubles_respects_the_floor_and_is_capped() {
    // EIP-7623 floor of the carrier payload: 21 000 + 10 x 94 500 zero tokens.
    assert_eq!(CalldataPayloadBuilder::calldata_floor_gas(&[], 94_500), 21_000 + 10 * 94_500);
    // A non-zero byte is 4 tokens, a zero byte 1.
    assert_eq!(CalldataPayloadBuilder::calldata_floor_gas(&[0, 1], 0), 21_000 + 10 * (1 + 4));

    let floor = CalldataPayloadBuilder::calldata_floor_gas(&[], 94_500) + CalldataPayloadBuilder::l1_margin(0);
    assert_eq!(CalldataPayloadBuilder::escalated_gas_limit(600_000, floor), floor, "floor dominates a small limit");
    assert_eq!(CalldataPayloadBuilder::escalated_gas_limit(1_500_000, 100_000), 3_000_000, "otherwise double");
    assert_eq!(CalldataPayloadBuilder::escalated_gas_limit(7_000_000, 0), payload::MAX_ESCALATED_GAS_LIMIT, "capped");
}

#[test]
fn padding_clamp_keeps_both_payload_shapes_under_the_sequencer_cap() {
    for swap_len in [0usize, 100, 2_000] {
        let padding = CalldataPayloadBuilder::clamp_padding(swap_len, 94_500);
        assert!(swap_len + padding + payload::TX_ENVELOPE_OVERHEAD <= payload::MAX_SEQUENCER_TX_BYTES);
    }
    assert_eq!(CalldataPayloadBuilder::clamp_padding(0, 94_500), 94_500, "the ~94.5 KB target fits whole");
}
