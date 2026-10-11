//! Latency of the AOT pre-signing pipeline, measured against the on-the-fly hot path.
//!
//! Run in release mode:
//!
//! ```text
//! cargo bench --bench presign_latency
//! ```
//!
//! Part A prices one transaction at the production buffer size (94.5 KB of calldata): the
//! signature hash, the ECDSA signature, the envelope hash/encoding and the hex + JSON body. This
//! is what staging moves off the hot path, per transaction.
//!
//! Part B fires a full entry (master + N carriers) at a local mock node and records when the node
//! has *read* each request completely, relative to the instant the dispatch starts. It compares
//!   * on-the-fly: nothing prepared (the former behaviour),
//!   * carriers staged: what the default Block +1 profile can prepare (the primary needs the
//!     curve address from T_LAUNCH, so it is signed in the hot path),
//!   * fully staged: carriers and primary prepared (an order that waits for a later block),
//!
//! with 2 and 4 runtime worker threads: signing N+1 transactions in parallel on fewer cores than
//! transactions serialises, which is exactly the contention staging removes.
//!
//! The mock node runs on this machine, so absolute numbers are a floor: on a real link the network
//! time adds equally to every mode. The *difference* between the modes is the saving.

use alloy::consensus::{SignableTransaction, TxEip1559};
use alloy::primitives::{keccak256, Address, TxKind, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use autotradesniper::config::AppConfig;
use autotradesniper::dispatcher::{send_raw_tx_body, sign_sync, Dispatcher, PresignPolicy};
use autotradesniper::payload::{self, CalldataPayloadBuilder, DynamicFeeAdjuster, TARGET_CALLDATA_BUFFER_SIZE};
use autotradesniper::types::{ExecutionMode, TradeSignal};
use autotradesniper::wallet::WalletManager;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

const MNEMONIC: &str = "test test test test test test test test test test test junk";
const TOKEN: Address = Address::new([0u8; 20]);

fn stats(mut v: Vec<f64>) -> (f64, f64, f64) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |p: f64| v[((v.len() as f64 - 1.0) * p).round() as usize];
    (q(0.0), q(0.5), q(0.95))
}

fn time_it<F: FnMut()>(iters: usize, mut f: F) -> (f64, f64, f64) {
    for _ in 0..(iters / 10).max(3) {
        f(); // warm up caches and the allocator
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f();
        samples.push(t.elapsed().as_secs_f64() * 1e3);
    }
    stats(samples)
}

fn part_a() {
    println!("── Part A: cost of ONE carrier-sized transaction ({} B calldata) ──", TARGET_CALLDATA_BUFFER_SIZE);
    let signer = PrivateKeySigner::random();
    let tx = TxEip1559 {
        chain_id: 4663,
        nonce: 1,
        gas_limit: 900_000,
        max_fee_per_gas: 100_000_000,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(signer.address()),
        value: U256::ZERO,
        input: vec![0u8; TARGET_CALLDATA_BUFFER_SIZE].into(),
        access_list: Default::default(),
    };

    let sig_hash = time_it(300, || {
        black_box(black_box(&tx).signature_hash());
    });
    let hash: B256 = tx.signature_hash();
    let ecdsa = time_it(300, || {
        black_box(signer.sign_hash_sync(black_box(&hash)).unwrap());
    });
    let all = time_it(300, || {
        black_box(sign_sync(&signer, black_box(tx.clone())).unwrap());
    });
    let signed = sign_sync(&signer, tx.clone()).unwrap();
    let body = time_it(300, || {
        black_box(send_raw_tx_body(black_box(&signed.raw)));
    });
    let keccak = time_it(300, || {
        black_box(keccak256(black_box(&signed.raw)));
    });
    println!("{:<46} {:>9} {:>9} {:>9}", "step (ms)", "min", "median", "p95");
    for (name, (lo, mid, hi)) in [
        ("signature hash (RLP + keccak of the unsigned tx)", sig_hash),
        ("ECDSA sign_hash (32-byte digest)", ecdsa),
        ("keccak of the signed envelope (tx hash)", keccak),
        ("hex + JSON body (eth_sendRawTransaction)", body),
        ("sign_sync total = what staging pre-computes", all),
    ] {
        println!("{:<46} {:>9.3} {:>9.3} {:>9.3}", name, lo, mid, hi);
    }
    println!(
        "signed bytes: {} B raw, {} B request body. Hot path after staging: socket writes only.\n",
        signed.raw.len(),
        signed.body.len()
    );
}

// ── Part B ──────────────────────────────────────────────────────────────────

type Arrivals = Arc<Mutex<Vec<Instant>>>;

/// Minimal keep-alive HTTP node: stamps the instant a request body has been fully read and answers
/// "already known". Deliberately does no decoding, so it adds almost no CPU competition.
async fn bench_node(arrivals: Arrivals) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let arrivals = arrivals.clone();
            tokio::spawn(async move {
                let mut buf: Vec<u8> = Vec::with_capacity(256 * 1024);
                let mut chunk = vec![0u8; 256 * 1024];
                loop {
                    let header_end = loop {
                        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break p + 4;
                        }
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
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let is_send = buf[header_end..header_end + len].windows(22).any(|w| w == b"eth_sendRawTransaction");
                    if is_send {
                        arrivals.lock().unwrap().push(Instant::now());
                    }
                    buf.drain(..header_end + len);
                    let body = if is_send {
                        // "already known" is accepted by the broadcaster as success with the locally computed
                        // hash: the node needs neither to decode the tx nor to hash it (no CPU stolen).
                        "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32000,\"message\":\"already known\"}}".to_string()
                    } else {
                        "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32000,\"message\":\"unsupported\"}}".to_string()
                    };
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

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    OnTheFly,
    CarriersStaged,
    FullyStaged,
}

fn signal(launch: B256, curve: Address, min_out: u64) -> TradeSignal {
    let size = U256::from(10_000_000_000_000_000u64);
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

/// Runs `rounds` entries in `mode`; returns (first arrival, last arrival) in ms after the dispatch started.
async fn scenario(mode: Mode, carriers: usize, rounds: usize) -> Vec<(f64, f64)> {
    let arrivals: Arrivals = Arc::new(Mutex::new(Vec::new()));
    let url = bench_node(arrivals.clone()).await;
    let mut config = AppConfig::default();
    config.send_rpcs = vec![url.clone()];
    config.rpc_http_url = url;
    *config.mnemonic_phrase.write().await = MNEMONIC.to_string();
    *config.execution_mode.write().await = ExecutionMode::MultiAccount;
    *config.sub_account_count.write().await = carriers;
    *config.buffer_size.write().await = TARGET_CALLDATA_BUFFER_SIZE;
    *config.gas_limit.write().await = payload::STATIC_GAS_LIMIT;
    *config.max_priority_fee_gwei.write().await = 0.0;
    *config.max_fee_gwei.write().await = 1_000.0;
    let wm = WalletManager::new(config.clone()).await.unwrap();
    let fee = Arc::new(DynamicFeeAdjuster::new(0.0, 1.0));
    let (report_tx, rx) = mpsc::channel(1024);
    std::mem::forget(rx);
    let dispatcher = Arc::new(Dispatcher::new(config, wm, fee, report_tx).with_presign_policy(PresignPolicy::default()));
    let curve = Address::repeat_byte(0xc0);

    let mut out = Vec::with_capacity(rounds);
    for round in 0..rounds + 3 {
        let launch = B256::from(U256::from(1 + round as u64));
        let sig = signal(launch, curve, 1_000_000_000_000);
        if mode != Mode::OnTheFly {
            assert!(dispatcher.arm_presign(launch, Instant::now() + Duration::from_secs(30)).await);
            if mode == Mode::FullyStaged {
                dispatcher.stage_primary(&sig).await;
            }
            // The staged packets exist before the target block: wait until they are signed.
            let want = if mode == Mode::FullyStaged { carriers + 1 } else { carriers };
            let t = Instant::now();
            while dispatcher.staged_batch(launch).map_or(0, |b| b.slot_specs().iter().filter(|(_, _, signed)| *signed).count()) < want {
                assert!(t.elapsed() < Duration::from_secs(60), "staging did not finish");
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        // Quiesce (previous round's stragglers), then fire.
        tokio::time::sleep(Duration::from_millis(30)).await;
        arrivals.lock().unwrap().clear();
        let t0 = Instant::now();
        dispatcher.dispatch_signal(&sig).await.expect("dispatch");
        tokio::time::sleep(Duration::from_millis(20)).await; // let slower endpoints' writes land
        let seen = arrivals.lock().unwrap().clone();
        assert_eq!(seen.len(), carriers + 1, "every account's tx reaches the node");
        if round >= 3 {
            // first 3 rounds warm connection pools / allocator
            let ms = |i: Instant| i.duration_since(t0).as_secs_f64() * 1e3;
            out.push((ms(*seen.iter().min().unwrap()), ms(*seen.iter().max().unwrap())));
        }
    }
    out
}

fn part_b() {
    println!("── Part B: dispatch start → node has fully received the transactions (loopback, ms) ──");
    println!("{:<16} {:>8} {:>9} | {:>20} | {:>20}", "mode", "workers", "carriers", "first arrival med/p95", "last arrival med/p95");
    for workers in [2usize, 4] {
        for carriers in [3usize, 7] {
            for (name, mode) in [("on-the-fly", Mode::OnTheFly), ("carriers staged", Mode::CarriersStaged), ("fully staged", Mode::FullyStaged)] {
                let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(workers).enable_all().build().unwrap();
                let rows = rt.block_on(scenario(mode, carriers, 25));
                let (_, f_med, f_p95) = stats(rows.iter().map(|r| r.0).collect());
                let (_, l_med, l_p95) = stats(rows.iter().map(|r| r.1).collect());
                println!("{:<16} {:>8} {:>9} | {:>9.2} /{:>9.2} | {:>9.2} /{:>9.2}", name, workers, carriers, f_med, f_p95, l_med, l_p95);
                rt.shutdown_timeout(Duration::from_secs(2));
            }
            println!();
        }
    }
}

fn main() {
    println!("cores available: {}\n", std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0));
    part_a();
    part_b();
}
