//! Regression tests for the in-memory curve replica (`risk::curve_mirror`), the zero-RPC risk loop
//! built on it, and the receipt-confirmed exit path.
//!
//! The chain is a mock JSON-RPC node that keeps a per-block history of the curve reserves and
//! answers `eth_call` pinned to a block number exactly like a real node (including "header not
//! found" for blocks it has not imported yet). Trades reach the replica as signed transactions in
//! L2 messages, the same bytes the sequencer feed carries; one test drives a real `StreamListener`
//! over WebSocket.

use alloy::consensus::{SignableTransaction, Transaction as _, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::{Decodable2718, Encodable2718};
use alloy::primitives::{address, keccak256, Address, Bytes, TxKind, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use autotradesniper::config::AppConfig;
use autotradesniper::dispatcher::BroadcasterPool;
use autotradesniper::models::PrysmEngine;
use autotradesniper::payload::{self, CalldataPayloadBuilder, DynamicFeeAdjuster};
use autotradesniper::risk::curve_mirror::{
    adaptive_exit_slippage, curve_buy, curve_sell, replay, snipe_tax_bps, CurveMirror, MirrorPolicy, MirrorRead, StaleReason,
    Trade, TradeKind, DEFAULT_TRADE_BPS,
};
use autotradesniper::risk::types::{ExitReason, ExitVenue};
use autotradesniper::risk::RiskManager;
use autotradesniper::stream::StreamListener;
use autotradesniper::types::{Position, TradeSignal};
use autotradesniper::wallet::WalletManager;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::tungstenite::Message;

const MNEMONIC: &str = "test test test test test test test test test test test junk";
const CURVE: Address = address!("00000000000000000000000000000000000c0e7e");
const TOKEN: Address = address!("00000000000000000000000000000000000070c3");
const ROUTER: Address = address!("000000000000000000000000000000000000a0a0");
const START: u64 = 1_000;

fn wei(eth: f64) -> U256 {
    U256::from((eth * 1e18) as u128)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

// ── Transactions and L2 messages ────────────────────────────────────────────

struct Trader {
    signer: PrivateKeySigner,
    nonce: AtomicU64,
}

impl Trader {
    fn new() -> Self {
        Self { signer: PrivateKeySigner::random(), nonce: AtomicU64::new(0) }
    }

    fn tx(&self, to: Address, value: U256, input: Vec<u8>) -> Vec<u8> {
        let tx = TxEip1559 {
            chain_id: 4663,
            nonce: self.nonce.fetch_add(1, Ordering::Relaxed),
            gas_limit: 300_000,
            max_fee_per_gas: 100_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(to),
            value,
            input: Bytes::from(input),
            access_list: Default::default(),
        };
        let sig = self.signer.sign_hash_sync(&tx.signature_hash()).unwrap();
        TxEnvelope::Eip1559(tx.into_signed(sig)).encoded_2718()
    }

    fn buy(&self, quote: U256, min_out: U256) -> Vec<u8> {
        self.tx(CURVE, quote, CalldataPayloadBuilder::build_pons_buy_payload(quote, min_out, self.signer.address()))
    }

    fn sell(&self, tokens: U256, min_out: U256) -> Vec<u8> {
        self.tx(CURVE, U256::ZERO, CalldataPayloadBuilder::build_pons_sell_payload(tokens, min_out, self.signer.address()))
    }
}

/// A batch message (kind 3) of signed transactions (kind 4).
fn batch(txs: &[Vec<u8>]) -> Vec<u8> {
    let mut msg = vec![3u8];
    for tx in txs {
        msg.extend_from_slice(&((tx.len() + 1) as u64).to_be_bytes());
        msg.push(4);
        msg.extend_from_slice(tx);
    }
    msg
}

// ── Reference curve (the "chain") ───────────────────────────────────────────

/// Pons V2 semantics written out independently of the replica: fee + tax come off the quote side
/// and leave the curve; a trade below its minimum reverts and changes nothing.
#[derive(Clone, Copy, Debug, PartialEq)]
struct RefCurve {
    e: U256,
    t: U256,
    bps: u32,
}

impl RefCurve {
    fn new(e: U256, t: U256) -> Self {
        Self { e, t, bps: DEFAULT_TRADE_BPS }
    }

    fn buy(&mut self, q: U256, min_out: U256) -> bool {
        let net = q - q * U256::from(self.bps) / U256::from(10_000u64);
        let out = self.t * net / (self.e + net);
        if out < min_out {
            return false;
        }
        self.e += net;
        self.t -= out;
        true
    }

    fn sell(&mut self, tin: U256, min_out: U256) -> bool {
        let gross = self.e * tin / (self.t + tin);
        if gross - gross * U256::from(self.bps) / U256::from(10_000u64) < min_out {
            return false;
        }
        self.e -= gross;
        self.t += tin;
        true
    }

    fn value_of(&self, tokens: U256) -> U256 {
        CalldataPayloadBuilder::calculate_pons_eth_out(self.e, self.t, tokens, self.bps, 0)
    }

    fn tokens_for(&self, q: U256) -> U256 {
        let net = q - q * U256::from(self.bps) / U256::from(10_000u64);
        self.t * net / (self.e + net)
    }
}

fn launched(dev_buy_eth: f64) -> RefCurve {
    let (e, t) = payload::pons_reserves_after_buy(wei(dev_buy_eth));
    RefCurve::new(e, t)
}

// ── Mock node ───────────────────────────────────────────────────────────────

struct Chain {
    /// Reserves after each block (blocks without a change inherit the previous entry).
    history: BTreeMap<u64, (U256, U256)>,
    /// Highest block the node has imported.
    node_head: u64,
    balance: U256,
    allowance: U256,
    /// Status of the next receipts, in send order ("0x1" once empty).
    statuses: VecDeque<&'static str>,
    receipts: HashMap<B256, &'static str>,
    sells: Vec<(Instant, Vec<u8>)>,
    /// (when, method, first 4 bytes of calldata / tag)
    log: Vec<(Instant, String, String)>,
    /// Transaction hashes per block for `eth_getBlockByNumber` (unanswered when absent).
    blocks: HashMap<u64, Vec<B256>>,
}

impl Chain {
    fn new(at: u64, curve: RefCurve) -> Self {
        Self {
            history: BTreeMap::from([(at, (curve.e, curve.t))]),
            node_head: at,
            balance: U256::ZERO,
            allowance: U256::MAX,
            statuses: VecDeque::new(),
            receipts: HashMap::new(),
            sells: Vec::new(),
            log: Vec::new(),
            blocks: HashMap::new(),
        }
    }

    fn reserves_at(&self, block: u64) -> Option<(U256, U256)> {
        self.history.range(..=block).next_back().map(|(_, r)| *r)
    }

    fn get_reserves_calls(&self) -> usize {
        self.log.iter().filter(|(_, m, sel)| m == "eth_call" && sel == "0902f1ac").count()
    }

    fn eth_calls_between(&self, from: Instant, to: Instant) -> Vec<String> {
        self.log.iter().filter(|(at, m, _)| m == "eth_call" && *at >= from && *at <= to).map(|(_, _, s)| s.clone()).collect()
    }
}

fn word(v: U256) -> String {
    alloy::hex::encode(v.to_be_bytes::<32>())
}

fn answer(chain: &Mutex<Chain>, req: &serde_json::Value) -> serde_json::Value {
    let mut c = chain.lock().unwrap();
    let method = req["method"].as_str().unwrap_or_default().to_string();
    let err = |msg: &str| serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":msg}});
    let result = match method.as_str() {
        "eth_call" => {
            let data = req["params"][0]["data"].as_str().unwrap_or_default().trim_start_matches("0x").to_string();
            let sel = data[..data.len().min(8)].to_string();
            c.log.push((Instant::now(), method.clone(), sel.clone()));
            let block = match req["params"][1].as_str().unwrap_or("latest") {
                "latest" => c.node_head,
                hex => u64::from_str_radix(hex.trim_start_matches("0x"), 16).unwrap(),
            };
            if block > c.node_head {
                return err("header not found");
            }
            match sel.as_str() {
                "0902f1ac" => match c.reserves_at(block) {
                    Some((e, t)) => serde_json::json!(format!("0x{}{}", word(e), word(t))),
                    None => return err("execution reverted"),
                },
                "70a08231" => serde_json::json!(format!("0x{}", word(c.balance))),
                "dd62ed3e" => serde_json::json!(format!("0x{}", word(c.allowance))),
                "3de35b79" => serde_json::json!(format!("0x{:0>64}", alloy::hex::encode(TOKEN.as_slice()))),
                _ => return err("unsupported"),
            }
        }
        "eth_blockNumber" => {
            c.log.push((Instant::now(), method.clone(), String::new()));
            serde_json::json!(format!("0x{:x}", c.node_head))
        }
        "eth_sendRawTransaction" => {
            let raw = alloy::hex::decode(req["params"][0].as_str().unwrap()).unwrap();
            let hash = keccak256(&raw);
            c.log.push((Instant::now(), method.clone(), String::new()));
            let status = c.statuses.pop_front().unwrap_or("0x1");
            if status != "none" {
                c.receipts.insert(hash, status);
            }
            let tx = TxEnvelope::decode_2718_exact(&raw[..]).unwrap();
            if tx.input().first_chunk::<4>() == Some(&payload::PONS_SELL_SELECTOR) {
                c.sells.push((Instant::now(), raw.clone()));
                if status == "0x1" {
                    c.balance = U256::ZERO;
                }
            }
            serde_json::json!(format!("{:?}", hash))
        }
        "eth_getBlockByNumber" => {
            let n = u64::from_str_radix(req["params"][0].as_str().unwrap().trim_start_matches("0x"), 16).unwrap();
            match c.blocks.get(&n) {
                Some(txs) => serde_json::json!({"number": format!("0x{n:x}"), "transactions": txs.iter().map(|h| format!("{h:?}")).collect::<Vec<_>>()}),
                None => return err("unsupported"),
            }
        }
        "eth_getTransactionReceipt" => {
            c.log.push((Instant::now(), method.clone(), String::new()));
            let hash: B256 = req["params"][0].as_str().unwrap().parse().unwrap();
            match c.receipts.get(&hash) {
                Some(status) => serde_json::json!({"status": status, "logs": []}),
                None => serde_json::Value::Null,
            }
        }
        _ => return err("unsupported"),
    };
    serde_json::json!({"jsonrpc":"2.0","id":1,"result":result})
}

async fn mock_node(chain: Arc<Mutex<Chain>>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let chain = chain.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                loop {
                    let header_end = loop {
                        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break p + 4;
                        }
                        let mut chunk = [0u8; 16384];
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
                        let mut chunk = [0u8; 16384];
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let req: serde_json::Value = serde_json::from_slice(&buf[header_end..header_end + len]).unwrap_or_default();
                    buf.drain(..header_end + len);
                    let body = answer(&chain, &req).to_string();
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

async fn wait_until(what: &str, within: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn fresh(m: &CurveMirror) -> Option<(U256, U256)> {
    m.read(&CURVE).fresh().map(|s| s.reserves())
}

/// A mirror tracking CURVE, anchored exactly at `START` on `curve`.
fn anchored(policy: MirrorPolicy, curve: RefCurve) -> Arc<CurveMirror> {
    let m = Arc::new(CurveMirror::new(policy));
    m.begin_block(START, unix_now(), true);
    m.end_block(START);
    m.track(CURVE);
    assert!(m.apply_anchor(CURVE, START, curve.e, curve.t));
    m
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Local state = chain state
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn snipe_tax_follows_the_launch_window_schedule() {
    assert_eq!([0, 1, 2, 3, 60].map(snipe_tax_bps), [9_900, 618, 19, 0, 0]);
}

#[test]
fn the_trade_model_uses_the_exact_integer_math_of_the_rpc_path() {
    let c = launched(0.3);
    for q in [wei(0.0001), wei(0.01), wei(1.7)] {
        let (_, _, out) = curve_buy(c.e, c.t, q, 300).unwrap();
        assert_eq!(out, CalldataPayloadBuilder::calculate_pons_tokens_out(c.e, c.t, q, 100, 200));
    }
    for tin in [U256::from(1u64), wei(1_000_000.0), wei(80_000_000.0)] {
        let (_, _, got) = curve_sell(c.e, c.t, tin, 300).unwrap();
        assert_eq!(got, CalldataPayloadBuilder::calculate_pons_eth_out(c.e, c.t, tin, 100, 200));
    }
}

#[test]
fn replay_tracks_the_reference_curve_exactly_over_random_trading() {
    let mut rng = StdRng::seed_from_u64(7);
    let trader = Trader::new();
    let mut chain = launched(0.5);
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, chain);
    let k0 = chain.e * chain.t;
    let mut reverted = 0;

    for block in START + 1..START + 120 {
        let mut txs = Vec::new();
        for _ in 0..rng.gen_range(0..4) {
            if rng.gen_bool(0.55) {
                let q = wei(rng.gen_range(0.001..0.2));
                // A quarter of the buys ask for more than the curve gives: they revert.
                let fair = chain.tokens_for(q);
                let min_out = if rng.gen_bool(0.25) { fair * U256::from(105u64) / U256::from(100u64) } else { fair * U256::from(9u64) / U256::from(10u64) };
                reverted += usize::from(!chain.buy(q, min_out));
                txs.push(trader.buy(q, min_out));
            } else {
                let tin = chain.t / U256::from(rng.gen_range(200u64..5_000));
                let min_out = if rng.gen_bool(0.2) { wei(100.0) } else { U256::ZERO };
                reverted += usize::from(!chain.sell(tin, min_out));
                txs.push(trader.sell(tin, min_out));
            }
        }
        m.ingest_l2_message(block, unix_now(), &batch(&txs));
        assert_eq!(fresh(&m), Some((chain.e, chain.t)), "block {block}: replica == chain");
    }
    assert!(reverted > 5, "the sequence exercised revert prediction ({reverted})");
    // Constant product on net amounts: only integer rounding moves it.
    let k1 = chain.e * chain.t;
    let dk = if k1 > k0 { k1 - k0 } else { k0 - k1 };
    assert!(dk * U256::from(1_000_000_000_000u64) < k0, "k drifted beyond rounding");
    let s = m.stats();
    assert_eq!(s.trades_reverted, reverted as u64, "every revert predicted exactly");
    assert!(s.trades_applied > 100);
}

#[test]
fn blocks_delivered_out_of_order_converge_to_the_in_order_state() {
    let trader = Trader::new();
    let mut chain = launched(0.4);
    let policy = MirrorPolicy { stale_after: Duration::from_secs(60), gap_grace: Duration::from_secs(60), ..MirrorPolicy::default() };
    let m = anchored(policy, chain);
    let blocks: Vec<(u64, Vec<u8>)> = (1..=10u64)
        .map(|i| {
            let q = wei(0.01 * i as f64);
            assert!(chain.buy(q, U256::ZERO));
            let tin = chain.t / U256::from(1_000u64 + i);
            assert!(chain.sell(tin, U256::ZERO));
            (START + i, batch(&[trader.buy(q, U256::ZERO), trader.sell(tin, U256::ZERO)]))
        })
        .collect();
    // Racing feeds: 1, 2, 5, 6, 3, 9, 4, 10, 7, 8.
    for i in [0usize, 1, 4, 5, 2, 8, 3, 9, 6, 7] {
        m.ingest_l2_message(blocks[i].0, unix_now(), &blocks[i].1);
    }
    assert_eq!(fresh(&m), Some((chain.e, chain.t)), "order of arrival does not change the state");
}

#[test]
fn a_launch_window_buy_pays_the_snipe_tax_in_the_replay() {
    let trader = Trader::new();
    let chain = launched(0.0);
    let m = Arc::new(CurveMirror::new(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }));
    let t0 = unix_now();
    m.begin_block(START, t0, true); // the launch block
    m.end_block(START);
    m.note_launch(CURVE, START);
    m.track(CURVE);
    assert!(m.apply_anchor(CURVE, START, chain.e, chain.t));
    let q = wei(0.05);
    // One second after launch: 618 bps snipe tax on top of fee + creator tax.
    m.ingest_l2_message(START + 1, t0 + 1, &batch(&[trader.buy(q, U256::ZERO)]));
    let (e, t, _) = curve_buy(chain.e, chain.t, q, DEFAULT_TRADE_BPS + 618).unwrap();
    assert_eq!(fresh(&m), Some((e, t)));
    // Three seconds after: no snipe tax.
    m.ingest_l2_message(START + 2, t0 + 3, &batch(&[trader.buy(q, U256::ZERO)]));
    let (e2, t2, _) = curve_buy(e, t, q, DEFAULT_TRADE_BPS).unwrap();
    assert_eq!(fresh(&m), Some((e2, t2)));
}

#[test]
fn the_total_buy_tax_is_capped_at_99_percent_never_zeroing_a_launch_second_buy() {
    let c = launched(0.0);
    let q = wei(0.05);
    // Launch second: fee + creator tax (300) + snipe tax (9,900) = 10,200 bps -> capped at 9,900.
    let (e, t, out) = curve_buy(c.e, c.t, q, DEFAULT_TRADE_BPS + snipe_tax_bps(0)).unwrap();
    assert_eq!((e, t, out), curve_buy(c.e, c.t, q, payload::PONS_MAX_TOTAL_BUY_TAX_BPS).unwrap());
    let one_percent = q / U256::from(100u64);
    assert_eq!(out, c.t * one_percent / (c.e + one_percent), "~1% of the quote is priced");
    assert!(out > U256::ZERO);
    // The RPC-path formula agrees (and never goes negative or to zero either).
    assert_eq!(out, CalldataPayloadBuilder::calculate_pons_tokens_out(c.e, c.t, q, 100, 200 + 9_900));
    assert_eq!(out, CalldataPayloadBuilder::calculate_pons_tokens_out(c.e, c.t, q, 100, 9_900));
    // Below the cap nothing changes.
    let (_, _, at_618) = curve_buy(c.e, c.t, q, DEFAULT_TRADE_BPS + 618).unwrap();
    assert_eq!(at_618, CalldataPayloadBuilder::calculate_pons_tokens_out(c.e, c.t, q, 100, 200 + 618));

    // The replica replays a launch-second buy at the cap: reserves move by ~1% of the quote.
    let trader = Trader::new();
    let m = Arc::new(CurveMirror::new(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }));
    let t0 = unix_now();
    m.begin_block(START, t0, true);
    m.end_block(START);
    m.note_launch(CURVE, START);
    m.track(CURVE);
    assert!(m.apply_anchor(CURVE, START, c.e, c.t));
    m.ingest_l2_message(START + 1, t0, &batch(&[trader.buy(q, U256::from(1u64))]));
    assert_eq!(fresh(&m), Some((e, t)), "applied, not predicted to revert on a zero fill");
}

#[test]
fn launch_second_buys_alone_never_calibrate_the_fee() {
    // At the 99% ceiling a buy is priced the same whatever the fee + creator tax: a window made of
    // launch-second buys says nothing about the fee and must not "calibrate" it to 0 bps.
    let trader = Trader::new();
    let c = launched(0.0);
    let m = Arc::new(CurveMirror::new(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }));
    let t0 = unix_now();
    m.begin_block(START, t0, true);
    m.end_block(START);
    m.note_launch(CURVE, START);
    m.track(CURVE);
    assert!(m.apply_anchor(CURVE, START, c.e, c.t));
    let (mut e, mut t) = (c.e, c.t);
    let mut txs = Vec::new();
    for q in [wei(0.2), wei(0.5)] {
        let (e2, t2, _) = curve_buy(e, t, q, DEFAULT_TRADE_BPS + snipe_tax_bps(0)).unwrap();
        (e, t) = (e2, t2);
        txs.push(trader.buy(q, U256::from(1u64)));
    }
    m.ingest_l2_message(START + 1, t0, &batch(&txs));
    assert!(m.apply_anchor(CURVE, START + 1, e, t));
    assert_eq!(m.stats().calibrations, 0, "nothing to calibrate on");
    assert_eq!(m.snapshot_any(&CURVE).unwrap().exit_fee_bps, DEFAULT_TRADE_BPS, "the fee model is untouched");
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Missed packets, reorgs, things the feed cannot show
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_block_is_tolerated_for_the_grace_then_forces_an_exact_reading() {
    let mut truth = launched(0.4);
    let policy = MirrorPolicy { gap_grace: Duration::from_millis(80), stale_after: Duration::from_secs(60), ..MirrorPolicy::default() };
    let chain = Arc::new(Mutex::new(Chain::new(START, truth)));
    let url = mock_node(chain.clone()).await;
    let m = anchored(policy, truth);
    m.set_reader(Arc::new(BroadcasterPool::new(vec![url])));

    m.ingest_l2_message(START + 1, unix_now(), &batch(&[]));
    // Block START+2 holds a dump the feed never delivers.
    assert!(truth.sell(truth.t / U256::from(20u64), U256::ZERO));
    chain.lock().unwrap().history.insert(START + 2, (truth.e, truth.t));
    m.ingest_l2_message(START + 3, unix_now(), &batch(&[]));
    chain.lock().unwrap().node_head = START + 3;

    assert!(m.read(&CURVE).fresh().is_some(), "within the grace a slower feed may still deliver it");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(m.read(&CURVE), MirrorRead::Stale(StaleReason::Gap), "lost: not trusted any more");

    assert_eq!(m.anchor_due_now().await, 1);
    assert_eq!(fresh(&m), Some((truth.e, truth.t)), "re-anchored past the gap: the dump is in");
    assert!(m.stats().gaps >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gap_filled_late_by_another_feed_needs_no_rpc() {
    let trader = Trader::new();
    let mut chain = launched(0.4);
    let policy = MirrorPolicy { gap_grace: Duration::from_millis(500), stale_after: Duration::from_secs(60), ..MirrorPolicy::default() };
    let m = anchored(policy, chain);
    let q = wei(0.05);
    assert!(chain.buy(q, U256::ZERO));
    let late = batch(&[trader.buy(q, U256::ZERO)]);
    m.ingest_l2_message(START + 2, unix_now(), &batch(&[]));
    m.ingest_l2_message(START + 1, unix_now(), &late); // the gap filler
    assert_eq!(fresh(&m), Some((chain.e, chain.t)));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(m.read(&CURVE).fresh().is_some(), "filled: no gap left to expire");
    assert_eq!(m.stats().anchors_ok, 1, "only the initial anchor");
}

#[test]
fn an_uninspected_block_is_a_gap_at_once() {
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, launched(0.4));
    m.begin_block(START + 1, unix_now(), false); // e.g. a retryable from L1, or an undecodable payload
    m.end_block(START + 1);
    assert_eq!(m.read(&CURVE), MirrorRead::Stale(StaleReason::Gap));
}

#[test]
fn calls_the_replica_cannot_decode_make_the_curve_suspect_and_approvals_do_not() {
    let trader = Trader::new();
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, launched(0.4));
    // approve(curve, x) on the token: harmless.
    let approve = CalldataPayloadBuilder::build_erc20_approve_payload(CURVE, U256::MAX);
    m.ingest_l2_message(START + 1, unix_now(), &batch(&[trader.tx(TOKEN, U256::ZERO, approve)]));
    assert!(m.read(&CURVE).fresh().is_some());
    // A router call that names the curve: the trade happens inside, invisible to us.
    let mut routed = vec![0x12, 0x34, 0x56, 0x78];
    routed.extend_from_slice(&[0u8; 12]);
    routed.extend_from_slice(CURVE.as_slice());
    m.ingest_l2_message(START + 2, unix_now(), &batch(&[trader.tx(ROUTER, wei(0.1), routed)]));
    assert_eq!(m.read(&CURVE), MirrorRead::Stale(StaleReason::Indirect));
    // So is a direct call with a selector we do not model, and a buy whose ethIn != msg.value.
    let m2 = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, launched(0.4));
    let lying = trader.tx(CURVE, wei(0.1), CalldataPayloadBuilder::build_pons_buy_payload(wei(0.2), U256::ZERO, Address::ZERO));
    m2.ingest_l2_message(START + 1, unix_now(), &batch(&[lying]));
    assert_eq!(m2.read(&CURVE), MirrorRead::Stale(StaleReason::Indirect));
    // An exact reading clears it.
    let c = launched(0.4);
    assert!(m.apply_anchor(CURVE, START + 2, c.e, c.t));
    assert!(m.read(&CURVE).fresh().is_some());
}

#[test]
fn a_revert_prediction_too_close_to_call_is_not_trusted() {
    let trader = Trader::new();
    let chain = launched(0.4);
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, chain);
    let q = wei(0.05);
    let exact = chain.tokens_for(q);
    // min = predicted output - 0.05%: inside the 10 bps band, the outcome depends on rounding we
    // cannot see (and on anything we got slightly wrong).
    let min_out = exact - exact / U256::from(2_000u64);
    m.ingest_l2_message(START + 1, unix_now(), &batch(&[trader.buy(q, min_out)]));
    assert_eq!(m.read(&CURVE), MirrorRead::Stale(StaleReason::Uncertain));
    assert_eq!(m.stats().uncertain, 1);
}

#[test]
fn feed_silence_beyond_the_tolerance_is_stale_and_any_block_revives_it() {
    let m = anchored(MirrorPolicy { stale_after: Duration::from_millis(50), ..MirrorPolicy::default() }, launched(0.4));
    assert!(m.read(&CURVE).fresh().is_some());
    std::thread::sleep(Duration::from_millis(70));
    assert_eq!(m.read(&CURVE), MirrorRead::Stale(StaleReason::FeedSilent));
    // A block that does not touch the curve is still news: nothing changed up to it.
    m.ingest_l2_message(START + 1, unix_now(), &batch(&[]));
    assert!(m.read(&CURVE).fresh().is_some());
}

#[test]
fn a_redelivered_block_with_new_content_forces_a_fresh_anchor() {
    let trader = Trader::new();
    let chain = launched(0.4);
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, chain);
    m.ingest_l2_message(START + 1, unix_now(), &batch(&[trader.buy(wei(0.3), U256::ZERO)]));
    m.on_reorg(START + 1);
    assert_eq!(m.read(&CURVE), MirrorRead::Stale(StaleReason::Reorg));
    // Canonical branch: the buy never happened; an anchor at the head re-bases on it.
    assert!(m.apply_anchor(CURVE, START + 1, chain.e, chain.t));
    assert_eq!(fresh(&m), Some((chain.e, chain.t)));
}

#[test]
fn an_anchor_rebases_replays_only_newer_trades_and_measures_the_drift() {
    let trader = Trader::new();
    let mut chain = launched(0.4);
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, chain);
    // Block 1: a sell the feed shows but that reverted on-chain (seller had no balance).
    let phantom = chain.t / U256::from(10u64);
    m.ingest_l2_message(START + 1, unix_now(), &batch(&[trader.sell(phantom, U256::ZERO)]));
    let at_1 = chain;
    // Block 2: a real buy.
    let q = wei(0.08);
    m.ingest_l2_message(START + 2, unix_now(), &batch(&[trader.buy(q, U256::ZERO)]));
    assert!(chain.buy(q, U256::ZERO));
    assert_ne!(fresh(&m), Some((chain.e, chain.t)), "the phantom sell is in the prediction");

    // Exact reading after block 1: no sell happened. Block 2's buy is replayed on top of it.
    assert!(m.apply_anchor(CURVE, START + 1, at_1.e, at_1.t));
    assert_eq!(fresh(&m), Some((chain.e, chain.t)));
    let s = m.stats();
    assert_eq!(s.drift_corrections, 1);
    assert!(s.max_drift_ppb > 1_000_000, "a 10% sell is a large miss: {} ppb", s.max_drift_ppb);
    // An older reading arriving late changes nothing.
    assert!(!m.apply_anchor(CURVE, START, at_1.e, at_1.t));
    assert_eq!(fresh(&m), Some((chain.e, chain.t)));
}

#[test]
fn the_real_creator_tax_is_calibrated_from_two_anchors() {
    let trader = Trader::new();
    // The curve charges fee 1% + creator tax 5% = 600 bps; the replica assumes 300.
    let mut chain = launched(0.4);
    chain.bps = 600;
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, chain);
    for i in 1..=3u64 {
        let q = wei(0.05 * i as f64);
        assert!(chain.buy(q, U256::ZERO));
        m.ingest_l2_message(START + i, unix_now(), &batch(&[trader.buy(q, U256::ZERO)]));
    }
    assert_ne!(fresh(&m), Some((chain.e, chain.t)), "300 bps assumed: off");
    assert!(m.apply_anchor(CURVE, START + 3, chain.e, chain.t));
    assert_eq!(m.stats().calibrations, 1);
    assert_eq!(m.read(&CURVE).fresh().unwrap().exit_fee_bps, 600);

    // From now on the prediction is exact: the next anchor finds no drift.
    let drift_before = m.stats().drift_corrections;
    for i in 4..=6u64 {
        let q = wei(0.04 * i as f64);
        assert!(chain.buy(q, U256::ZERO));
        let tin = chain.t / U256::from(500u64);
        assert!(chain.sell(tin, U256::ZERO));
        m.ingest_l2_message(START + i, unix_now(), &batch(&[trader.buy(q, U256::ZERO), trader.sell(tin, U256::ZERO)]));
        assert_eq!(fresh(&m), Some((chain.e, chain.t)));
    }
    assert!(m.apply_anchor(CURVE, START + 6, chain.e, chain.t));
    assert_eq!(m.stats().drift_corrections, drift_before);
}

#[test]
fn only_an_unbroken_chain_of_exact_readings_validates_past_blocks() {
    let trader = Trader::new();
    let mut chain = launched(0.4);
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, chain);
    let range = |m: &CurveMirror| m.snapshot_any(&CURVE).map(|s| (s.validated_from, s.validated_through)).unwrap();
    assert_eq!(range(&m), (START, START), "first reading: nothing proven yet");

    // Block 1: a buy that reverted on-chain (feed only). The reading after it misses: block 1 is
    // never proven, whatever comes later.
    m.ingest_l2_message(START + 1, unix_now(), &batch(&[trader.buy(wei(0.3), U256::ZERO)]));
    assert!(m.apply_anchor(CURVE, START + 1, chain.e, chain.t));
    assert_eq!(range(&m), (START + 1, START + 1));

    // Blocks 2-3: real trades, readings match to the wei: (1, 3] is proven, block 1 still is not.
    for b in START + 2..=START + 3 {
        assert!(chain.buy(wei(0.02), U256::ZERO));
        m.ingest_l2_message(b, unix_now(), &batch(&[trader.buy(wei(0.02), U256::ZERO)]));
        assert!(m.apply_anchor(CURVE, b, chain.e, chain.t));
    }
    let (from, through) = range(&m);
    assert_eq!((from, through), (START + 1, START + 3));
    let fake_pump_block = START + 1;
    assert!(!(from < fake_pump_block && fake_pump_block <= through), "the fake pump's block is outside the proven range");

    // A reorg reaching back to the last reading discards it: the next reading proves nothing
    // about what came before it.
    m.ingest_l2_message(START + 4, unix_now(), &batch(&[]));
    m.on_reorg(START + 3);
    assert!(m.apply_anchor(CURVE, START + 4, chain.e, chain.t));
    assert_eq!(range(&m), (START + 4, START + 4));
}

#[test]
fn a_curve_that_breaks_the_constant_product_is_flagged() {
    let chain = launched(0.4);
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, chain);
    m.ingest_l2_message(START + 1, unix_now(), &batch(&[]));
    // Fees kept inside the reserves (or a donation): eth * tok grows by 1%.
    assert!(m.apply_anchor(CURVE, START + 1, chain.e * U256::from(101u64) / U256::from(100u64), chain.t));
    assert_eq!(m.stats().invariant_breaks, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pinned_reads_wait_for_the_node_to_import_the_block() {
    let mut truth = launched(0.4);
    let chain = Arc::new(Mutex::new(Chain::new(START, truth)));
    let url = mock_node(chain.clone()).await;
    let m = Arc::new(CurveMirror::new(MirrorPolicy { stale_after: Duration::from_secs(60), anchor_lag_blocks: 0, ..MirrorPolicy::default() }));
    m.set_reader(Arc::new(BroadcasterPool::new(vec![url])));
    m.begin_block(START + 5, unix_now(), true);
    m.end_block(START + 5);
    m.track(CURVE);
    assert!(truth.buy(wei(0.2), U256::ZERO));
    chain.lock().unwrap().history.insert(START + 5, (truth.e, truth.t));
    // The feed is 5 blocks ahead of the node; the node catches up after 300 ms.
    let c2 = chain.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        c2.lock().unwrap().node_head = START + 5;
    });
    let t0 = Instant::now();
    assert_eq!(m.anchor_due_now().await, 1);
    assert!(t0.elapsed() >= Duration::from_millis(250), "it waited for the block, it did not read `latest`");
    assert_eq!(fresh(&m), Some((truth.e, truth.t)), "state of exactly block START+5");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_block_numbering_disables_the_replica() {
    let chain = Arc::new(Mutex::new(Chain::new(5, launched(0.4))));
    let url = mock_node(chain.clone()).await;
    let m = Arc::new(CurveMirror::new(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }));
    m.set_reader(Arc::new(BroadcasterPool::new(vec![url])));
    m.begin_block(9_000_000, unix_now(), true); // FEED_GENESIS_BLOCK off by millions
    m.end_block(9_000_000);
    m.track(CURVE);
    assert_eq!(m.anchor_due_now().await, 0);
    assert!(m.is_disabled());
    assert_eq!(m.read(&CURVE), MirrorRead::Stale(StaleReason::Disabled));
}

#[test]
fn reads_are_sub_microsecond_class_and_allocation_free_in_spirit() {
    let trader = Trader::new();
    let mut chain = launched(0.4);
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, chain);
    for i in 1..=16u64 {
        let q = wei(0.01);
        assert!(chain.buy(q, U256::ZERO));
        m.ingest_l2_message(START + i, unix_now(), &batch(&[trader.buy(q, U256::ZERO)]));
    }
    let held = wei(1_000_000.0);
    let n = 200_000;
    let t0 = Instant::now();
    let mut acc = U256::ZERO;
    for _ in 0..n {
        acc += m.read(&CURVE).fresh().unwrap().value_of(held);
    }
    let per = t0.elapsed() / n;
    assert!(acc > U256::ZERO);
    // Debug build: generous bound. `cargo bench --bench mirror_latency` has the release number.
    assert!(per < Duration::from_micros(20), "one read + valuation took {per:?}");
}

#[test]
fn adaptive_exit_slippage_widens_with_volatility_within_bounds() {
    assert_eq!(adaptive_exit_slippage(3.0, 0, false), 3.0);
    assert_eq!(adaptive_exit_slippage(3.0, 400, false), 9.0); // 4% adverse steps -> +6%
    assert_eq!(adaptive_exit_slippage(3.0, 5_000, false), 25.0); // capped
    assert_eq!(adaptive_exit_slippage(50.0, 5_000, true), 90.0);
    assert_eq!(adaptive_exit_slippage(35.0, 0, false), 35.0, "an escalated retry is never narrowed");
    assert_eq!(adaptive_exit_slippage(f64::NAN, 0, false), 5.0);
}

#[test]
fn replay_is_order_sensitive_only_through_the_sort_key() {
    let c = launched(0.4);
    let t = |block, index, kind| Trade { block, index, kind };
    let trades = [
        t(START + 1, 0, TradeKind::Buy { quote_in: wei(0.1), min_out: U256::ZERO }),
        t(START + 1, 1, TradeKind::Sell { tokens_in: c.t / U256::from(100u64), min_out: U256::ZERO }),
    ];
    let out = replay(c.e, c.t, &trades, 300, 10, |_| 0);
    assert_eq!((out.executed, out.reverted), (2, 0));
    let swapped = [trades[1], trades[0]];
    assert_ne!(replay(c.e, c.t, &swapped, 300, 10, |_| 0).eth, out.eth, "chain order matters, hence the sorted journal");
}

#[test]
fn hostile_amounts_cannot_wrap_the_reserves() {
    let trader = Trader::new();
    let chain = launched(0.4);
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, chain);
    // sell(2^256 - tok + 1e24): with wrapping math the reserves became ~2^256 ETH. It is more than
    // the whole supply, so it certainly reverts: not applied, the state stays fresh.
    let wrap = U256::MAX - chain.t + U256::from(10u64).pow(U256::from(24u64)) + U256::from(1u64);
    m.ingest_l2_message(START + 1, unix_now(), &batch(&[trader.sell(wrap, U256::ZERO)]));
    assert_eq!(fresh(&m), Some((chain.e, chain.t)));
    assert_eq!(m.stats().trades_reverted, 1);
    // A sell just within the supply but overflowing the product: unmodellable, suspect.
    let (e, t) = (U256::MAX / U256::from(2u64), U256::from(1u64));
    assert_eq!(curve_sell(e, t, U256::from(10u64), 300), None);
    assert_eq!(curve_buy(U256::MAX, U256::from(1u64), U256::from(10u64), 0), None);
}

#[test]
fn a_block_with_a_delayed_inbox_unsigned_tx_is_not_trusted() {
    let trader = Trader::new();
    let m = anchored(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }, launched(0.4));
    // Batch: a normal signed tx, then an L2 kind-0 (unsigned user tx from L1): Nitro executes it,
    // we cannot read it as a signed tx.
    let mut msg = batch(&[trader.buy(wei(0.01), U256::ZERO)]);
    let unsigned = [0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    msg.extend_from_slice(&(unsigned.len() as u64).to_be_bytes());
    msg.extend_from_slice(&unsigned);
    m.ingest_l2_message(START + 1, unix_now(), &msg);
    assert_eq!(m.read(&CURVE), MirrorRead::Stale(StaleReason::Gap));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_one_block_numbering_offset_is_caught_by_the_tx_probe() {
    let trader = Trader::new();
    let truth = launched(0.4);
    let chain = Arc::new(Mutex::new(Chain::new(START, truth)));
    let url = mock_node(chain.clone()).await;
    let m = Arc::new(CurveMirror::new(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }));
    m.set_reader(Arc::new(BroadcasterPool::new(vec![url])));
    m.begin_block(START, unix_now(), true);
    m.end_block(START);
    m.track(CURVE);
    let raw = trader.buy(wei(0.01), U256::ZERO);
    m.ingest_l2_message(START + 1, unix_now(), &batch(std::slice::from_ref(&raw)));
    {
        let mut c = chain.lock().unwrap();
        c.node_head = START + 1;
        // The node's block START+1 holds *other* txs: our feed numbering is one block off.
        c.blocks.insert(START + 1, vec![B256::repeat_byte(7)]);
    }
    m.anchor_due_now().await;
    assert!(!m.is_disabled(), "one missing sample may be a tx Nitro rejected: not yet");
    // A second sample, 16 blocks later, is missing too: the numbering is off.
    let raw2 = trader.buy(wei(0.01), U256::ZERO);
    for b in START + 2..=START + 17 {
        m.ingest_l2_message(b, unix_now(), &batch(std::slice::from_ref(&raw2)));
    }
    {
        let mut c = chain.lock().unwrap();
        c.node_head = START + 17;
        c.blocks.insert(START + 17, vec![B256::repeat_byte(8)]);
    }
    tokio::time::sleep(Duration::from_millis(550)).await;
    m.anchor_due_now().await;
    assert!(m.is_disabled(), "heads agree, but the exact probe does not");
    assert_eq!(m.read(&CURVE), MirrorRead::Stale(StaleReason::Disabled));

    // Correct numbering: the tx is where the feed said it was.
    let m2 = Arc::new(CurveMirror::new(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }));
    let chain2 = Arc::new(Mutex::new(Chain::new(START, truth)));
    let url2 = mock_node(chain2.clone()).await;
    m2.set_reader(Arc::new(BroadcasterPool::new(vec![url2])));
    m2.begin_block(START, unix_now(), true);
    m2.end_block(START);
    m2.track(CURVE);
    m2.ingest_l2_message(START + 1, unix_now(), &batch(std::slice::from_ref(&raw)));
    {
        let mut c = chain2.lock().unwrap();
        c.node_head = START + 1;
        c.history.insert(START + 1, (truth.e, truth.t));
        c.blocks.insert(START + 1, vec![B256::repeat_byte(1), keccak256(&raw)]);
    }
    m2.anchor_due_now().await;
    assert!(m2.numbering_verified());
    assert!(!m2.is_disabled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_curve_whose_reserves_cannot_be_read_backs_off() {
    // A pool tagged as a curve whose getReserves reverts: no endless 10/s retry loop.
    let chain = Arc::new(Mutex::new(Chain::new(START, launched(0.4))));
    chain.lock().unwrap().history.clear(); // every getReserves -> "no reserves" error
    let url = mock_node(chain.clone()).await;
    let m = Arc::new(CurveMirror::new(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }));
    let worker = m.spawn_anchor_worker(Arc::new(BroadcasterPool::new(vec![url])));
    m.begin_block(START, unix_now(), true);
    m.end_block(START);
    m.track(CURVE);
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    let calls = chain.lock().unwrap().get_reserves_calls();
    worker.abort();
    // Unbounded: ~10/s for 3 s (30+). With back-off: 200 ms, 400, 800, 1.6 s, ...
    assert!(calls <= 8, "{calls} reads in 3 s");
    assert!(m.stats().anchors_failed >= 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Zero-RPC risk loop, transitions on the replica, receipt-confirmed exits
// ═══════════════════════════════════════════════════════════════════════════

/// Trailing-stop distance of the risk-engine worlds below, in percent.
const TRAIL_PERCENT: f64 = 15.0;

struct World {
    chain: Arc<Mutex<Chain>>,
    mirror: Option<Arc<CurveMirror>>,
    _rm: Arc<RiskManager>,
    positions: Arc<RwLock<Vec<Position>>>,
    truth: RefCurve,
    held: U256,
    block: u64,
    trader: Trader,
    monitor: tokio::task::JoinHandle<()>,
    _worker: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for World {
    fn drop(&mut self) {
        self.monitor.abort();
        if let Some(w) = &self._worker {
            w.abort();
        }
    }
}

impl World {
    /// A filled 0.01 ETH position on CURVE; `policy = None` builds the pre-replica engine.
    async fn new(policy: Option<MirrorPolicy>) -> Self {
        let mut truth = launched(0.3);
        let entry = wei(0.01);
        let held = truth.tokens_for(entry);
        assert!(truth.buy(entry, U256::ZERO));
        let chain = Arc::new(Mutex::new(Chain::new(START, truth)));
        chain.lock().unwrap().balance = held;
        let url = mock_node(chain.clone()).await;

        let mut config = AppConfig::default();
        config.send_rpcs = vec![url.clone()];
        config.rpc_http_url = "http://127.0.0.1:1".to_string();
        *config.mnemonic_phrase.write().await = MNEMONIC.to_string();
        // These scenarios were laid out for a 15% trailing distance (set explicitly: the default is 10%).
        *config.trailing_stop_percent.write().await = TRAIL_PERCENT;
        *config.sub_account_count.write().await = 0;
        let wm = WalletManager::new(config.clone()).await.unwrap();
        let positions = Arc::new(RwLock::new(Vec::new()));
        let (exit_tx, exit_rx) = mpsc::channel(64);
        std::mem::forget(exit_rx);
        let pool = Arc::new(BroadcasterPool::new(vec![url]));
        let mut rm = RiskManager::new(config, wm, positions.clone(), exit_tx)
            .with_execution(pool.clone(), Arc::new(DynamicFeeAdjuster::new(0.0, 1.0)))
            .with_reader(pool.clone());
        let mut mirror = None;
        let mut worker = None;
        if let Some(policy) = policy {
            let m = Arc::new(CurveMirror::new(policy));
            m.begin_block(START, unix_now(), true);
            m.end_block(START);
            worker = Some(m.spawn_anchor_worker(pool.clone()));
            rm = rm.with_curve_mirror(m.clone());
            mirror = Some(m);
        }
        let rm = Arc::new(rm);
        rm.register_entry(TOKEN, CURVE, 0.01, None, ExitVenue::PonsCurve).await;
        let runner = rm.clone();
        let monitor = tokio::spawn(async move { runner.start_lifecycle_monitor().await });
        Self { chain, mirror, _rm: rm, positions, truth, held, block: START, trader: Trader::new(), monitor, _worker: worker }
    }

    /// Next block: `trades` go to the feed (the replica); those flagged `true` also execute on the chain.
    fn block(&mut self, trades: &[(TradeKind, bool)]) {
        self.block += 1;
        let mut txs = Vec::new();
        for (kind, on_chain) in trades {
            match *kind {
                TradeKind::Buy { quote_in, min_out } => {
                    if *on_chain {
                        self.truth.buy(quote_in, min_out);
                    }
                    txs.push(self.trader.buy(quote_in, min_out));
                }
                TradeKind::Sell { tokens_in, min_out } => {
                    if *on_chain {
                        self.truth.sell(tokens_in, min_out);
                    }
                    txs.push(self.trader.sell(tokens_in, min_out));
                }
            }
        }
        {
            let mut c = self.chain.lock().unwrap();
            c.history.insert(self.block, (self.truth.e, self.truth.t));
            c.node_head = self.block;
        }
        if let Some(m) = &self.mirror {
            m.ingest_l2_message(self.block, unix_now(), &batch(&txs));
        }
    }

    /// A block with a router call that names the curve (no state change): the replica re-anchors.
    fn touch_curve(&mut self) {
        let mut routed = vec![0x12, 0x34, 0x56, 0x78];
        routed.extend_from_slice(&[0u8; 12]);
        routed.extend_from_slice(CURVE.as_slice());
        let raw = self.trader.tx(ROUTER, U256::ZERO, routed);
        self.block += 1;
        {
            let mut c = self.chain.lock().unwrap();
            c.history.insert(self.block, (self.truth.e, self.truth.t));
            c.node_head = self.block;
        }
        if let Some(m) = &self.mirror {
            m.ingest_l2_message(self.block, unix_now(), &batch(&[raw]));
        }
    }

    fn value(&self) -> f64 {
        f64::from(self.truth.value_of(self.held)) / 1e18
    }

    fn buy(&mut self, eth: f64) {
        self.block(&[(TradeKind::Buy { quote_in: wei(eth), min_out: U256::ZERO }, true)]);
    }

    fn sell_fraction(&mut self, divisor: u64, on_chain: bool) {
        let tin = self.truth.t / U256::from(divisor);
        self.block(&[(TradeKind::Sell { tokens_in: tin, min_out: U256::ZERO }, on_chain)]);
    }

    async fn position(&self) -> Position {
        self.positions.read().await[0].clone()
    }

    /// PnL the loop must report for the current chain state (same formula, same rounding).
    fn expected_pnl(&self) -> f64 {
        let v = f64::from(self.truth.value_of(self.held)) / 1e18;
        (v - 0.01) / 0.01 * 100.0
    }

    async fn settle(&self) {
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    async fn wait_filled_and_mirrored(&self) {
        let m = self.mirror.clone().unwrap();
        wait_until("anchor + fill", Duration::from_secs(5), || m.read(&CURVE).fresh().is_some()).await;
        let deadline = Instant::now() + Duration::from_secs(5);
        while (self.position().await.pnl_percent - self.expected_pnl()).abs() > 1e-9 {
            assert!(Instant::now() < deadline, "loop never valued the filled position");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn quiet(policy: MirrorPolicy) -> MirrorPolicy {
    // Long verification interval: anchors happen only when needed, so every RPC read in a test is
    // one the test caused on purpose.
    MirrorPolicy { verify_every: Duration::from_secs(3_600), ..policy }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn positions_are_valued_in_memory_with_no_getreserves_traffic() {
    const RUN: Duration = Duration::from_millis(2_000);

    // Control: the pre-replica engine, same position, same duration.
    let control = World::new(None).await;
    let c0 = control.chain.lock().unwrap().get_reserves_calls();
    tokio::time::sleep(RUN).await;
    let control_calls = control.chain.lock().unwrap().get_reserves_calls() - c0;
    drop(control);

    let mut w = World::new(Some(quiet(MirrorPolicy::default()))).await;
    w.wait_filled_and_mirrored().await;
    tokio::time::sleep(Duration::from_millis(200)).await; // the fill's one-off exit pre-approval check
    let before = w.chain.lock().unwrap().get_reserves_calls();
    let calls_before = w.chain.lock().unwrap().log.iter().filter(|(_, m, _)| m == "eth_call").count();
    let t0 = Instant::now();
    let mut i = 0u64;
    while t0.elapsed() < RUN {
        // Steady trading: a buy or a sell every 50 ms (a busy curve).
        if i % 3 == 2 { w.sell_fraction(400, true) } else { w.buy(0.004) }
        i += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    w.settle().await;
    let mirror_calls = w.chain.lock().unwrap().get_reserves_calls() - before;
    let all_calls = w.chain.lock().unwrap().log.iter().filter(|(_, m, _)| m == "eth_call").count() - calls_before;

    let per_min = |n: usize| n as f64 * 60_000.0 / RUN.as_millis() as f64;
    eprintln!(
        "getReserves per minute for one position: {:.0} via RPC loop vs {:.0} with the replica ({} trades replayed); all eth_call: {:.0}/min",
        per_min(control_calls),
        per_min(mirror_calls),
        i,
        per_min(all_calls)
    );
    assert!(control_calls >= 12, "the old loop reads reserves every tick ({control_calls})");
    assert_eq!(mirror_calls, 0, "no reserve read while the replica is fresh");
    assert_eq!(all_calls, 0, "token and balance known: the loop issues no eth_call at all");
    // And it is not blind: the reported PnL is the chain's, to the last bit of the f64.
    assert_eq!(w.position().await.pnl_percent, w.expected_pnl());
    let s = w.mirror.as_ref().unwrap().stats();
    assert!(s.reads_fresh > 20, "the loop read the replica ({})", s.reads_fresh);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_trailing_stop_fires_on_the_exact_block_from_the_replica_with_zero_reads_before_the_sell() {
    let mut w = World::new(Some(quiet(MirrorPolicy { confirm_exits: false, ..MirrorPolicy::default() }))).await;
    w.wait_filled_and_mirrored().await;
    // A second consumer of the curve: the replica outlives the position's close for inspection.
    w.mirror.as_ref().unwrap().track(CURVE);

    // Pump to well above +20% (activation), then bleed 3% of the token side per block.
    for _ in 0..8 {
        w.buy(0.06);
        w.settle().await;
    }
    assert!(w.expected_pnl() > 20.0, "activated: {}", w.expected_pnl());
    let mut peak = f64::MIN;
    let mut trigger_block = None;
    for _ in 0..30 {
        let v = f64::from(w.truth.value_of(w.held)) / 1e18;
        peak = peak.max(v);
        w.sell_fraction(33, true);
        let v_after = f64::from(w.truth.value_of(w.held)) / 1e18;
        let drawdown = (peak - v_after) / peak * 100.0;
        let crossed_at = Instant::now();
        w.settle().await;
        let sent = w.chain.lock().unwrap().sells.len();
        if drawdown >= TRAIL_PERCENT {
            // This very block: the loop was woken by the replica, valued, triggered and sold.
            wait_until("the trailing-stop sell", Duration::from_secs(2), || !w.chain.lock().unwrap().sells.is_empty()).await;
            trigger_block = Some(w.block);
            let sent_at = w.chain.lock().unwrap().sells[0].0;
            let reads = w.chain.lock().unwrap().eth_calls_between(crossed_at, sent_at);
            assert!(reads.is_empty(), "no RPC read between the trigger and the broadcast: {reads:?}");
            break;
        }
        assert_eq!(sent, 0, "drawdown {drawdown:.2}% < {TRAIL_PERCENT}%: no exit yet");
    }
    let block = trigger_block.expect("the path crosses -15% from the peak");

    // minOut: the replica's quote for exactly our balance, minus the adaptive tolerance.
    let snap = w.mirror.as_ref().unwrap().snapshot_any(&CURVE).unwrap();
    let raw = w.chain.lock().unwrap().sells[0].1.clone();
    let tx = TxEnvelope::decode_2718_exact(&raw[..]).unwrap();
    let input = tx.input();
    assert_eq!(U256::from_be_slice(&input[4..36]), w.held, "sells exactly the tokens held");
    let slip = adaptive_exit_slippage(3.0, snap.volatility_bps, false);
    let expected_min = w.truth.value_of(w.held) * U256::from(10_000 - (slip * 100.0) as u64) / U256::from(10_000u64);
    assert_eq!(U256::from_be_slice(&input[36..68]), expected_min);
    assert!(slip > 3.0, "a bleeding curve widens the tolerance ({slip}%)");

    wait_until("closed on the receipt", Duration::from_secs(3), || {
        futures_util::FutureExt::now_or_never(w.positions.read()).is_some_and(|p| !p[0].is_open)
    })
    .await;
    let pos = w.position().await;
    assert!(pos.exit_reason.as_deref().unwrap_or_default().contains(&ExitReason::TrailingStop.to_string()), "{:?}", pos.exit_reason);
    assert!(pos.exit_tx_hash.is_some());
    eprintln!("trailing stop fired on block {block} (peak-to-current drawdown crossed 15% there)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_phantom_dump_is_rejected_by_the_chain_and_a_real_one_is_sold() {
    let mut w = World::new(Some(quiet(MirrorPolicy::default()))).await; // confirm_exits on
    w.wait_filled_and_mirrored().await;
    let m = w.mirror.clone().unwrap();

    // A huge sell shows up in the feed but reverts on-chain (the seller held nothing).
    w.sell_fraction(3, false);
    wait_until("the confirmation", Duration::from_secs(3), || m.stats().phantom_exits_rejected == 1).await;
    w.settle().await;
    assert!(w.chain.lock().unwrap().sells.is_empty(), "nothing sold on a move the chain never made");
    assert!(w.position().await.is_open);
    assert_eq!(fresh(&m), Some((w.truth.e, w.truth.t)), "the confirmation re-anchored the replica on the truth");
    assert!(m.stats().exit_confirmations >= 1);

    // The same dump for real.
    w.sell_fraction(3, true);
    wait_until("the stop-loss sell", Duration::from_secs(3), || !w.chain.lock().unwrap().sells.is_empty()).await;
    wait_until("closed", Duration::from_secs(3), || {
        futures_util::FutureExt::now_or_never(w.positions.read()).is_some_and(|p| !p[0].is_open)
    })
    .await;
    let pos = w.position().await;
    assert!(pos.exit_reason.as_deref().unwrap_or_default().contains(&ExitReason::StopLoss.to_string()), "{:?}", pos.exit_reason);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pre_graduation_guard_fires_from_the_replica() {
    let mut w = World::new(Some(quiet(MirrorPolicy::default()))).await;
    w.wait_filled_and_mirrored().await;
    // Raise the ETH reserve towards 90% of the graduation target in big buys.
    let target = U256::from(payload::PONS_INITIAL_ETH_RESERVE_WEI) + U256::from(payload::PONS_GRADUATION_ETH_WEI) * U256::from(91u64) / U256::from(100u64);
    while w.truth.e < target {
        w.buy(0.6);
        w.settle().await;
        if !w.chain.lock().unwrap().sells.is_empty() {
            break;
        }
    }
    wait_until("the pre-graduation exit", Duration::from_secs(3), || !w.chain.lock().unwrap().sells.is_empty()).await;
    wait_until("closed", Duration::from_secs(3), || {
        futures_util::FutureExt::now_or_never(w.positions.read()).is_some_and(|p| !p[0].is_open)
    })
    .await;
    let pos = w.position().await;
    assert!(
        pos.exit_reason.as_deref().unwrap_or_default().contains(&ExitReason::PreGraduationGuard.to_string()),
        "{:?}",
        pos.exit_reason
    );
    assert!(payload::pons_curve_capacity_percent(w.truth.e) >= 90.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_feed_falls_back_to_rpc_and_a_live_one_stops_it() {
    let policy = quiet(MirrorPolicy { stale_after: Duration::from_millis(300), gap_grace: Duration::from_millis(100), ..MirrorPolicy::default() });
    let mut w = World::new(Some(policy)).await;
    w.wait_filled_and_mirrored().await;
    let m = w.mirror.clone().unwrap();

    // The feed goes quiet while the chain moves on (blocks the replica never sees).
    let mirror_saved = w.mirror.take();
    for _ in 0..3 {
        w.buy(0.02);
    }
    let before = w.chain.lock().unwrap().get_reserves_calls();
    tokio::time::sleep(Duration::from_millis(900)).await;
    let during = w.chain.lock().unwrap().get_reserves_calls() - before;
    assert_eq!(m.read(&CURVE), MirrorRead::Stale(StaleReason::FeedSilent));
    assert!(during >= 4, "stale replica: the loop reads reserves over RPC again ({during})");
    assert_eq!(w.position().await.pnl_percent, w.expected_pnl(), "valuation kept following the chain");

    // The feed comes back: the missed blocks are a gap, re-anchored past, then memory again.
    w.mirror = mirror_saved;
    for _ in 0..12 {
        w.buy(0.001);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    wait_until("fresh again", Duration::from_secs(3), || m.read(&CURVE).fresh().is_some()).await;
    assert_eq!(fresh(&m), Some((w.truth.e, w.truth.t)), "the blocks it missed are in the state");
    let settled = w.chain.lock().unwrap().get_reserves_calls();
    for _ in 0..10 {
        w.buy(0.001);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(w.chain.lock().unwrap().get_reserves_calls(), settled, "live feed: back to zero reserve reads");
    assert_eq!(w.position().await.pnl_percent, w.expected_pnl());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_position_closes_only_on_a_successful_receipt() {
    let mut w = World::new(Some(quiet(MirrorPolicy { confirm_exits: false, ..MirrorPolicy::default() }))).await;
    w.wait_filled_and_mirrored().await;
    let m = w.mirror.clone().unwrap();
    // The first sell reverts on-chain, the second one lands.
    w.chain.lock().unwrap().statuses.extend(["0x0", "0x1"]);

    w.sell_fraction(3, true); // a real dump: stop-loss
    wait_until("two sells", Duration::from_secs(5), || w.chain.lock().unwrap().sells.len() == 2).await;
    wait_until("closed", Duration::from_secs(3), || {
        futures_util::FutureExt::now_or_never(w.positions.read()).is_some_and(|p| !p[0].is_open)
    })
    .await;
    let (first, second) = {
        let c = w.chain.lock().unwrap();
        (keccak256(&c.sells[0].1), keccak256(&c.sells[1].1))
    };
    let pos = w.position().await;
    assert_eq!(pos.exit_tx_hash.as_deref(), Some(format!("{:?}", second).as_str()), "closed on the sell that succeeded");
    assert_ne!(pos.exit_tx_hash.as_deref(), Some(format!("{:?}", first).as_str()), "never on the reverted one");
    // The revert told the replica it was wrong: the retry's floor came from the chain (a pinned
    // re-anchor or a direct quote), not from the state that produced the reverted one.
    let c = w.chain.lock().unwrap();
    let between = c.eth_calls_between(c.sells[0].0, c.sells[1].0);
    assert!(between.iter().any(|sel| sel == "0902f1ac"), "reserves re-read between the two sells: {between:?}");
    drop(c);
    let _ = m;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_phantom_pump_cannot_inflate_the_trailing_peak() {
    let mut w = World::new(Some(quiet(MirrorPolicy::default()))).await; // confirm_exits on
    w.wait_filled_and_mirrored().await;
    let m = w.mirror.clone().unwrap();
    // A big buy shows up in the feed but reverts on-chain (out of gas): +60% that never happened.
    w.block(&[(TradeKind::Buy { quote_in: wei(0.5), min_out: U256::ZERO }, false)]);
    w.settle().await;
    assert!(w.position().await.pnl_percent > 20.0, "the loop saw the phantom pump (trailing armed on it)");
    assert!(w.chain.lock().unwrap().sells.is_empty(), "a pump is not an exit");

    // Something makes the replica re-anchor (here: a router call naming the curve). The chain
    // never had the pump, so the value 'drops' far below the phantom peak.
    w.touch_curve();
    wait_until("the peak check", Duration::from_secs(5), || m.stats().phantom_exits_rejected >= 1).await;
    w.settle().await;
    assert!(w.chain.lock().unwrap().sells.is_empty(), "no trailing exit off a peak the chain never had");
    let pos = w.position().await;
    assert!(pos.is_open);
    assert_eq!(pos.pnl_percent, w.expected_pnl(), "valuation back on the chain's numbers");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_phantom_dump_on_top_of_a_phantom_pump_triggers_nothing() {
    let mut w = World::new(Some(quiet(MirrorPolicy::default()))).await;
    w.wait_filled_and_mirrored().await;
    let m = w.mirror.clone().unwrap();
    // Feed-only pump (reverted buy), then a feed-only dump that trips the stop-loss on the replica.
    // Re-evaluated on the chain: no stop-loss, and the trailing drawdown must not be measured from
    // the pump either.
    w.block(&[(TradeKind::Buy { quote_in: wei(0.6), min_out: U256::ZERO }, false)]);
    w.settle().await;
    let tin = w.truth.t; // more than the curve holds after the fake pump: a deep fake dump
    w.block(&[(TradeKind::Sell { tokens_in: tin, min_out: U256::ZERO }, false)]);
    wait_until("the confirmation", Duration::from_secs(5), || m.stats().phantom_exits_rejected >= 1).await;
    w.settle().await;
    assert!(w.chain.lock().unwrap().sells.is_empty(), "neither the fake dump nor the fake peak sells");
    let pos = w.position().await;
    assert!(pos.is_open);
    assert_eq!(pos.pnl_percent, w.expected_pnl());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuting_a_fake_peak_keeps_a_stop_armed_on_a_real_one() {
    let mut w = World::new(Some(quiet(MirrorPolicy::default()))).await;
    w.wait_filled_and_mirrored().await;
    let m = w.mirror.clone().unwrap();

    // 1. A real pump to well above +20%; an exact reading then validates it (zero drift).
    for _ in 0..8 {
        w.buy(0.06);
        w.settle().await;
    }
    let real_peak = w.value();
    assert!(w.expected_pnl() > 20.0);
    w.touch_curve();
    wait_until("validated", Duration::from_secs(5), || m.read(&CURVE).fresh().is_some_and(|s| s.validated_through >= START + 9)).await;
    w.settle().await;

    // 2. A real pullback of under 15% from the real peak: armed, no exit.
    w.sell_fraction(25, true);
    w.settle().await;
    let dd = (real_peak - w.value()) / real_peak * 100.0;
    assert!((3.0..TRAIL_PERCENT).contains(&dd), "pullback {dd:.2}%");
    assert!(w.chain.lock().unwrap().sells.is_empty());

    // 3. A fake pump raises the replica's peak; a re-anchor drops the value back: the drawdown from
    //    the fake peak is huge. The peak check must restore the *real* peak, still armed.
    w.block(&[(TradeKind::Buy { quote_in: wei(0.8), min_out: U256::ZERO }, false)]);
    w.settle().await;
    w.touch_curve();
    wait_until("the peak refuted", Duration::from_secs(5), || m.stats().phantom_exits_rejected >= 1).await;
    w.settle().await;
    assert!(w.chain.lock().unwrap().sells.is_empty(), "{dd:.2}% from the real peak: no exit yet");

    // 4. A real drop through -15% from the real peak: the stop fires.
    let mut crossed = false;
    for _ in 0..10 {
        w.sell_fraction(40, true);
        w.settle().await;
        if (real_peak - w.value()) / real_peak * 100.0 >= TRAIL_PERCENT {
            crossed = true;
            break;
        }
        assert!(w.chain.lock().unwrap().sells.is_empty(), "not crossed yet");
    }
    assert!(crossed);
    wait_until("the trailing exit", Duration::from_secs(3), || !w.chain.lock().unwrap().sells.is_empty()).await;
    wait_until("closed", Duration::from_secs(3), || {
        futures_util::FutureExt::now_or_never(w.positions.read()).is_some_and(|p| !p[0].is_open)
    })
    .await;
    let pos = w.position().await;
    assert!(pos.exit_reason.as_deref().unwrap_or_default().contains(&ExitReason::TrailingStop.to_string()), "{:?}", pos.exit_reason);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_balance_hint_cannot_pass_a_reverted_sell_as_sold() {
    let mut w = World::new(Some(quiet(MirrorPolicy { confirm_exits: false, ..MirrorPolicy::default() }))).await;
    w.wait_filled_and_mirrored().await;
    {
        let mut c = w.chain.lock().unwrap();
        // Half the tokens left the wallet since the fill (another position on the same token sold).
        c.balance = w.held / U256::from(2u64);
        // The first sell (of the stale, larger amount) gets no receipt; the retry lands.
        c.statuses.extend(["none", "0x1"]);
    }
    w.sell_fraction(3, true); // stop-loss
    wait_until("the first sell", Duration::from_secs(3), || !w.chain.lock().unwrap().sells.is_empty()).await;
    let first_at = w.chain.lock().unwrap().sells[0].0;
    // The old rule (balance now < stale hint) would close it here, with the tokens still held.
    tokio::time::sleep(Duration::from_millis(5_500)).await;
    assert!(w.position().await.is_open, "unconfirmed sell + balance not zero: still open");
    wait_until("the retry", Duration::from_secs(5), || w.chain.lock().unwrap().sells.len() == 2).await;
    wait_until("closed", Duration::from_secs(3), || {
        futures_util::FutureExt::now_or_never(w.positions.read()).is_some_and(|p| !p[0].is_open)
    })
    .await;
    let (second_hash, second_amount) = {
        let c = w.chain.lock().unwrap();
        let raw = &c.sells[1].1;
        (keccak256(raw), U256::from_be_slice(&TxEnvelope::decode_2718_exact(&raw[..]).unwrap().input()[4..36]))
    };
    assert!(first_at.elapsed() > Duration::from_secs(5));
    assert_eq!(w.position().await.exit_tx_hash.as_deref(), Some(format!("{:?}", second_hash).as_str()));
    assert_eq!(second_amount, w.held / U256::from(2u64), "the retry sold what the wallet really held");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Through the real stream: sells reach the replica, paused intake, reorg detection
// ═══════════════════════════════════════════════════════════════════════════

fn feed_message(seq: u64, l2: &[u8]) -> String {
    format!(
        r#"{{"version":1,"messages":[{{"sequenceNumber":{seq},"message":{{"message":{{"header":{{"kind":3,"timestamp":{ts}}},"l2Msg":"{b64}"}},"delayedMessagesRead":1}}}}]}}"#,
        ts = unix_now(),
        b64 = base64::engine::general_purpose::STANDARD.encode(l2)
    )
}

/// WebSocket feed that sends `frames` at their offsets and then holds the connection open.
async fn feed_server(frames: Vec<(Duration, String)>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let Ok((tcp, _)) = listener.accept().await else { return };
        let Ok(mut ws) = tokio_tungstenite::accept_async(tcp).await else { return };
        let start = Instant::now();
        for (at, text) in frames {
            tokio::time::sleep_until((start + at).into()).await;
            if ws.send(Message::Text(text.into())).await.is_err() {
                return;
            }
        }
        while ws.next().await.is_some() {}
    });
    (url, task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stream_feeds_sells_to_the_replica_even_while_paused_and_detects_reorgs() {
    let trader = Trader::new();
    let mut chain = launched(0.4);
    let tin = chain.t / U256::from(50u64);
    let sell_block = batch(&[trader.sell(tin, U256::ZERO)]);
    let other_block = batch(&[trader.buy(wei(0.5), U256::ZERO)]);
    let ms = |n| Duration::from_millis(n);
    // Feed A: block 0 (genesis START), then a sell in block 1, then an empty block 2.
    let (url_a, _a) = feed_server(vec![
        (ms(0), feed_message(0, &batch(&[]))),
        (ms(500), feed_message(1, &sell_block)),
        (ms(700), feed_message(2, &batch(&[]))),
    ])
    .await;
    // Feed B: late with block 1, and with *different* content: the sequencer reorganised.
    let (url_b, _b) = feed_server(vec![(ms(1_200), feed_message(1, &other_block))]).await;

    let mut config = AppConfig::default();
    config.feed_url = url_a.clone();
    config.feed_urls = vec![url_a, url_b];
    config.feed_genesis_block = START;
    config.rpc_http_url = "http://127.0.0.1:9".to_string();
    let mirror = Arc::new(CurveMirror::new(MirrorPolicy { stale_after: Duration::from_secs(60), ..MirrorPolicy::default() }));
    let (tx, _rx) = mpsc::channel::<TradeSignal>(64);
    let prysm = Arc::new(PrysmEngine::new(config.clone(), tx.clone()));
    let listener = Arc::new(
        StreamListener::new(config.clone(), Arc::new(DynamicFeeAdjuster::new(0.0, 1.0)), prysm, tx).with_curve_mirror(mirror.clone()),
    );
    let runner = listener.clone();
    let task = tokio::spawn(async move {
        let _ = runner.run().await;
    });

    wait_until("block 0 from the feed", Duration::from_secs(5), || mirror.head() == START).await;
    mirror.track(CURVE);
    assert!(mirror.apply_anchor(CURVE, START, chain.e, chain.t));
    // Intake paused (e.g. after /panic): open positions are still supervised.
    config.is_engine_active.store(false, Ordering::SeqCst);

    assert!(chain.sell(tin, U256::ZERO));
    wait_until("the sell in the replica", Duration::from_secs(5), || fresh(&mirror) == Some((chain.e, chain.t))).await;
    wait_until("the reorg", Duration::from_secs(5), || mirror.stats().reorgs == 1).await;
    assert_eq!(mirror.read(&CURVE), MirrorRead::Stale(StaleReason::Reorg));
    task.abort();
}
