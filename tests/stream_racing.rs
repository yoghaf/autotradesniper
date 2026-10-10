//! End-to-end tests of the multi-feed racing listener against real local WebSocket servers.
//!
//! Every server speaks the Nitro broadcaster framing, so these exercise the production
//! connect / reconnect / de-duplication / HTTP-bridge code, not a model of it.

use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::Encodable2718;
use alloy::primitives::{Address, Bytes, TxKind, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use alloy::sol_types::SolCall;
use autotradesniper::config::{AppConfig, Watchlist};
use autotradesniper::models::PrysmEngine;
use autotradesniper::payload::DynamicFeeAdjuster;
use autotradesniper::stream::feed::{connect_feed, REQUESTED_SEQ_HEADER};
use autotradesniper::stream::{FeedTiming, StreamListener};
use autotradesniper::types::TradeSignal;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_hdr_async, MaybeTlsStream};

alloy::sol! {
    function swapExactETHForTokens(uint256 amountOutMin, address[] path, address to, uint256 deadline);
}

// ── fixtures ────────────────────────────────────────────────────────────────────────────

fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

/// A signed V2 `swapExactETHForTokens` from `signer`, one distinct tx per nonce.
fn signed_swap(signer: &PrivateKeySigner, nonce: u64) -> Vec<u8> {
    let input = swapExactETHForTokensCall {
        amountOutMin: U256::from(1u64),
        path: vec![Address::repeat_byte(0xee), Address::repeat_byte(0x7a)],
        to: signer.address(),
        deadline: U256::from(1_900_000_000u64),
    }
    .abi_encode();
    let tx = TxEip1559 {
        chain_id: 4663,
        nonce,
        gas_limit: 300_000,
        max_fee_per_gas: 100_000_000,
        max_priority_fee_per_gas: 1,
        to: TxKind::Call(Address::repeat_byte(0x55)),
        value: U256::from(10u64).pow(U256::from(16u64)),
        input: Bytes::from(input),
        access_list: Default::default(),
    };
    let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    TxEnvelope::Eip1559(tx.into_signed(sig)).encoded_2718()
}

fn tx_hash(raw: &[u8]) -> B256 {
    use alloy::eips::eip2718::Decodable2718;
    *TxEnvelope::decode_2718_exact(raw).unwrap().tx_hash()
}

/// One broadcaster message (`kind` 3 = L2 message, kind 4 payload = a single signed tx).
fn message_json(seq: u64, kind: u8, l2: Option<&[u8]>) -> String {
    let l2_msg = match l2 {
        Some(tx) => {
            let mut payload = vec![4u8];
            payload.extend_from_slice(tx);
            format!("\"{}\"", base64::engine::general_purpose::STANDARD.encode(payload))
        }
        None => "null".to_string(),
    };
    format!(
        r#"{{"sequenceNumber":{seq},"message":{{"message":{{"header":{{"kind":{kind},"timestamp":{ts}}},"l2Msg":{l2_msg}}},"delayedMessagesRead":1}}}}"#,
        ts = unix_now()
    )
}

fn frame(messages: &[String]) -> String {
    format!(r#"{{"version":1,"messages":[{}]}}"#, messages.join(","))
}

fn swap_frame(seq: u64, raw: &[u8]) -> String {
    frame(&[message_json(seq, 3, Some(raw))])
}

struct Plan {
    /// (offset from the moment the socket is established, text frame)
    frames: Vec<(Duration, String)>,
    /// `Some`: drop the TCP connection this long after the last frame. `None`: hold it open.
    drop_after: Option<Duration>,
    /// Hold the socket open but never read or write again: a half-open / black-holed peer.
    deaf: bool,
}

impl Plan {
    fn new(frames: Vec<(Duration, String)>, drop_after: Option<Duration>) -> Self {
        Self { frames, drop_after, deaf: false }
    }
}

struct FeedServer {
    addr: SocketAddr,
    /// `Arbitrum-Requested-Sequence-Number` of each accepted connection, in order.
    resumes: Arc<Mutex<Vec<Option<u64>>>>,
    task: JoinHandle<()>,
}

impl FeedServer {
    fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }
}

impl Drop for FeedServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `plan_for(connection_index, requested_seq)` decides what each connection is sent.
#[allow(clippy::result_large_err)] // the handshake callback's error type is fixed by tungstenite
async fn serve(plan_for: impl Fn(usize, Option<u64>) -> Plan + Send + Sync + 'static) -> FeedServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let resumes = Arc::new(Mutex::new(Vec::new()));
    let plan_for = Arc::new(plan_for);
    let seen = resumes.clone();
    let task = tokio::spawn(async move {
        let mut index = 0usize;
        loop {
            let Ok((tcp, _)) = listener.accept().await else { return };
            let n = index;
            index += 1;
            let plan_for = plan_for.clone();
            let seen = seen.clone();
            tokio::spawn(async move {
                let requested = Arc::new(Mutex::new(None));
                let slot = requested.clone();
                let callback = move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                    *slot.lock().unwrap() = req
                        .headers()
                        .get(REQUESTED_SEQ_HEADER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok());
                    Ok(resp)
                };
                let Ok(mut ws) = accept_hdr_async(tcp, callback).await else { return };
                let requested = *requested.lock().unwrap();
                seen.lock().unwrap().push(requested);
                let plan = plan_for(n, requested);
                let start = Instant::now();
                for (at, text) in plan.frames {
                    tokio::time::sleep_until((start + at).into()).await;
                    if ws.send(Message::Text(text.into())).await.is_err() {
                        return;
                    }
                }
                if plan.deaf {
                    std::future::pending::<()>().await; // keep `ws` alive, never poll it
                }
                match plan.drop_after {
                    Some(d) => {
                        tokio::time::sleep(d).await;
                        drop(ws); // abrupt: no close handshake
                    }
                    None => while ws.next().await.is_some() {},
                }
            });
        }
    });
    FeedServer { addr, resumes, task }
}

fn config_for(urls: &[String], watched: Address) -> AppConfig {
    let mut config = AppConfig::default();
    config.feed_url = urls[0].clone();
    config.feed_urls = urls.to_vec();
    // Nothing listens here: if the HTTP bridge ever starts it fails fast instead of reaching out.
    config.rpc_http_url = "http://127.0.0.1:9".to_string();
    config.watchlist_accounts = Arc::new(Watchlist::new([watched].into_iter().collect()));
    config
}

struct Harness {
    listener: Arc<StreamListener>,
    signals: mpsc::Receiver<TradeSignal>,
    task: JoinHandle<()>,
}

impl Harness {
    fn start(config: AppConfig) -> Self {
        Self::start_with(config, FeedTiming::default())
    }

    fn start_with(config: AppConfig, timing: FeedTiming) -> Self {
        let (tx, signals) = mpsc::channel(256);
        let prysm = Arc::new(PrysmEngine::new(config.clone(), tx.clone()));
        let listener = Arc::new(
            StreamListener::new(config, Arc::new(DynamicFeeAdjuster::new(2.5, 1.25)), prysm, tx).with_timing(timing),
        );
        let runner = listener.clone();
        let task = tokio::spawn(async move {
            let _ = runner.run().await;
        });
        Self { listener, signals, task }
    }

    /// Wait for `n` signals, then linger to prove no further duplicate shows up.
    async fn collect(&mut self, n: usize, within: Duration) -> Vec<TradeSignal> {
        let deadline = tokio::time::Instant::now() + within;
        let mut out = Vec::new();
        while out.len() < n {
            match tokio::time::timeout_at(deadline, self.signals.recv()).await {
                Ok(Some(s)) => out.push(s),
                _ => break,
            }
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        while let Ok(extra) = self.signals.try_recv() {
            out.push(extra);
        }
        out
    }

    fn stats_of(&self, url: &str) -> autotradesniper::stream::FeedStatsSnapshot {
        let label = url.trim_end_matches('/').to_string();
        self.listener.feed_stats().into_iter().find(|s| s.label == label).expect("feed present")
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn hashes(signals: &[TradeSignal]) -> HashSet<B256> {
    signals.iter().map(|s| s.tx_hash).collect()
}

fn at_ms(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

// ── racing ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_faster_feed_wins_and_every_block_is_traded_exactly_once() {
    let signer = PrivateKeySigner::random();
    let txs: Vec<Vec<u8>> = (0..6).map(|n| signed_swap(&signer, n)).collect();
    let make = |lag_ms: u64, txs: Vec<Vec<u8>>| {
        move |_: usize, _: Option<u64>| Plan::new(txs.iter().enumerate().map(|(i, raw)| (at_ms(lag_ms + i as u64 * 25), swap_frame(i as u64, raw))).collect(), None)
    };
    let slow = serve(make(250, txs.clone())).await;
    let fast = serve(make(0, txs.clone())).await;

    // The slow feed is listed (and so connects) first: order in the config must not matter.
    let mut h = Harness::start(config_for(&[slow.url(), fast.url()], signer.address()));
    let signals = h.collect(6, Duration::from_secs(8)).await;

    assert_eq!(signals.len(), 6, "one signal per block, duplicates from the slower feed dropped");
    assert_eq!(hashes(&signals), txs.iter().map(|t| tx_hash(t)).collect::<HashSet<_>>());

    let (s, f) = (h.stats_of(&slow.url()), h.stats_of(&fast.url()));
    assert_eq!((f.wins, f.dups), (6, 0), "fast feed: {f:?}");
    assert_eq!((s.wins, s.dups), (0, 6), "slow feed: {s:?}");
    let lag = s.avg_lag_us.expect("loser measures how late it is");
    assert!((150_000..2_000_000).contains(&lag), "slow feed should trail by ~250 ms, measured {lag} µs");
    assert_eq!(h.listener.gap_stats(), (0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_feeds_still_yield_one_signal_per_block() {
    let signer = PrivateKeySigner::random();
    let txs: Vec<Vec<u8>> = (0..5).map(|n| signed_swap(&signer, n)).collect();
    let mut servers = Vec::new();
    for lag in [0u64, 40, 90] {
        let txs = txs.clone();
        servers.push(
            serve(move |_, _| Plan::new(txs.iter().enumerate().map(|(i, raw)| (at_ms(lag + i as u64 * 30), swap_frame(i as u64, raw))).collect(), None))
            .await,
        );
    }
    let urls: Vec<String> = servers.iter().map(|s| s.url()).collect();
    let mut h = Harness::start(config_for(&urls, signer.address()));
    let signals = h.collect(5, Duration::from_secs(8)).await;
    assert_eq!(signals.len(), 5);
    assert_eq!(hashes(&signals).len(), 5);
    let stats = h.listener.feed_stats();
    assert_eq!(stats.iter().map(|s| s.wins).sum::<u64>(), 5);
    assert_eq!(stats.iter().map(|s| s.dups).sum::<u64>(), 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dying_feed_costs_nothing_and_is_reconnected() {
    let signer = PrivateKeySigner::random();
    let txs: Vec<Vec<u8>> = (0..10).map(|n| signed_swap(&signer, n)).collect();
    // A is fast but dies after block 3 (abrupt TCP drop) and then refuses to carry data.
    let flaky = {
        let txs = txs.clone();
        serve(move |conn, _| {
            if conn == 0 {
                Plan::new((0..4).map(|i| (at_ms(i as u64 * 20), swap_frame(i as u64, &txs[i]))).collect(), Some(at_ms(30)))
            } else {
                Plan::new(vec![], Some(at_ms(10)))
            }
        })
        .await
    };
    // B starts later and carries everything.
    let steady = {
        let txs = txs.clone();
        serve(move |_, _| Plan::new((0..10).map(|i| (at_ms(150 + i as u64 * 60), swap_frame(i as u64, &txs[i]))).collect(), None)).await
    };

    let mut h = Harness::start(config_for(&[flaky.url(), steady.url()], signer.address()));
    let signals = h.collect(10, Duration::from_secs(10)).await;
    assert_eq!(signals.len(), 10, "no block lost, none doubled");
    assert_eq!(hashes(&signals).len(), 10);

    let f = h.stats_of(&flaky.url());
    assert_eq!(f.wins, 4);
    assert!(f.sessions >= 2, "the dead feed must have been retried: {f:?}");
    assert_eq!(h.stats_of(&steady.url()).wins, 6);
    assert!(h.listener.live_feed_count() >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gap_left_by_one_feed_is_filled_by_the_other() {
    // Feed A (fast) loses blocks 2 and 3 on its way to us. With a plain "highest sequence seen"
    // check, B's later 2 and 3 would be discarded as duplicates and those blocks never traded.
    let signer = PrivateKeySigner::random();
    let txs: Vec<Vec<u8>> = (0..5).map(|n| signed_swap(&signer, n)).collect();
    let lossy = {
        let txs = txs.clone();
        serve(move |_, _| Plan::new([0usize, 1, 4].iter().enumerate().map(|(k, &i)| (at_ms(k as u64 * 20), swap_frame(i as u64, &txs[i]))).collect(), None))
        .await
    };
    let complete = {
        let txs = txs.clone();
        serve(move |_, _| Plan::new((0..5).map(|i| (at_ms(300 + i as u64 * 20), swap_frame(i as u64, &txs[i]))).collect(), None)).await
    };

    let mut h = Harness::start(config_for(&[lossy.url(), complete.url()], signer.address()));
    let signals = h.collect(5, Duration::from_secs(8)).await;
    assert_eq!(hashes(&signals), txs.iter().map(|t| tx_hash(t)).collect::<HashSet<_>>(), "blocks 2 and 3 must still be traded");
    assert_eq!(signals.len(), 5);
    assert_eq!(h.listener.gap_stats(), (2, 2), "two blocks skipped over, both recovered");
}

// ── resilience ──────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_asks_the_server_to_resume_after_the_last_block() {
    let signer = PrivateKeySigner::random();
    let txs: Vec<Vec<u8>> = (0..6).map(|n| signed_swap(&signer, n)).collect();
    let server = {
        let txs = txs.clone();
        serve(move |conn, requested| {
            if conn == 0 {
                Plan::new((0..3).map(|i| (at_ms(i as u64 * 20), swap_frame(i as u64, &txs[i]))).collect(), Some(at_ms(50)))
            } else {
                let from = requested.unwrap_or(0) as usize;
                Plan::new((from..6).map(|i| (at_ms((i - from) as u64 * 20), swap_frame(i as u64, &txs[i]))).collect(), None)
            }
        })
        .await
    };
    let mut h = Harness::start(config_for(&[server.url()], signer.address()));
    let signals = h.collect(6, Duration::from_secs(10)).await;
    assert_eq!(signals.len(), 6);
    assert_eq!(hashes(&signals).len(), 6);
    let resumes = server.resumes.lock().unwrap().clone();
    assert_eq!(resumes[..2], [None, Some(3)], "first connect has no hint, the reconnect resumes at block 3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_bad_message_or_frame_does_not_cost_the_good_ones() {
    let signer = PrivateKeySigner::random();
    let raw = signed_swap(&signer, 0);
    let server = {
        let raw = raw.clone();
        serve(move |_, _| {
            Plan::new(
                vec![
                    (at_ms(0), "{this is not json".to_string()),
                    // seq 0 has a null l2Msg (Go marshals an empty []byte as null); seq 1 is the swap.
                    (at_ms(20), frame(&[message_json(0, 6, None), message_json(1, 3, Some(&raw))])),
                ],
                None,
            )
        })
        .await
    };
    let mut h = Harness::start(config_for(&[server.url()], signer.address()));
    let signals = h.collect(1, Duration::from_secs(5)).await;
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].tx_hash, tx_hash(&raw));
    assert_eq!(h.stats_of(&server.url()).parse_errors, 1, "the garbage frame is counted, not silently ignored");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_connects_back_off_instead_of_hammering_the_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let acceptor = tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            counter.fetch_add(1, Ordering::SeqCst);
            drop(tcp); // never completes the WebSocket handshake
        }
    });
    let signer = PrivateKeySigner::random();
    let h = Harness::start(config_for(&[url], signer.address()));
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    acceptor.abort();
    let n = attempts.load(Ordering::SeqCst);
    // Delays are drawn from [100,200], [200,400], [400,800], [800,1600] ms ...: about 5 tries in
    // 2.5 s. Hammering (no backoff) would be dozens; never retrying would be 1.
    assert!((3..=9).contains(&n), "unexpected number of connection attempts: {n}");
    assert_eq!(h.listener.live_feed_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_socket_has_nagle_disabled_and_sends_the_resume_hint() {
    let server = serve(|_, _| Plan::new(vec![], None)).await;
    let (ws, _) = connect_feed(&server.url(), 12_345).await.expect("connect");
    let MaybeTlsStream::Plain(tcp) = ws.get_ref() else { panic!("ws:// must be a plain socket") };
    assert!(tcp.nodelay().unwrap(), "TCP_NODELAY must be set before the first byte is written");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(server.resumes.lock().unwrap().as_slice(), [Some(12_345)]);

    let (_ws2, _) = connect_feed(&server.url(), 0).await.expect("connect without hint");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(server.resumes.lock().unwrap()[1], None, "no hint on a cold start");
}

#[tokio::test]
async fn a_server_that_never_answers_the_upgrade_is_abandoned_at_the_deadline() {
    // Accepts the TCP connection (so the connect itself succeeds) and then says nothing: the
    // situation of a black-holed or wedged edge. Without a deadline this hangs for good.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let _hold = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((tcp, _)) = listener.accept().await {
            held.push(tcp);
        }
    });
    let started = Instant::now();
    let err = autotradesniper::stream::feed::connect_feed_within(&url, 0, Duration::from_millis(300))
        .await
        .expect_err("must not succeed");
    assert!(err.to_string().contains("timed out"), "{err:#}");
    assert!(started.elapsed() < Duration::from_secs(3), "took {:?}", started.elapsed());
}

// ── heartbeat / liveness ────────────────────────────────────────────────────────────────

/// Production timings shrunk ~100x: same logic, observable in milliseconds.
fn fast_timing() -> FeedTiming {
    FeedTiming {
        heartbeat: at_ms(100),
        idle_timeout: at_ms(600),
        laggard_messages: 64,
        laggard_quiet: at_ms(300),
    }
}

/// One frame carrying `n` empty (null payload) feed messages starting at `from`.
fn bulk_frame(from: u64, n: u64) -> String {
    frame(&(from..from + n).map(|seq| message_json(seq, 6, None)).collect::<Vec<_>>())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_half_open_connection_is_detected_and_replaced() {
    // The peer completes the handshake and then goes dark: no data, no pong, no close. Only the
    // idle deadline can notice; without it the engine would look healthy and see nothing.
    let deaf = serve(|_, _| Plan { frames: vec![], drop_after: None, deaf: true }).await;
    let signer = PrivateKeySigner::random();
    let h = Harness::start_with(config_for(&[deaf.url()], signer.address()), fast_timing());
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let s = h.stats_of(&deaf.url());
    assert!(s.sessions >= 2, "the silent connection should have been dropped and retried: {s:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_feed_that_keeps_answering_pings_but_delivers_nothing_is_recycled() {
    // `wedged` is alive at the TCP/WebSocket level (pongs) but its upstream is stuck; `healthy`
    // keeps delivering. Only the comparison between feeds can expose the first one.
    let healthy = serve(|_, _| Plan::new(vec![(at_ms(100), bulk_frame(0, 100))], None)).await;
    let wedged = serve(|_, _| Plan::new(vec![], None)).await;
    let signer = PrivateKeySigner::random();
    let h = Harness::start_with(config_for(&[healthy.url(), wedged.url()], signer.address()), fast_timing());
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert_eq!(h.stats_of(&healthy.url()).sessions, 1, "the healthy feed is left alone");
    let w = h.stats_of(&wedged.url());
    assert!(w.sessions >= 2, "the wedged feed must have been recycled: {w:?}");
    assert!(w.rtt_us.is_some(), "it did answer our pings, which is exactly why a TCP check would not catch it");
    assert!(w.connected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_feed_joining_late_on_a_quiet_chain_is_not_mistaken_for_a_laggard() {
    // Feed A delivers 100 messages at once, then the chain goes quiet. Feed B's first connection
    // fails, so it joins after those 100 and then has nothing to deliver, legitimately. Judged
    // from sequence 0 it would look 100 messages behind and be recycled every few seconds.
    let a = serve(|_, _| Plan::new(vec![(at_ms(0), bulk_frame(0, 100))], None)).await;
    let b = serve(|conn, _| if conn == 0 { Plan::new(vec![], Some(at_ms(0))) } else { Plan::new(vec![], None) }).await;
    let signer = PrivateKeySigner::random();
    let h = Harness::start_with(config_for(&[a.url(), b.url()], signer.address()), fast_timing());
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    let s = h.stats_of(&b.url());
    assert_eq!(s.sessions, 2, "one failed first session, then a stable one: {s:?}");
    assert!(s.connected);
    assert_eq!(b.resumes.lock().unwrap().as_slice(), [None, Some(100)], "the retry resumed after the 100 delivered messages");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_after_is_honoured_and_never_undercut() {
    // A rate-limited handshake (429 + Retry-After: 1). The old code jittered the wait by +-20 %,
    // i.e. it could come back at 0.8 s and be refused again.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let arrivals = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let log = arrivals.clone();
    let acceptor = tokio::spawn(async move {
        let mut n = 0;
        while let Ok((tcp, _)) = listener.accept().await {
            log.lock().unwrap().push(Instant::now());
            n += 1;
            let limit = n == 1;
            tokio::spawn(async move {
                let callback = move |_: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                    if limit {
                        Err(tokio_tungstenite::tungstenite::http::Response::builder()
                            .status(429)
                            .header("retry-after", "1")
                            .body(None)
                            .unwrap())
                    } else {
                        Ok(resp)
                    }
                };
                if let Ok(mut ws) = accept_hdr_async(tcp, callback).await {
                    while ws.next().await.is_some() {}
                }
            });
        }
    });
    let signer = PrivateKeySigner::random();
    let h = Harness::start(config_for(&[url.clone()], signer.address()));
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    acceptor.abort();
    let seen = arrivals.lock().unwrap().clone();
    assert!(seen.len() >= 2, "expected a retry, saw {} connection(s)", seen.len());
    let waited = seen[1].duration_since(seen[0]);
    assert!(waited >= Duration::from_secs(1), "came back after {waited:?}, before the server's Retry-After");
    assert!(waited < Duration::from_millis(1_700), "waited {waited:?}, far beyond Retry-After + jitter");
    assert!(h.stats_of(&url).connected, "second attempt is accepted");
}
