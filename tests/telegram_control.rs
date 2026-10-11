//! Operator interface (Telegram) regressions: button state transitions as they reach the engine,
//! non-blocking alert handling, the circuit breaker, HTML safety and thread isolation.
//!
//! Engine pieces run against a local mock JSON-RPC node; Telegram itself is replaced by a
//! `TelegramApi` mock (the outbox) or not needed at all (the control plane is Telegram-free).

use alloy::consensus::transaction::SignerRecoverable;
use alloy::consensus::{Transaction as _, TxEnvelope};
use alloy::eips::eip2718::Decodable2718;
use alloy::primitives::{address, keccak256, Address, B256, U256};
use autotradesniper::config::AppConfig;
use autotradesniper::dispatcher::{BroadcasterPool, Dispatcher, PresignPolicy, PresignSnapshot, ReshapeReport};
use autotradesniper::models::PrysmEngine;
use autotradesniper::payload::{self, CalldataPayloadBuilder, DynamicFeeAdjuster, TARGET_CALLDATA_BUFFER_SIZE};
use autotradesniper::risk::curve_mirror::{CurveSnapshot, MirrorRead, MirrorStatsSnapshot, StaleReason};
use autotradesniper::risk::types::ExitVenue;
use autotradesniper::risk::{PanicReport, RiskManager};
use autotradesniper::stream::FeedStatsSnapshot;
use autotradesniper::telegram::control::{
    AccountView, ApplyReport, BreakerOutcome, CurveView, FeedView, MirrorView, PositionView, PrysmView, SettingsView, SystemSnapshot,
};
use autotradesniper::telegram::outbox::{run_outbox, OutboxSnapshot, SendError, TelegramApi};
use autotradesniper::telegram::{
    html, looks_like_mnemonic, screens, spawn_io_thread, Action, ControlPlane, EngineHandles, FeePreset, Notifier, OutboxPolicy,
};
use autotradesniper::types::{DispatchReport, ExecutionMode, GasRebalanceResult, Position, PrysmExitMode, TradeSignal};
use autotradesniper::wallet::{TreasuryAccount, WalletManager};
use futures_util::future::BoxFuture;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, RwLock};

const MNEMONIC: &str = "test test test test test test test test test test test junk";
const TOKEN: Address = address!("00000000000000000000000000000000000070c3");
const CURVE: Address = address!("00000000000000000000000000000000000c0e7e");
const BUFFER: usize = 2_000;
const GWEI: u128 = 1_000_000_000;

// ── Mock JSON-RPC node ──────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Sent {
    from: Address,
    to: Option<Address>,
    nonce: u64,
    input_len: usize,
    is_sell: bool,
}

struct Node {
    /// `eth_getTransactionCount` per address (0 when absent).
    nonces: HashMap<Address, u64>,
    /// `eth_getBalance` per address (`default_balance` when absent).
    balances: HashMap<Address, U256>,
    default_balance: U256,
    reserves: (U256, U256),
    /// Token balance of `token_holder` (`balanceOf` of anyone else is 0; a confirmed sell empties it).
    token_balance: U256,
    token_holder: Address,
    sent: Vec<Sent>,
    receipts: HashMap<B256, &'static str>,
    /// Delay of every `eth_sendRawTransaction` answer (a slow sequencer).
    send_delay: Duration,
    /// Delay of every `eth_getTransactionCount` answer (a slow read RPC).
    nonce_delay: Duration,
    /// `eth_getTransactionCount` of these addresses fails.
    nonce_fail: Vec<Address>,
}

impl Node {
    fn new() -> Arc<Mutex<Self>> {
        let (e, t) = payload::pons_reserves_after_buy(U256::from(300_000_000_000_000_000u64));
        Arc::new(Mutex::new(Self {
            nonces: HashMap::new(),
            balances: HashMap::new(),
            default_balance: U256::from(50_000_000_000_000_000u64), // 0.05 ETH
            reserves: (e, t),
            token_balance: U256::ZERO,
            token_holder: Address::ZERO,
            sent: Vec::new(),
            receipts: HashMap::new(),
            send_delay: Duration::ZERO,
            nonce_delay: Duration::ZERO,
            nonce_fail: Vec::new(),
        }))
    }
}

fn word(v: U256) -> String {
    alloy::hex::encode(v.to_be_bytes::<32>())
}

fn answer(node: &Mutex<Node>, req: &serde_json::Value) -> serde_json::Value {
    let mut n = node.lock().unwrap();
    let err = |msg: &str| serde_json::json!({"jsonrpc":"2.0","id":req["id"].clone(),"error":{"code":-32000,"message":msg}});
    let addr_param = || -> Address { req["params"][0].as_str().unwrap_or_default().parse().unwrap_or_default() };
    let result = match req["method"].as_str().unwrap_or_default() {
        "eth_chainId" => serde_json::json!("0x1237"),
        "eth_blockNumber" => serde_json::json!("0x64"),
        "eth_getTransactionCount" => {
            if n.nonce_fail.contains(&addr_param()) {
                return err("upstream timeout");
            }
            serde_json::json!(format!("0x{:x}", n.nonces.get(&addr_param()).copied().unwrap_or(0)))
        }
        "eth_getBalance" => {
            let b = n.balances.get(&addr_param()).copied().unwrap_or(n.default_balance);
            serde_json::json!(format!("0x{:x}", b))
        }
        "eth_call" => {
            let data = req["params"][0]["data"].as_str().or(req["params"][0]["input"].as_str()).unwrap_or_default();
            let data = data.trim_start_matches("0x");
            match &data[..data.len().min(8)] {
                "0902f1ac" => serde_json::json!(format!("0x{}{}", word(n.reserves.0), word(n.reserves.1))),
                "70a08231" => {
                    let owner = Address::from_slice(&alloy::hex::decode(&data[8 + 24..8 + 64]).unwrap_or_else(|_| vec![0; 20]));
                    let held = if owner == n.token_holder { n.token_balance } else { U256::ZERO };
                    serde_json::json!(format!("0x{}", word(held)))
                }
                "dd62ed3e" => serde_json::json!(format!("0x{}", word(U256::MAX))),
                "3de35b79" => serde_json::json!(format!("0x{:0>64}", alloy::hex::encode(TOKEN.as_slice()))),
                _ => return err("unsupported"),
            }
        }
        "eth_sendRawTransaction" => {
            let raw = alloy::hex::decode(req["params"][0].as_str().unwrap()).unwrap();
            let hash = keccak256(&raw);
            let tx = TxEnvelope::decode_2718_exact(&raw[..]).unwrap();
            let is_sell = tx.input().first_chunk::<4>() == Some(&payload::PONS_SELL_SELECTOR);
            n.sent.push(Sent {
                from: tx.recover_signer().unwrap(),
                to: tx.to(),
                nonce: tx.nonce(),
                input_len: tx.input().len(),
                is_sell,
            });
            n.receipts.insert(hash, "0x1");
            if is_sell && Some(n.token_holder) == tx.recover_signer().ok() {
                n.token_balance = U256::ZERO;
            }
            serde_json::json!(format!("{:?}", hash))
        }
        "eth_getTransactionReceipt" => {
            let hash: B256 = req["params"][0].as_str().unwrap().parse().unwrap();
            match n.receipts.get(&hash) {
                Some(status) => serde_json::json!({"status": status, "logs": []}),
                None => serde_json::Value::Null,
            }
        }
        _ => return err("unsupported"),
    };
    serde_json::json!({"jsonrpc":"2.0","id":req["id"].clone(),"result":result})
}

/// Minimal HTTP/1.1 keep-alive JSON-RPC server.
async fn mock_node(node: Arc<Mutex<Node>>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let node = node.clone();
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
                    let delay = match req["method"].as_str() {
                        Some("eth_sendRawTransaction") => node.lock().unwrap().send_delay,
                        Some("eth_getTransactionCount") => node.lock().unwrap().nonce_delay,
                        _ => Duration::ZERO,
                    };
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    let body = match &req {
                        serde_json::Value::Array(batch) => {
                            serde_json::Value::Array(batch.iter().map(|r| answer(&node, r)).collect()).to_string()
                        }
                        single => answer(&node, single).to_string(),
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

// ── Engine rig ──────────────────────────────────────────────────────────────

struct Rig {
    node: Arc<Mutex<Node>>,
    config: AppConfig,
    wallet: WalletManager,
    dispatcher: Arc<Dispatcher>,
    fee: Arc<DynamicFeeAdjuster>,
    risk: Arc<RiskManager>,
    positions: Arc<RwLock<Vec<Position>>>,
    control: ControlPlane,
}

/// Engine with `carriers` satellites, padding on (`BUFFER`), fee preset Optimal (2.5G / 10G).
async fn rig(carriers: usize) -> Rig {
    let node = Node::new();
    let url = mock_node(node.clone()).await;
    let mut config = AppConfig::default();
    config.send_rpcs = vec![url.clone()];
    config.rpc_http_url = url.clone();
    config.telegram_authorized_chat_id = 42;
    *config.mnemonic_phrase.write().await = MNEMONIC.to_string();
    *config.execution_mode.write().await = ExecutionMode::MultiAccount;
    *config.sub_account_count.write().await = carriers;
    *config.buffer_size.write().await = BUFFER;
    *config.gas_limit.write().await = payload::STATIC_GAS_LIMIT;
    *config.min_sub_account_eth.write().await = 0.005;
    config.set_fee_preset(2.5, 10.0).await;
    let wallet = WalletManager::new(config.clone()).await.unwrap();
    // The production multiplier: the preset must still be the exact tip.
    let fee = Arc::new(DynamicFeeAdjuster::new(2.5, 1.25));
    let (report_tx, report_rx) = mpsc::channel(256);
    std::mem::forget(report_rx);
    let dispatcher = Arc::new(
        Dispatcher::new(config.clone(), wallet.clone(), fee.clone(), report_tx).with_presign_policy(PresignPolicy::default()),
    );
    let positions = Arc::new(RwLock::new(Vec::new()));
    let (exit_tx, exit_rx) = mpsc::channel(64);
    std::mem::forget(exit_rx);
    let pool = Arc::new(BroadcasterPool::new(vec![url]));
    let risk = Arc::new(
        RiskManager::new(config.clone(), wallet.clone(), positions.clone(), exit_tx)
            .with_execution(pool.clone(), fee.clone())
            .with_reader(pool),
    );
    let (signal_tx, signal_rx) = mpsc::channel(16);
    std::mem::forget(signal_rx);
    let prysm = Arc::new(PrysmEngine::new(config.clone(), signal_tx).with_presigner(dispatcher.clone()));
    let control = ControlPlane::new(EngineHandles {
        config: config.clone(),
        wallet: wallet.clone(),
        risk: risk.clone(),
        prysm,
        positions: positions.clone(),
        dispatcher: Some(dispatcher.clone()),
        fee: Some(fee.clone()),
        stream: None,
        mirror: None,
        engine: tokio::runtime::Handle::current(),
    });
    Rig { node, config, wallet, dispatcher, fee, risk, positions, control }
}

impl Rig {
    async fn accounts(&self, carriers: usize) -> Vec<TreasuryAccount> {
        self.wallet.prepare_accounts(carriers).await.unwrap().to_vec()
    }

    fn sent(&self) -> Vec<Sent> {
        self.node.lock().unwrap().sent.clone()
    }
}

fn launch(n: u8) -> B256 {
    B256::repeat_byte(n)
}

fn signal_for(launch: B256) -> TradeSignal {
    let size = U256::from(10_000_000_000_000_000u64); // 0.01 ETH
    TradeSignal {
        tx_hash: launch,
        monitored_account: Address::ZERO,
        target_token: TOKEN,
        pool_or_router: CURVE,
        entry_amount_wei: size,
        swap_calldata: CalldataPayloadBuilder::build_pons_buy_payload(size, U256::from(1_000_000u64), Address::ZERO),
        detected_at: Instant::now(),
        block_number: 100,
        strategy_tag: Some("Prysm: Pola A".to_string()),
    }
}

fn cursor(acc: &TreasuryAccount) -> u64 {
    acc.nonce_cursor.load(Ordering::SeqCst)
}

fn far() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Button whitelist
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn only_whitelisted_callback_data_changes_runtime_state() {
    use Action::*;
    for (data, want) in [
        ("cfg_sub:1", SetCarriers(1)),
        ("cfg_sub:2", SetCarriers(2)),
        ("cfg_sub:3", SetCarriers(3)),
        ("cfg_sub:5", SetCarriers(5)),
        ("tr_sub:5", SetCarriers(5)),
        ("cfg_fee:1.0:5.0", SetFeePreset(FeePreset::Normal)),
        ("cfg_fee:2.5:10.0", SetFeePreset(FeePreset::Optimal)),
        ("cfg_fee:5.0:20.0", SetFeePreset(FeePreset::Race)),
        ("cfg_buffer:94500", SetPadding(true)),
        ("cfg_buffer_default", SetPadding(true)),
        ("cfg_buffer:0", SetPadding(false)),
        ("cfg_mode:single", SetMode(ExecutionMode::Single)),
        ("cfg_mode:multi", SetMode(ExecutionMode::MultiAccount)),
        ("intake:on", Intake(true)),
        ("intake:off", Intake(false)),
        ("breaker_release:3", ReleaseBreaker(3)),
        ("op_panic", Panic),
    ] {
        assert_eq!(Action::parse(data), Some(want), "{data}");
    }
    // Client-controlled data outside the offered buttons never reaches the engine.
    for forged in [
        "cfg_sub:4",
        "cfg_sub:10",
        "cfg_sub:1000000",
        "tr_sub:0",
        "cfg_sub:-1",
        "cfg_fee:0.0001:9999",
        "cfg_fee:5.0:5.0",
        "cfg_fee:2.5:10",
        "cfg_buffer:50000",
        "cfg_buffer:999999999",
        "cfg_buffer:",
        "op_panic ",
        // Old dashboards: a toggle without a target state must never act.
        "toggle_engine",
        "cfg_toggle_mode",
        "intake:maybe",
        // A release must name its breaker.
        "breaker_release",
        "breaker_release:0",
        "breaker_release:-1",
        "breaker_release:+1",
        "",
    ] {
        assert_eq!(Action::parse(forged), None, "{forged:?} must be refused");
    }
}

#[test]
fn every_keyboard_button_fits_telegram_limits_and_state_buttons_are_whitelisted() {
    use teloxide::types::InlineKeyboardButtonKind;
    let view = SettingsView {
        mode: ExecutionMode::MultiAccount,
        carriers: 3,
        padding_on: true,
        buffer_size: TARGET_CALLDATA_BUFFER_SIZE,
        priority_gwei: 2.5,
        max_fee_gwei: 10.0,
        preset: Some(FeePreset::Optimal),
        target_profit_percent: 25.0,
        time_decay_exit_secs: 45,
        gas_limit: 300_000,
        trade_eth: 0.01,
    };
    let keyboards = [
        screens::main_menu_keyboard(true, None),
        screens::main_menu_keyboard(false, None),
        screens::main_menu_keyboard(false, Some(7)),
        screens::settings_keyboard(&view),
        screens::treasury_keyboard(),
        screens::signals_keyboard(),
        screens::models_keyboard(),
        screens::prysm_keyboard(true),
        screens::telemetry_keyboard(),
        screens::new_wallet_confirm_keyboard(),
    ];
    for kb in keyboards {
        for b in kb.inline_keyboard.into_iter().flatten() {
            let InlineKeyboardButtonKind::CallbackData(data) = &b.kind else { panic!("callback buttons only") };
            assert!(data.len() <= 64, "Telegram caps callback data at 64 bytes: {data}");
            let state_changing = data.starts_with("cfg_sub")
                || data.starts_with("tr_sub")
                || data.starts_with("cfg_fee")
                || data.starts_with("cfg_buffer")
                || data.starts_with("cfg_mode")
                || data.starts_with("intake")
                || data.starts_with("breaker");
            if state_changing {
                assert!(Action::parse(data).is_some(), "button {data} would be refused by the whitelist");
            }
        }
    }
    // Selection marks follow the live state: exactly one carrier, one fee and one padding mark.
    let buttons: Vec<_> = screens::settings_keyboard(&view).inline_keyboard.into_iter().flatten().collect();
    let marked = |prefix: &str| buttons.iter().filter(|b| matches!(&b.kind, InlineKeyboardButtonKind::CallbackData(d) if d.starts_with(prefix)) && b.text.contains('✅')).count();
    assert_eq!((marked("cfg_sub"), marked("cfg_fee"), marked("cfg_buffer")), (1, 1, 1));
    let off = SettingsView { padding_on: false, buffer_size: 0, preset: None, ..view };
    let buttons: Vec<_> = screens::settings_keyboard(&off).inline_keyboard.into_iter().flatten().collect();
    let zero = buttons.iter().find(|b| matches!(&b.kind, InlineKeyboardButtonKind::CallbackData(d) if d == "cfg_buffer:0")).unwrap();
    assert!(zero.text.contains('✅'));
    assert!(!buttons.iter().any(|b| matches!(&b.kind, InlineKeyboardButtonKind::CallbackData(d) if d.starts_with("cfg_fee")) && b.text.contains('✅')), "a custom fee marks no preset");
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Carrier count: staged AOT batches are reshaped, new carriers synced first
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn carrier_buttons_reshape_staged_batches_and_sync_joining_carriers_before_use() {
    let r = rig(2).await;
    let all = r.accounts(5).await;
    // Carriers #3..#5 were used before: the chain is at nonce 9 for them.
    {
        let mut n = r.node.lock().unwrap();
        for acc in &all[3..] {
            n.nonces.insert(acc.address, 9);
        }
        // Carrier #5 is underfunded.
        n.balances.insert(all[5].address, U256::from(1_000_000_000_000u64));
    }
    let l = launch(1);
    assert!(r.dispatcher.arm_presign(l, far()).await);
    let batch = r.dispatcher.staged_batch(l).expect("staged");
    assert_eq!(batch.leased_nonces(), vec![(1, 0), (2, 0)]);

    // 2 → 5
    let rep = r.control.apply(Action::SetCarriers(5)).await;
    assert!(!rep.aborted && !rep.unchanged, "{rep:?}");
    assert_eq!(*r.config.sub_account_count.read().await, 5);
    assert_eq!(rep.accounts, 6);
    assert_eq!(rep.nonces_synced, 3, "the three joining carriers were read from the chain");
    assert_eq!(rep.presign, ReshapeReport { batches: 1, slots_released: 0, slots_added: 3, slots_resigned: 0 });
    assert_eq!(batch.leased_nonces(), vec![(1, 0), (2, 0), (3, 9), (4, 9), (5, 9)], "joining carriers lease from the synced cursor");
    assert_eq!(batch.slot_specs().iter().filter(|(_, _, signed)| *signed).count(), 5, "signed before the target block");
    assert!(rep.warnings.iter().any(|w| w.contains("#5") && w.contains("Rebalance")), "underfunded carrier flagged: {:?}", rep.warnings);

    // Same button again: nothing is touched.
    let again = r.control.apply(Action::SetCarriers(5)).await;
    assert!(again.unchanged);
    assert_eq!(again.presign, ReshapeReport::default());

    // 5 → 1: the four dropped carriers hand their nonces back.
    let rep = r.control.apply(Action::SetCarriers(1)).await;
    assert_eq!(rep.presign.slots_released, 4);
    assert_eq!(batch.leased_nonces(), vec![(1, 0)]);
    assert_eq!(cursor(&all[2]), 0);
    for acc in &all[3..] {
        assert_eq!(cursor(acc), 9, "{}: the released lease is returned, not leaked", acc.role);
        assert!(acc.nonce_holes.leased_snapshot().is_empty());
    }

    // The next entry goes out with exactly the new layout, from the reshaped batch.
    let report = r.dispatcher.dispatch_signal(&signal_for(l)).await.expect("dispatch");
    assert_eq!(report.tx_hashes.len(), 2);
    let sent = r.sent();
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().any(|s| s.from == all[1].address && s.nonce == 0));
    assert!(!sent.iter().any(|s| all[2..].iter().any(|a| a.address == s.from)));
    assert_eq!(r.dispatcher.presign_snapshot().prebuilt_sent, 1, "the carrier packet signed by the reshape was sent as is");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_carrier_change_is_prepared_before_it_is_committed() {
    let r = rig(1).await;
    // The control plane derives the new layout before switching to it: the dispatch path then
    // finds it cached (no PBKDF2 on the hot path).
    assert_eq!(r.wallet.get_all_accounts_shared().await.unwrap().len(), 2, "the running layout");
    assert!(!r.wallet.has_layout(3).await);
    let rep = r.control.apply(Action::SetCarriers(3)).await;
    assert!(!rep.aborted);
    assert!(r.wallet.has_layout(3).await, "derived before the switch, not by the next dispatch");
    assert_eq!(r.wallet.get_all_accounts_shared().await.unwrap().len(), 4);
    // Both layouts stay cached: switching back costs no derivation either.
    assert!(r.wallet.has_layout(1).await);
    let rep = r.control.apply(Action::SetCarriers(1)).await;
    assert!(!rep.aborted);
    assert_eq!(r.wallet.get_all_accounts_shared().await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_carrier_change_is_aborted_when_a_joining_carrier_nonce_cannot_be_read() {
    let r = rig(1).await;
    let all = r.accounts(3).await;
    r.node.lock().unwrap().nonce_fail.push(all[3].address);
    let l = launch(13);
    assert!(r.dispatcher.arm_presign(l, far()).await);
    let rep = r.control.apply(Action::SetCarriers(3)).await;
    assert!(rep.aborted, "{rep:?}");
    assert!(rep.warnings.iter().any(|w| w.contains("#3")), "{:?}", rep.warnings);
    assert_eq!(*r.config.sub_account_count.read().await, 1, "the old layout stays in force");
    assert_eq!(r.dispatcher.staged_batch(l).unwrap().leased_nonces(), vec![(1, 0)], "nothing leased from a stale cursor");
    // Once the RPC answers, the same button goes through.
    r.node.lock().unwrap().nonce_fail.clear();
    let rep = r.control.apply(Action::SetCarriers(3)).await;
    assert!(!rep.aborted, "{rep:?}");
    assert_eq!(*r.config.sub_account_count.read().await, 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Fee presets: proactive re-signing in both directions, floor follows
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fee_preset_buttons_resign_staged_packets_under_their_nonces_and_move_the_floor() {
    let r = rig(3).await;
    let l = launch(2);
    assert!(r.dispatcher.arm_presign(l, far()).await);
    let batch = r.dispatcher.staged_batch(l).unwrap();
    let nonces = batch.leased_nonces();
    let prio = |i: usize| batch.slot_spec(i).unwrap().0.max_priority_fee_per_gas;
    assert_eq!(prio(1), 25 * GWEI / 10, "the preset is the tip, not tip x 1.25");

    // Up: Race.
    let rep = r.control.apply(Action::SetFeePreset(FeePreset::Race)).await;
    assert_eq!(rep.presign.slots_resigned, 3, "{rep:?}");
    assert_eq!(r.config.fee_caps().await, (5.0, 20.0));
    assert_eq!(r.fee.priority_floor_gwei(), 5.0);
    for i in 1..=3 {
        assert_eq!(prio(i), 5 * GWEI);
        assert!(batch.slot_spec(i).unwrap().1, "slot {i} re-signed before the block");
    }
    assert_eq!(batch.leased_nonces(), nonces, "same nonces: only one version can ever execute");

    // Down: Normal. A lower floor must reach the packets too (no overpaying after a downgrade).
    let rep = r.control.apply(Action::SetFeePreset(FeePreset::Normal)).await;
    assert_eq!(rep.presign.slots_resigned, 3);
    assert_eq!(r.config.fee_caps().await, (1.0, 5.0));
    assert_eq!(r.fee.priority_floor_gwei(), 1.0);
    for i in 1..=3 {
        assert_eq!(prio(i), GWEI);
        assert!(batch.slot_spec(i).unwrap().0.max_fee_per_gas <= 5 * GWEI, "max fee within the new cap");
    }
    assert_eq!(batch.leased_nonces(), nonces);

    // Network blocks with no tip information (what the feed reports) do not lift the tip.
    for _ in 0..20 {
        r.fee.update_block_fees(0.02, 0.0);
    }
    assert_eq!(r.fee.capped_fees(5.0).0, GWEI);

    // Pressing the active preset again changes nothing.
    let rep = r.control.apply(Action::SetFeePreset(FeePreset::Normal)).await;
    assert!(rep.unchanged);
    assert_eq!(rep.presign.slots_resigned, 0);

    let report = r.dispatcher.dispatch_signal(&signal_for(l)).await.unwrap();
    assert_eq!(report.tx_hashes.len(), 4);
    let s = r.dispatcher.presign_snapshot();
    assert_eq!(s.resigned_stale, 0, "nothing left to re-sign on the hot path");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Padding switch: 0 KB = one bare master tx; 95 KB = carriers back, re-signed
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn padding_off_releases_carriers_and_strips_the_master_then_on_restores_95_kb() {
    let r = rig(3).await;
    let all = r.accounts(3).await;
    let l = launch(3);
    assert!(r.dispatcher.arm_presign(l, far()).await);
    let batch = r.dispatcher.staged_batch(l).unwrap();
    assert_eq!(batch.leased_nonces().len(), 3);

    let rep = r.control.apply(Action::SetPadding(false)).await;
    assert_eq!(rep.presign.slots_released, 3, "{rep:?}");
    assert!(!r.config.carrier_padding_enabled());
    assert_eq!(*r.config.buffer_size.read().await, 0);
    assert!(batch.leased_nonces().is_empty());
    for acc in &all[1..] {
        assert_eq!(cursor(acc), 0, "{}: nonce handed back", acc.role);
    }

    let sig = signal_for(l);
    let report = r.dispatcher.dispatch_signal(&sig).await.unwrap();
    assert_eq!(report.tx_hashes.len(), 1, "no carriers while padding is off");
    let sent = r.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].from, all[0].address);
    assert_eq!(sent[0].input_len, sig.swap_calldata.len(), "the master carries no padding");

    // Back on: a batch armed in the meantime gains its carriers, padded to ~94.5 KB.
    let l2 = launch(4);
    assert!(r.dispatcher.arm_presign(l2, far()).await);
    let batch2 = r.dispatcher.staged_batch(l2).unwrap();
    assert!(batch2.leased_nonces().is_empty(), "single-account layout: nothing to stage ahead");
    let rep = r.control.apply(Action::SetPadding(true)).await;
    assert_eq!(rep.presign.slots_added, 3, "{rep:?}");
    assert!(r.config.carrier_padding_enabled());
    assert_eq!(*r.config.buffer_size.read().await, TARGET_CALLDATA_BUFFER_SIZE);
    for i in 1..=3 {
        let (spec, signed) = batch2.slot_spec(i).unwrap();
        assert!(signed);
        assert!(spec.input.len() >= 94_000, "carrier padded to ~94.5 KB, got {}", spec.input.len());
    }
    let report = r.dispatcher.dispatch_signal(&signal_for(l2)).await.unwrap();
    assert_eq!(report.tx_hashes.len(), 4);
    let carriers: Vec<_> = r.sent().into_iter().skip(1).filter(|s| s.from != all[0].address).collect();
    assert_eq!(carriers.len(), 3);
    assert!(carriers.iter().all(|s| s.input_len >= 94_000 && s.to == Some(s.from)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mode_toggle_reshapes_like_the_padding_switch() {
    let r = rig(2).await;
    let l = launch(5);
    assert!(r.dispatcher.arm_presign(l, far()).await);
    let batch = r.dispatcher.staged_batch(l).unwrap();
    let rep = r.control.apply(Action::SetMode(ExecutionMode::Single)).await;
    assert_eq!(*r.config.execution_mode.read().await, ExecutionMode::Single);
    assert_eq!(rep.presign.slots_released, 2);
    // A repeated press (queued, or from an older message) does not flip it back.
    assert!(r.control.apply(Action::SetMode(ExecutionMode::Single)).await.unchanged);
    let rep = r.control.apply(Action::SetMode(ExecutionMode::MultiAccount)).await;
    assert_eq!(*r.config.execution_mode.read().await, ExecutionMode::MultiAccount);
    assert_eq!(rep.presign.slots_added, 2);
    assert_eq!(batch.leased_nonces().len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Pause gate
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_refuses_entries_and_releases_staged_work_resume_restores_intake() {
    let r = rig(2).await;
    let all = r.accounts(2).await;
    let l = launch(6);
    assert!(r.dispatcher.arm_presign(l, far()).await);
    assert_eq!(r.dispatcher.staged_count(), 1);

    let rep = r.control.apply(Action::Intake(false)).await;
    assert!(!r.config.is_engine_running());
    assert!(rep.changes.iter().any(|c| c.contains("batch pre-sign dilepas")), "{rep:?}");
    assert_eq!(r.dispatcher.staged_count(), 0);
    assert!(all[1..].iter().all(|a| cursor(a) == 0), "staged nonces returned");

    let err = r.dispatcher.dispatch_signal(&signal_for(l)).await.unwrap_err();
    assert!(format!("{err:#}").contains("paused"), "{err:#}");
    assert!(!r.dispatcher.arm_presign(launch(7), far()).await, "nothing is staged while paused");
    assert!(r.sent().is_empty(), "not one transaction left while paused");

    // Pausing twice keeps it paused (the old /pause reported "resumed" without resuming).
    assert!(r.control.set_intake(false).await.unchanged);
    assert!(!r.config.is_engine_running());

    r.control.set_intake(true).await;
    assert!(r.dispatcher.dispatch_signal(&signal_for(launch(8))).await.is_ok());
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Circuit breaker
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn circuit_breaker_stops_intake_at_once_and_liquidates_through_the_receipt_confirmed_exit() {
    let r = rig(2).await;
    let (e, t) = r.node.lock().unwrap().reserves;
    let held = CalldataPayloadBuilder::calculate_pons_tokens_out(e, t, U256::from(10_000_000_000_000_000u64), 100, 618);
    let master = r.wallet.get_master_account().await.unwrap().address;
    {
        let mut n = r.node.lock().unwrap();
        n.token_balance = held;
        n.token_holder = master;
    }
    r.risk.register_entry(TOKEN, CURVE, 0.01, None, ExitVenue::PonsCurve).await;
    assert!(r.dispatcher.arm_presign(launch(9), far()).await);
    // A slow sequencer: every broadcast answers after 400 ms.
    r.node.lock().unwrap().send_delay = Duration::from_millis(400);

    let t0 = Instant::now();
    let pending = r.control.trigger_breaker();
    let returned_after = t0.elapsed();
    assert!(!r.config.is_engine_running(), "intake is off when trigger_breaker returns");
    assert!(returned_after < Duration::from_millis(100), "the operator never waits for the sells ({returned_after:?})");
    // Nothing new can go out from this instant on.
    assert!(r.dispatcher.dispatch_signal(&signal_for(launch(10))).await.is_err());

    let outcome: BreakerOutcome = tokio::time::timeout(Duration::from_secs(20), pending).await.expect("breaker finishes").unwrap();
    let rep = &outcome.report;
    assert_eq!(outcome.presign_batches_released, 1, "staged batch cut");
    assert_eq!(rep.positions, 1);
    assert_eq!(rep.sold.len(), 1, "{rep:?}");
    assert!(rep.still_open.is_empty() && rep.already_exiting.is_empty());
    assert!(rep.elapsed >= Duration::from_millis(400), "the sell itself was slow");

    let sells: Vec<Sent> = r.sent().into_iter().filter(|s| s.is_sell).collect();
    assert_eq!(sells.len(), 1, "one sell, from the account holding the tokens");
    assert_eq!(sells[0].from, master);
    assert!(r.sent().iter().all(|s| s.is_sell), "no entry or carrier tx after the breaker");
    let pos = r.positions.read().await[0].clone();
    assert!(!pos.is_open);
    assert_eq!(pos.exit_tx_hash.as_deref(), Some(rep.sold[0].2.as_str()), "closed on the confirmed sell");
    assert_eq!(r.risk.exits_in_flight(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_breaker_does_not_wait_for_a_settings_change_in_progress() {
    let r = rig(1).await;
    // A carrier change stuck on a slow read RPC (nonce sync of the joining carriers).
    r.node.lock().unwrap().nonce_delay = Duration::from_millis(1_500);
    let slow = {
        let control = r.control.clone();
        tokio::spawn(async move { control.apply(Action::SetCarriers(5)).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let t0 = Instant::now();
    let handle = r.control.trigger_breaker();
    assert!(t0.elapsed() < Duration::from_millis(100), "breaker took {:?}", t0.elapsed());
    assert!(!r.config.is_engine_running());
    assert!(!slow.is_finished(), "test premise: the settings change is still running");
    let _ = handle.await.unwrap();
    let rep = slow.await.unwrap();
    assert!(!rep.aborted, "{rep:?}");
    assert!(!r.config.is_engine_running());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tripped_breaker_stays_latched_until_released_after_the_liquidation() {
    let r = rig(1).await;
    let (e, t) = r.node.lock().unwrap().reserves;
    let master = r.wallet.get_master_account().await.unwrap().address;
    {
        let mut n = r.node.lock().unwrap();
        n.token_balance = CalldataPayloadBuilder::calculate_pons_tokens_out(e, t, U256::from(10_000_000_000_000_000u64), 100, 618);
        n.token_holder = master;
        n.send_delay = Duration::from_millis(400);
    }
    r.risk.register_entry(TOKEN, CURVE, 0.01, None, ExitVenue::PonsCurve).await;

    let pending = r.control.trigger_breaker();
    let epoch = r.control.breaker().expect("latched");
    // A resume pressed right after the panic (queued behind it, or a stale "Intake OFF → ON"
    // button) is refused while the latch holds.
    let rep = r.control.apply(Action::Intake(true)).await;
    assert!(rep.aborted, "{rep:?}");
    assert!(!r.config.is_engine_running());
    // Releasing is refused while the sells are still going out.
    let rep = r.control.apply(Action::ReleaseBreaker(epoch)).await;
    assert!(rep.aborted, "release refused mid-liquidation: {rep:?}");
    assert!(r.control.breaker_engaged());

    let outcome = pending.await.unwrap();
    assert_eq!(outcome.report.sold.len(), 1);
    let rep = r.control.apply(Action::ReleaseBreaker(epoch)).await;
    assert!(!rep.aborted, "{rep:?}");
    assert!(!r.control.breaker_engaged());
    assert!(r.config.is_engine_running(), "release resumes intake explicitly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_in_flight_when_the_breaker_fires_is_liquidated_as_it_registers() {
    let r = rig(1).await;
    let (e, t) = r.node.lock().unwrap().reserves;
    let master = r.wallet.get_master_account().await.unwrap().address;
    // The entry passed the intake check and its tx is out; it is not a position yet.
    let in_flight = r.dispatcher.entry_guard();
    let outcome = r.control.trigger_breaker().await.unwrap();
    let epoch = r.control.breaker().expect("latched");
    assert_eq!(outcome.entries_in_flight, 1);
    assert_eq!(outcome.report.positions, 0, "nothing to list yet");
    assert!(html::to_plain(&screens::breaker_report(&outcome)).contains("1 entri sedang dikirim"));
    assert!(r.control.apply(Action::ReleaseBreaker(epoch)).await.aborted, "not released while an entry is in flight");

    // The buy was accepted; the position registers before its tokens are visible to the read
    // RPC (they show up 400 ms later). It must not be closed as "nothing held".
    let risk = r.risk.clone();
    let registering = tokio::spawn(async move { risk.register_entry(TOKEN, CURVE, 0.01, None, ExitVenue::PonsCurve).await });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!registering.is_finished(), "waits for the fill instead of closing an empty position");
    {
        let mut n = r.node.lock().unwrap();
        n.token_balance = CalldataPayloadBuilder::calculate_pons_tokens_out(e, t, U256::from(10_000_000_000_000_000u64), 100, 618);
        n.token_holder = master;
    }
    let pos = registering.await.unwrap();
    drop(in_flight);
    assert!(!pos.is_open, "liquidated once filled: {pos:?}");
    assert!(pos.exit_tx_hash.is_some(), "{pos:?}");
    assert_eq!(r.sent().iter().filter(|s| s.is_sell && s.from == master).count(), 1);
    assert!(!r.control.apply(Action::ReleaseBreaker(epoch)).await.aborted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_release_can_never_undo_a_newer_breaker() {
    let r = rig(1).await;
    let _ = r.control.trigger_breaker().await.unwrap();
    let first = r.control.breaker().unwrap();
    // The operator presses "release" on the first breaker's button, but a second panic fires
    // before that press is handled.
    let _ = r.control.trigger_breaker().await.unwrap();
    let second = r.control.breaker().unwrap();
    assert!(second > first);
    let rep = r.control.apply(Action::ReleaseBreaker(first)).await;
    assert!(rep.aborted, "{rep:?}");
    assert_eq!(r.control.breaker(), Some(second));
    assert!(!r.config.is_engine_running());
    // Even a raw intake flag set behind the latch's back does not open intake.
    r.config.is_engine_active.store(true, Ordering::SeqCst);
    assert!(!r.config.is_engine_running(), "the latch gates intake on its own");
    assert!(r.dispatcher.dispatch_signal(&signal_for(launch(15))).await.is_err());
    assert!(!r.control.apply(Action::ReleaseBreaker(second)).await.aborted);
    assert!(r.config.is_engine_running());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_can_be_staged_or_sent_once_the_breaker_fired() {
    let r = rig(2).await;
    let _ = r.control.trigger_breaker().await.unwrap();
    assert!(!r.dispatcher.arm_presign(launch(14), far()).await);
    assert_eq!(r.dispatcher.staged_count(), 0);
    assert!(r.dispatcher.dispatch_signal(&signal_for(launch(14))).await.is_err());
    assert!(r.sent().is_empty());
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Wallet replacement guard
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_master_wallet_cannot_be_replaced_while_anything_depends_on_it() {
    let r = rig(1).await;
    let master = r.wallet.get_master_account().await.unwrap().address;

    let err = r.control.replace_wallet(None, 42).await.unwrap_err();
    assert!(err.contains("intake masih AKTIF"), "{err}");
    // An entry on its way (dispatched, not yet registered) blocks it too.
    {
        let _in_flight = r.dispatcher.entry_guard();
        r.control.set_intake(false).await;
        let err = r.control.replace_wallet(None, 42).await.unwrap_err();
        assert!(err.contains("entri masih dalam pengiriman"), "{err}");
    }

    r.risk.register_entry(TOKEN, CURVE, 0.01, None, ExitVenue::PonsCurve).await;
    let err = r.control.replace_wallet(None, 42).await.unwrap_err();
    assert!(err.contains("posisi masih terbuka"), "{err}");
    assert_eq!(r.wallet.get_master_account().await.unwrap().address, master);
    r.positions.write().await[0].is_open = false;

    r.control.set_intake(true).await;
    assert!(r.dispatcher.arm_presign(launch(11), far()).await);
    r.config.is_engine_active.store(false, Ordering::SeqCst); // paused without the cleanup
    let err = r.control.replace_wallet(None, 42).await.unwrap_err();
    assert!(err.contains("batch pre-sign"), "{err}");
    r.dispatcher.cancel_all_presigned();

    let phrase = r.control.replace_wallet(None, 42).await.expect("nothing depends on the wallet");
    assert_ne!(phrase, MNEMONIC);
    assert_ne!(r.wallet.get_master_account().await.unwrap().address, master);
    // Persisted into this configuration's own state file, never the operator's runtime_config.json.
    let path = r.config.state_path().to_string();
    assert!(path.starts_with(std::env::temp_dir().to_string_lossy().as_ref()), "{path}");
    let saved = std::fs::read_to_string(&path).unwrap();
    assert!(saved.contains(&phrase));
    let _ = std::fs::remove_file(path);
}

#[test]
fn the_default_configuration_is_hermetic() {
    let a = AppConfig::default();
    let b = AppConfig::default();
    assert_ne!(a.state_path(), "runtime_config.json");
    assert_ne!(a.state_path(), b.state_path(), "each configuration persists into its own scratch file");
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Isolation: Telegram thread vs engine runtime
// ═══════════════════════════════════════════════════════════════════════════

/// Blocks the whole thread it runs on (not just the task) for `block` per send.
struct Frozen {
    block: Duration,
    calls: Arc<AtomicUsize>,
}

impl TelegramApi for Frozen {
    fn send(&self, _chat: i64, _text: String, _html: bool) -> BoxFuture<'_, Result<(), SendError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(self.block);
            Ok(())
        })
    }
}

/// One engine worker only: if anything Telegram did ran on the engine runtime, the timer below
/// would be held for the full blocking second.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_frozen_telegram_thread_adds_no_latency_to_the_engine_runtime() {
    let calls = Arc::new(AtomicUsize::new(0));
    let policy = OutboxPolicy { capacity: 64, min_interval: Duration::ZERO, ..OutboxPolicy::default() };
    let (notifier, rx) = Notifier::channel(policy);
    let api = Arc::new(Frozen { block: Duration::from_millis(1_000), calls: calls.clone() });
    let _io = spawn_io_thread("telegram-io-test", move || async move { run_outbox(api, rx).await }).unwrap();

    // Engine-side timer: 1 ms ticks for 600 ms, worst lateness recorded. The schedule starts
    // now (not when the task first runs), so a worker held before the task starts counts too.
    let mut next = tokio::time::Instant::now();
    let ticker = tokio::spawn(async move {
        let mut worst = Duration::ZERO;
        for _ in 0..600 {
            next += Duration::from_millis(1);
            tokio::time::sleep_until(next).await;
            worst = worst.max(tokio::time::Instant::now() - next);
        }
        worst
    });
    // Engine-side alerts while Telegram is frozen.
    let mut notify_times = Vec::with_capacity(5_000);
    for i in 0..5_000 {
        let text = format!("<b>alert</b> {i}");
        let t = Instant::now();
        notifier.notify(42, text);
        notify_times.push(t.elapsed());
        if i % 100 == 0 {
            tokio::task::yield_now().await;
        }
    }
    let worst_tick = ticker.await.unwrap();
    notify_times.sort();
    let (p99, worst_notify) = (notify_times[notify_times.len() * 99 / 100], *notify_times.last().unwrap());
    println!("engine timer worst lateness {worst_tick:?}; notify p99 {p99:?}, worst {worst_notify:?} (Telegram thread frozen 1 s)");
    assert!(calls.load(Ordering::SeqCst) >= 1, "the outbox did start sending (and froze its thread)");
    // Thresholds sit far below the 1 s freeze (which is what a shared runtime would add) and far
    // above OS scheduling noise on a loaded machine.
    assert!(p99 < Duration::from_millis(1), "notify is a non-blocking enqueue: p99 {p99:?}");
    assert!(worst_notify < Duration::from_millis(250), "notify never waits on Telegram: worst {worst_notify:?}");
    assert!(worst_tick < Duration::from_millis(250), "engine timers unaffected by the frozen thread: worst {worst_tick:?}");
    let s = notifier.stats();
    assert!(s.dropped_full > 0, "a stalled Telegram makes the queue drop, not grow");
    // Waiting = accepted − what the one (frozen) in-flight message absorbed by coalescing.
    let waiting = s.enqueued - s.coalesced - 1;
    assert!(waiting <= 64, "the backlog is bounded by the queue capacity: {s:?}");
    assert_eq!(s.sent, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_work_runs_on_the_engine_runtime_not_on_the_caller() {
    let r = rig(1).await;
    let l = launch(12);
    assert!(r.dispatcher.arm_presign(l, far()).await);
    // Apply from a separate thread + runtime (as the Telegram handlers do), then destroy that
    // runtime. Anything the change left behind (connections, signing tasks) must not depend on it.
    let control = r.control.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let io = spawn_io_thread("telegram-io-test", move || async move {
        let rep = control.apply(Action::SetCarriers(3)).await;
        tx.send(rep).unwrap();
    })
    .unwrap();
    let rep: ApplyReport = tokio::task::spawn_blocking(move || rx.recv().unwrap()).await.unwrap();
    tokio::task::spawn_blocking(move || io.join().unwrap()).await.unwrap();
    assert_eq!(rep.presign.slots_added, 2, "{rep:?}");
    let report = r.dispatcher.dispatch_signal(&signal_for(l)).await.expect("engine keeps working after the caller's runtime is gone");
    assert_eq!(report.tx_hashes.len(), 4);
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Outbox: bounded, retried within limits, never a backlog
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone)]
enum Script {
    Ok,
    RetryAfter(u64),
    Parse,
    Transient,
    Fatal,
    Sleep(Duration),
    /// Answer after a while with flood control.
    SlowRetryAfter(Duration, u64),
}

struct Scripted {
    script: Mutex<Vec<Script>>,
    /// (chat, text, html) of every attempt.
    log: Mutex<Vec<(i64, String, bool, Instant)>>,
}

impl Scripted {
    fn new(script: Vec<Script>) -> Arc<Self> {
        Arc::new(Self { script: Mutex::new(script), log: Mutex::new(Vec::new()) })
    }
    fn log(&self) -> Vec<(i64, String, bool, Instant)> {
        self.log.lock().unwrap().clone()
    }
}

impl TelegramApi for Scripted {
    fn send(&self, chat: i64, text: String, html: bool) -> BoxFuture<'_, Result<(), SendError>> {
        Box::pin(async move {
            self.log.lock().unwrap().push((chat, text, html, Instant::now()));
            let step = {
                let mut s = self.script.lock().unwrap();
                if s.is_empty() { Script::Ok } else { s.remove(0) }
            };
            match step {
                Script::Ok => Ok(()),
                Script::RetryAfter(s) => Err(SendError::RetryAfter(Duration::from_secs(s))),
                Script::Parse => Err(SendError::Parse("can't parse entities".into())),
                Script::Transient => Err(SendError::Transient("connection reset".into())),
                Script::Fatal => Err(SendError::Fatal("chat not found".into())),
                Script::Sleep(d) => {
                    tokio::time::sleep(d).await;
                    Ok(())
                }
                Script::SlowRetryAfter(d, secs) => {
                    tokio::time::sleep(d).await;
                    Err(SendError::RetryAfter(Duration::from_secs(secs)))
                }
            }
        })
    }
}

fn quick() -> OutboxPolicy {
    OutboxPolicy {
        capacity: 8,
        critical_capacity: 4,
        min_interval: Duration::ZERO,
        max_attempts: 3,
        max_retry_after: Duration::from_secs(2),
        ttl: Duration::from_secs(60),
        coalesce_chars: 3_500,
    }
}

async fn settle(n: &Notifier, until: impl Fn(&OutboxSnapshot) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !until(&n.stats()) {
        assert!(Instant::now() < deadline, "outbox did not settle: {:?}", n.stats());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn retry_after_is_honoured_once_within_the_cap_and_never_beyond_it() {
    let api = Scripted::new(vec![Script::RetryAfter(1), Script::Ok, Script::RetryAfter(3600)]);
    let (n, rx) = Notifier::channel(quick());
    tokio::spawn(run_outbox(api.clone(), rx));
    n.notify(42, "first".into());
    settle(&n, |s| s.sent == 1).await;
    let log = api.log();
    assert_eq!(log.len(), 2);
    assert!(log[1].3 - log[0].3 >= Duration::from_millis(990), "waited the flood-control delay");

    // An hour of flood control is beyond the cap: dropped at once, not waited out.
    let t = Instant::now();
    n.notify(42, "second".into());
    settle(&n, |s| s.failed == 1).await;
    assert!(t.elapsed() < Duration::from_millis(500));
    assert_eq!(n.stats().retries, 1);
}

#[tokio::test]
async fn transient_errors_are_retried_up_to_the_attempt_budget_then_dropped() {
    let api = Scripted::new(vec![Script::Transient, Script::Transient, Script::Transient, Script::Fatal]);
    let (n, rx) = Notifier::channel(quick());
    tokio::spawn(run_outbox(api.clone(), rx));
    n.notify(42, "a".into());
    settle(&n, |s| s.failed == 1).await;
    assert_eq!(api.log().len(), 3, "max_attempts = 3");
    n.notify(42, "b".into());
    settle(&n, |s| s.failed == 2).await;
    assert_eq!(api.log().len(), 4, "a fatal error is not retried");
    // The next message that gets through tells the operator what was lost.
    n.notify(42, "c".into());
    settle(&n, |s| s.sent == 1).await;
    let last = api.log().last().unwrap().1.clone();
    assert!(last.contains("2 notifikasi sebelumnya dilewati") && last.ends_with('c'), "{last}");
    assert_eq!(html::validate(&last), Ok(()));
}

#[tokio::test]
async fn html_telegram_rejects_is_resent_as_plain_text_and_invalid_html_never_leaves_as_html() {
    let api = Scripted::new(vec![Script::Parse]);
    let (n, rx) = Notifier::channel(quick());
    tokio::spawn(run_outbox(api.clone(), rx));
    n.notify(42, "<b>ok</b> tag".into());
    settle(&n, |s| s.sent == 1).await;
    let log = api.log();
    assert_eq!((log[0].2, log[1].2), (true, false));
    assert_eq!(log[1].1, "ok tag");

    // A raw '&' / '<' from a token name would make Telegram drop the whole alert.
    n.notify(42, "Rug & Pull <script> > 0.1 ETH".into());
    settle(&n, |s| s.sent == 2).await;
    let (_, text, as_html, _) = api.log().last().unwrap().clone();
    assert!(!as_html);
    assert_eq!(text, "Rug & Pull <script> > 0.1 ETH");
    assert_eq!(n.stats().sanitized, 2);
}

#[tokio::test]
async fn a_burst_coalesces_stale_notices_expire_and_critical_ones_jump_the_queue() {
    let policy = OutboxPolicy { capacity: 32, ttl: Duration::from_millis(300), ..quick() };
    // The first send hangs 500 ms (Telegram slow): everything queued behind it piles up.
    let api = Scripted::new(vec![Script::Sleep(Duration::from_millis(500))]);
    let (n, rx) = Notifier::channel(policy);
    tokio::spawn(run_outbox(api.clone(), rx));
    n.notify(42, "slow".into());
    tokio::time::sleep(Duration::from_millis(50)).await;
    for i in 0..5 {
        n.notify(42, format!("old {i}"));
    }
    tokio::time::sleep(Duration::from_millis(350)).await; // these five are now past the TTL
    for i in 0..5 {
        n.notify(42, format!("burst {i}"));
    }
    n.notify_critical(42, "🚨 breaker".into());
    settle(&n, |s| s.sent >= 3).await;
    let log = api.log();
    assert!(log[1].1.contains("breaker"), "critical first: {:?}", log[1].1);
    let burst = &log[2].1;
    assert!((0..5).all(|i| burst.contains(&format!("burst {i}"))), "one message for the whole burst: {burst}");
    assert!(!burst.contains("old"), "expired notices are never delivered late");
    let s = n.stats();
    assert_eq!(s.expired, 5);
    assert_eq!(s.coalesced, 4);
    assert_eq!(s.sent, 3);
    assert!(burst.contains("5 notifikasi sebelumnya dilewati"));
}

#[tokio::test]
async fn enqueue_is_non_blocking_and_a_full_queue_drops_instead_of_growing() {
    let api = Scripted::new(vec![Script::Sleep(Duration::from_secs(3600))]);
    let (n, rx) = Notifier::channel(OutboxPolicy { capacity: 4, ..quick() });
    tokio::spawn(run_outbox(api, rx));
    n.notify(42, "stuck".into());
    tokio::time::sleep(Duration::from_millis(20)).await;
    let t = Instant::now();
    let accepted = (0..1_000).filter(|i| n.notify(42, format!("{i}"))).count();
    assert!(t.elapsed() < Duration::from_millis(50), "1000 enqueues against a stuck API: {:?}", t.elapsed());
    assert_eq!(accepted, 4);
    assert_eq!(n.stats().dropped_full, 996);
    assert!(!n.notify(0, "no chat".into()), "unconfigured chat: nothing queued");
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Every screen is valid Telegram HTML, whatever the data contains
// ═══════════════════════════════════════════════════════════════════════════

const HOSTILE: &str = "<script>alert(1)</script> & co > \"rug\" <b>";

fn hostile_position(open: bool) -> Position {
    Position {
        id: "p1".into(),
        token_address: format!("{:?}", TOKEN),
        token_symbol: HOSTILE.into(),
        entry_eth_amount: 0.01,
        current_eth_value: 0.0123,
        entry_timestamp_secs: 0,
        pool_address: format!("{:?}", CURVE),
        is_open: open,
        pnl_percent: 23.0,
        exit_reason: Some(format!("Quiet market (< 0.1 ETH) {HOSTILE}")),
        exit_tx_hash: Some(HOSTILE.into()),
        strategy: Some(HOSTILE.into()),
        exit_mode: None,
        entry_mcap_usd: None,
        current_mcap_usd: None,
    }
}

fn hostile_snapshot(open_positions: usize) -> SystemSnapshot {
    let view = SettingsView {
        mode: ExecutionMode::MultiAccount,
        carriers: 3,
        padding_on: true,
        buffer_size: TARGET_CALLDATA_BUFFER_SIZE,
        priority_gwei: 2.5,
        max_fee_gwei: 10.0,
        preset: Some(FeePreset::Optimal),
        target_profit_percent: 25.0,
        time_decay_exit_secs: 45,
        gas_limit: 300_000,
        trade_eth: 0.01,
    };
    let fresh = CurveSnapshot {
        curve: CURVE,
        eth_reserve: U256::from(1u64),
        token_reserve: U256::from(1u64),
        as_of_block: 10,
        anchor_block: 9,
        unconfirmed_trades: 1,
        exit_fee_bps: 100,
        volatility_bps: 5,
        validated_from: 5,
        validated_through: 9,
    };
    SystemSnapshot {
        engine_running: true,
        breaker: None,
        chain_id: 4663,
        settings: view,
        accounts: (0..4)
            .map(|i| AccountView {
                index: i,
                role: format!("Master Account (Treasury & Core Executor) {HOSTILE}"),
                address: Address::repeat_byte(i as u8 + 1),
                balance_eth: if i == 2 { None } else { Some(0.001) },
                low: i == 3,
            })
            .collect(),
        feeds: vec![FeedView {
            stats: FeedStatsSnapshot {
                label: format!("wss://feed?a=1&b=<2> {HOSTILE}"),
                connected: true,
                frames: 10,
                wins: 7,
                dups: 3,
                parse_errors: 0,
                sessions: 1,
                avg_lag_us: Some(800),
                rtt_us: Some(12_300),
            },
            win_pct: 70.0,
        }],
        live_feeds: 1,
        gaps: (1, 1),
        mirror: Some(MirrorView {
            disabled: false,
            numbering_verified: true,
            head: 10,
            curves: vec![
                CurveView { curve: CURVE, read: MirrorRead::Fresh(fresh) },
                CurveView { curve: TOKEN, read: MirrorRead::Stale(StaleReason::Gap) },
            ],
            stats: MirrorStatsSnapshot::default(),
        }),
        presign: Some((true, 2, PresignSnapshot::default())),
        open: (0..open_positions).map(|_| PositionView { position: hostile_position(true), in_memory: Some(true) }).collect(),
        recent_closed: vec![hostile_position(false)],
        closed_count: 1,
        exits_in_flight: 0,
        entries_in_flight: 1,
        prysm_pending: 0,
        watchlist: 2,
        outbox: Some(OutboxSnapshot::default()),
        master_address: Some(Address::repeat_byte(1)),
    }
}

#[test]
fn every_screen_is_valid_telegram_html_with_hostile_data() {
    let snap = hostile_snapshot(3);
    let report = ApplyReport {
        changes: vec![format!("Carrier: 2 → 5 {HOSTILE}")],
        warnings: vec!["Carrier #5 saldo 0.00001 ETH < minimum 0.005 ETH & more".into()],
        presign: ReshapeReport { batches: 1, slots_released: 0, slots_added: 3, slots_resigned: 1 },
        nonces_synced: 3,
        ..Default::default()
    };
    let breaker = BreakerOutcome {
        prysm_orders_dropped: 1,
        presign_batches_released: 2,
        entries_in_flight: 1,
        report: PanicReport {
            positions: 4,
            sold: vec![("a".into(), HOSTILE.into(), HOSTILE.into())],
            nothing_held: vec![("b".into(), HOSTILE.into())],
            already_exiting: vec![("c".into(), HOSTILE.into())],
            still_open: vec![("d".into(), HOSTILE.into(), HOSTILE.into())],
            elapsed: Duration::from_millis(1500),
        },
    };
    let signal = TradeSignal { strategy_tag: Some(HOSTILE.into()), ..signal_for(launch(1)) };
    let dispatch = DispatchReport {
        tx_hashes: vec![HOSTILE.into(), "0xabc".into()],
        account_roles: vec![HOSTILE.into()],
        block_number: 1,
        calldata_size_bytes: 100,
        execution_mode: ExecutionMode::MultiAccount,
        gas_limit: 1,
        priority_fee_gwei: 2.5,
        latency_micros: 10,
        trade_eth: 0.01,
    };
    let rebalance = vec![GasRebalanceResult { sub_account_address: HOSTILE.into(), amount_eth: 0.01, tx_hash: HOSTILE.into(), success: false }];
    let screens_to_check: Vec<(&str, String)> = vec![
        ("dashboard", screens::dashboard(&snap)),
        ("telemetry", screens::telemetry(&snap)),
        ("settings", screens::settings(&snap.settings)),
        ("apply", screens::apply_report(&report)),
        ("portfolio", screens::portfolio(&snap)),
        ("treasury", screens::treasury(&snap)),
        ("breaker started", screens::breaker_started(3)),
        ("breaker report", screens::breaker_report(&breaker)),
        ("pause", screens::pause_result(&report, false)),
        ("sweep", screens::sweep(&[hostile_position(true)])),
        ("watchlist", screens::watchlist(&[TOKEN, CURVE])),
        ("signals", screens::signals_menu(2)),
        ("wallet blocked", screens::new_wallet_confirm(Some(HOSTILE))),
        ("wallet confirm", screens::new_wallet_confirm(None)),
        ("wallet created", screens::new_wallet_created(&TOKEN, HOSTILE)),
        ("rebalance", screens::rebalance_result(&rebalance)),
        (
            "prysm",
            screens::prysm_dashboard(&PrysmView { enabled: true, size_eth: 0.01, exit_mode: PrysmExitMode::Target, max_mcap_usd: 8000.0, pending: 2 }),
        ),
        ("signal alert", screens::signal_alert(&signal)),
        ("dispatch alert", screens::dispatch_alert(&dispatch)),
        ("exit alert", screens::position_alert(&hostile_position(true))),
        ("closed alert", screens::position_alert(&hostile_position(false))),
    ];
    for (name, text) in &screens_to_check {
        assert_eq!(html::validate(text), Ok(()), "{name} is not valid Telegram HTML:\n{text}");
    }
    // Escaping is lossless: the reader sees the hostile text exactly.
    assert!(html::to_plain(&screens::portfolio(&snap)).contains(HOSTILE));
    assert!(html::to_plain(&screens::apply_report(&report)).contains("< minimum 0.005 ETH & more"));
}

#[test]
fn an_oversized_dashboard_is_cut_to_a_valid_message() {
    let snap = hostile_snapshot(120);
    let text = screens::dashboard(&snap);
    assert!(html::plain_len(&text) > html::MAX_MESSAGE_CHARS, "test premise: the raw screen is too long");
    let cut = html::fit(&text, html::MAX_MESSAGE_CHARS);
    assert_eq!(html::validate(&cut), Ok(()));
    assert!(html::plain_len(&cut) <= html::MAX_MESSAGE_CHARS);
}

#[test]
fn the_dashboard_reports_mirror_mode_and_feed_race_figures() {
    let mut snap = hostile_snapshot(1);
    let text = html::to_plain(&screens::dashboard(&snap));
    assert!(text.contains("CAMPURAN (1/2 kurva lokal"), "{text}");
    assert!(text.contains("menang 70.0%") && text.contains("RTT 12.3 ms"), "{text}");
    assert!(text.contains("🧮 lokal"));
    assert!(text.contains("0.0010 ETH") && text.contains("? ETH"), "unknown balances are not shown as 0");
    snap.mirror.as_mut().unwrap().curves.truncate(1);
    assert!(html::to_plain(&screens::dashboard(&snap)).contains("ZERO-RPC (1/1"));
    snap.mirror.as_mut().unwrap().disabled = true;
    assert!(html::to_plain(&screens::dashboard(&snap)).contains("NONAKTIF"));
}

#[test]
fn the_panic_button_skips_the_per_chat_queue() {
    use teloxide::types::Update;
    // teloxide parses updates from text (`from_value` yields `UpdateKind::Error`).
    let update = |data: &str| -> Update {
        serde_json::from_str(&serde_json::json!({
            "update_id": 1,
            "callback_query": {
                "id": "77",
                "from": {"id": 42, "is_bot": false, "first_name": "op"},
                "chat_instance": "ci",
                "data": data,
                "message": {
                    "message_id": 5,
                    "date": 0,
                    "chat": {"id": 42, "type": "private", "first_name": "op"},
                    "from": {"id": 7, "is_bot": true, "first_name": "bot"},
                    "text": "menu"
                }
            }
        })
        .to_string())
        .expect("callback update")
    };
    assert!(matches!(update("op_panic").kind, teloxide::types::UpdateKind::CallbackQuery(_)), "test premise");
    assert_eq!(autotradesniper::telegram::route_key(&update("op_panic")), None);
    assert_eq!(autotradesniper::telegram::route_key(&update("menu_main")), Some(42));
    let msg = |text: &str| -> Update {
        serde_json::from_str(&serde_json::json!({
            "update_id": 2,
            "message": {
                "message_id": 6,
                "date": 0,
                "chat": {"id": 42, "type": "private", "first_name": "op"},
                "from": {"id": 42, "is_bot": false, "first_name": "op"},
                "text": text
            }
        })
        .to_string())
        .expect("message update")
    };
    assert_eq!(autotradesniper::telegram::route_key(&msg("/panic")), None);
    assert_eq!(autotradesniper::telegram::route_key(&msg("/panic@sniper_bot")), None);
    assert_eq!(autotradesniper::telegram::route_key(&msg("/status")), Some(42));
}

#[test]
fn legacy_settings_keyboard_keeps_its_marks() {
    use teloxide::types::InlineKeyboardButtonKind;
    let kb = autotradesniper::telegram::TelegramBot::settings_keyboard(ExecutionMode::MultiAccount, 5, 5.0, 0);
    let buttons: Vec<_> = kb.inline_keyboard.into_iter().flatten().collect();
    let marked: Vec<String> = buttons
        .iter()
        .filter(|b| b.text.contains('✅'))
        .filter_map(|b| match &b.kind {
            InlineKeyboardButtonKind::CallbackData(d) => Some(d.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(marked, vec!["cfg_sub:5", "cfg_fee:5.0:20.0", "cfg_buffer:0"]);
}


// ═══════════════════════════════════════════════════════════════════════════
// 11. Further isolation and safety properties
// ═══════════════════════════════════════════════════════════════════════════

/// The writer of a setting runs on the engine runtime. Were it granted on the Telegram thread
/// while that thread is busy, every engine reader queued behind it would wait for Telegram.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_busy_telegram_thread_never_holds_an_engine_lock() {
    let r = rig(1).await;
    let cfg = r.config.clone();
    // An engine reader holds the lock for 50 ms; the Telegram side then queues a write and its
    // thread is frozen for a second right after.
    let holder = {
        let cfg = cfg.clone();
        tokio::spawn(async move {
            let _g = cfg.target_profit_percent.read().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    let control = r.control.clone();
    let _io = spawn_io_thread("telegram-io-test", move || async move {
        let change = tokio::spawn(async move { control.set_take_profit(30.0).await });
        tokio::task::yield_now().await; // the write request is issued
        std::thread::sleep(Duration::from_millis(1_000)); // ...and the Telegram thread freezes
        let _ = change.await;
    })
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Another engine reader arrives while the write is queued.
    let t = Instant::now();
    let tp = *cfg.target_profit_percent.read().await;
    let waited = t.elapsed();
    holder.await.unwrap();
    println!("engine reader waited {waited:?} behind a write issued from a frozen Telegram thread");
    assert!(waited < Duration::from_millis(400), "engine read held up by the Telegram thread: {waited:?}");
    assert_eq!(tp, 30.0, "the write itself went through on the engine runtime");
}

#[test]
fn seed_phrases_are_recognised_wherever_they_are_pasted() {
    assert!(looks_like_mnemonic(MNEMONIC));
    assert!(looks_like_mnemonic(&MNEMONIC.to_uppercase()));
    assert!(looks_like_mnemonic(&format!("  {}\n", MNEMONIC.replace(' ', "   "))));
    assert!(!looks_like_mnemonic("/status"));
    assert!(!looks_like_mnemonic("test test test test test test test test test test test"), "11 words");
    assert!(!looks_like_mnemonic("test test test test test test test test test zzzz yyyy xxxx"), "3 words not in the list");
    assert!(!looks_like_mnemonic("0x00000000000000000000000000000000000070c3"));
    // Typed as a numbered list, comma separated, or with a typo: still caught.
    let numbered: String = MNEMONIC.split(' ').enumerate().map(|(i, w)| format!("{}. {}\n", i + 1, w)).collect();
    assert!(looks_like_mnemonic(&numbered));
    assert!(looks_like_mnemonic(&MNEMONIC.replace(' ', ", ")));
    assert!(looks_like_mnemonic(&MNEMONIC.replacen("junk", "junkk", 1)));
    assert!(!looks_like_mnemonic("please show me the dashboard and the telemetry of every feed right now thanks"));
}

#[tokio::test]
async fn critical_notices_never_expire_and_cut_a_routine_retry_short() {
    let policy = OutboxPolicy { ttl: Duration::from_millis(100), max_retry_after: Duration::from_secs(5), ..quick() };
    // The routine message hits flood control after 300 ms (retry asked in 3 s).
    let api = Scripted::new(vec![Script::SlowRetryAfter(Duration::from_millis(300), 3)]);
    let (n, rx) = Notifier::channel(policy);
    let t0 = Instant::now();
    tokio::spawn(run_outbox(api.clone(), rx));
    n.notify(42, "routine".into());
    tokio::time::sleep(Duration::from_millis(50)).await;
    n.notify_critical(42, "🚨 breaker report".into());
    settle(&n, |s| s.sent == 1).await;
    let log = api.log();
    assert_eq!(log.len(), 2);
    assert!(log[1].1.contains("breaker report"), "{:?}", log[1].1);
    assert!(log[1].3 - t0 < Duration::from_secs(1), "not held back by the routine message's 3 s flood wait");
    let s = n.stats();
    assert_eq!(s.expired, 0, "the critical notice waited past the 100 ms TTL and was still delivered");
    assert_eq!(s.failed, 1, "the routine message was given up");
}

#[tokio::test]
async fn every_notice_of_a_lost_merged_message_is_counted() {
    let api = Scripted::new(vec![Script::Sleep(Duration::from_millis(200)), Script::Fatal]);
    let (n, rx) = Notifier::channel(quick());
    tokio::spawn(run_outbox(api.clone(), rx));
    n.notify(42, "first".into());
    tokio::time::sleep(Duration::from_millis(50)).await;
    for i in 0..3 {
        n.notify(42, format!("merged {i}"));
    }
    settle(&n, |s| s.failed == 3).await;
    n.notify(42, "next".into());
    settle(&n, |s| s.sent == 2).await;
    let last = api.log().last().unwrap().1.clone();
    assert!(last.contains("3 notifikasi sebelumnya dilewati"), "{last}");
}
