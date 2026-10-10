//! Regression tests for the second audit round (AUDIT.md §8). Each test pins a fixed behavior;
//! the pre-fix code failed every one of them.

use alloy::consensus::transaction::SignerRecoverable;
use alloy::consensus::{Transaction as _, TxEnvelope};
use alloy::eips::eip2718::Decodable2718;
use alloy::primitives::{address, keccak256, Address, B256, U256};
use alloy::sol_types::SolCall;
use autotradesniper::config::AppConfig;
use autotradesniper::dispatcher::{send_raw_tx_body, BroadcasterPool, Dispatcher};
use autotradesniper::models::prysm::{parse_address_from_topic, parse_launch_receipt, LaunchResolution};
use autotradesniper::payload::{self, CalldataPayloadBuilder, DynamicFeeAdjuster};
use autotradesniper::risk::types::ExitVenue;
use autotradesniper::risk::RiskManager;
use autotradesniper::types::TradeSignal;
use autotradesniper::wallet::WalletManager;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, RwLock};

alloy::sol! {
    function swapExactETHForTokens(uint256 amountOutMin, address[] path, address to, uint256 deadline);
    function swapExactTokensForETHSupportingFeeOnTransferTokens(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline);
    function getAmountsOut(uint256 amountIn, address[] path);
    function gasEstimateL1Component(address to, bool contractCreation, bytes data);
}

const MNEMONIC: &str = "test test test test test test test test test test test junk";
const TOKEN: Address = address!("00000000000000000000000000000000000070c3");
const CURVE: Address = address!("00000000000000000000000000000000000c0e7e");
const WETH: Address = address!("4200000000000000000000000000000000000006");
const ROUTER: Address = address!("000000000000000000000000000000000000a0a0");

// ── Mock JSON-RPC node ──────────────────────────────────────────────────────

#[derive(Default)]
struct MockChain {
    reserves: Option<(U256, U256)>,
    balance: U256,
    /// Accept `eth_sendRawTransaction` (true) or reject it with a JSON-RPC error.
    accept_txs: bool,
    sent: Vec<Vec<u8>>,
}

fn word(v: U256) -> String {
    alloy::hex::encode(v.to_be_bytes::<32>())
}

fn answer(chain: &Mutex<MockChain>, req: &serde_json::Value) -> serde_json::Value {
    let mut chain = chain.lock().unwrap();
    let method = req["method"].as_str().unwrap_or_default();
    let result = match method {
        "eth_sendRawTransaction" => {
            let raw = alloy::hex::decode(req["params"][0].as_str().unwrap()).unwrap();
            let hash = keccak256(&raw);
            chain.sent.push(raw);
            if !chain.accept_txs {
                return serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted (mock)"}});
            }
            serde_json::json!(format!("{:?}", hash))
        }
        "eth_call" => {
            let data = req["params"][0]["data"].as_str().unwrap_or_default().trim_start_matches("0x");
            match &data[..data.len().min(8)] {
                "0902f1ac" => match chain.reserves {
                    Some((e, t)) => serde_json::json!(format!("0x{}{}", word(e), word(t))),
                    None => return serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"no reserves"}}),
                },
                "70a08231" => serde_json::json!(format!("0x{}", word(chain.balance))),
                "dd62ed3e" => serde_json::json!(format!("0x{}", word(U256::ZERO))),
                _ => return serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"unsupported"}}),
            }
        }
        "eth_getTransactionReceipt" => serde_json::Value::Null,
        "eth_chainId" => serde_json::json!("0x1237"),
        _ => return serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"unsupported"}}),
    };
    serde_json::json!({"jsonrpc":"2.0","id":1,"result":result})
}

/// Minimal HTTP/1.1 keep-alive JSON-RPC server.
async fn mock_node(chain: Arc<Mutex<MockChain>>) -> String {
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
                        let mut chunk = [0u8; 4096];
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
                        let mut chunk = [0u8; 4096];
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let req: serde_json::Value = serde_json::from_slice(&buf[header_end..header_end + len]).unwrap_or_default();
                    buf.drain(..header_end + len);
                    let body = answer(&chain, &req).to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                        body.len(),
                        body
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

async fn test_config(rpc: &str) -> AppConfig {
    let mut config = AppConfig::default();
    config.send_rpcs = vec![rpc.to_string()];
    config.rpc_http_url = "http://127.0.0.1:1".to_string(); // nonce resyncs fail fast
    *config.mnemonic_phrase.write().await = MNEMONIC.to_string();
    *config.execution_mode.write().await = autotradesniper::types::ExecutionMode::MultiAccount;
    *config.sub_account_count.write().await = 2;
    config
}

// ── Dispatcher: exact value, per-account calldata ───────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatcher_sends_exact_value_and_per_account_pons_amounts() {
    let chain = Arc::new(Mutex::new(MockChain { accept_txs: true, ..Default::default() }));
    let url = mock_node(chain.clone()).await;
    let config = test_config(&url).await;
    let wm = WalletManager::new(config.clone()).await.unwrap();
    let fee = Arc::new(DynamicFeeAdjuster::new(0.0, 1.0));
    fee.set_l1_component_gas(1_000_000);
    let (report_tx, _report_rx) = mpsc::channel(8);
    let dispatcher = Dispatcher::new(config, wm.clone(), fee, report_tx);

    // 0.0021 ETH: the old wei -> f64 -> wei round trip sent msg.value = ethIn - 1 wei.
    let size = U256::from(2_100_000_000_000_000u64);
    let min_out = U256::from(1_000_000_000_000_000_000_000u128);
    let signal = TradeSignal {
        tx_hash: B256::ZERO,
        monitored_account: Address::ZERO,
        target_token: TOKEN,
        pool_or_router: CURVE,
        entry_amount_wei: size,
        swap_calldata: CalldataPayloadBuilder::build_pons_buy_payload(size, min_out, Address::ZERO),
        detected_at: Instant::now(),
        block_number: 1,
        strategy_tag: Some("Prysm: Pola A".to_string()),
    };
    let report = dispatcher.dispatch_signal(&signal).await.expect("dispatch");
    assert_eq!(report.tx_hashes.len(), 3);

    let accounts = wm.get_all_accounts().await.unwrap();
    let sent = chain.lock().unwrap().sent.clone();
    assert_eq!(sent.len(), 3);
    for raw in sent {
        let tx = TxEnvelope::decode_2718_exact(&raw[..]).unwrap();
        let from = tx.recover_signer().unwrap();
        let input = tx.input();
        let eth_in = U256::from_be_slice(&input[4..36]);
        let min = U256::from_be_slice(&input[36..68]);
        let recipient = Address::from_slice(&input[80..100]);
        assert_eq!(tx.value(), eth_in, "msg.value must equal ethIn exactly");
        assert_eq!(recipient, from, "tokens go to the sending account");
        assert!(tx.gas_limit() >= 1_500_000, "gas limit covers the L1 component (+50%)");
        if from == accounts[0].address {
            assert_eq!(tx.value(), size);
            assert_eq!(min, min_out);
        } else {
            // Satellites: 10% of size, floored at 0.001 ETH; minimum scaled with the size.
            assert_eq!(tx.value(), U256::from(1_000_000_000_000_000u64));
            assert_eq!(min, min_out * tx.value() / size);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_broadcast_rolls_the_nonce_back_without_rpc() {
    let chain = Arc::new(Mutex::new(MockChain { accept_txs: false, ..Default::default() }));
    let url = mock_node(chain.clone()).await;
    let config = test_config(&url).await;
    *config.execution_mode.write().await = autotradesniper::types::ExecutionMode::Single;
    let wm = WalletManager::new(config.clone()).await.unwrap();
    let master = wm.get_master_account().await.unwrap();
    master.nonce_cursor.store(7, std::sync::atomic::Ordering::SeqCst);
    let (report_tx, _rx) = mpsc::channel(8);
    let dispatcher = Dispatcher::new(config, wm.clone(), Arc::new(DynamicFeeAdjuster::new(0.0, 1.0)), report_tx);

    let size = U256::from(10_000_000_000_000_000u64);
    let signal = TradeSignal {
        tx_hash: B256::ZERO,
        monitored_account: Address::ZERO,
        target_token: TOKEN,
        pool_or_router: CURVE,
        entry_amount_wei: size,
        swap_calldata: CalldataPayloadBuilder::build_pons_buy_payload(size, U256::from(1u64), Address::ZERO),
        detected_at: Instant::now(),
        block_number: 1,
        strategy_tag: Some("Prysm: Pola A".to_string()),
    };
    assert!(dispatcher.dispatch_signal(&signal).await.is_err());
    // The read RPC is unreachable, so only the compare-exchange rollback can restore it.
    assert_eq!(master.nonce_cursor.load(std::sync::atomic::Ordering::SeqCst), 7);
}

// ── Risk engine: real valuation, failed exits stay open ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn position_value_follows_the_curve_and_failed_exit_keeps_it_open() {
    let e0 = U256::from(payload::PONS_INITIAL_ETH_RESERVE_WEI);
    let t0 = U256::from(payload::PONS_INITIAL_TOKEN_RESERVE_WEI);
    let entry = U256::from(10_000_000_000_000_000u64); // 0.01 ETH
    let held = CalldataPayloadBuilder::calculate_pons_tokens_out(e0, t0, entry, 100, 618);
    let k = e0 * t0;
    let state = |eth: U256| Some((eth, k / eth));

    let chain = Arc::new(Mutex::new(MockChain {
        reserves: state(e0 + entry),
        balance: held,
        accept_txs: false,
        sent: vec![],
    }));
    let url = mock_node(chain.clone()).await;
    let config = test_config(&url).await;
    let wm = WalletManager::new(config.clone()).await.unwrap();
    let positions = Arc::new(RwLock::new(Vec::new()));
    let (exit_tx, _exit_rx) = mpsc::channel(16);
    let rm = Arc::new(
        RiskManager::new(config, wm, positions.clone(), exit_tx)
            .with_execution(Arc::new(BroadcasterPool::new(vec![url])), Arc::new(DynamicFeeAdjuster::new(0.0, 1.0))),
    );
    rm.register_entry(TOKEN, CURVE, 0.01, None, ExitVenue::PonsCurve).await;
    let monitor = rm.clone();
    let task = tokio::spawn(async move { monitor.start_lifecycle_monitor().await });
    let pnl = || async { positions.read().await[0].pnl_percent };

    tokio::time::sleep(Duration::from_millis(400)).await;
    let flat = pnl().await;
    assert!((-12.0..0.0).contains(&flat), "entry taxes + exit fees only, got {flat}");
    let mcap = positions.read().await[0].current_mcap_usd.unwrap();
    assert!(mcap > 1_000.0, "market cap in USD from reserves, got {mcap}");

    // The curve pumps (ethReserve 1.69 -> 2.5 ETH): the old formula stayed at ~ -3%.
    chain.lock().unwrap().reserves = state(U256::from(2_500_000_000_000_000_000u128));
    tokio::time::sleep(Duration::from_millis(400)).await;
    let pumped = pnl().await;
    assert!(pumped > 80.0, "value must follow the price, got {pumped}");

    // The curve dumps: stop-loss fires, every broadcast is rejected -> the position must
    // stay open (the old code closed it as "[SIMULATED, no sell tx]" and abandoned the tokens).
    chain.lock().unwrap().reserves = state(U256::from(1_200_000_000_000_000_000u128));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let pos = positions.read().await[0].clone();
    assert!(pos.pnl_percent <= -20.0);
    assert!(pos.is_open, "failed exit must keep the position open: {:?}", pos.exit_reason);
    assert!(!chain.lock().unwrap().sent.is_empty(), "an exit was attempted");
    task.abort();
}

// ── Calldata / ABI helpers ───────────────────────────────────────────────────

#[test]
fn adapt_buy_calldata_rescales_v2_copy_trades() {
    let watched = Address::repeat_byte(0x77);
    let ours = Address::repeat_byte(0x99);
    let calldata = swapExactETHForTokensCall {
        amountOutMin: U256::from(1_000_000u64),
        path: vec![WETH, TOKEN],
        to: watched,
        deadline: U256::from(1u64),
    }
    .abi_encode();
    let their_value = U256::from(100u64);

    // We buy 1/4 of their size: their minimum would revert our order every time.
    let ours_cd = CalldataPayloadBuilder::adapt_buy_calldata(&calldata, ours, U256::from(25u64), their_value).unwrap();
    let decoded = swapExactETHForTokensCall::abi_decode(&ours_cd).unwrap();
    assert_eq!(decoded.amountOutMin, U256::from(250_000u64));
    assert_eq!(decoded.to, ours);
    assert_eq!(decoded.path, vec![WETH, TOKEN]);

    // We buy more: keep their minimum (never a false revert).
    let bigger = CalldataPayloadBuilder::adapt_buy_calldata(&calldata, ours, U256::from(400u64), their_value).unwrap();
    assert_eq!(swapExactETHForTokensCall::abi_decode(&bigger).unwrap().amountOutMin, U256::from(1_000_000u64));

    // Unknown reference size: refuse.
    assert!(CalldataPayloadBuilder::adapt_buy_calldata(&calldata, ours, U256::from(1u64), U256::ZERO).is_none());
}

#[test]
fn v2_path_is_abi_decoded_not_read_from_the_tail() {
    let mut calldata = swapExactETHForTokensCall {
        amountOutMin: U256::ZERO,
        path: vec![WETH, TOKEN],
        to: Address::ZERO,
        deadline: U256::ZERO,
    }
    .abi_encode();
    calldata.extend_from_slice(&[0xab; 32]); // trailing tracking bytes
    assert_eq!(CalldataPayloadBuilder::decode_v2_path_ends(&calldata), Some((WETH, TOKEN)));
    assert_eq!(ExitVenue::from_entry_calldata(&calldata), ExitVenue::V2Router { weth: WETH });

    // Hostile offsets/lengths: no panic, no address.
    let mut hostile = calldata.clone();
    hostile[4 + 32..4 + 64].copy_from_slice(&U256::MAX.to_be_bytes::<32>());
    assert_eq!(CalldataPayloadBuilder::decode_v2_path_ends(&hostile), None);
    assert_eq!(CalldataPayloadBuilder::decode_v2_path_ends(&calldata[..40]), None);
}

#[test]
fn v2_exit_quote_and_sell_encoders_match_the_abi() {
    let quote = CalldataPayloadBuilder::build_v2_get_amounts_out_payload(U256::from(5u64), &[TOKEN, WETH]);
    let q = getAmountsOutCall::abi_decode(&quote).unwrap();
    assert_eq!((q.amountIn, q.path), (U256::from(5u64), vec![TOKEN, WETH]));

    let sell = CalldataPayloadBuilder::build_v2_exit_fot_payload(U256::from(5u64), U256::from(4u64), &[TOKEN, WETH], &ROUTER, 99);
    let d = swapExactTokensForETHSupportingFeeOnTransferTokensCall::abi_decode(&sell).unwrap();
    assert_eq!((d.amountIn, d.amountOutMin, d.to, d.deadline), (U256::from(5u64), U256::from(4u64), ROUTER, U256::from(99u64)));

    // getAmountsOut returns uint256[]: [amountIn, amountOut]
    let mut ret = Vec::new();
    for w in [U256::from(32u64), U256::from(2u64), U256::from(5u64), U256::from(3u64)] {
        ret.extend_from_slice(&w.to_be_bytes::<32>());
    }
    assert_eq!(CalldataPayloadBuilder::decode_v2_amounts_out_last(&ret), Some(U256::from(3u64)));
    assert_eq!(CalldataPayloadBuilder::decode_v2_amounts_out_last(&ret[..40]), None);
}

#[test]
fn selectors_match_their_signatures() {
    let sel = |s: &str| <[u8; 4]>::try_from(&keccak256(s.as_bytes())[..4]).unwrap();
    assert_eq!(payload::V2_GET_AMOUNTS_OUT_SELECTOR, sel("getAmountsOut(uint256,address[])"));
    assert_eq!(
        payload::SWAP_EXACT_TOKENS_FOR_ETH_FOT_SELECTOR,
        sel("swapExactTokensForETHSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)")
    );
    assert_eq!(payload::GAS_ESTIMATE_L1_COMPONENT_SELECTOR, sel("gasEstimateL1Component(address,bool,bytes)"));
    assert_eq!(payload::PONS_BUY_SELECTOR, sel("buy(uint256,uint256,address)"));
    assert_eq!(payload::PONS_SELL_SELECTOR, sel("sell(uint256,uint256,address)"));
}

#[test]
fn l1_component_estimate_payload_is_valid_abi() {
    let data = CalldataPayloadBuilder::build_pons_buy_payload(U256::from(1u64), U256::from(2u64), TOKEN);
    let call = CalldataPayloadBuilder::build_l1_component_estimate_payload(CURVE, &data);
    let d = gasEstimateL1ComponentCall::abi_decode(&call).unwrap();
    assert_eq!((d.to, d.contractCreation, d.data.to_vec()), (CURVE, false, data));
}

#[test]
fn send_raw_tx_body_matches_serde_encoding() {
    let raw = [0x02u8, 0xf8, 0x00, 0xff];
    let expected = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x02f800ff"]});
    let body: serde_json::Value = serde_json::from_slice(&send_raw_tx_body(&raw)).unwrap();
    assert_eq!(body, expected);
}

#[test]
fn revert_guard_quotes_post_dev_buy_reserves_and_fails_closed() {
    let size = U256::from(10_000_000_000_000_000u64);
    let dev_buy = U256::from(60_000_000_000_000_000u64);
    let (e1, t1) = payload::pons_reserves_after_buy(dev_buy);
    let naive = CalldataPayloadBuilder::calculate_pons_buy_revert_guard(size, None, None, 25.0);
    let real = CalldataPayloadBuilder::calculate_pons_buy_revert_guard(size, Some(e1), Some(t1), 25.0);
    // A 0.06 ETH dev buy alone eats ~7% of the naive quote's slippage budget.
    assert!(real < naive * U256::from(94u64) / U256::from(100u64));
    // A Block +1 fill right after the dev buy still clears the guard.
    let fill = CalldataPayloadBuilder::calculate_pons_tokens_out(e1, t1, size, 100, 618);
    assert!(fill >= real);
    // NaN slippage used to cast to minTokensOut = 0 (no guard at all).
    let nan = CalldataPayloadBuilder::calculate_pons_buy_revert_guard(size, Some(e1), Some(t1), f64::NAN);
    assert_eq!(nan, CalldataPayloadBuilder::calculate_pons_buy_revert_guard(size, Some(e1), Some(t1), 0.0));

    let (e0, t0) = (U256::from(payload::PONS_INITIAL_ETH_RESERVE_WEI), U256::from(payload::PONS_INITIAL_TOKEN_RESERVE_WEI));
    let mcap = payload::pons_mcap_eth(e0, t0).unwrap();
    assert!((mcap - 1.68).abs() < 1e-9, "initial fully diluted mcap is 1.68 ETH, got {mcap}");
}

// ── Prysm receipt parsing ────────────────────────────────────────────────────

#[test]
fn topic_parser_rejects_non_ascii_instead_of_panicking() {
    assert_eq!(parse_address_from_topic(&format!("0x{}a", "é".repeat(25))), None);
    assert_eq!(
        parse_address_from_topic("0x00000000000000000000000016d69c46f20965157584371788cdf557dfde2c53"),
        Some(address!("16d69c46f20965157584371788cdf557dfde2c53"))
    );
}

#[test]
fn launch_receipt_needs_success_and_an_allowlisted_emitter() {
    let launchpad = address!("00000000000000000000000000000000000fac70");
    let pads: HashSet<Address> = [launchpad].into_iter().collect();
    let topic = |a: Address| format!("0x000000000000000000000000{}", alloy::hex::encode(a));
    let receipt = |status: &str, emitter: Address| {
        serde_json::json!({
            "status": status,
            "logs": [{
                "address": format!("{:?}", emitter),
                "topics": [payload::PONS_LAUNCH_TOPIC_HEX, topic(TOKEN), topic(CURVE)],
            }]
        })
    };
    assert_eq!(
        parse_launch_receipt(&receipt("0x1", launchpad), &pads),
        LaunchResolution::Resolved { token: TOKEN, curve: CURVE, reserves: None }
    );
    assert_eq!(parse_launch_receipt(&receipt("0x0", launchpad), &pads), LaunchResolution::Failed);
    assert_eq!(parse_launch_receipt(&receipt("0x1", Address::repeat_byte(0xee)), &pads), LaunchResolution::Failed);
}
