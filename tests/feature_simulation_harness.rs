//! Feature verification harness: per-feature checks and a full life-cycle dry run.
//!
//! Everything runs in this process, with no external network:
//!
//! * `Chain` — a deterministic Arbitrum / Pons V2 state machine:
//!   - bonding curves with base fee + creator tax (per curve), the time-based launch snipe tax
//!     (9900 / 618 / 19 / 0 bps in seconds 0 / 1 / 2 / 3+ after the launch block) and graduation
//!     at 4.2 ETH raised (the curve then refuses trades);
//!   - ERC-20 balances and allowances (a sell needs the curve approved);
//!   - ETH balances and gas: a submission is refused when it is larger than the sequencer's
//!     95,000-byte limit, when its gas limit is below intrinsic gas + the L1 component, or when the
//!     sender cannot cover `gas_limit * max_fee + value`; executed transactions pay
//!     `gas_used * base_fee` (Arbitrum does not pay priority fees), reverted ones included;
//!   - per-account nonces, receipts, and the reserves after every block (so `eth_call` pinned to a
//!     block answers like a node).
//!
//!   Blocks are sealed only by the test (`World::seal`), on a schedule (`BLOCK_TIME` in the launch
//!   timelines, never "when the engine is ready"), or by an explicit auto-sealer. Block timestamps
//!   follow the wall clock unless a test pins them (launch timelines: L at second T, L+1 at T,
//!   L+2 at T+1 or T).
//! * a JSON-RPC node over HTTP/1.1 on loopback, answering from `Chain`. It runs on its own
//!   threads, can serve blocks with a lag (`rpc_lag`), and can answer `eth_sendRawTransaction`
//!   with a latency. Every request is logged.
//! * the Nitro sequencer feed over WebSocket: every sealed block goes out as a kind-3 batch of the
//!   signed transactions it contains, exactly the bytes the engine parses in production.
//! * the engine, wired as `main.rs` wires it (stream → Prysm / copy-trade → signal queue →
//!   dispatcher with AOT pre-signing → risk engine on the in-memory curve mirror → receipt-confirmed
//!   exits), plus the Telegram control plane. Its configuration is set explicitly (production
//!   defaults), so shell variables cannot change what the tests see.
//!
//! What is *not* modelled: the EVM itself (curve, token and launchpad behaviour is written out
//! here from the Pons V2 rules the engine targets), reorgs, and L1 pricing (the L1 component is a
//! fixed 50,000 gas, as the simulated NodeInterface reports).
//!
//! Timing assertions are strict in release builds (`cargo test --release --test
//! feature_simulation_harness`) and loose in debug builds, where unoptimised U256 math, signing and
//! keccak over 95 KB make everything 10-50x slower. Every test prints what it measured.

use alloy::consensus::transaction::SignerRecoverable;
use alloy::consensus::{SignableTransaction, Transaction as _, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::{Decodable2718, Encodable2718};
use alloy::primitives::{address, keccak256, Address, Bytes, TxKind, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use autotradesniper::config::{AppConfig, Watchlist};
use autotradesniper::dispatcher::{BroadcasterPool, Dispatcher, PresignPolicy, TxSpec};
use autotradesniper::models::prysm::{COMPANION_ACCOUNTS, PRYSMCLOUD_SUBSTRING};
use autotradesniper::models::PrysmEngine;
use autotradesniper::payload::{self, CalldataPayloadBuilder, DynamicFeeAdjuster, TARGET_CALLDATA_BUFFER_SIZE};
use autotradesniper::risk::curve_mirror::{CurveMirror, MirrorPolicy, MirrorRead};
use autotradesniper::risk::types::{ExitVenue, DEFAULT_TRAILING_STOP_PERCENT};
use autotradesniper::risk::RiskManager;
use autotradesniper::stream::StreamListener;
use autotradesniper::telegram::{html, screens, Action, ControlPlane, EngineHandles};
use autotradesniper::types::{DispatchReport, ExecutionMode, Position, PrysmExitMode, TradeSignal};
use autotradesniper::wallet::WalletManager;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

const MNEMONIC: &str = "test test test test test test test test test test test junk";
const LAUNCHPAD: Address = address!("00000000000000000000000000000000000fac70");
/// Arbitrum's NodeInterface (the fee refresher asks it for the L1 component).
const NODE_INTERFACE: Address = address!("00000000000000000000000000000000000000c8");
const GENESIS: u64 = 7_000_000;
const CHAIN_ID: u64 = 4663;
/// Pons V2 default: base fee 1% + creator tax 2%, taken from the quote side, leaving the reserves.
const TRADE_BPS: u64 = 300;
/// L2 base fee (0.02 gwei); `eth_gasPrice` reports the same.
const BASE_FEE_WEI: u128 = 20_000_000;
/// L1 component of every transaction, in L2 gas (what the simulated NodeInterface reports).
const L1_GAS: u64 = 50_000;
/// Nitro sequencer's maximum encoded transaction size.
const MAX_TX_BYTES: usize = 95_000;
/// Ceiling of fee + creator tax + snipe tax on a buy (the launch second: 99%).
const MAX_TOTAL_BUY_TAX_BPS: u64 = 9_900;
/// EIP-7623 calldata floor (gas per token; zero byte = 1 token, non-zero = 4).
const CALLDATA_FLOOR_PER_TOKEN: u64 = 10;
/// Execution gas beyond intrinsic + L1, per kind of call.
const LAUNCH_EXEC_GAS: u64 = 150_000;
const CURVE_TRADE_EXEC_GAS: u64 = 110_000;
const TOKEN_CALL_EXEC_GAS: u64 = 30_000;
/// Every carrier account (indices 1..=5) is funded with this much at start.
const CARRIER_FUNDING_ETH: f64 = 0.5;
const MASTER_FUNDING_ETH: f64 = 1.0;
/// Trailing-stop distance in force in these tests: the production default (`TRAILING_STOP_PERCENT`).
const TRAIL: f64 = DEFAULT_TRAILING_STOP_PERCENT;
/// `main.rs`: a signal older than this when the dispatcher picks it up is dropped.
const MAX_SIGNAL_QUEUE_AGE: Duration = Duration::from_secs(3);
const RELEASE: bool = !cfg!(debug_assertions);
/// Block time in the launch timelines (Arbitrum: 250 ms). Debug builds sign and hash 95 KB
/// packets ~50x slower, so there the simulated chain gives the engine 1.5 s per block.
const BLOCK_TIME: Duration = if RELEASE { Duration::from_millis(250) } else { Duration::from_millis(1_500) };
/// In the launch timelines the node serves a block's receipts this long after the feed has it.
const RPC_LAG: Duration = if RELEASE { Duration::from_millis(100) } else { Duration::from_millis(600) };
/// `eth_sendRawTransaction` response time in the timing tests: a sender that waited for one
/// answer before sending the next packet would spread its burst over at least this much.
const SEND_LATENCY: Duration = Duration::from_millis(25);

fn wei(eth: f64) -> U256 {
    U256::from((eth * 1e18).round() as u128)
}

fn eth(w: U256) -> f64 {
    f64::from(w) / 1e18
}

fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

/// Pons V2 launch snipe tax on buys, by whole seconds since the launch block.
fn snipe_bps(elapsed_secs: u64) -> u64 {
    match elapsed_secs {
        0 => 9_900,
        1 => 618,
        2 => 19,
        _ => 0,
    }
}

fn intrinsic_gas(input: &[u8]) -> u64 {
    21_000 + input.iter().map(|b| if *b == 0 { 4 } else { 16 }).sum::<u64>() + L1_GAS
}

/// EIP-7623: `21000 + 10 x (zero bytes + 4 x non-zero bytes)`, plus the L1 component.
fn floor_gas(input: &[u8]) -> u64 {
    21_000 + CALLDATA_FLOOR_PER_TOKEN * input.iter().map(|b| if *b == 0 { 1 } else { 4 }).sum::<u64>() + L1_GAS
}

fn word(v: U256) -> String {
    alloy::hex::encode(v.to_be_bytes::<32>())
}

fn arg(input: &[u8], i: usize) -> Option<U256> {
    input.get(4 + 32 * i..4 + 32 * (i + 1)).map(U256::from_be_slice)
}

fn arg_address(input: &[u8], i: usize) -> Option<Address> {
    input.get(4 + 32 * i + 12..4 + 32 * (i + 1)).map(Address::from_slice)
}

/// Deterministic `(token, curve)` of a launch tx.
fn launch_addresses(launch_tx: B256) -> (Address, Address) {
    let derive = |tag: &[u8]| Address::from_slice(&keccak256([launch_tx.as_slice(), tag].concat())[12..]);
    (derive(b"token"), derive(b"curve"))
}

fn pola_a_calldata() -> Vec<u8> {
    format!("\0\0{{\"image\":\"https://cdn.example/{}/a1b2.png\"}}\0", PRYSMCLOUD_SUBSTRING).into_bytes()
}

fn pola_b_calldata() -> Vec<u8> {
    b"\0\0{\"website\":\"https://moonshot.lat\"}\0".to_vec()
}

/// A launch whose metadata carries none of the Prysm markers.
fn plain_launch_calldata() -> Vec<u8> {
    b"\0\0{\"website\":\"https://example.com\"}\0".to_vec()
}

// ════════════════════════════════════════════════════════════════════════════
// The simulated chain
// ════════════════════════════════════════════════════════════════════════════

#[derive(Clone, Debug)]
struct Curve {
    token: Address,
    e: U256,
    t: U256,
    /// Base fee + creator tax of this curve, in bps.
    bps: u64,
    launch_ts: u64,
    /// 4.2 ETH raised: the curve migrated and refuses every further trade.
    graduated: bool,
    /// Reserves after each block (blocks without a change inherit the previous entry).
    history: BTreeMap<u64, (U256, U256)>,
}

impl Curve {
    /// ETH the curve would pay for `tokens` right now (after its fee + creator tax).
    fn value_of(&self, tokens: U256) -> U256 {
        if tokens.is_zero() {
            return U256::ZERO;
        }
        let gross = self.e * tokens / (self.t + tokens);
        gross - gross * U256::from(self.bps) / U256::from(10_000u64)
    }

    fn raised_eth(&self) -> f64 {
        eth(self.e) - eth(U256::from(payload::PONS_INITIAL_ETH_RESERVE_WEI))
    }
}

/// One `eth_sendRawTransaction` as the node received it.
#[derive(Clone, Debug)]
struct SentTx {
    /// First byte of the request at the node.
    first_byte: Instant,
    /// The whole request (the complete signed tx) at the node.
    at: Instant,
    /// Head when the node accepted it into its mempool: the tx lands in the next block.
    head_at_arrival: u64,
    /// Refusal reason, if the node refused it.
    refused: Option<String>,
    hash: B256,
    from: Address,
    to: Option<Address>,
    value: U256,
    nonce: u64,
    input_head: Vec<u8>,
    input_len: usize,
    encoded_len: usize,
    gas_limit: u64,
    /// Every calldata byte is zero (a block-booking carrier).
    all_zero: bool,
    priority_fee: u128,
}

impl SentTx {
    fn selector(&self) -> Option<[u8; 4]> {
        self.input_head.first_chunk::<4>().copied()
    }

    fn accepted(&self) -> bool {
        self.refused.is_none()
    }
}

#[derive(Clone, Debug)]
struct Receipt {
    ok: bool,
    block: u64,
    logs: serde_json::Value,
    gas_used: u64,
    /// `gas_used * base fee`, paid by the sender whether the tx succeeded or not.
    fee: U256,
    /// ETH a successful sell paid out.
    eth_out: U256,
    /// Tokens a successful buy delivered.
    tokens_out: U256,
}

struct Pending {
    raw: Vec<u8>,
    hash: B256,
    from: Address,
    nonce: u64,
}

struct Exec {
    ok: bool,
    logs: serde_json::Value,
    eth_out: U256,
    tokens_out: U256,
}

impl Exec {
    fn fail() -> Self {
        Exec { ok: false, logs: serde_json::json!([]), eth_out: U256::ZERO, tokens_out: U256::ZERO }
    }

    fn ok() -> Self {
        Exec { ok: true, ..Self::fail() }
    }
}

#[derive(Default)]
struct Chain {
    head: u64,
    ts: u64,
    clock_floor: u64,
    /// Timestamp for the next blocks (launch timelines); `None`: the wall clock.
    pinned_ts: Option<u64>,
    /// How long after sealing a block the node's RPC serves it (receipts, `eth_blockNumber`,
    /// pinned reads). The feed carries the block at once, as a Nitro sequencer feed does.
    rpc_lag: Duration,
    /// Response time of `eth_sendRawTransaction` (a remote sequencer endpoint is not instant).
    send_latency: Duration,
    /// Fee + creator tax of the next launches (0: the 3% default).
    next_launch_bps: u64,
    /// Enforce the EIP-7623 calldata floor on L2 gas (whether Arbitrum does is not established).
    calldata_floor: bool,
    sealed_at: BTreeMap<u64, Instant>,
    curves: HashMap<Address, Curve>,
    curve_of_token: HashMap<Address, Address>,
    tokens: HashMap<(Address, Address), U256>,
    /// `(token, owner, spender) -> amount`
    allowances: HashMap<(Address, Address, Address), U256>,
    eth: HashMap<Address, U256>,
    gas_paid: HashMap<Address, U256>,
    nonces: HashMap<Address, u64>,
    mempool: Vec<Pending>,
    receipts: HashMap<B256, Receipt>,
    blocks: BTreeMap<u64, Vec<B256>>,
    sent: Vec<SentTx>,
    /// Every request: `(when, method, detail)`; detail = `to:selector@block` for `eth_call`.
    rpc: Vec<(Instant, String, String)>,
}

impl Chain {
    fn balance_of(&self, who: Address) -> U256 {
        self.eth.get(&who).copied().unwrap_or_default()
    }

    fn token_balance(&self, token: Address, owner: Address) -> U256 {
        self.tokens.get(&(token, owner)).copied().unwrap_or_default()
    }

    fn debit(&mut self, who: Address, amount: U256) -> bool {
        let bal = self.balance_of(who);
        if bal < amount {
            return false;
        }
        self.eth.insert(who, bal - amount);
        true
    }

    fn credit(&mut self, who: Address, amount: U256) {
        *self.eth.entry(who).or_default() += amount;
    }

    /// Execution gas (beyond intrinsic + L1) of a call to `to`.
    fn exec_gas(&self, to: Option<Address>) -> u64 {
        match to {
            Some(a) if a == LAUNCHPAD => LAUNCH_EXEC_GAS,
            Some(a) if self.curves.contains_key(&a) => CURVE_TRADE_EXEC_GAS,
            Some(a) if self.curve_of_token.contains_key(&a) => TOKEN_CALL_EXEC_GAS,
            _ => 0,
        }
    }

    /// Why the node refuses `tx` from `from` right now, if it does.
    fn refusal(&self, raw_len: usize, tx: &TxEnvelope, from: Address) -> Option<String> {
        if raw_len > MAX_TX_BYTES {
            return Some(format!("oversized data: transaction size {raw_len}, limit {MAX_TX_BYTES}"));
        }
        if tx.chain_id() != Some(CHAIN_ID) {
            return Some("invalid chain id".into());
        }
        if tx.nonce() < self.nonces.get(&from).copied().unwrap_or(0) {
            return Some("nonce too low".into());
        }
        if tx.gas_limit() < intrinsic_gas(tx.input()) {
            return Some(format!("intrinsic gas too low: have {}, want {}", tx.gas_limit(), intrinsic_gas(tx.input())));
        }
        if self.calldata_floor && tx.gas_limit() < floor_gas(tx.input()) {
            return Some(format!("insufficient gas for floor data gas cost: have {}, want {}", tx.gas_limit(), floor_gas(tx.input())));
        }
        if tx.max_fee_per_gas() < BASE_FEE_WEI {
            return Some("max fee per gas less than block base fee".into());
        }
        let upfront = U256::from(tx.gas_limit()) * U256::from(tx.max_fee_per_gas()) + tx.value();
        if self.balance_of(from) < upfront {
            return Some(format!("insufficient funds for gas * price + value: have {} want {}", self.balance_of(from), upfront));
        }
        None
    }

    /// Execute one transaction's call against the state (gas is charged by the caller).
    fn execute(&mut self, tx: &TxEnvelope, from: Address, hash: B256, ts: u64) -> Exec {
        let input = tx.input();
        let value = tx.value();
        let Some(to) = tx.to() else { return Exec::fail() };
        if to == LAUNCHPAD {
            // Pons launch: the curve starts at the initial virtual reserves, the dev buy pays no tax.
            if !self.debit(from, value) {
                return Exec::fail();
            }
            let (token, curve) = launch_addresses(hash);
            let e0 = U256::from(payload::PONS_INITIAL_ETH_RESERVE_WEI);
            let t0 = U256::from(payload::PONS_INITIAL_TOKEN_RESERVE_WEI);
            let out = t0 * value / (e0 + value);
            let bps = if self.next_launch_bps == 0 { TRADE_BPS } else { self.next_launch_bps };
            let c = Curve { token, e: e0 + value, t: t0 - out, bps, launch_ts: ts, graduated: false, history: BTreeMap::new() };
            self.curves.insert(curve, c);
            self.curve_of_token.insert(token, curve);
            *self.tokens.entry((token, from)).or_default() += out;
            let topic = |a: Address| format!("0x{:0>64}", alloy::hex::encode(a.as_slice()));
            let logs = serde_json::json!([{
                "address": format!("{:?}", LAUNCHPAD),
                "topics": [payload::PONS_LAUNCH_TOPIC_HEX, topic(token), topic(curve)],
                "data": "0x"
            }]);
            return Exec { logs, tokens_out: out, ..Exec::ok() };
        }
        if let Some(curve) = self.curves.get(&to).cloned() {
            let mut c = curve;
            if c.graduated {
                return Exec::fail();
            }
            let result = match input.first_chunk::<4>() {
                Some(sel) if *sel == payload::PONS_BUY_SELECTOR => {
                    let (Some(eth_in), Some(min_out), Some(recipient)) = (arg(input, 0), arg(input, 1), arg_address(input, 2)) else {
                        return Exec::fail();
                    };
                    // Fee + creator tax + snipe tax, at most 99% in total: a buy in the launch second
                    // still gets ~1% of its quote priced (as the engine's Revert Guard expects).
                    let bps = (c.bps + snipe_bps(ts.saturating_sub(c.launch_ts))).min(MAX_TOTAL_BUY_TAX_BPS);
                    let net = value - value * U256::from(bps) / U256::from(10_000u64);
                    let out = c.t * net / (c.e + net);
                    if eth_in != value || out < min_out || out >= c.t || !self.debit(from, value) {
                        Exec::fail()
                    } else {
                        c.e += net;
                        c.t -= out;
                        *self.tokens.entry((c.token, recipient)).or_default() += out;
                        if c.e >= U256::from(payload::PONS_INITIAL_ETH_RESERVE_WEI + payload::PONS_GRADUATION_ETH_WEI) {
                            c.graduated = true;
                        }
                        Exec { tokens_out: out, ..Exec::ok() }
                    }
                }
                Some(sel) if *sel == payload::PONS_SELL_SELECTOR => {
                    let (Some(tokens_in), Some(min_out), Some(recipient)) = (arg(input, 0), arg(input, 1), arg_address(input, 2)) else {
                        return Exec::fail();
                    };
                    let held = self.token_balance(c.token, from);
                    let allowance = self.allowances.get(&(c.token, from, to)).copied().unwrap_or_default();
                    let gross = c.e * tokens_in / (c.t + tokens_in);
                    let out = gross - gross * U256::from(c.bps) / U256::from(10_000u64);
                    if tokens_in.is_zero() || held < tokens_in || allowance < tokens_in || out < min_out || !value.is_zero() {
                        Exec::fail()
                    } else {
                        c.e -= gross;
                        c.t += tokens_in;
                        self.tokens.insert((c.token, from), held - tokens_in);
                        if allowance != U256::MAX {
                            self.allowances.insert((c.token, from, to), allowance - tokens_in);
                        }
                        self.credit(recipient, out);
                        Exec { eth_out: out, ..Exec::ok() }
                    }
                }
                // Anything else sent to the curve reverts.
                _ => Exec::fail(),
            };
            self.curves.insert(to, c);
            return result;
        }
        if self.curve_of_token.contains_key(&to) {
            // The token: `approve(spender, amount)`; nothing else is used here.
            if !value.is_zero() {
                return Exec::fail();
            }
            return match (input.first_chunk::<4>(), arg_address(input, 0), arg(input, 1)) {
                (Some(sel), Some(spender), Some(amount)) if *sel == payload::ERC20_APPROVE_SELECTOR => {
                    self.allowances.insert((to, from, spender), amount);
                    Exec::ok()
                }
                _ => Exec::fail(),
            };
        }
        // An EOA: no code runs; a zero-value call (a carrier's self-call) cannot revert.
        if !value.is_zero() {
            if !self.debit(from, value) {
                return Exec::fail();
            }
            self.credit(to, value);
        }
        Exec::ok()
    }

    /// Seal the next block: the mempool, then `extra` (scripted txs), in order. Returns
    /// `(block, timestamp, L2 message of the included txs)`.
    fn seal(&mut self, extra: Vec<Vec<u8>>) -> (u64, u64, Vec<u8>) {
        let block = self.head + 1;
        let ts = self.pinned_ts.unwrap_or_else(|| self.clock_floor.max(unix_now())).max(self.ts);
        let mut candidates: Vec<Vec<u8>> = std::mem::take(&mut self.mempool).into_iter().map(|p| p.raw).collect();
        candidates.extend(extra);
        let mut included = Vec::new();
        let mut l2 = vec![3u8]; // kind-3 batch of kind-4 signed txs
        for raw in candidates {
            let Ok(tx) = TxEnvelope::decode_2718_exact(&raw[..]) else { continue };
            let Ok(from) = tx.recover_signer() else { continue };
            let hash = *tx.tx_hash();
            let expected = self.nonces.get(&from).copied().unwrap_or(0);
            if tx.nonce() < expected || self.receipts.contains_key(&hash) {
                continue; // replaced / duplicate: never executes
            }
            if tx.nonce() > expected {
                self.mempool.push(Pending { raw, hash, from, nonce: tx.nonce() }); // waits for the gap
                continue;
            }
            if self.refusal(raw.len(), &tx, from).is_some() {
                continue; // cannot pay any more: dropped
            }
            self.nonces.insert(from, expected + 1);
            // Out of gas before it starts: reverts, the whole limit is used.
            let mut needed = intrinsic_gas(tx.input()) + self.exec_gas(tx.to());
            if self.calldata_floor {
                needed = needed.max(floor_gas(tx.input()));
            }
            let (exec, gas_used) = if needed > tx.gas_limit() {
                (Exec::fail(), tx.gas_limit())
            } else {
                (self.execute(&tx, from, hash, ts), needed)
            };
            let fee = U256::from(gas_used) * U256::from(BASE_FEE_WEI);
            assert!(self.debit(from, fee), "upfront check covers the fee");
            *self.gas_paid.entry(from).or_default() += fee;
            let receipt =
                Receipt { ok: exec.ok, block, logs: exec.logs, gas_used, fee, eth_out: exec.eth_out, tokens_out: exec.tokens_out };
            self.receipts.insert(hash, receipt);
            included.push(hash);
            l2.extend_from_slice(&((raw.len() + 1) as u64).to_be_bytes());
            l2.push(4);
            l2.extend_from_slice(&raw);
        }
        for c in self.curves.values_mut() {
            c.history.insert(block, (c.e, c.t));
        }
        self.blocks.insert(block, included);
        self.sealed_at.insert(block, Instant::now());
        self.head = block;
        self.ts = ts;
        (block, ts, l2)
    }

    fn pending_nonce(&self, who: Address) -> u64 {
        let mut n = self.nonces.get(&who).copied().unwrap_or(0);
        let mut pending: Vec<u64> = self.mempool.iter().filter(|p| p.from == who).map(|p| p.nonce).collect();
        pending.sort_unstable();
        for p in pending {
            if p == n {
                n += 1;
            }
        }
        n
    }

    /// Newest block the RPC side serves (see `rpc_lag`).
    fn visible_head(&self) -> u64 {
        if self.rpc_lag.is_zero() {
            return self.head;
        }
        let now = Instant::now();
        self.sealed_at.iter().rev().find(|(_, at)| **at + self.rpc_lag <= now).map(|(b, _)| *b).unwrap_or(GENESIS - 1)
    }

    fn answer(&mut self, req: &serde_json::Value, arrival: (Instant, Instant)) -> serde_json::Value {
        let vhead = self.visible_head();
        let id = req["id"].clone();
        let err = |msg: &str| serde_json::json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":msg}});
        let method = req["method"].as_str().unwrap_or_default().to_string();
        let params = &req["params"];
        let addr = |v: &serde_json::Value| v.as_str().and_then(|s| s.parse::<Address>().ok()).unwrap_or_default();
        let block_tag = |v: &serde_json::Value, head: u64| -> Option<u64> {
            match v.as_str() {
                None | Some("latest") | Some("pending") => Some(head),
                Some(hex) => u64::from_str_radix(hex.trim_start_matches("0x"), 16).ok(),
            }
        };
        let call_data = params[0]["data"].as_str().or(params[0]["input"].as_str()).unwrap_or_default().trim_start_matches("0x").to_string();
        let selector = call_data[..call_data.len().min(8)].to_string();
        let detail = if method == "eth_call" {
            format!("{:?}:{}@{}", addr(&params[0]["to"]), selector, params[1].as_str().unwrap_or("latest"))
        } else {
            String::new()
        };
        // Logged before anything can return.
        self.rpc.push((Instant::now(), method.clone(), detail));
        let result = match method.as_str() {
            "eth_chainId" => serde_json::json!(format!("0x{:x}", CHAIN_ID)),
            "net_version" => serde_json::json!(CHAIN_ID.to_string()),
            "eth_blockNumber" => serde_json::json!(format!("0x{:x}", vhead)),
            "eth_gasPrice" => serde_json::json!(format!("0x{:x}", BASE_FEE_WEI)),
            "eth_maxPriorityFeePerGas" => serde_json::json!("0x0"),
            "eth_getBalance" => serde_json::json!(format!("0x{:x}", self.balance_of(addr(&params[0])))),
            "eth_getTransactionCount" => {
                let who = addr(&params[0]);
                let n = if params[1].as_str() == Some("latest") { self.nonces.get(&who).copied().unwrap_or(0) } else { self.pending_nonce(who) };
                serde_json::json!(format!("0x{:x}", n))
            }
            "eth_call" => {
                let to = addr(&params[0]["to"]);
                let Some(at) = block_tag(&params[1], vhead) else { return err("bad block tag") };
                if at > vhead {
                    return err("header not found");
                }
                let raw = alloy::hex::decode(&call_data).unwrap_or_default();
                if to == NODE_INTERFACE {
                    serde_json::json!(format!("0x{}{}{}", word(U256::from(L1_GAS)), word(U256::from(BASE_FEE_WEI)), word(U256::ZERO)))
                } else if let Some(c) = self.curves.get(&to) {
                    match selector.as_str() {
                        "0902f1ac" => match c.history.range(..=at).next_back() {
                            Some((_, (e, t))) => serde_json::json!(format!("0x{}{}", word(*e), word(*t))),
                            None => return err("execution reverted"),
                        },
                        "3de35b79" => serde_json::json!(format!("0x{:0>64}", alloy::hex::encode(c.token.as_slice()))),
                        _ => return err("execution reverted"),
                    }
                } else if self.curve_of_token.contains_key(&to) {
                    match selector.as_str() {
                        "70a08231" => {
                            let owner = arg_address(&raw, 0).unwrap_or_default();
                            serde_json::json!(format!("0x{}", word(self.token_balance(to, owner))))
                        }
                        "dd62ed3e" => {
                            let (owner, spender) = (arg_address(&raw, 0).unwrap_or_default(), arg_address(&raw, 1).unwrap_or_default());
                            serde_json::json!(format!("0x{}", word(self.allowances.get(&(to, owner, spender)).copied().unwrap_or_default())))
                        }
                        _ => return err("execution reverted"),
                    }
                } else {
                    return err("execution reverted");
                }
            }
            "eth_sendRawTransaction" => {
                let raw = alloy::hex::decode(params[0].as_str().unwrap_or_default()).unwrap_or_default();
                let Ok(tx) = TxEnvelope::decode_2718_exact(&raw[..]) else { return err("invalid transaction") };
                let Ok(from) = tx.recover_signer() else { return err("invalid signature") };
                let hash = *tx.tx_hash();
                let known = self.receipts.contains_key(&hash) || self.mempool.iter().any(|p| p.hash == hash);
                let refused = if known { Some("already known".to_string()) } else { self.refusal(raw.len(), &tx, from) };
                let input = tx.input();
                self.sent.push(SentTx {
                    first_byte: arrival.0,
                    at: arrival.1,
                    head_at_arrival: self.head,
                    refused: refused.clone(),
                    hash,
                    from,
                    to: tx.to(),
                    value: tx.value(),
                    nonce: tx.nonce(),
                    input_head: input[..input.len().min(100)].to_vec(),
                    input_len: input.len(),
                    encoded_len: raw.len(),
                    gas_limit: tx.gas_limit(),
                    all_zero: input.iter().all(|b| *b == 0),
                    priority_fee: tx.max_priority_fee_per_gas().unwrap_or_default(),
                });
                if let Some(why) = refused {
                    return err(&why);
                }
                self.mempool.push(Pending { raw, hash, from, nonce: tx.nonce() });
                serde_json::json!(format!("{:?}", hash))
            }
            "eth_getTransactionReceipt" => {
                let hash: B256 = params[0].as_str().unwrap_or_default().parse().unwrap_or_default();
                match self.receipts.get(&hash).filter(|r| r.block <= vhead) {
                    Some(r) => serde_json::json!({
                        "transactionHash": format!("{:?}", hash),
                        "blockNumber": format!("0x{:x}", r.block),
                        "status": if r.ok { "0x1" } else { "0x0" },
                        "logs": r.logs,
                        "gasUsed": format!("0x{:x}", r.gas_used),
                        "cumulativeGasUsed": format!("0x{:x}", r.gas_used),
                        "effectiveGasPrice": format!("0x{:x}", BASE_FEE_WEI),
                        "type": "0x2"
                    }),
                    None => serde_json::Value::Null,
                }
            }
            "eth_getBlockByNumber" => {
                if params[1].as_bool() == Some(true) {
                    return err("hydrated blocks not served"); // keeps the HTTP bridge out
                }
                let Some(n) = block_tag(&params[0], vhead) else { return err("bad block tag") };
                match self.blocks.get(&n).filter(|_| n <= vhead) {
                    Some(txs) => serde_json::json!({
                        "number": format!("0x{:x}", n),
                        "timestamp": format!("0x{:x}", self.ts),
                        "baseFeePerGas": format!("0x{:x}", BASE_FEE_WEI),
                        "transactions": txs.iter().map(|h| format!("{:?}", h)).collect::<Vec<_>>()
                    }),
                    None => serde_json::Value::Null,
                }
            }
            _ => return err("unsupported"),
        };
        serde_json::json!({"jsonrpc":"2.0","id":id,"result":result})
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Node (JSON-RPC over HTTP) and sequencer feed (WebSocket)
// ════════════════════════════════════════════════════════════════════════════

async fn rpc_server(chain: Arc<Mutex<Chain>>) -> (String, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let _ = sock.set_nodelay(true);
            let chain = chain.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                loop {
                    let mut first_byte = (!buf.is_empty()).then(Instant::now);
                    let header_end = loop {
                        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break p + 4;
                        }
                        let mut chunk = [0u8; 65_536];
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                first_byte.get_or_insert_with(Instant::now);
                                buf.extend_from_slice(&chunk[..n]);
                            }
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..header_end]).to_lowercase();
                    let len: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                    while buf.len() < header_end + len {
                        let mut chunk = [0u8; 65_536];
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let arrival = (first_byte.unwrap_or_else(Instant::now), Instant::now());
                    let raw_req: Vec<u8> = buf.drain(..header_end + len).skip(header_end).collect();
                    // Parsing, signature recovery and hashing of ~95 KB transactions happen off the
                    // I/O threads, so they never delay reading (and timestamping) other requests.
                    let chain = chain.clone();
                    let Ok((body, latency)) = tokio::task::spawn_blocking(move || {
                        let req: serde_json::Value = serde_json::from_slice(&raw_req).unwrap_or_default();
                        let mut c = chain.lock().unwrap();
                        let latency = if req["method"] == "eth_sendRawTransaction" { c.send_latency } else { Duration::ZERO };
                        let body = match &req {
                            serde_json::Value::Array(batch) => serde_json::Value::Array(batch.iter().map(|r| c.answer(r, arrival)).collect()),
                            single => c.answer(single, arrival),
                        };
                        (body.to_string(), latency)
                    })
                    .await
                    else {
                        return;
                    };
                    tokio::time::sleep_until((arrival.1 + latency).into()).await;
                    let resp = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}", body.len(), body);
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (url, task)
}

fn feed_frame(seq: u64, ts: u64, l2: &[u8]) -> String {
    format!(
        r#"{{"version":1,"messages":[{{"sequenceNumber":{seq},"message":{{"message":{{"header":{{"kind":3,"timestamp":{ts}}},"l2Msg":"{b64}"}},"delayedMessagesRead":1}}}}]}}"#,
        b64 = base64::engine::general_purpose::STANDARD.encode(l2)
    )
}

/// The sequencer feed: one live connection at a time consumes the frames, in order. A frame whose
/// send failed (the connection dropped) is the first one the next connection gets.
async fn feed_server() -> (String, mpsc::UnboundedSender<String>, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let state = Arc::new(tokio::sync::Mutex::new((rx, None::<String>)));
    let task = tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let state = state.clone();
            tokio::spawn(async move {
                let _ = tcp.set_nodelay(true);
                let Ok(mut ws) = tokio_tungstenite::accept_async(tcp).await else { return };
                let mut st = state.lock().await;
                enum Event {
                    Frame(Option<String>),
                    Incoming(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
                }
                loop {
                    let event = match st.1.take() {
                        Some(unsent) => Event::Frame(Some(unsent)),
                        None => tokio::select! {
                            frame = st.0.recv() => Event::Frame(frame),
                            incoming = ws.next() => Event::Incoming(incoming),
                        },
                    };
                    match event {
                        Event::Frame(Some(f)) => {
                            if ws.send(Message::Text(f.clone().into())).await.is_err() {
                                st.1 = Some(f);
                                return;
                            }
                        }
                        Event::Frame(None) => return,
                        Event::Incoming(Some(Ok(Message::Ping(p)))) => {
                            let _ = ws.send(Message::Pong(p)).await;
                        }
                        Event::Incoming(Some(Ok(_))) => {}
                        Event::Incoming(_) => return,
                    }
                }
            });
        }
    });
    (url, tx, task)
}

// ════════════════════════════════════════════════════════════════════════════
// World = chain + node + feed
// ════════════════════════════════════════════════════════════════════════════

/// Tests that measure timing or run on a block schedule run alone (`World::alone`); the others
/// share the machine. Without this, a dozen engines and simulated networks compete for the same
/// cores and the measurement is of the scheduler, not of the engine.
static LANE: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

enum Lane {
    Shared { _guard: tokio::sync::RwLockReadGuard<'static, ()> },
    Alone { _guard: tokio::sync::RwLockWriteGuard<'static, ()> },
}

struct World {
    chain: Arc<Mutex<Chain>>,
    rpc_url: String,
    feed_url: String,
    feed_tx: mpsc::UnboundedSender<String>,
    next_trader: AtomicU64,
    tasks: Vec<JoinHandle<()>>,
    /// The node and the sequencer feed are other machines: they run on their own threads, not on
    /// the engine's runtime.
    net: Option<tokio::runtime::Runtime>,
    _lane: Lane,
}

impl Drop for World {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
        if let Some(net) = self.net.take() {
            net.shutdown_background();
        }
    }
}

impl World {
    async fn new() -> Self {
        Self::build(Lane::Shared { _guard: LANE.read().await }).await
    }

    /// A world for timing measurements and block schedules: no other test runs meanwhile.
    async fn alone() -> Self {
        Self::build(Lane::Alone { _guard: LANE.write().await }).await
    }

    async fn build(lane: Lane) -> Self {
        let net = tokio::runtime::Builder::new_multi_thread().worker_threads(2).thread_name("sim-network").enable_all().build().unwrap();
        let chain = Arc::new(Mutex::new(Chain { head: GENESIS - 1, ..Chain::default() }));
        let (rpc_url, rpc_task) = net.spawn(rpc_server(chain.clone())).await.unwrap();
        let (feed_url, feed_tx, feed_task) = net.spawn(feed_server()).await.unwrap();
        Self { chain, rpc_url, feed_url, feed_tx, next_trader: AtomicU64::new(1), tasks: vec![rpc_task, feed_task], net: Some(net), _lane: lane }
    }

    /// A market participant with a deterministic key (the n-th trader of this world).
    fn trader(&self) -> Trader {
        let n = self.next_trader.fetch_add(1, Ordering::Relaxed);
        let key = keccak256(format!("feature-simulation-trader-{n}"));
        Trader { signer: PrivateKeySigner::from_bytes(&key).unwrap(), nonce: AtomicU64::new(0), approved: Mutex::new(HashSet::new()) }
    }

    /// Seal the next block (mempool first, then `extra`) and broadcast it on the feed.
    fn seal(&self, extra: Vec<Vec<u8>>) -> u64 {
        seal_into(&self.chain, &self.feed_tx, extra)
    }

    /// Seal a block every `every` until the guard is dropped (a live chain).
    fn auto_seal(&self, every: Duration) -> AutoSeal {
        let stop = Arc::new(AtomicBool::new(false));
        let (chain, feed, flag) = (self.chain.clone(), self.feed_tx.clone(), stop.clone());
        let task = tokio::spawn(async move {
            while !flag.load(Ordering::SeqCst) {
                tokio::time::sleep(every).await;
                seal_into(&chain, &feed, Vec::new());
            }
        });
        AutoSeal { stop, task }
    }

    /// Later blocks carry timestamps at least `secs` after the last one (the snipe tax window of a
    /// launch sealed just before is then over).
    fn advance_clock(&self, secs: u64) {
        let mut c = self.chain.lock().unwrap();
        c.clock_floor = c.ts.max(unix_now()) + secs;
    }

    /// Timestamp of the next blocks (`None`: wall clock). Timestamps never go backwards.
    fn pin_block_time(&self, ts: Option<u64>) {
        self.chain.lock().unwrap().pinned_ts = ts;
    }

    fn last_block_time(&self) -> u64 {
        self.chain.lock().unwrap().ts
    }

    /// `eth_sendRawTransaction` answers `latency` after the request arrived.
    fn set_send_latency(&self, latency: Duration) {
        self.chain.lock().unwrap().send_latency = latency;
    }

    /// The node's RPC serves each block `lag` after it went out on the feed.
    fn set_rpc_lag(&self, lag: Duration) {
        self.chain.lock().unwrap().rpc_lag = lag;
    }

    fn enforce_calldata_floor(&self) {
        self.chain.lock().unwrap().calldata_floor = true;
    }

    /// Fee + creator tax of the curves launched from now on.
    fn set_launch_tax_bps(&self, bps: u64) {
        self.chain.lock().unwrap().next_launch_bps = bps;
    }

    fn fund(&self, who: Address, amount_eth: f64) {
        self.chain.lock().unwrap().credit(who, wei(amount_eth));
    }

    fn head(&self) -> u64 {
        self.chain.lock().unwrap().head
    }

    fn sent(&self) -> Vec<SentTx> {
        self.chain.lock().unwrap().sent.clone()
    }

    fn sent_from(&self, who: Address) -> Vec<SentTx> {
        self.sent().into_iter().filter(|s| s.from == who).collect()
    }

    fn receipt(&self, hash: B256) -> Option<Receipt> {
        self.chain.lock().unwrap().receipts.get(&hash).cloned()
    }

    fn curve(&self, curve: Address) -> Curve {
        self.chain.lock().unwrap().curves.get(&curve).cloned().expect("curve launched")
    }

    fn tokens(&self, token: Address, owner: Address) -> U256 {
        self.chain.lock().unwrap().token_balance(token, owner)
    }

    fn eth_of(&self, who: Address) -> U256 {
        self.chain.lock().unwrap().balance_of(who)
    }

    fn gas_paid(&self, who: Address) -> U256 {
        self.chain.lock().unwrap().gas_paid.get(&who).copied().unwrap_or_default()
    }

    /// Every RPC request since `since`: `(method, detail)`.
    fn rpc_since(&self, since: Instant) -> Vec<(String, String)> {
        self.chain.lock().unwrap().rpc.iter().filter(|(at, _, _)| *at >= since).map(|(_, m, d)| (m.clone(), d.clone())).collect()
    }

    fn served(&self, method: &str, detail_prefix: &str) -> bool {
        self.chain.lock().unwrap().rpc.iter().any(|(_, m, d)| m == method && d.starts_with(detail_prefix))
    }
}

fn seal_into(chain: &Mutex<Chain>, feed: &mpsc::UnboundedSender<String>, extra: Vec<Vec<u8>>) -> u64 {
    let (block, ts, l2) = chain.lock().unwrap().seal(extra);
    let _ = feed.send(feed_frame(block - GENESIS, ts, &l2));
    block
}

struct AutoSeal {
    stop: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl Drop for AutoSeal {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.task.abort();
    }
}

/// Requests in a window that read chain state for valuation: anything except block numbering
/// (the mirror's feed/node consistency check), fee data and transaction submission / receipts.
fn state_reads(traffic: &[(String, String)]) -> Vec<String> {
    let node_interface = format!("{:?}:", NODE_INTERFACE);
    traffic
        .iter()
        .filter(|(m, d)| {
            !matches!(
                m.as_str(),
                "eth_blockNumber"
                    | "eth_getBlockByNumber"
                    | "eth_gasPrice"
                    | "eth_maxPriorityFeePerGas"
                    | "eth_chainId"
                    | "eth_getTransactionCount"
                    | "eth_getTransactionReceipt"
                    | "eth_sendRawTransaction"
            ) && !(m == "eth_call" && d.starts_with(&node_interface))
        })
        .map(|(m, d)| format!("{m} {d}"))
        .collect()
}

fn histogram(traffic: &[(String, String)]) -> BTreeMap<String, usize> {
    let mut h = BTreeMap::new();
    for (m, _) in traffic {
        *h.entry(m.clone()).or_default() += 1;
    }
    h
}

// ════════════════════════════════════════════════════════════════════════════
// Other market participants
// ════════════════════════════════════════════════════════════════════════════

struct Trader {
    signer: PrivateKeySigner,
    nonce: AtomicU64,
    /// `(token, spender)` pairs already approved.
    approved: Mutex<HashSet<(Address, Address)>>,
}

impl Trader {
    fn address(&self) -> Address {
        self.signer.address()
    }

    fn tx(&self, to: Address, value: U256, input: Vec<u8>) -> Vec<u8> {
        let tx = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: self.nonce.fetch_add(1, Ordering::Relaxed),
            gas_limit: 400_000,
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

    fn launch(&self, dev_buy: U256, metadata: Vec<u8>) -> Vec<u8> {
        self.tx(LAUNCHPAD, dev_buy, metadata)
    }

    fn buy(&self, curve: Address, quote: U256, min_out: U256) -> Vec<u8> {
        self.tx(curve, quote, CalldataPayloadBuilder::build_pons_buy_payload(quote, min_out, self.address()))
    }

    /// A sell, preceded by the curve's approval the first time.
    fn sell(&self, token: Address, curve: Address, tokens: U256) -> Vec<Vec<u8>> {
        let mut txs = Vec::new();
        if self.approved.lock().unwrap().insert((token, curve)) {
            let mut approve = payload::ERC20_APPROVE_SELECTOR.to_vec();
            approve.extend_from_slice(&[0u8; 12]);
            approve.extend_from_slice(curve.as_slice());
            approve.extend_from_slice(&U256::MAX.to_be_bytes::<32>());
            txs.push(self.tx(token, U256::ZERO, approve));
        }
        txs.push(self.tx(curve, U256::ZERO, CalldataPayloadBuilder::build_pons_sell_payload(tokens, U256::ZERO, self.address())));
        txs
    }

    fn transfer(&self, to: Address, value: U256) -> Vec<u8> {
        self.tx(to, value, Vec::new())
    }
}

/// A distinct, deterministic hash for transactions that never hit the chain (scripted signals).
fn unique_hash() -> B256 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    keccak256(NEXT.fetch_add(1, Ordering::Relaxed).to_be_bytes())
}

fn hash_of(raw: &[u8]) -> B256 {
    *TxEnvelope::decode_2718_exact(raw).unwrap().tx_hash()
}

// ════════════════════════════════════════════════════════════════════════════
// The engine, wired as main.rs wires it
// ════════════════════════════════════════════════════════════════════════════

#[derive(Clone)]
struct Opts {
    carriers: usize,
    padding: bool,
    prysm_delay: u64,
    /// Dispatch signals (as main.rs does) or only record them.
    dispatch: bool,
    watch: Vec<Address>,
    trade_eth: f64,
    exit_mode: PrysmExitMode,
    presign_hold: Duration,
    /// Periodic exact re-verification of the mirror (production: 30 s). The RPC-counting windows
    /// run with it out of the way; the calibration test turns it up.
    mirror_verify: Duration,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            carriers: 2,
            padding: true,
            prysm_delay: 1,
            dispatch: true,
            watch: Vec::new(),
            trade_eth: 0.01,
            exit_mode: PrysmExitMode::System,
            presign_hold: Duration::from_secs(8),
            mirror_verify: Duration::from_secs(600),
        }
    }
}

struct Engine {
    config: AppConfig,
    #[allow(dead_code)]
    fee: Arc<DynamicFeeAdjuster>,
    dispatcher: Arc<Dispatcher>,
    risk: Arc<RiskManager>,
    prysm: Arc<PrysmEngine>,
    stream: Arc<StreamListener>,
    mirror: Arc<CurveMirror>,
    positions: Arc<RwLock<Vec<Position>>>,
    control: ControlPlane,
    signal_tx: mpsc::Sender<TradeSignal>,
    signals: Arc<Mutex<Vec<(Instant, TradeSignal)>>>,
    reports: Arc<Mutex<Vec<DispatchReport>>>,
    exits: Arc<Mutex<Vec<Position>>>,
    master: Address,
    /// The configured carriers at start.
    carriers: Vec<Address>,
    /// Signing keys of accounts 0 (master) ..= 5, for reconstructing staged packets.
    keys: Vec<PrivateKeySigner>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl Engine {
    async fn start(world: &World, opts: Opts) -> Engine {
        // Every value the tests depend on is set here (production defaults), so variables in the
        // shell that runs `cargo test` cannot change the outcome.
        let mut config = AppConfig::default();
        config.feed_url = world.feed_url.clone();
        config.feed_urls = vec![world.feed_url.clone()];
        config.feed_data_timeout_secs = 0;
        config.feed_genesis_block = GENESIS;
        config.rpc_http_url = world.rpc_url.clone();
        config.send_rpcs = vec![world.rpc_url.clone()];
        config.chain_id = CHAIN_ID;
        config.telegram_authorized_chat_id = 42;
        config.prysm_launchpads = Arc::new([LAUNCHPAD].into_iter().collect());
        config.watchlist_accounts = Arc::new(Watchlist::new(opts.watch.iter().copied().collect()));
        *config.mnemonic_phrase.write().await = MNEMONIC.to_string();
        config.prysm_enabled.store(true, Ordering::SeqCst);
        *config.prysm_min_block_delay.write().await = opts.prysm_delay;
        *config.prysm_size_eth.write().await = 0.01;
        *config.prysm_exit_mode.write().await = opts.exit_mode;
        *config.prysm_max_signal_age_secs.write().await = 15;
        *config.prysm_slippage_percent.write().await = 25.0;
        *config.prysm_target_stop_loss_percent.write().await = 30.0;
        *config.trailing_stop_percent.write().await = TRAIL;
        // USD thresholds at the default $2,600 reference, held fixed in ETH terms whatever
        // `ETH_USD_PRICE` says: production defaults $8,000 (entry ceiling) and $10,000 (take-profit).
        let usd = payload::eth_usd_reference() / payload::DEFAULT_ETH_USD_PRICE;
        *config.prysm_max_mcap_usd.write().await = 8_000.0 * usd;
        *config.prysm_target_mcap_usd.write().await = 10_000.0 * usd;
        *config.execution_mode.write().await = ExecutionMode::MultiAccount;
        *config.sub_account_count.write().await = opts.carriers;
        *config.gas_limit.write().await = payload::STATIC_GAS_LIMIT;
        *config.default_trade_eth.write().await = opts.trade_eth;
        *config.slippage_percent.write().await = 15.0;
        *config.target_profit_percent.write().await = 35.0;
        *config.time_decay_exit_seconds.write().await = 45;
        *config.min_sub_account_eth.write().await = 0.005;
        *config.rebalance_target_eth.write().await = 0.015;
        config.set_carrier_padding(opts.padding, TARGET_CALLDATA_BUFFER_SIZE).await;
        config.set_fee_preset(2.5, 10.0).await;

        let wallet = WalletManager::new(config.clone()).await.unwrap();
        let accounts = wallet.get_all_accounts().await.unwrap();
        let master = accounts[0].address;
        let carriers: Vec<Address> = accounts[1..].iter().map(|a| a.address).collect();
        // Master plus every carrier account the control plane may switch to (up to 5).
        let mut keys = Vec::new();
        for i in 0..=5 {
            let acc = wallet.derive_account(i).await.unwrap();
            if let Some(configured) = accounts.get(i) {
                assert_eq!(configured.address, acc.address);
            }
            world.fund(acc.address, if i == 0 { MASTER_FUNDING_ETH } else { CARRIER_FUNDING_ETH });
            keys.push(acc.signer.clone());
        }
        wallet.sync_initial_nonces().await.unwrap();

        let fee = Arc::new(DynamicFeeAdjuster::new(2.5, 1.25));
        fee.set_l1_component_gas(L1_GAS);
        let (report_tx, mut report_rx) = mpsc::channel::<DispatchReport>(256);
        let (signal_tx, mut signal_rx) = mpsc::channel::<TradeSignal>(1_000);
        let (exit_tx, mut exit_rx) = mpsc::channel::<Position>(256);
        let policy = PresignPolicy { max_hold: opts.presign_hold, ..PresignPolicy::default() };
        let dispatcher = Arc::new(Dispatcher::new(config.clone(), wallet.clone(), fee.clone(), report_tx).with_presign_policy(policy));
        let reader = Arc::new(BroadcasterPool::new(vec![world.rpc_url.clone()]));
        let mirror = Arc::new(CurveMirror::new(MirrorPolicy { verify_every: opts.mirror_verify, ..MirrorPolicy::default() }));
        let positions = Arc::new(RwLock::new(Vec::<Position>::new()));
        let risk = Arc::new(
            RiskManager::new(config.clone(), wallet.clone(), positions.clone(), exit_tx)
                .with_execution(dispatcher.broadcaster(), fee.clone())
                .with_reader(reader.clone())
                .with_curve_mirror(mirror.clone()),
        );
        let prysm = Arc::new(
            PrysmEngine::new(config.clone(), signal_tx.clone())
                .with_broadcaster(reader.clone())
                .with_presigner(dispatcher.clone())
                .with_curve_mirror(mirror.clone()),
        );
        prysm.set_master_address(master).await;
        let (pool_buy_tx, mut pool_buy_rx) = mpsc::channel(4_096);
        let stream = Arc::new(
            StreamListener::new(config.clone(), fee.clone(), prysm.clone(), signal_tx.clone())
                .with_pool_buy_sink(pool_buy_tx)
                .with_curve_mirror(mirror.clone()),
        );

        let mut tasks = vec![mirror.spawn_anchor_worker(reader.clone())];
        let risk_flow = risk.clone();
        tasks.push(tokio::spawn(async move {
            while let Some(buy) = pool_buy_rx.recv().await {
                risk_flow.on_pool_buy(buy.pool, buy.buyer, buy.amount_wei).await;
            }
        }));
        // main.rs Task A: drop signals that waited too long (and hand back their pre-signed
        // nonces), one task per signal, in flight until the position is registered.
        let signals = Arc::new(Mutex::new(Vec::new()));
        {
            let (signals, dispatcher, risk, dispatch) = (signals.clone(), dispatcher.clone(), risk.clone(), opts.dispatch);
            tasks.push(tokio::spawn(async move {
                while let Some(signal) = signal_rx.recv().await {
                    signals.lock().unwrap().push((Instant::now(), signal.clone()));
                    if !dispatch {
                        continue;
                    }
                    if signal.detected_at.elapsed() > MAX_SIGNAL_QUEUE_AGE {
                        dispatcher.cancel_presigned(signal.tx_hash);
                        continue;
                    }
                    let (d, r) = (dispatcher.clone(), risk.clone());
                    tokio::spawn(async move {
                        let _in_flight = d.entry_guard();
                        match d.dispatch_signal(&signal).await {
                            Ok(report) => {
                                let venue = ExitVenue::from_entry_calldata(&signal.swap_calldata);
                                r.register_entry(signal.target_token, signal.pool_or_router, report.trade_eth, signal.strategy_tag.clone(), venue)
                                    .await;
                            }
                            Err(e) => eprintln!("dispatch refused: {e:#}"),
                        }
                    });
                }
            }));
        }
        let reports = Arc::new(Mutex::new(Vec::new()));
        {
            let reports = reports.clone();
            tasks.push(tokio::spawn(async move {
                while let Some(r) = report_rx.recv().await {
                    reports.lock().unwrap().push(r);
                }
            }));
        }
        let exits = Arc::new(Mutex::new(Vec::new()));
        {
            let exits = exits.clone();
            tasks.push(tokio::spawn(async move {
                while let Some(p) = exit_rx.recv().await {
                    exits.lock().unwrap().push(p);
                }
            }));
        }
        {
            let risk = risk.clone();
            tasks.push(tokio::spawn(async move { risk.start_lifecycle_monitor().await }));
        }
        {
            let stream = stream.clone();
            tasks.push(tokio::spawn(async move {
                let _ = stream.run().await;
            }));
        }
        let control = ControlPlane::new(EngineHandles {
            config: config.clone(),
            wallet: wallet.clone(),
            risk: risk.clone(),
            prysm: prysm.clone(),
            positions: positions.clone(),
            dispatcher: Some(dispatcher.clone()),
            fee: Some(fee.clone()),
            stream: Some(stream.clone()),
            mirror: Some(mirror.clone()),
            engine: tokio::runtime::Handle::current(),
        });
        let engine = Engine {
            config,
            fee,
            dispatcher,
            risk,
            prysm,
            stream,
            mirror,
            positions,
            control,
            signal_tx,
            signals,
            reports,
            exits,
            master,
            carriers,
            keys,
            tasks,
        };
        until("the sequencer feed to connect", Duration::from_secs(10), || engine.stream.live_feed_count() == 1).await;
        engine
    }

    fn signals_for(&self, launch: B256) -> Vec<TradeSignal> {
        self.signals.lock().unwrap().iter().filter(|(_, s)| s.tx_hash == launch).map(|(_, s)| s.clone()).collect()
    }

    async fn open_positions(&self) -> Vec<Position> {
        self.positions.read().await.iter().filter(|p| p.is_open).cloned().collect()
    }

    async fn position(&self, id: &str) -> Position {
        self.positions.read().await.iter().find(|p| p.id == id).cloned().expect("position")
    }

    /// Block processed by the stream (the mirror sees every block).
    async fn processed(&self, block: u64) {
        until(&format!("block #{block} processed"), Duration::from_secs(10), || self.mirror.head() >= block).await;
    }

    /// Enter `curve` through the engine's own signal queue (what Prysm / the watchlist emit), seal
    /// the block that fills it and wait until the risk engine sees the tokens.
    async fn enter(&self, world: &World, token: Address, curve: Address, size_eth: f64, tag: Option<&str>) -> Position {
        let before = self.positions.read().await.len();
        let size = wei(size_eth);
        let signal = TradeSignal {
            tx_hash: unique_hash(),
            monitored_account: Address::ZERO,
            target_token: token,
            pool_or_router: curve,
            entry_amount_wei: size,
            swap_calldata: CalldataPayloadBuilder::build_pons_buy_payload(size, U256::from(1u64), Address::ZERO),
            detected_at: Instant::now(),
            block_number: world.head(),
            strategy_tag: tag.map(str::to_string),
        };
        self.signal_tx.send(signal).await.unwrap();
        until_async("the entry registered", Duration::from_secs(20), || async { self.positions.read().await.len() > before }).await;
        world.seal(Vec::new());
        let id = self.positions.read().await[before].id.clone();
        self.settled_entry(world, &id, token).await
    }

    /// Wait until the entry is filled and its one-off setup is done: `balanceOf` polling until the
    /// fill shows up, and the exit pre-approval (`allowance` read, then `approve` on chain). From
    /// then on the position is valued from the mirror only.
    async fn settled_entry(&self, world: &World, id: &str, token: Address) -> Position {
        until_async("the fill valued", Duration::from_secs(20), || async { self.position(id).await.pnl_percent != 0.0 }).await;
        let allowance = format!("{:?}:{}", token, alloy::hex::encode(payload::ERC20_ALLOWANCE_SELECTOR));
        until("the exit pre-approval read", Duration::from_secs(10), || world.served("eth_call", &allowance)).await;
        until("the exit pre-approval sent", Duration::from_secs(10), || {
            world.sent_from(self.master).iter().any(|s| s.accepted() && s.to == Some(token) && s.selector() == Some(payload::ERC20_APPROVE_SELECTOR))
        })
        .await;
        world.seal(Vec::new()); // the approval lands
        self.position(id).await
    }
}

async fn until(what: &str, within: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn until_async<F, Fut>(what: &str, within: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + within;
    while !cond().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Launch a token and let the snipe-tax window pass (later blocks are >= 3 s younger).
/// `(launch tx, token, curve)`.
fn launch_curve(world: &World, dev_buy_eth: f64, metadata: Vec<u8>) -> (B256, Address, Address) {
    let creator = world.trader();
    world.fund(creator.address(), 1.0);
    let raw = creator.launch(wei(dev_buy_eth), metadata);
    let hash = hash_of(&raw);
    world.seal(vec![raw]);
    world.advance_clock(3);
    let (token, curve) = launch_addresses(hash);
    (hash, token, curve)
}

/// In-memory value / chain value of a position's tokens.
async fn engine_vs_chain(world: &World, engine: &Engine, id: &str, token: Address, curve: Address) -> (f64, f64) {
    let p = engine.position(id).await;
    let held = world.tokens(token, engine.master);
    let truth = eth(world.curve(curve).value_of(held));
    (p.current_eth_value, truth)
}

// ════════════════════════════════════════════════════════════════════════════
// Launch timeline: L at second T, trigger L+1 one block later, L+2 one more block later
// ════════════════════════════════════════════════════════════════════════════

/// What the engine holds for a launch right before its trigger block: per slot, the transaction
/// spec, its leased nonce and whether its packet is already signed. Captured with two cheap reads
/// (nothing is signed here, so taking it does not delay the trigger block).
struct Staged {
    slots: Vec<(usize, Arc<TxSpec>, bool)>,
    nonces: HashMap<usize, u64>,
}

impl Staged {
    fn all_signed(&self) -> bool {
        self.slots.iter().all(|(_, _, signed)| *signed)
    }

    /// Hashes of the exact transactions the slots will send (spec + leased nonce, signed here
    /// with the account keys; signatures are deterministic, so these are the packets' hashes).
    /// They prove what was sent was fixed before the trigger; *when* it was signed rests on
    /// `all_signed` and the engine's counters.
    fn expected(&self, engine: &Engine) -> HashSet<B256> {
        self.slots
            .iter()
            .map(|(i, spec, _)| {
                let tx = spec.to_tx(self.nonces[i]);
                let sig = engine.keys[*i].sign_hash_sync(&tx.signature_hash()).unwrap();
                *TxEnvelope::Eip1559(tx.into_signed(sig)).tx_hash()
            })
            .collect()
    }
}

fn staged_now(engine: &Engine, launch: B256) -> Option<Staged> {
    let batch = engine.dispatcher.staged_batch(launch)?;
    Some(Staged { slots: batch.slot_specs(), nonces: batch.leased_nonces().into_iter().collect() })
}

struct Burst {
    txs: Vec<SentTx>,
    /// Trigger block sealed -> first complete packet at the node.
    trigger_to_first: Duration,
    /// First -> last complete packet at the node.
    spread: Duration,
    /// First -> last first byte at the node (when each send started arriving).
    start_spread: Duration,
    /// Last packet's first byte relative to the first complete packet, in ms (negative: every
    /// packet was on the wire before any of them was complete at the node).
    last_start_after_first_done_ms: f64,
}

fn spread(txs: &[SentTx], at: impl Fn(&SentTx) -> Instant) -> Duration {
    txs.iter().map(&at).max().unwrap() - txs.iter().map(&at).min().unwrap()
}

impl Burst {
    fn of(txs: Vec<SentTx>, trigger: Instant) -> Burst {
        let first_done = txs.iter().map(|s| s.at).min().unwrap();
        let last_start = txs.iter().map(|s| s.first_byte).max().unwrap();
        let last_start_after_first_done_ms = if last_start >= first_done { ms(last_start - first_done) } else { -ms(first_done - last_start) };
        Burst {
            trigger_to_first: first_done.saturating_duration_since(trigger),
            spread: spread(&txs, |s| s.at),
            start_spread: spread(&txs, |s| s.first_byte),
            last_start_after_first_done_ms,
            txs,
        }
    }
}

struct LaunchRun {
    launch: B256,
    token: Address,
    curve: Address,
    /// Launch block L, trigger block L+1, inclusion block L+2.
    blocks: (u64, u64, u64),
    staged_at_trigger: Option<Staged>,
    /// Everything the engine sent between L and L+2, as the node received it.
    burst: Burst,
}

/// Block L (the launch) at second T; after one block time the trigger block L+1 (still T); after
/// another the block L+2 at T + `land_offset_secs`. Nothing waits for the engine: it gets one block
/// time to stage, and what is not at the node when a block is sealed misses that block.
async fn launch_timeline(world: &World, engine: &Engine, dev_buy: f64, metadata: Vec<u8>, land_offset_secs: u64) -> LaunchRun {
    // A fresh second for T, aligned with the wall clock.
    let last = world.last_block_time();
    while unix_now() <= last {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let t = unix_now();
    world.pin_block_time(Some(t));
    world.set_rpc_lag(RPC_LAG);
    let creator = world.trader();
    world.fund(creator.address(), 1.0);
    let raw = creator.launch(wei(dev_buy), metadata);
    let launch = hash_of(&raw);
    let (token, curve) = launch_addresses(launch);
    let already = world.sent().len();
    let l = world.seal(vec![raw]);
    tokio::time::sleep(BLOCK_TIME).await;
    let staged_at_trigger = staged_now(engine, launch);
    let trigger = Instant::now();
    let l1 = world.seal(Vec::new());
    tokio::time::sleep(BLOCK_TIME).await;
    world.pin_block_time(Some(t + land_offset_secs));
    let l2 = world.seal(Vec::new());
    world.pin_block_time(None);
    world.set_rpc_lag(Duration::ZERO);
    // The swarm of this launch: buys on its curve and carriers' zero-filled self-calls. (Earlier
    // positions' approvals or exits may be sent in the same window; they are not part of it.)
    let txs: Vec<SentTx> = world.sent()[already..]
        .iter()
        .filter(|s| s.accepted() && (s.to == Some(curve) || (s.to == Some(s.from) && s.all_zero)))
        .cloned()
        .collect();
    assert!(!txs.is_empty(), "nothing was sent for the launch");
    LaunchRun { launch, token, curve, blocks: (l, l1, l2), staged_at_trigger, burst: Burst::of(txs, trigger) }
}

// ════════════════════════════════════════════════════════════════════════════
// 1. Seri Prysm: signal validation through the feed
// ════════════════════════════════════════════════════════════════════════════

/// Launches with the given dev buys, all in one block through the real feed; `(hash, dev buy,
/// expected match)` in, how many signals each produced out.
async fn run_launch_window(metadata: fn() -> Vec<u8>, samples: &[(f64, bool)], pattern: &str) {
    let world = World::new().await;
    let engine = Engine::start(&world, Opts { dispatch: false, ..Opts::default() }).await;
    let mut txs = Vec::new();
    let mut launches = Vec::new();
    for (dev_buy, want) in samples {
        let creator = world.trader();
        world.fund(creator.address(), 1.0);
        let raw = creator.launch(wei(*dev_buy), metadata());
        launches.push((hash_of(&raw), *dev_buy, *want));
        txs.push(raw);
    }
    let block = world.seal(txs);
    engine.processed(block).await;
    let expected = launches.iter().filter(|(_, _, w)| *w).count();
    until("every expected match to fire", Duration::from_secs(15), || {
        launches.iter().filter(|(h, _, _)| !engine.signals_for(*h).is_empty()).count() >= expected
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await; // nothing else may show up
    println!("{pattern}: dev buy -> verdict");
    for (hash, dev_buy, want) in &launches {
        let fired = engine.signals_for(*hash);
        println!("  {:>7.4} ETH -> {}", dev_buy, if fired.is_empty() { "REJECT" } else { "MATCH" });
        assert_eq!(fired.len(), usize::from(*want), "{pattern} dev buy {dev_buy} ETH: expected {}", if *want { "MATCH" } else { "REJECT" });
        if let Some(s) = fired.first() {
            assert!(s.strategy_tag.as_deref().unwrap_or_default().contains(pattern), "{:?}", s.strategy_tag);
            let (token, curve) = launch_addresses(*hash);
            assert_eq!((s.target_token, s.pool_or_router), (token, curve), "entry aims at the token / curve of the T_LAUNCH log");
            assert_eq!(s.entry_amount_wei, wei(0.01));
        }
    }
    let stats = engine.prysm.stats();
    let matched = match pattern {
        "Pola A" => stats.pattern_a_matches.load(Ordering::SeqCst),
        _ => stats.pattern_b_matches.load(Ordering::SeqCst),
    };
    assert_eq!(matched as usize, expected, "{pattern} match counter");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s1_pola_a_prysmcloud_image_matches_only_dev_buys_from_0_005_to_0_06_eth() {
    run_launch_window(
        pola_a_calldata,
        &[(0.004, false), (0.0049, false), (0.005, true), (0.021, true), (0.06, true), (0.0601, false), (0.1, false)],
        "Pola A",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s1_pola_b_lat_domain_matches_only_dev_buys_from_0_019_to_0_024_eth() {
    run_launch_window(
        pola_b_calldata,
        &[(0.018, false), (0.0189, false), (0.019, true), (0.021, true), (0.024, true), (0.0241, false), (0.03, false)],
        "Pola B",
    )
    .await;
}

/// The companion wallets' keys are not ours, so their buys cannot be signed into the feed: they
/// are handed to `PrysmEngine::inspect_transaction` as the stream does after the prefilter and
/// `ecrecover` (so the latency below leaves out decoding and signature recovery). The launches
/// themselves go through the feed and resolve from their receipts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s1_pola_c_companion_buy_fires_at_once_whatever_the_dev_buy_within_the_mcap_ceiling() {
    let world = World::alone().await;
    let engine = Engine::start(&world, Opts { dispatch: false, ..Opts::default() }).await;
    // Dev buys that Pola A / B reject (below and above both windows), market cap under the
    // $8,000 entry ceiling; and one launch above the ceiling.
    let launches: Vec<(f64, Vec<u8>)> =
        vec![(0.001, pola_a_calldata()), (0.1, pola_a_calldata()), (0.5, pola_b_calldata()), (2.0, pola_b_calldata())];
    let mut txs = Vec::new();
    let mut hashes = Vec::new();
    for (dev_buy, meta) in &launches {
        let creator = world.trader();
        world.fund(creator.address(), 5.0);
        let raw = creator.launch(wei(*dev_buy), meta.clone());
        hashes.push(hash_of(&raw));
        txs.push(raw);
    }
    let block = world.seal(txs);
    engine.processed(block).await;
    // Each launch resolves in the background (receipt, then `getReserves`), A/B verdict or not.
    // Read-only wait: the reserves read is the resolver's last step. (If a launch were not resolved
    // yet, its positive check below would fail: every case ends in an assertion that needs it.)
    for h in &hashes {
        let (_, curve) = launch_addresses(*h);
        until("the launch resolved", Duration::from_secs(10), || world.served("eth_call", &format!("{:?}:0902f1ac", curve))).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(hashes.iter().all(|h| engine.signals_for(*h).is_empty()), "A/B rejected every launch");

    let stranger = world.trader();
    for (i, h) in hashes.iter().enumerate() {
        let (token, curve) = launch_addresses(*h);
        let head = world.head();
        let companion = COMPANION_ACCOUNTS[i % 2];
        let buy = CalldataPayloadBuilder::build_pons_buy_payload(wei(0.05), U256::ZERO, companion);
        // Not a companion: nothing.
        engine.prysm.inspect_transaction(stranger.address(), Some(curve), wei(0.05), &buy, unique_hash(), head, Instant::now()).await;
        // A companion *sell*: never an entry.
        let sell = CalldataPayloadBuilder::build_pons_sell_payload(wei(1.0), U256::ZERO, companion);
        engine.prysm.inspect_transaction(companion, Some(curve), U256::ZERO, &sell, unique_hash(), head, Instant::now()).await;
        assert!(engine.signals_for(*h).is_empty());

        let ceiling_before = engine.prysm.stats().rejected_mcap_ceiling.load(Ordering::SeqCst);
        let t0 = Instant::now();
        engine.prysm.inspect_transaction(companion, Some(curve), wei(0.05), &buy, unique_hash(), head, t0).await;
        let (dev_buy, _) = &launches[i];
        let mcap_usd = payload::pons_mcap_eth(world.curve(curve).e, world.curve(curve).t).unwrap() * payload::DEFAULT_ETH_USD_PRICE;
        if mcap_usd < 8_000.0 {
            until("the Pola C entry", Duration::from_secs(5), || !engine.signals_for(*h).is_empty()).await;
            let (at, s) = engine.signals.lock().unwrap().iter().find(|(_, s)| s.tx_hash == *h).cloned().unwrap();
            println!("Pola C, launch with dev buy {dev_buy:.3} ETH (mcap ${mcap_usd:.0}): signal {:.3} ms after the companion buy", ms(at - t0));
            assert!(s.strategy_tag.as_deref().unwrap_or_default().contains("Pola C"), "{:?}", s.strategy_tag);
            assert_eq!((s.target_token, s.pool_or_router), (token, curve), "entry aims at the launch's token / curve");
            assert!(at - t0 < Duration::from_millis(if RELEASE { 5 } else { 100 }), "0-delay: {:?}", at - t0);
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            println!("Pola C, launch with dev buy {dev_buy:.3} ETH (mcap ${mcap_usd:.0}): refused by the ${:.0} entry ceiling", 8_000.0);
            assert!(engine.signals_for(*h).is_empty(), "above the market-cap ceiling: no entry");
            assert_eq!(engine.prysm.stats().rejected_mcap_ceiling.load(Ordering::SeqCst), ceiling_before + 1);
        }
    }
    // An unknown curve (no launch seen): nothing.
    let unknown = Address::repeat_byte(0x77);
    let buy = CalldataPayloadBuilder::build_pons_buy_payload(wei(0.05), U256::ZERO, COMPANION_ACCOUNTS[0]);
    let before = engine.signals.lock().unwrap().len();
    engine.prysm.inspect_transaction(COMPANION_ACCOUNTS[0], Some(unknown), wei(0.05), &buy, unique_hash(), world.head(), Instant::now()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(engine.signals.lock().unwrap().len(), before);
    assert_eq!(engine.signals.lock().unwrap().len(), 3, "three Pola C entries");
}

// ════════════════════════════════════════════════════════════════════════════
// 2. Block booking (95 KB) and the AOT pre-signed swarm
// ════════════════════════════════════════════════════════════════════════════

/// Shape of a swarm at the node and on chain: one padded master buy on the curve, carriers'
/// zero-filled self-calls, all in one block, carriers holding nothing.
fn assert_swarm(world: &World, engine: &Engine, run: &LaunchRun, carriers: &[Address], master_value: f64) {
    let (token, curve) = (run.token, run.curve);
    let txs = &run.burst.txs;
    assert_eq!(txs.len(), 1 + carriers.len(), "master + {} carriers", carriers.len());
    let master: Vec<&SentTx> = txs.iter().filter(|s| s.from == engine.master).collect();
    assert_eq!(master.len(), 1);
    let m = master[0];
    assert_eq!(m.to, Some(curve), "the master buys on the curve");
    assert_eq!(m.value, wei(master_value));
    assert_eq!(m.selector(), Some(payload::PONS_BUY_SELECTOR));
    assert_eq!(arg_address(&m.input_head, 2), Some(engine.master), "tokens to the master");
    assert!((94_000..=payload::MAX_SEQUENCER_TX_BYTES).contains(&m.input_len), "master calldata {} B", m.input_len);
    assert!(m.encoded_len <= MAX_TX_BYTES, "within the sequencer's size limit");
    for c in txs.iter().filter(|s| s.from != engine.master) {
        assert!(carriers.contains(&c.from));
        assert_eq!(c.to, Some(c.from), "a carrier calls itself (an EOA: no contract, nothing to revert)");
        assert_eq!(c.value, U256::ZERO);
        assert!(c.all_zero && c.input_len >= 94_000 && c.encoded_len <= MAX_TX_BYTES, "carrier calldata {} B of zeros", c.input_len);
        assert_eq!(world.tokens(token, c.from), U256::ZERO, "carriers never hold the token");
    }
    let landed: HashSet<u64> = txs.iter().map(|s| world.receipt(s.hash).expect("included").block).collect();
    assert_eq!(landed, HashSet::from([run.blocks.2]), "the whole swarm landed in block L+2");
    assert!(txs.iter().all(|s| world.receipt(s.hash).unwrap().ok), "every packet executed (status 1)");
    let block_txs = world.chain.lock().unwrap().blocks[&run.blocks.2].clone();
    let to_curve = world.sent().into_iter().filter(|s| block_txs.contains(&s.hash) && s.to == Some(curve)).count();
    assert_eq!(to_curve, 1, "nothing but the master's buy touched the curve");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s2_block_booking_swarm_fires_prebuilt_95kb_packets_in_one_burst() {
    let world = World::alone().await;
    world.set_send_latency(SEND_LATENCY);
    let engine = Engine::start(&world, Opts { carriers: 3, prysm_delay: 2, ..Opts::default() }).await;
    let carrier_eth: Vec<U256> = engine.carriers.iter().map(|c| world.eth_of(*c)).collect();

    // One cold launch (first sends to the node), then five warm ones (pooled connections). Each
    // lands at T+1: 618 bps snipe tax, inside the Revert Guard.
    let mut runs = Vec::new();
    for (i, dev_buy) in [0.021, 0.025, 0.03, 0.035, 0.04, 0.045].into_iter().enumerate() {
        let run = launch_timeline(&world, &engine, dev_buy, pola_a_calldata(), 1).await;
        runs.push((if i == 0 { "cold" } else { "warm" }, run));
    }

    for (name, run) in &runs {
        let b = &run.burst;
        println!(
            "{name} burst: {} txs, trigger block -> first complete tx at node {:.3} ms; spread first -> last: start {:.3} ms, complete {:.3} ms; last start vs first complete {:+.3} ms",
            b.txs.len(),
            ms(b.trigger_to_first),
            ms(b.start_spread),
            ms(b.spread),
            b.last_start_after_first_done_ms
        );
        let staged = run.staged_at_trigger.as_ref().expect("a batch was staged before the trigger block");
        assert!(staged.all_signed() && staged.slots.len() == 4, "all 4 packets signed when the trigger block arrived");
        let sent: HashSet<B256> = b.txs.iter().map(|s| s.hash).collect();
        assert_eq!(sent, staged.expected(&engine), "the packets sent are exactly the ones staged before the trigger");
        assert!(b.txs.iter().all(|s| s.head_at_arrival == run.blocks.1), "fired on the trigger block L+1, in before L+2 was sealed");
        assert!(
            b.last_start_after_first_done_ms < ms(SEND_LATENCY),
            "every packet left before any answer could have come back (no serial send): {:+.3} ms",
            b.last_start_after_first_done_ms
        );
        assert_swarm(&world, &engine, run, &engine.carriers, 0.01);
        // The fill paid the 618 bps snipe tax on top of the 3% (L+2 is one second after L).
        let r = world.receipt(b.txs.iter().find(|s| s.from == engine.master).unwrap().hash).unwrap();
        let before = world.curve(run.curve).history[&run.blocks.1];
        let net = wei(0.01) - wei(0.01) * U256::from(TRADE_BPS + 618) / U256::from(10_000u64);
        assert_eq!(r.tokens_out, before.1 * net / (before.0 + net));
    }
    for (c, before) in engine.carriers.iter().zip(carrier_eth) {
        let paid = world.gas_paid(*c);
        assert_eq!(world.eth_of(*c), before - paid, "carriers spend gas only");
        println!("carrier {:?}: {} packets, gas {:.9} ETH", c, runs.len(), eth(paid));
    }
    let p = engine.dispatcher.presign_snapshot();
    println!("pre-sign: {:?}", p);
    assert_eq!(p.prebuilt_sent, 24, "every packet of every burst was signed before its trigger block");
    assert_eq!((p.signed_inline_fresh, p.signed_inline_leased, p.resigned_stale), (0, 0, 0), "zero signatures on the hot path");
    // Spread of the warm bursts at the node: "start" = when each packet began arriving (the
    // engine letting go of it), "complete" = when the whole ~190 KB request was in. The engine and
    // the simulated node share the same cores here, so single bursts carry scheduler noise.
    let stats = |pick: fn(&Burst) -> Duration| {
        let mut v: Vec<Duration> = runs.iter().filter(|(n, _)| *n == "warm").map(|(_, r)| pick(&r.burst)).collect();
        v.sort();
        (v[0], v[v.len() / 2], v[v.len() - 1])
    };
    let (start, complete) = (stats(|b| b.start_spread), stats(|b| b.spread));
    for (what, (best, median, worst)) in [("start", start), ("complete", complete)] {
        println!(
            "warm burst spread ({what}) over 5 bursts: best {:.3} ms, median {:.3} ms, worst {:.3} ms ({})",
            ms(best),
            ms(median),
            ms(worst),
            if RELEASE { "release" } else { "debug" }
        );
    }
    let (best_limit, median_limit, worst_limit) = if RELEASE {
        (Duration::from_millis(1), Duration::from_millis(2), Duration::from_millis(10))
    } else {
        (Duration::from_millis(10), Duration::from_millis(25), Duration::from_millis(100))
    };
    for (what, (best, median, worst)) in [("start", start), ("complete", complete)] {
        assert!(best < best_limit && median < median_limit && worst < worst_limit, "{what} spread {best:?} / {median:?} / {worst:?}");
    }
}

/// The Revert Guard: an entry that lands in the launch's own second pays the 99% snipe tax, so its
/// `minTokensOut` (quoted for the 6.18% second) makes it revert. The master keeps the ETH (gas
/// aside); the position is closed as never filled, with nothing to sell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s2_revert_guard_refuses_a_fill_in_the_launch_second() {
    let world = World::alone().await;
    let engine = Engine::start(&world, Opts { carriers: 2, prysm_delay: 2, ..Opts::default() }).await;
    let eth0 = world.eth_of(engine.master);
    let run = launch_timeline(&world, &engine, 0.021, pola_a_calldata(), 0).await;
    let m = run.burst.txs.iter().find(|s| s.from == engine.master).expect("the master's buy").clone();
    let r = world.receipt(m.hash).expect("included");
    let (e, t) = world.curve(run.curve).history[&run.blocks.1];
    let quote = |bps: u64| {
        let net = wei(0.01) - wei(0.01) * U256::from(bps) / U256::from(10_000u64);
        t * net / (e + net)
    };
    let (quote_99, quote_618) = (quote(MAX_TOTAL_BUY_TAX_BPS), quote(TRADE_BPS + 618));
    let min_out = arg(&m.input_head, 1).unwrap();
    println!(
        "landed in the launch second: status {}; minTokensOut {:.0} vs {:.0} tokens a 99%-taxed fill delivers ({:.0} at 6.18%); master paid gas {:.9} ETH",
        u8::from(r.ok),
        eth(min_out),
        eth(quote_99),
        eth(quote_618),
        eth(r.fee)
    );
    assert_eq!(r.block, run.blocks.2);
    assert!(!r.ok, "reverted on the Revert Guard");
    assert!(quote_99 > U256::ZERO && min_out > quote_99, "the guard is above what a 99%-taxed fill delivers");
    assert!(min_out <= quote_618, "and below what the 6.18% second delivers (that fill is allowed)");
    // Control: in the same second a buy without a guard does fill (at 99% tax).
    let naive = world.trader();
    world.fund(naive.address(), 1.0);
    let naive_buy = naive.buy(run.curve, wei(0.01), U256::from(1u64));
    world.pin_block_time(Some(world.last_block_time()));
    world.seal(vec![naive_buy.clone()]);
    world.pin_block_time(None);
    let naive_r = world.receipt(hash_of(&naive_buy)).unwrap();
    println!("control, same second, minTokensOut 1: status {}, {:.0} tokens for 0.01 ETH", u8::from(naive_r.ok), eth(naive_r.tokens_out));
    assert!(naive_r.ok && naive_r.tokens_out > U256::ZERO && naive_r.tokens_out < min_out, "an unguarded buy fills at the 99% tax");
    assert_eq!(world.tokens(run.token, engine.master), U256::ZERO);
    assert_eq!(world.eth_of(engine.master), eth0 - world.gas_paid(engine.master), "only gas left the master");
    let _chain = world.auto_seal(Duration::from_millis(250));
    until_async("the entry closed as not filled", Duration::from_secs(40), || async {
        engine.positions.read().await.first().is_some_and(|p| !p.is_open)
    })
    .await;
    let p = engine.positions.read().await[0].clone();
    println!("position: {:?}", p.exit_reason);
    assert!(p.exit_reason.as_deref().unwrap_or_default().contains("not filled"), "{:?}", p.exit_reason);
    assert!(!world.sent_from(engine.master).iter().any(|s| s.selector() == Some(payload::PONS_SELL_SELECTOR)), "nothing to sell");
}

/// The default profile (`PRYSM_MIN_BLOCK_DELAY=1`): the order is due as soon as the launch
/// resolves, so only the carriers can be signed ahead; the primary is signed at dispatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s2_default_block_plus_one_profile_prebuilds_the_carriers() {
    let world = World::alone().await;
    let engine = Engine::start(&world, Opts { carriers: 2, prysm_delay: 1, ..Opts::default() }).await;
    // The entry fires once the receipt is served (RPC_LAG after L) and lands in L+1, sealed one
    // block time after L at T+1.
    let last = world.last_block_time();
    while unix_now() <= last {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let t = unix_now();
    world.pin_block_time(Some(t));
    world.set_rpc_lag(RPC_LAG);
    let creator = world.trader();
    world.fund(creator.address(), 1.0);
    let raw = creator.launch(wei(0.021), pola_a_calldata());
    let (token, curve) = launch_addresses(hash_of(&raw));
    let l = world.seal(vec![raw]);
    tokio::time::sleep(BLOCK_TIME).await;
    world.pin_block_time(Some(t + 1));
    let l1 = world.seal(Vec::new());
    world.pin_block_time(None);
    world.set_rpc_lag(Duration::ZERO);
    let txs: Vec<SentTx> = world.sent().into_iter().filter(|s| s.accepted()).collect();
    println!(
        "default profile: {} packets, spread first -> last: start {:.3} ms, complete {:.3} ms",
        txs.len(),
        ms(spread(&txs, |s| s.first_byte)),
        ms(spread(&txs, |s| s.at))
    );
    assert_eq!(txs.len(), 3);
    assert!(txs.iter().all(|s| s.head_at_arrival == l), "fired while block L was the head");
    assert!(txs.iter().all(|s| world.receipt(s.hash).is_some_and(|r| r.ok && r.block == l1)), "all in L+1");
    assert_eq!(txs.iter().filter(|s| s.to == Some(curve)).count(), 1);
    assert!(world.tokens(token, engine.master) > U256::ZERO);
    let p = engine.dispatcher.presign_snapshot();
    println!("default profile pre-sign: {:?}", p);
    assert_eq!(p.prebuilt_sent, 2, "both carriers pre-signed");
    assert_eq!(p.signed_inline_fresh + p.signed_inline_leased, 1, "the primary signed at dispatch");
}

/// Variant: a node that enforces the EIP-7623 calldata floor on L2 gas (whether Arbitrum does is
/// not established). 94.5 KB of calldata then needs ~1M gas; the engine sizes every padded packet
/// for that floor up front, so all of them are accepted on the first attempt and still go out
/// pre-signed, in one burst.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s2_variant_enforced_eip7623_floor_is_cleared_on_the_first_attempt() {
    let world = World::alone().await;
    world.enforce_calldata_floor();
    world.set_send_latency(SEND_LATENCY);
    let engine = Engine::start(&world, Opts { carriers: 2, prysm_delay: 2, ..Opts::default() }).await;
    let already = world.sent().len();
    let run = launch_timeline(&world, &engine, 0.021, pola_a_calldata(), 1).await;
    let all: Vec<SentTx> = world.sent()[already..].iter().filter(|s| s.to == Some(run.curve) || (s.to == Some(s.from) && s.all_zero)).cloned().collect();
    let refused: Vec<&SentTx> = all.iter().filter(|s| !s.accepted()).collect();
    for (i, tx) in world.sent().iter().enumerate() {
        println!("SENT[{}]: from={:?}, to={:?}, input_len={}, gas={}, acc={}, ref={:?}", i, tx.from, tx.to, tx.input_len, tx.gas_limit, tx.accepted(), tx.refused);
    }
    let b = &run.burst;
    let p = engine.dispatcher.presign_snapshot();
    println!(
        "floor enforced: {} refused, {} accepted on the first attempt with gas limits {:?} (floor for 94.5 KB: {}); at the node {:.3} ms after the trigger, spread {:.3} ms",
        refused.len(),
        b.txs.len(),
        b.txs.iter().map(|s| s.gas_limit).collect::<Vec<_>>(),
        floor_gas(&vec![0u8; payload::TARGET_CALLDATA_BUFFER_SIZE]),
        ms(b.trigger_to_first),
        ms(b.spread)
    );
    println!("pre-sign: {:?}", p);
    assert!(refused.is_empty(), "no packet refused for the floor: {:?}", refused.iter().map(|s| &s.refused).collect::<Vec<_>>());
    assert_eq!(all.len(), 3, "one attempt per account: no retry");
    // The node accepted each one, i.e. its limit covers max(intrinsic, floor) + L1 (checked there).
    let staged = run.staged_at_trigger.as_ref().expect("staged before the trigger");
    assert!(staged.all_signed(), "pre-signed before the trigger block");
    assert_eq!(b.txs.iter().map(|s| s.hash).collect::<HashSet<_>>(), staged.expected(&engine), "the staged packets, unchanged");
    assert_eq!((p.prebuilt_sent, p.signed_inline_fresh + p.signed_inline_leased, p.resigned_stale), (3, 0, 0), "no signature on the hot path");
    assert!(b.last_start_after_first_done_ms < ms(SEND_LATENCY), "one burst: no serial send");
    assert_swarm(&world, &engine, &run, &engine.carriers, 0.01);
}

// ════════════════════════════════════════════════════════════════════════════
// 3. Watchlist copy-trade: retargeting and rescaling
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_watched_wallet_curve_buy_is_copied_to_the_master_at_our_budget_and_transfers_are_ignored() {
    let world = World::alone().await;
    let target = world.trader();
    let stranger = world.trader();
    world.fund(target.address(), 5.0);
    world.fund(stranger.address(), 5.0);
    let engine = Engine::start(&world, Opts { watch: vec![target.address()], trade_eth: 0.005, ..Opts::default() }).await;
    // A curve past its launch window (copy-trading is not a launch snipe).
    let (_, token, curve) = launch_curve(&world, 0.3, plain_launch_calldata());
    world.seal(Vec::new());

    let their_min = wei(1_000_000.0); // 1e6 tokens (18 decimals)
    let copy = target.buy(curve, wei(0.2), their_min);
    let ignored_transfer = target.transfer(Address::repeat_byte(0x99), wei(1.0));
    let unwatched = stranger.buy(curve, wei(0.1), U256::ZERO);
    let t0 = Instant::now();
    let block = world.seal(vec![copy.clone(), ignored_transfer, unwatched]);
    // The next block comes one block time later, whatever the engine does.
    tokio::time::sleep(BLOCK_TIME).await;
    let next = world.seal(Vec::new());
    tokio::time::sleep(Duration::from_millis(300)).await;

    let signals: Vec<TradeSignal> = engine.signals.lock().unwrap().iter().map(|(_, s)| s.clone()).collect();
    assert_eq!(signals.len(), 1, "only the watched wallet's curve buy is copied: {signals:#?}");
    assert_eq!(signals[0].tx_hash, hash_of(&copy));
    assert_eq!(signals[0].monitored_account, target.address());
    let ours: Vec<SentTx> = world.sent_from(engine.master).into_iter().filter(|s| s.to == Some(curve)).collect();
    assert_eq!(ours.len(), 1);
    let m = &ours[0];
    println!("copy-trade: target's block -> our tx at the node {:.3} ms (next block sealed {:.0} ms after it)", ms(m.at - t0), ms(BLOCK_TIME));
    assert!(m.accepted());
    assert_eq!(m.head_at_arrival, block, "in before the next block");
    assert_eq!(m.value, wei(0.005), "our budget, not their 0.2 ETH");
    assert_eq!(m.selector(), Some(payload::PONS_BUY_SELECTOR));
    assert_eq!(arg(&m.input_head, 0), Some(wei(0.005)), "ethIn rewritten to our size");
    assert_eq!(arg(&m.input_head, 1), Some(their_min * wei(0.005) / wei(0.2)), "minTokensOut scaled by 0.005/0.2");
    assert_eq!(arg_address(&m.input_head, 2), Some(engine.master), "recipient retargeted to the master");
    let r = world.receipt(m.hash).expect("included");
    assert!(r.ok && r.block == next, "executed in the block right after the target's");
    assert_eq!(world.tokens(token, engine.master), r.tokens_out, "the tokens went to the master");
}

// ════════════════════════════════════════════════════════════════════════════
// 4. In-memory curve mirror and the risk loop
// ════════════════════════════════════════════════════════════════════════════

/// The risk engine's view of a position after one block.
#[derive(Clone, Debug)]
struct Obs {
    block: u64,
    pnl: f64,
    /// Fall of the position's value from its highest valuation so far, in percent.
    drawdown: f64,
    /// Curve progress to graduation (`raised / 4.2 ETH`), in percent, from the chain.
    capacity: f64,
    /// An exit had been decided (claimed, sent or settled) when this valuation was read.
    exiting: bool,
}

/// Drives one position block by block: every step seals a block, waits until the risk engine
/// has valued the position at exactly the chain's new state, and records what it decided.
struct Track<'a> {
    world: &'a World,
    engine: &'a Engine,
    id: String,
    token: Address,
    curve: Address,
    peak: f64,
    obs: Vec<Obs>,
}

impl<'a> Track<'a> {
    /// Starts from the engine's valuation of the current chain state (the first high-water mark).
    async fn new(world: &'a World, engine: &'a Engine, pos: &Position, token: Address, curve: Address) -> Track<'a> {
        let truth = eth(world.curve(curve).value_of(world.tokens(token, engine.master)));
        until_async("the valuation of the current state", Duration::from_secs(10), || async {
            (engine.position(&pos.id).await.current_eth_value / truth - 1.0).abs() < 1e-6
        })
        .await;
        let peak = engine.position(&pos.id).await.current_eth_value;
        Track { world, engine, id: pos.id.clone(), token, curve, peak, obs: Vec::new() }
    }

    fn exiting(&self) -> bool {
        self.engine.risk.exits_in_flight() > 0
            || self
                .world
                .sent_from(self.engine.master)
                .iter()
                .any(|s| s.accepted() && s.to == Some(self.curve) && s.selector() == Some(payload::PONS_SELL_SELECTOR))
    }

    async fn step(&mut self, txs: Vec<Vec<u8>>) -> Obs {
        let block = self.world.seal(txs);
        self.engine.processed(block).await;
        let held = self.world.tokens(self.token, self.engine.master);
        let truth = eth(self.world.curve(self.curve).value_of(held));
        // Valued at this block (the loop wakes on the mirror change), unless already exiting.
        until_async("the valuation of the new block", Duration::from_secs(10), || async {
            let p = self.engine.position(&self.id).await;
            !p.is_open || self.exiting() || (p.current_eth_value / truth - 1.0).abs() < 1e-6
        })
        .await;
        // A decision taken on that valuation spawns its liquidation task right after the pass.
        tokio::time::sleep(Duration::from_millis(if RELEASE { 30 } else { 150 })).await;
        let p = self.engine.position(&self.id).await;
        let exiting = !p.is_open || self.exiting();
        if !exiting {
            self.peak = self.peak.max(p.current_eth_value);
        }
        let obs = Obs {
            block,
            pnl: p.pnl_percent,
            drawdown: (1.0 - p.current_eth_value / self.peak) * 100.0,
            capacity: self.world.curve(self.curve).raised_eth() / 4.2 * 100.0,
            exiting,
        };
        self.obs.push(obs.clone());
        obs
    }

    /// Step until `stop` holds (or an exit is decided); panics after `max` blocks.
    async fn until(&mut self, max: usize, tx: impl Fn() -> Vec<Vec<u8>>, stop: impl Fn(&Obs) -> bool) -> Obs {
        for _ in 0..max {
            let o = self.step(tx()).await;
            if o.exiting || stop(&o) {
                return o;
            }
        }
        panic!("not reached within {max} blocks: {:?}", self.path());
    }

    /// Observations while held, and the first one with the exit decided.
    fn split(&self) -> (Vec<Obs>, Obs) {
        let i = self.obs.iter().position(|o| o.exiting).expect("an exit was decided");
        (self.obs[..i].to_vec(), self.obs[i].clone())
    }

    fn path(&self) -> Vec<(f64, f64)> {
        self.obs.iter().map(|o| ((o.pnl * 10.0).round() / 10.0, (o.drawdown * 10.0).round() / 10.0)).collect()
    }

    /// Keep the chain going until the exit settles; the closed position.
    async fn settle(&self) -> Position {
        let _chain = self.world.auto_seal(Duration::from_millis(60));
        until_async("the exit settled", Duration::from_secs(30), || async { !self.engine.position(&self.id).await.is_open }).await;
        self.engine.position(&self.id).await
    }
}

fn whale_sell_part(world: &World, whale: &Trader, token: Address, curve: Address, div: u64) -> Vec<Vec<u8>> {
    let held = world.tokens(token, whale.address());
    if held.is_zero() {
        return Vec::new();
    }
    whale.sell(token, curve, (held / U256::from(div)).max(U256::from(1u64)))
}

/// The sell that closed `closed`: settled on chain, nothing left on the master.
fn assert_exit_settled(world: &World, engine: &Engine, closed: &Position, token: Address) -> Receipt {
    let sell: B256 = closed.exit_tx_hash.as_deref().expect("exit tx").parse().unwrap();
    let r = world.receipt(sell).expect("exit sell included");
    assert!(r.ok, "the exit sell succeeded on chain");
    assert_eq!(world.tokens(token, engine.master), U256::ZERO, "everything sold");
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s4_in_memory_valuation_without_rpc_reads_and_the_trailing_stop() {
    let world = World::new().await;
    let whale = world.trader();
    world.fund(whale.address(), 20.0);
    let engine = Engine::start(&world, Opts::default()).await;
    let (_, token, curve) = launch_curve(&world, 0.3, plain_launch_calldata());
    world.seal(Vec::new());
    let pos = engine.enter(&world, token, curve, 0.01, None).await;
    until("the mirror vouches for the curve", Duration::from_secs(10), || matches!(engine.mirror.read(&curve), MirrorRead::Fresh(_))).await;
    let mut track = Track::new(&world, &engine, &pos, token, curve).await;

    // ── Pump to +40%: every valuation from memory, no state read over RPC.
    let window = Instant::now();
    let fresh0 = engine.mirror.stats().reads_fresh;
    let top = track.until(80, || vec![whale.buy(curve, wei(0.03), U256::ZERO)], |o| o.pnl >= 40.0).await;
    let traffic = world.rpc_since(window);
    let reads = state_reads(&traffic);
    let (engine_value, chain_value) = engine_vs_chain(&world, &engine, &track.id, token, curve).await;
    println!(
        "pump: {} blocks, PnL {:.1}% -> {:.1}%, {} in-memory valuations; RPC traffic in the window {:?}; value engine {:.9} vs chain {:.9} ETH",
        track.obs.len(),
        track.obs[0].pnl,
        top.pnl,
        engine.mirror.stats().reads_fresh - fresh0,
        histogram(&traffic),
        engine_value,
        chain_value
    );
    assert!(!top.exiting);
    assert!(reads.is_empty(), "zero-RPC risk loop, but read: {reads:?}");
    assert!((engine_value / chain_value - 1.0).abs() < 1e-6, "the in-memory value is the chain's: {engine_value} vs {chain_value}");

    // Cost of the in-memory part of one valuation: mirror read + position value + graduation
    // capacity (the loop's locking and bookkeeping around it are not included).
    let held = world.tokens(token, engine.master);
    let mut samples = Vec::with_capacity(200);
    for _ in 0..200 {
        let t = Instant::now();
        for _ in 0..50 {
            let snap = engine.mirror.read(std::hint::black_box(&curve)).fresh().unwrap();
            std::hint::black_box(snap.value_of(held));
            std::hint::black_box(payload::pons_curve_capacity_percent(snap.eth_reserve));
        }
        samples.push(t.elapsed() / 50);
    }
    samples.sort();
    let median = samples[samples.len() / 2];
    println!("mirror read + valuation + graduation capacity: median {:?} per position ({})", median, if RELEASE { "release" } else { "debug" });
    assert!(median < if RELEASE { Duration::from_micros(1) } else { Duration::from_micros(50) }, "{median:?}");

    // ── Pull back: held inside the trailing distance, exit decided on the first valuation past it.
    let start = track.obs.len();
    track.until(60, || whale_sell_part(&world, &whale, token, curve, 12), |_| false).await;
    let (held_obs, trigger) = track.split();
    println!("pull-back (PnL %, drawdown %): {:?}", &track.path()[start..]);
    assert!(held_obs.iter().all(|o| o.drawdown < TRAIL), "no exit inside the {TRAIL}% distance");
    assert!(held_obs[start..].iter().any(|o| o.drawdown > 6.0), "held through a pull-back deeper than 6%");
    assert!(trigger.drawdown >= TRAIL, "the exit came on crossing the distance: {trigger:?}");
    let closed = track.settle().await;
    // Decided on feed-predicted state, the exit was first re-checked with an exact reading pinned
    // to the trigger block.
    let pinned = format!("{:?}:0902f1ac@0x{:x}", curve, trigger.block);
    println!("exit decided at block #{} (drawdown {:.1}%); confirmation read pinned to that block", trigger.block, trigger.drawdown);
    assert!(world.served("eth_call", &pinned), "no confirmation read pinned to block #{}", trigger.block);
    println!("exit: {:?} at PnL {:.1}%", closed.exit_reason, closed.pnl_percent);
    assert!(closed.exit_reason.as_deref().unwrap_or_default().contains("High-Watermark Trailing"), "{:?}", closed.exit_reason);
    assert_exit_settled(&world, &engine, &closed, token);
}

/// A curve whose creator tax is not the default 2%: the mirror starts on its 3% model, the first
/// exact reading after trades shows the drift and calibrates the fee, and from then on the
/// in-memory value is the chain's again, still without per-block reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s4_mirror_calibrates_a_non_default_creator_tax() {
    let world = World::new().await;
    world.set_launch_tax_bps(600); // 1% base fee + 5% creator tax
    let whale = world.trader();
    world.fund(whale.address(), 20.0);
    let verify = Duration::from_secs(1);
    let engine = Engine::start(&world, Opts { mirror_verify: verify, ..Opts::default() }).await;
    let (_, token, curve) = launch_curve(&world, 0.3, plain_launch_calldata());
    world.seal(Vec::new());
    let pos = engine.enter(&world, token, curve, 0.01, None).await;

    // Busy curve: a whale buy every ~150 ms until the mirror has calibrated.
    let mut worst_before = 0.0f64;
    let mut blocks = 0;
    while engine.mirror.stats().calibrations == 0 {
        assert!(blocks < 100, "no calibration after {blocks} blocks: {:?}", engine.mirror.stats());
        let block = world.seal(vec![whale.buy(curve, wei(0.02), U256::ZERO)]);
        engine.processed(block).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let truth = eth(world.curve(curve).value_of(world.tokens(token, engine.master)));
        worst_before = worst_before.max((engine.position(&pos.id).await.current_eth_value / truth - 1.0).abs());
        blocks += 1;
    }
    let stats = engine.mirror.stats();
    println!(
        "calibrated after {blocks} blocks: worst valuation error before {:.3}%, drift corrections {}, max drift {} ppb",
        worst_before * 100.0,
        stats.drift_corrections,
        stats.max_drift_ppb
    );
    assert!(stats.drift_corrections >= 1, "the exact reading showed the 3% model was off");

    // After calibration: exact again, block after block, with no per-block reads.
    let mut track = Track::new(&world, &engine, &engine.position(&pos.id).await, token, curve).await;
    let window = Instant::now();
    for _ in 0..8 {
        let o = track.step(vec![whale.buy(curve, wei(0.02), U256::ZERO)]).await;
        assert!(!o.exiting);
        let (engine_value, chain_value) = engine_vs_chain(&world, &engine, &track.id, token, curve).await;
        assert!((engine_value / chain_value - 1.0).abs() < 1e-6, "calibrated: {engine_value} vs {chain_value}");
    }
    let elapsed = window.elapsed();
    let reads = state_reads(&world.rpc_since(window));
    let periodic = (elapsed.as_secs_f64() / verify.as_secs_f64()).ceil() as usize + 1;
    println!("after calibration: 8 blocks valued exactly in {:?}; state reads {} (periodic verification every {:?})", elapsed, reads.len(), verify);
    assert!(reads.len() <= periodic, "only the periodic verification reads: {reads:?}");
}

/// The trailing-stop distance changed from Telegram while a position is open: a 6% pull-back is
/// held at the default 10%, and the "Trail 5%" button makes the very next evaluation sell it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s4_trailing_distance_changed_at_runtime_applies_to_the_open_position() {
    let world = World::new().await;
    let whale = world.trader();
    world.fund(whale.address(), 20.0);
    let engine = Engine::start(&world, Opts::default()).await;
    let (_, token, curve) = launch_curve(&world, 0.3, plain_launch_calldata());
    world.seal(Vec::new());
    let pos = engine.enter(&world, token, curve, 0.01, None).await;
    let mut track = Track::new(&world, &engine, &pos, token, curve).await;
    track.until(80, || vec![whale.buy(curve, wei(0.03), U256::ZERO)], |o| o.pnl >= 30.0).await;
    let six = track.until(40, || whale_sell_part(&world, &whale, token, curve, 40), |o| o.drawdown >= 6.0).await;
    assert!(!six.exiting && six.drawdown < TRAIL, "held at {:.1}% with the {TRAIL}% default", six.drawdown);
    // Two empty blocks: still held, nothing changed.
    for _ in 0..2 {
        assert!(!track.step(Vec::new()).await.exiting);
    }

    // The button carries the target distance (callback "cfg_trail:5").
    let rep = engine.control.set_trailing_stop(5.0).await;
    println!("cfg_trail:5 -> {:?}", rep.changes);
    assert!(!rep.aborted && rep.changes.iter().any(|c| c.contains("-10.0% → -5.0%")), "{rep:?}");
    assert_eq!(engine.config.trailing_stop().await, (5.0, true));
    let next = track.step(Vec::new()).await;
    println!("next block: drawdown {:.1}% -> exiting {}", next.drawdown, next.exiting);
    assert!(next.exiting, "{:.1}% from the peak is beyond the new 5% distance", next.drawdown);
    let closed = track.settle().await;
    assert!(closed.exit_reason.as_deref().unwrap_or_default().contains("High-Watermark Trailing"), "{:?}", closed.exit_reason);
    assert_exit_settled(&world, &engine, &closed, token);

    // Out of range is refused and changes nothing; reset returns to the `.env` value.
    assert!(engine.control.set_trailing_stop(2.0).await.aborted);
    assert_eq!(engine.config.trailing_stop().await, (5.0, true));
    engine.control.reset_trailing_stop().await;
    assert_eq!(engine.config.trailing_stop().await, (engine.config.trailing_stop_from_env, false));
}

/// Prysm "Target" exit mode: take-profit at $10k market cap, strict stop-loss at -30%; the
/// regular -20% hard stop-loss does not apply in this mode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s4_strict_stop_loss_minus_30_in_prysm_target_mode() {
    let world = World::new().await;
    let whale = world.trader();
    world.fund(whale.address(), 20.0);
    let engine = Engine::start(&world, Opts { exit_mode: PrysmExitMode::Target, ..Opts::default() }).await;
    let (_, token, curve) = launch_curve(&world, 0.02, plain_launch_calldata());
    // An early whale (market cap stays below the $10k take-profit), then our entry at its price.
    world.seal(vec![whale.buy(curve, wei(0.45), U256::ZERO)]);
    let pos = engine.enter(&world, token, curve, 0.01, Some("Prysm: Pola A (Prysmcloud Image Host)")).await;
    assert_eq!(pos.exit_mode.as_deref(), Some(PrysmExitMode::Target.to_string().as_str()));
    let mut track = Track::new(&world, &engine, &pos, token, curve).await;
    track.until(80, || whale_sell_part(&world, &whale, token, curve, 14), |_| false).await;
    let (held, trigger) = track.split();
    println!("Target mode (PnL %, drawdown %): {:?}", track.path());
    assert!(held.iter().all(|o| o.pnl > -30.0), "no exit above -30%");
    assert!(held.iter().any(|o| o.pnl <= -20.5), "held below -20%: the -20% hard stop is off in Target mode");
    assert!(trigger.pnl <= -30.0, "exit on the first valuation at or below -30%: {trigger:?}");
    let closed = track.settle().await;
    println!("exit: {:?}", closed.exit_reason);
    assert!(closed.exit_reason.as_deref().unwrap_or_default().contains("Strict Stop-Loss"), "{:?}", closed.exit_reason);
    assert_exit_settled(&world, &engine, &closed, token);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s4_hard_stop_loss_minus_20_in_the_default_system_mode() {
    let world = World::new().await;
    let whale = world.trader();
    world.fund(whale.address(), 20.0);
    let engine = Engine::start(&world, Opts::default()).await;
    let (_, token, curve) = launch_curve(&world, 0.02, plain_launch_calldata());
    world.seal(vec![whale.buy(curve, wei(0.45), U256::ZERO)]);
    let pos = engine.enter(&world, token, curve, 0.01, Some("Prysm: Pola A (Prysmcloud Image Host)")).await;
    let mut track = Track::new(&world, &engine, &pos, token, curve).await;
    track.until(80, || whale_sell_part(&world, &whale, token, curve, 14), |_| false).await;
    let (held, trigger) = track.split();
    println!("System mode (PnL %, drawdown %): {:?}", track.path());
    assert!(held.iter().all(|o| o.pnl > -20.0), "no exit above -20%");
    assert!(trigger.pnl <= -20.0, "exit on the first valuation at or below -20%: {trigger:?}");
    let closed = track.settle().await;
    println!("exit: {:?}", closed.exit_reason);
    assert!(closed.exit_reason.as_deref().unwrap_or_default().contains("Hard Stop-Loss"), "{:?}", closed.exit_reason);
    assert_exit_settled(&world, &engine, &closed, token);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s4_pre_graduation_guard_sells_before_the_curve_reaches_4_2_eth() {
    let world = World::new().await;
    let whale = world.trader();
    world.fund(whale.address(), 30.0);
    let engine = Engine::start(&world, Opts::default()).await;
    let (_, token, curve) = launch_curve(&world, 0.02, plain_launch_calldata());
    world.seal(Vec::new());
    let pos = engine.enter(&world, token, curve, 0.01, None).await;
    let mut track = Track::new(&world, &engine, &pos, token, curve).await;
    // Buys that move the curve to a given share of the 4.2 ETH graduation target (net of the 3%).
    let buy_to = |share: f64| {
        let gap = share * 4.2 - world.curve(curve).raised_eth();
        vec![whale.buy(curve, wei((gap / 0.97).max(0.001)), U256::ZERO)]
    };
    for share in [0.30, 0.50, 0.70, 0.80, 0.88] {
        let o = track.step(buy_to(share)).await;
        assert!(!o.exiting, "held at {:.1}% of the graduation target: {o:?}", o.capacity);
    }
    let trigger = track.step(buy_to(0.91)).await;
    println!("capacity path (%): {:?}", track.obs.iter().map(|o| (o.capacity * 10.0).round() / 10.0).collect::<Vec<_>>());
    assert!(trigger.exiting && (90.0..100.0).contains(&trigger.capacity), "exit decided at 90-100% of the target: {trigger:?}");
    // The very next block (one block time later) carries a buy that graduates the curve. Our
    // sell is in that block only if it reached the node in time; then it executes first (the
    // mempool goes before the scripted buy), else it lands after graduation and reverts.
    tokio::time::sleep(BLOCK_TIME).await;
    let grad = world.seal(vec![whale.buy(curve, wei(1.5), U256::ZERO)]);
    assert!(world.curve(curve).graduated, "the curve graduated in block #{grad}");
    let closed = track.settle().await;
    let r = assert_exit_settled(&world, &engine, &closed, token);
    let raised_at_exit = eth(world.curve(curve).history[&(r.block - 1)].0) - eth(U256::from(payload::PONS_INITIAL_ETH_RESERVE_WEI));
    println!("exit {:?} in block #{}, the block that graduated the curve (raised before it {:.3} ETH of 4.2)", closed.exit_reason, r.block, raised_at_exit);
    assert!(closed.exit_reason.as_deref().unwrap_or_default().contains("Pre-Graduation"), "{:?}", closed.exit_reason);
    assert_eq!(r.block, grad, "sold in the block right after the decision, ahead of the graduating buy");
    // After graduation the curve takes no more trades: a later exit there fails.
    let late_sell = whale_sell_part(&world, &whale, token, curve, 2);
    let late_hash = hash_of(late_sell.last().unwrap());
    world.seal(late_sell);
    assert!(!world.receipt(late_hash).unwrap().ok, "a sell on the graduated curve reverts");
}

// ════════════════════════════════════════════════════════════════════════════
// 5. Telegram control plane on the live swarm
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s5_buttons_reshape_the_live_swarm_and_panic_cuts_queues_and_liquidates() {
    let world = World::new().await;
    // Every reshape and re-sign below must keep the packets above the EIP-7623 floor.
    world.enforce_calldata_floor();
    let engine = Engine::start(&world, Opts { carriers: 1, prysm_delay: 2, presign_hold: Duration::from_secs(60), ..Opts::default() }).await;
    let (launch, token, _) = launch_curve(&world, 0.021, pola_a_calldata());
    let batch = || engine.dispatcher.staged_batch(launch).expect("staged");
    until("carrier + primary staged", Duration::from_secs(20), || {
        engine.dispatcher.staged_batch(launch).is_some_and(|b| b.slot_specs().iter().filter(|(_, _, s)| *s).count() == 2)
    })
    .await;
    // The same callback data the Telegram buttons carry.
    let press = |data: &str| Action::parse(data).unwrap_or_else(|| panic!("{data} is a button"));

    // Carriers 1 -> 2 -> 3 -> 5.
    for (data, n) in [("cfg_sub:2", 2), ("cfg_sub:3", 3), ("cfg_sub:5", 5)] {
        let rep = engine.control.apply(press(data)).await;
        let specs = batch().slot_specs();
        println!("{data}: {} slot(s) signed, reshape {:?}, warnings {:?}", specs.len(), rep.presign, rep.warnings);
        assert!(!rep.aborted, "{rep:?}");
        assert!(rep.warnings.is_empty(), "funded carriers: {:?}", rep.warnings);
        assert_eq!(specs.len(), n + 1, "master + {n} carriers");
        assert!(specs.iter().all(|(_, _, signed)| *signed), "every slot signed before the trigger block");
    }
    let nonces = batch().leased_nonces();

    // Fee 1.0G -> 2.5G -> 5.0G: same nonces, new tips.
    for (data, gwei) in [("cfg_fee:1.0:5.0", 1.0), ("cfg_fee:2.5:10.0", 2.5), ("cfg_fee:5.0:20.0", 5.0)] {
        let rep = engine.control.apply(press(data)).await;
        let tips: Vec<u128> = (0..=5).map(|i| batch().slot_spec(i).unwrap().0.max_priority_fee_per_gas).collect();
        println!("{data}: re-signed {} packet(s), tips {:?} wei", rep.presign.slots_resigned, tips);
        assert!(tips.iter().all(|t| *t == (gwei * 1e9) as u128), "{tips:?}");
        assert_eq!(batch().leased_nonces(), nonces, "re-signed under the same nonces");
    }

    // Buffer 95 KB -> 0 KB -> 95 KB.
    let rep = engine.control.apply(press("cfg_buffer:0")).await;
    let specs = batch().slot_specs();
    println!("cfg_buffer:0: {} slot(s), master calldata {} B, released {}", specs.len(), specs[0].1.input.len(), rep.presign.slots_released);
    assert_eq!(specs.len(), 1, "no carriers without padding");
    assert!(specs[0].1.input.len() < 200, "master unpadded");
    let rep = engine.control.apply(press("cfg_buffer:94500")).await;
    let specs = batch().slot_specs();
    println!("cfg_buffer:94500: {} slot(s), added {}", specs.len(), rep.presign.slots_added);
    assert_eq!(specs.len(), 6);
    assert!(specs.iter().all(|(_, s, signed)| *signed && s.input.len() >= 94_000), "carriers and master padded to ~94.5 KB again");

    // The trigger block fires exactly what the buttons shaped.
    let staged = staged_now(&engine, launch).unwrap();
    let already = world.sent().len();
    world.seal(Vec::new());
    until("the burst", Duration::from_secs(10), || world.sent().len() >= already + 6).await;
    let burst = world.sent()[already..].to_vec();
    assert!(burst.iter().all(|s| s.accepted() && s.priority_fee == 5_000_000_000 && s.input_len >= 94_000));
    assert_eq!(burst.iter().map(|s| s.hash).collect::<HashSet<_>>(), staged.expected(&engine), "exactly the packets the buttons shaped");
    let master_tx = burst.iter().find(|s| s.from == engine.master).expect("the master's buy");
    assert_eq!(master_tx.nonce, nonces[0].1, "the packet that left carries the nonce leased before the three fee re-signs");
    assert_eq!(burst.iter().map(|s| s.from).collect::<HashSet<_>>().len(), 6, "six distinct accounts: master + 5 carriers");
    assert_eq!(engine.dispatcher.presign_snapshot().prebuilt_sent, 6);
    let entry_block = world.seal(Vec::new());
    assert!(burst.iter().all(|s| world.receipt(s.hash).is_some_and(|r| r.ok && r.block == entry_block)));
    until_async("the position filled", Duration::from_secs(20), || async { engine.open_positions().await.first().is_some_and(|p| p.pnl_percent != 0.0) }).await;

    // A second launch: its order waits for its block, its batch is staged.
    let (launch2, _, _) = launch_curve(&world, 0.02, pola_a_calldata());
    until("the second batch staged", Duration::from_secs(20), || engine.dispatcher.staged_batch(launch2).is_some()).await;
    assert_eq!(engine.prysm.pending_count(), 1);

    // PANIC.
    let _chain = world.auto_seal(Duration::from_millis(80));
    let t0 = Instant::now();
    let pending = engine.control.trigger_breaker();
    let returned = t0.elapsed();
    assert!(!engine.config.is_engine_running(), "intake off when the button returns");
    let outcome = tokio::time::timeout(Duration::from_secs(30), pending).await.expect("breaker finished").unwrap();
    println!(
        "panic: returned in {:?}; {} batch(es) released, {} order(s) dropped, sold {:?} in {:?}",
        returned, outcome.presign_batches_released, outcome.prysm_orders_dropped, outcome.report.sold, outcome.report.elapsed
    );
    assert!(returned < Duration::from_millis(100));
    assert_eq!(outcome.presign_batches_released, 1);
    assert_eq!(outcome.prysm_orders_dropped, 1);
    assert_eq!(outcome.report.sold.len(), 1);
    let sell: B256 = outcome.report.sold[0].2.parse().unwrap();
    assert!(world.receipt(sell).unwrap().ok, "liquidated by a confirmed sell");
    assert_eq!(world.tokens(token, engine.master), U256::ZERO);
    // Locked: later blocks and a new matching launch produce nothing.
    let sent_before = world.sent_from(engine.master).len();
    let (launch3, _, _) = launch_curve(&world, 0.02, pola_a_calldata());
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(world.sent_from(engine.master).len(), sent_before, "no entry while the breaker is latched");
    assert!(engine.signals_for(launch2).is_empty() && engine.signals_for(launch3).is_empty());
    assert!(engine.control.breaker_engaged());
}

// ════════════════════════════════════════════════════════════════════════════
// 6. Full life cycle
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s6_full_life_cycle_dry_run() {
    let world = World::alone().await;
    world.set_send_latency(SEND_LATENCY);
    let whale = world.trader();
    world.fund(whale.address(), 20.0);
    let engine = Engine::start(&world, Opts { carriers: 2, prysm_delay: 2, ..Opts::default() }).await;
    let master_eth0 = world.eth_of(engine.master);
    let t_start = Instant::now();
    let step = |what: &str| println!("[{:>8.1} ms] {}", ms(t_start.elapsed()), what);

    // 1-4. Launch on the feed (second T) -> Pola A -> carriers staged while the receipt is
    // outstanding -> T_LAUNCH resolved, primary staged -> trigger block L+1 -> the swarm -> L+2 at T+1.
    let run = launch_timeline(&world, &engine, 0.021, pola_a_calldata(), 1).await;
    let (token, curve) = (run.token, run.curve);
    let (l, l1, l2) = run.blocks;
    let staged = run.staged_at_trigger.as_ref().expect("staged before the trigger");
    step(&format!(
        "block #{l} (T): Pola A launch on the feed; by the trigger block {} packets staged, all signed: {}",
        staged.slots.len(),
        staged.all_signed()
    ));
    assert!(staged.all_signed() && staged.slots.len() == 3, "master + 2 carriers pre-signed within one block time");
    let signals = engine.signals_for(run.launch);
    assert_eq!(signals.len(), 1, "one entry for the launch");
    assert!(signals[0].strategy_tag.as_deref().unwrap_or_default().contains("Pola A"), "{:?}", signals[0].strategy_tag);
    let b = &run.burst;
    step(&format!(
        "block #{l1} (trigger, T): {} pre-signed packets at the node {:.3} ms after the block; spread start {:.3} ms, complete {:.3} ms",
        b.txs.len(),
        ms(b.trigger_to_first),
        ms(b.start_spread),
        ms(b.spread)
    ));
    assert_eq!(b.txs.iter().map(|s| s.hash).collect::<HashSet<_>>(), staged.expected(&engine), "the staged packets, unchanged");
    assert!(b.last_start_after_first_done_ms < ms(SEND_LATENCY), "all left before any answer came back: no serial send");
    assert_swarm(&world, &engine, &run, &engine.carriers, 0.01);
    assert_eq!(engine.dispatcher.presign_snapshot().prebuilt_sent, 3);
    let entry = b.txs.iter().find(|s| s.from == engine.master).unwrap().clone();
    let entry_r = world.receipt(entry.hash).unwrap();
    step(&format!("block #{l2} (T+1): entry executed at the 6.18% snipe second, {:.0} tokens to the master", eth(entry_r.tokens_out)));

    // 5. The position enters the mirror, valued in memory at exactly the chain's state.
    until_async("the position registered", Duration::from_secs(20), || async { !engine.open_positions().await.is_empty() }).await;
    let id = engine.open_positions().await[0].id.clone();
    let pos = engine.settled_entry(&world, &id, token).await;
    until("mirror fresh", Duration::from_secs(10), || matches!(engine.mirror.read(&curve), MirrorRead::Fresh(_))).await;
    let mut track = Track::new(&world, &engine, &pos, token, curve).await;
    // The value equals the chain's because the mirror replayed our 6.18%-taxed fill (predicted on
    // an older exact reading), or because an exact reading confirmed that prediction to the wei.
    let snap = engine.mirror.read(&curve).fresh().expect("fresh");
    let replayed = snap.anchor_block < l2 && snap.unconfirmed_trades >= 1;
    let confirmed = snap.validated_from < l2 && l2 <= snap.validated_through;
    step(&format!(
        "position {id} in the mirror: value {:.6} ETH = chain value (PnL {:.1}% after fee + snipe tax); anchor #{}, {} predicted trade(s), validated ({}, {}]",
        pos.current_eth_value, pos.pnl_percent, snap.anchor_block, snap.unconfirmed_trades, snap.validated_from, snap.validated_through
    ));
    assert!(replayed || confirmed, "the snipe-taxed fill went through the mirror's replay: {snap:?}");

    // 6. +40%, valued in memory block by block.
    let window = Instant::now();
    let top = track.until(80, || vec![whale.buy(curve, wei(0.03), U256::ZERO)], |o| o.pnl >= 40.0).await;
    let reads = state_reads(&world.rpc_since(window));
    step(&format!("price up: PnL {:.1}% after {} blocks, valued in memory ({} state reads over RPC)", top.pnl, track.obs.len(), reads.len()));
    assert!(reads.is_empty() && !top.exiting, "{reads:?}");

    // 7. A 6% correction from the peak: inside the trailing distance, the position is held.
    let six = track.until(40, || whale_sell_part(&world, &whale, token, curve, 40), |o| o.drawdown >= 6.0).await;
    assert!(!six.exiting, "a 6% pull-back is inside the {TRAIL}% trailing distance: {six:?}");
    step(&format!("correction {:.1}% from the peak (PnL {:.1}%): held, trailing stop armed at -{TRAIL}%", six.drawdown, six.pnl));

    // 8. The correction deepens -> trailing stop -> receipt-confirmed sell.
    track.until(60, || whale_sell_part(&world, &whale, token, curve, 12), |_| false).await;
    let (held, trigger) = track.split();
    assert!(held.iter().all(|o| o.drawdown < TRAIL) && trigger.drawdown >= TRAIL, "{:?}", track.path());
    step(&format!("block #{}: drawdown {:.1}% from the peak (PnL {:.1}%) -> exit decided", trigger.block, trigger.drawdown, trigger.pnl));
    let closed = track.settle().await;
    let r = assert_exit_settled(&world, &engine, &closed, token);
    step(&format!("{}: sell confirmed in block #{} (status 0x1), master holds 0 tokens", closed.exit_reason.as_deref().unwrap_or_default(), r.block));
    assert!(closed.exit_reason.as_deref().unwrap_or_default().contains("High-Watermark Trailing"), "{:?}", closed.exit_reason);
    println!("           path (PnL %, drawdown %): {:?}", track.path());

    // 9. Capital back on the master, to the wei: start - entry - gas + sale proceeds.
    let master_eth1 = world.eth_of(engine.master);
    let gas = world.gas_paid(engine.master);
    assert_eq!(master_eth1, master_eth0 - wei(0.01) - gas + r.eth_out, "the master's ETH, to the wei");
    let net = eth(r.eth_out) - 0.01 - eth(gas);
    step(&format!(
        "master ETH {:.6} -> {:.6}: entry 0.010000, sale proceeds {:.6}, gas {:.6} (entry, approval, exit), net {:+.6} ETH",
        eth(master_eth0),
        eth(master_eth1),
        eth(r.eth_out),
        eth(gas),
        net
    ));
    assert!(net > 0.0, "the trailing stop locked in part of the run-up");

    // 10. The final report, as the Telegram alerts render it.
    until("the exit alert", Duration::from_secs(5), || engine.exits.lock().unwrap().iter().any(|p| p.id == id && !p.is_open)).await;
    let report = engine.reports.lock().unwrap().first().cloned().expect("dispatch report");
    let final_alert = engine.exits.lock().unwrap().iter().find(|p| p.id == id).cloned().unwrap();
    let texts = [screens::dispatch_alert(&report), screens::position_alert(&final_alert)];
    for t in &texts {
        assert_eq!(html::validate(t), Ok(()));
    }
    println!("\n── final report (as sent to Telegram) ──\n{}\n\n{}\n", html::to_plain(&texts[0]), html::to_plain(&texts[1]));
}
