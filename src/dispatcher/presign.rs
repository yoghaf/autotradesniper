//! Ahead-of-time (AOT) transaction pre-signing: the staging side of the dispatcher.
//!
//! A Prysm entry is a *primary* tx (the real buy on the launched curve) plus N zero-value *carrier*
//! txs (one per satellite account). Signing one of them costs a keccak over ~95 KB twice, an ECDSA
//! sign, a 190 KB hex body and the encode (see AUDIT.md §8): about 1.3 ms each. Done when the
//! trigger block is seen, that is pure latency on the critical path. This module lets the engine pay
//! it earlier:
//!
//! ```text
//!   Pola A/B matched ──► arm: lease carrier nonces, sign carriers (background, 1 blocking task)
//!   T_LAUNCH resolved ─► stage primary: lease master nonce, sign (only if the order still waits)
//!   target block seen ─► Dispatcher::dispatch_signal: claim batch, validate, BLAST stored bytes
//!   cancel / expiry   ─► batch terminated, every leased nonce handed back
//! ```
//!
//! # Thread safety and determinism
//!
//! A batch has one atomic state and every transition is a compare-exchange from `Staged`, so
//! **exactly one** of dispatch / cancel / expiry wins, whatever the interleaving:
//!
//! ```text
//!            claim (dispatch)            mark_dispatched
//!   Staged ────────────────────► Claimed ─────────────────► Dispatched
//!     │ cancel / expiry (CAS)
//!     ├──────────────────────────► Cancelled | Expired      (leases released by the winner)
//! ```
//!
//! Nonces are *leased* ([`NonceLease`]) by the batch itself from the moment a slot is created, never
//! by the background signer. A signer that loses the race against `claim` therefore cannot strand a
//! nonce, and dispatch signs a not-yet-ready slot inline **under the leased nonce**, so there is
//! never a hole between a staged nonce and a freshly allocated one.
//!
//! Lock discipline: the book lock and a batch lock are plain `std::sync::Mutex`es, never held across
//! an `.await` or across signing, and never held together (the book lock is released before a
//! batch is touched), so no lock cycle can exist.

use crate::payload::{self, CalldataPayloadBuilder};
use crate::wallet::{NonceLease, TreasuryAccount};
use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::Encodable2718;
use alloy::primitives::{Address, Bytes, TxKind, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// A tombstone (dispatched / cancelled launch) is remembered this long, so a late `arm` for the same
/// launch is refused instead of leasing nonces nobody will ever use.
const TOMBSTONE_TTL: Duration = Duration::from_secs(60);

// ── Policy ───────────────────────────────────────────────────────────────────

/// Knobs of the pre-signing pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PresignPolicy {
    /// Master switch. Off: the dispatcher signs on the fly exactly as before.
    pub enabled: bool,
    /// A staged batch is torn down (nonces handed back) at the latest this long after it was armed.
    /// Short on purpose: a leased nonce holds back every later tx of the same account.
    pub max_hold: Duration,
    /// A staged tx is re-signed when its fee cap / priority fee fell more than this many bps below
    /// what a fresh tx would use. Fees above the live value are harmless (Arbitrum charges the base fee).
    pub fee_tolerance_bps: u32,
    /// A staged primary is re-signed when its Revert Guard (`minTokensOut`) differs from the fresh
    /// quote by more than this many bps of the fresh one.
    pub quote_tolerance_bps: u32,
}

impl Default for PresignPolicy {
    fn default() -> Self {
        Self { enabled: true, max_hold: Duration::from_secs(8), fee_tolerance_bps: 1_000, quote_tolerance_bps: 200 }
    }
}

impl PresignPolicy {
    /// `PRESIGN_ENABLED`, `PRESIGN_MAX_HOLD_MS`, `PRESIGN_FEE_TOLERANCE_BPS`, `PRESIGN_QUOTE_TOLERANCE_BPS`.
    pub fn from_env() -> Self {
        let mut p = Self::default();
        if let Ok(v) = std::env::var("PRESIGN_ENABLED") {
            p.enabled = !matches!(v.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off" | "no");
        }
        let num = |key: &str| std::env::var(key).ok().and_then(|v| v.trim().parse::<u64>().ok());
        if let Some(ms) = num("PRESIGN_MAX_HOLD_MS") {
            p.max_hold = Duration::from_millis(ms.max(1));
        }
        if let Some(bps) = num("PRESIGN_FEE_TOLERANCE_BPS") {
            p.fee_tolerance_bps = bps.min(10_000) as u32;
        }
        if let Some(bps) = num("PRESIGN_QUOTE_TOLERANCE_BPS") {
            p.quote_tolerance_bps = bps.min(10_000) as u32;
        }
        p
    }
}

/// Why a staged packet cannot be sent as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invalidation {
    /// The batch outlived `max_hold`.
    Expired,
    /// A node reported a pending nonce above the leased one: the chain already used it.
    NonceOvertaken,
    /// Other destination, value, chain or calldata than the signal asks for now.
    TargetMismatch,
    /// Buffer size or configured gas limit changed since signing.
    ParamsChanged,
    /// Same buy, but the Revert Guard moved more than the quote tolerance.
    QuoteDrift,
    /// Fee cap or priority fee fell below the live value by more than the fee tolerance.
    FeeDrift,
    /// The L1 poster-gas estimate rose past what the staged gas limit covers.
    GasDrift,
}

impl std::fmt::Display for Invalidation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Invalidation::Expired => "expired",
            Invalidation::NonceOvertaken => "nonce overtaken by the chain",
            Invalidation::TargetMismatch => "target/calldata mismatch",
            Invalidation::ParamsChanged => "buffer/gas parameters changed",
            Invalidation::QuoteDrift => "revert-guard quote drifted",
            Invalidation::FeeDrift => "fee below live value",
            Invalidation::GasDrift => "L1 poster gas outgrew the gas limit",
        })
    }
}

// ── Transaction spec and signed bytes ────────────────────────────────────────

/// Fee and gas inputs of the moment (what a tx signed *now* would use).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryCtx {
    pub chain_id: u64,
    /// Master padding (0 when the padding switch is off).
    pub buffer_size: usize,
    /// Padding switch: satellite carriers take part in an entry only while it is on.
    pub carriers_enabled: bool,
    pub configured_gas_limit: u64,
    /// Latest `NodeInterface.gasEstimateL1Component` (0 = never measured).
    pub l1_component_gas: u64,
    pub priority_fee_wei: u128,
    pub max_fee_wei: u128,
}

/// Everything that fixes one account's tx except the nonce.
#[derive(Debug, Clone)]
pub struct TxSpec {
    pub chain_id: u64,
    pub to: Address,
    pub value: U256,
    pub input: Bytes,
    pub gas_limit: u64,
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
    /// Lower bound of an escalated gas limit after a "gas too low" rejection.
    pub floor_with_l1: u64,
    /// Re-send the identical tx once after a 429 (primary only: carriers must not add load).
    pub retry_rate_limited: bool,
    /// `intrinsic + execution reserve`: what the limit needs besides the L1 poster component.
    pub gas_floor_without_l1: u64,
    /// `GAS_LIMIT` as configured when this spec was built.
    pub configured_gas_limit: u64,
    /// L1 poster-gas estimate this spec was sized with.
    pub l1_component_gas: u64,
    /// The primary's swap calldata without padding (empty for carriers): the quote-drift test.
    pub swap: Vec<u8>,
}

impl TxSpec {
    pub fn to_tx(&self, nonce: u64) -> TxEip1559 {
        TxEip1559 {
            chain_id: self.chain_id,
            nonce,
            gas_limit: self.gas_limit,
            max_fee_per_gas: self.max_fee_per_gas,
            max_priority_fee_per_gas: self.max_priority_fee_per_gas,
            to: TxKind::Call(self.to),
            value: self.value,
            input: self.input.clone(),
            access_list: Default::default(),
        }
    }

    /// The same tx (destination, value, calldata) re-sized for the fees and gas of `ctx`.
    pub fn refreshed(&self, ctx: &EntryCtx) -> TxSpec {
        let reserve_and_intrinsic = self.gas_floor_without_l1;
        let gas_limit = self
            .configured_gas_limit
            .max(reserve_and_intrinsic.saturating_add(CalldataPayloadBuilder::l1_margin(ctx.l1_component_gas)));
        // `floor_with_l1` = calldata floor (unchanged) + the new L1 margin.
        let calldata_floor = self.floor_with_l1.saturating_sub(CalldataPayloadBuilder::l1_margin(self.l1_component_gas));
        TxSpec {
            gas_limit,
            max_fee_per_gas: ctx.max_fee_wei,
            max_priority_fee_per_gas: ctx.priority_fee_wei,
            floor_with_l1: calldata_floor.saturating_add(CalldataPayloadBuilder::l1_margin(ctx.l1_component_gas)),
            l1_component_gas: ctx.l1_component_gas,
            ..self.clone()
        }
    }

    /// Fee and gas part of the validity test (also used by the proactive per-block re-validation).
    pub fn fee_gas_check(&self, now_max_fee: u128, now_priority: u128, now_l1: u64, policy: &PresignPolicy) -> Result<(), Invalidation> {
        let keep = 10_000u128.saturating_sub(policy.fee_tolerance_bps as u128);
        let scaled = |v: u128| v.saturating_mul(keep) / 10_000;
        if self.max_fee_per_gas < scaled(now_max_fee) || self.max_priority_fee_per_gas < scaled(now_priority) {
            return Err(Invalidation::FeeDrift);
        }
        // The limit must still cover intrinsic + reserve + the *current* L1 component (1.0x). The extra
        // 0.5x the limit was built with is the slack that absorbs a rising L1 fee without re-signing.
        let l1 = if now_l1 == 0 { payload::L1_COMPONENT_FALLBACK_GAS } else { now_l1 };
        if self.gas_limit < self.gas_floor_without_l1.saturating_add(l1) {
            return Err(Invalidation::GasDrift);
        }
        Ok(())
    }

    /// Can this staged spec stand in for `now` (the spec a tx built from the current signal and the
    /// current fees would have)? `Ok` means the signed bytes are good to send unchanged.
    pub fn valid_against(&self, now: &TxSpec, policy: &PresignPolicy) -> Result<(), Invalidation> {
        if self.chain_id != now.chain_id || self.to != now.to || self.value != now.value {
            return Err(Invalidation::TargetMismatch);
        }
        if self.input.len() != now.input.len() || self.configured_gas_limit != now.configured_gas_limit {
            return Err(Invalidation::ParamsChanged);
        }
        if self.swap != now.swap {
            // Only the Revert Guard word may differ; any other difference is a different trade.
            if !CalldataPayloadBuilder::pons_buy_same_but_guard(&self.swap, &now.swap) {
                return Err(Invalidation::TargetMismatch);
            }
            if !CalldataPayloadBuilder::pons_buy_quote_compatible(&self.swap, &now.swap, policy.quote_tolerance_bps) {
                return Err(Invalidation::QuoteDrift);
            }
        }
        self.fee_gas_check(now.max_fee_per_gas, now.max_priority_fee_per_gas, now.l1_component_gas, policy)
    }
}

/// A fully signed tx, ready to be written to a socket.
#[derive(Debug, Clone)]
pub struct SignedBytes {
    /// EIP-2718 envelope (what `eth_sendRawTransaction` carries).
    pub raw: Bytes,
    /// The complete JSON-RPC request body, hex encoded once. Broadcasting is a pure socket write.
    pub body: bytes::Bytes,
    pub hash: B256,
    /// `0x…` form of `hash`, the value endpoints are expected to echo back.
    pub hash_hex: String,
}

/// Sign `tx` and pre-encode everything the broadcaster needs. CPU-bound (~1.3 ms for a 94.5 KB
/// payload): call it from `spawn_blocking` or a dispatch worker, never from the feed task.
pub fn sign_sync(signer: &PrivateKeySigner, tx: TxEip1559) -> Result<SignedBytes, String> {
    let sig = signer.sign_hash_sync(&tx.signature_hash()).map_err(|e| e.to_string())?;
    let signed = tx.into_signed(sig);
    let hash = *signed.hash();
    let raw = TxEnvelope::Eip1559(signed).encoded_2718();
    let body = super::send_raw_tx_body(&raw);
    Ok(SignedBytes { raw: Bytes::from(raw), body, hash, hash_hex: format!("{:?}", hash) })
}

// ── Stats ────────────────────────────────────────────────────────────────────

/// Lifetime counters of the pipeline (relaxed atomics; for logs, the dashboard and tests).
#[derive(Debug, Default)]
pub struct PresignStats {
    pub armed: AtomicU64,
    pub late_arm_refused: AtomicU64,
    pub packets_signed_ahead: AtomicU64,
    /// A signing result that lost the race against claim/cancel/replace and was thrown away.
    pub packets_discarded: AtomicU64,
    pub primary_staged: AtomicU64,
    pub primary_restaged: AtomicU64,
    pub claimed: AtomicU64,
    /// Staged bytes broadcast untouched: zero signing on the hot path.
    pub prebuilt_sent: AtomicU64,
    /// A staged packet existed but was stale; re-signed under the SAME leased nonce.
    pub resigned_stale: AtomicU64,
    /// The slot held a nonce but no signed bytes yet (signer still running): signed inline, same nonce.
    pub signed_inline_leased: AtomicU64,
    /// No staged slot at all for this account: classic allocate + sign.
    pub signed_inline_fresh: AtomicU64,
    pub leases_overtaken: AtomicU64,
    pub leases_released_unused: AtomicU64,
    pub cancelled: AtomicU64,
    pub expired: AtomicU64,
    pub proactive_resigns: AtomicU64,
}

/// Plain copy of [`PresignStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PresignSnapshot {
    pub armed: u64,
    pub late_arm_refused: u64,
    pub packets_signed_ahead: u64,
    pub packets_discarded: u64,
    pub primary_staged: u64,
    pub primary_restaged: u64,
    pub claimed: u64,
    pub prebuilt_sent: u64,
    pub resigned_stale: u64,
    pub signed_inline_leased: u64,
    pub signed_inline_fresh: u64,
    pub leases_overtaken: u64,
    pub leases_released_unused: u64,
    pub cancelled: u64,
    pub expired: u64,
    pub proactive_resigns: u64,
}

impl PresignStats {
    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> PresignSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        PresignSnapshot {
            armed: l(&self.armed),
            late_arm_refused: l(&self.late_arm_refused),
            packets_signed_ahead: l(&self.packets_signed_ahead),
            packets_discarded: l(&self.packets_discarded),
            primary_staged: l(&self.primary_staged),
            primary_restaged: l(&self.primary_restaged),
            claimed: l(&self.claimed),
            prebuilt_sent: l(&self.prebuilt_sent),
            resigned_stale: l(&self.resigned_stale),
            signed_inline_leased: l(&self.signed_inline_leased),
            signed_inline_fresh: l(&self.signed_inline_fresh),
            leases_overtaken: l(&self.leases_overtaken),
            leases_released_unused: l(&self.leases_released_unused),
            cancelled: l(&self.cancelled),
            expired: l(&self.expired),
            proactive_resigns: l(&self.proactive_resigns),
        }
    }
}

// ── Staged batch ─────────────────────────────────────────────────────────────

const STAGED: u8 = 0;
const CLAIMED: u8 = 1;
const DISPATCHED: u8 = 2;
const CANCELLED: u8 = 3;
const EXPIRED: u8 = 4;

/// Lifecycle of a [`StagedBatch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchState {
    /// Armed: leases held, packets being / already signed. The only state that can still change.
    Staged,
    /// A dispatch took ownership of the leases and packets.
    Claimed,
    /// The dispatch finished.
    Dispatched,
    /// Torn down on request (order rejected, cancelled, launch failed).
    Cancelled,
    /// Torn down because `max_hold` passed.
    Expired,
}

impl BatchState {
    fn from_u8(v: u8) -> Self {
        match v {
            STAGED => BatchState::Staged,
            CLAIMED => BatchState::Claimed,
            DISPATCHED => BatchState::Dispatched,
            CANCELLED => BatchState::Cancelled,
            _ => BatchState::Expired,
        }
    }
}

struct Slot {
    lease: NonceLease,
    spec: Arc<TxSpec>,
    signed: Option<SignedBytes>,
    /// Bumped whenever `spec` is replaced; a signing result installs only under the version it was
    /// started with, so a stale result can never overwrite a newer spec.
    version: u64,
}

/// What a signer needs to produce one slot's bytes, and to hand them back.
#[derive(Debug, Clone)]
pub struct SlotTicket {
    pub index: usize,
    pub nonce: u64,
    version: u64,
    pub spec: Arc<TxSpec>,
}

/// One slot handed to the dispatch that won the batch.
pub struct ClaimedSlot {
    pub lease: NonceLease,
    pub spec: Arc<TxSpec>,
    pub signed: Option<SignedBytes>,
}

/// Slots by account index (`None` = nothing was staged for that account).
pub struct Claim {
    pub slots: Vec<Option<ClaimedSlot>>,
}

/// The pre-signed packets of one entry: primary (index 0) + carriers (index 1..).
pub struct StagedBatch {
    key: B256,
    state: AtomicU8,
    armed_at: Instant,
    expires_at: Instant,
    slots: Mutex<Vec<Option<Slot>>>,
    closed: Notify,
}

impl std::fmt::Debug for StagedBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedBatch").field("key", &self.key).field("state", &self.state()).finish()
    }
}

impl StagedBatch {
    pub fn new(key: B256, accounts: usize, armed_at: Instant, expires_at: Instant) -> Self {
        Self {
            key,
            state: AtomicU8::new(STAGED),
            armed_at,
            expires_at,
            slots: Mutex::new((0..accounts).map(|_| None).collect()),
            closed: Notify::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Option<Slot>>> {
        self.slots.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn key(&self) -> B256 {
        self.key
    }

    pub fn state(&self) -> BatchState {
        BatchState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn armed_at(&self) -> Instant {
        self.armed_at
    }

    pub fn expires_at(&self) -> Instant {
        self.expires_at
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }

    /// Resolves once the batch left `Staged` (claimed or torn down).
    pub async fn closed(&self) {
        self.closed.notified().await;
    }

    /// Create (or re-spec) the slot of account `index`, leasing a nonce for it on first use.
    ///
    /// `None` when the batch is no longer `Staged`: nothing was leased, nothing to clean up. The
    /// state check and the lease happen under the batch lock, so they cannot interleave with
    /// [`Self::claim`] taking the slots.
    pub fn lease_slot(&self, index: usize, acc: &TreasuryAccount, spec: TxSpec) -> Option<SlotTicket> {
        let mut slots = self.lock();
        if self.state.load(Ordering::Acquire) != STAGED || index >= slots.len() {
            return None;
        }
        let spec = Arc::new(spec);
        let slot = &mut slots[index];
        match slot {
            Some(existing) => {
                // Same nonce, new content: the old bytes were never sent, so only one of the two can
                // ever execute and no nonce churn is needed.
                existing.spec = spec.clone();
                existing.signed = None;
                existing.version += 1;
                Some(SlotTicket { index, nonce: existing.lease.nonce(), version: existing.version, spec })
            }
            None => {
                let lease = acc.lease_nonce();
                let nonce = lease.nonce();
                *slot = Some(Slot { lease, spec: spec.clone(), signed: None, version: 0 });
                Some(SlotTicket { index, nonce, version: 0, spec })
            }
        }
    }

    /// Attach freshly signed bytes. False when the batch moved on or the slot was re-spec'd since the
    /// ticket was issued; the caller drops the bytes.
    pub fn install_signed(&self, ticket: &SlotTicket, signed: SignedBytes) -> bool {
        let mut slots = self.lock();
        if self.state.load(Ordering::Acquire) != STAGED {
            return false;
        }
        match slots.get_mut(ticket.index).and_then(Option::as_mut) {
            Some(slot) if slot.version == ticket.version => {
                slot.signed = Some(signed);
                true
            }
            _ => false,
        }
    }

    /// `(spec, has signed bytes)` of one slot, for idempotent re-staging decisions.
    pub fn slot_spec(&self, index: usize) -> Option<(Arc<TxSpec>, bool)> {
        self.lock().get(index).and_then(Option::as_ref).map(|s| (s.spec.clone(), s.signed.is_some()))
    }

    /// Number of account slots (the account layout this batch was shaped for).
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// The account layout changed while staged (carrier count, single/multi mode, padding switch):
    /// keep slots `[0, accounts)` and drop the rest, handing their leased nonces back. Returns how
    /// many leased slots were dropped. A no-op once the batch left `Staged`.
    pub fn resize(&self, accounts: usize) -> usize {
        let dropped: Vec<Slot> = {
            let mut slots = self.lock();
            if self.state.load(Ordering::Acquire) != STAGED {
                return 0;
            }
            if accounts < slots.len() {
                slots.drain(accounts..).flatten().collect()
            } else {
                slots.resize_with(accounts, || None);
                Vec::new()
            }
        };
        // Dropping a slot drops its lease, which returns the never-broadcast nonce.
        dropped.len()
    }

    /// `(index, spec, has signed bytes)` of every filled slot.
    pub fn slot_specs(&self) -> Vec<(usize, Arc<TxSpec>, bool)> {
        self.lock()
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_ref().map(|s| (i, s.spec.clone(), s.signed.is_some())))
            .collect()
    }

    /// Nonces leased by this batch, by account index.
    pub fn leased_nonces(&self) -> Vec<(usize, u64)> {
        self.lock().iter().enumerate().filter_map(|(i, s)| s.as_ref().map(|s| (i, s.lease.nonce()))).collect()
    }

    /// Take ownership of every lease and packet. Exactly one caller can win (`Staged -> Claimed`).
    pub fn claim(&self) -> Option<Claim> {
        self.state.compare_exchange(STAGED, CLAIMED, Ordering::AcqRel, Ordering::Acquire).ok()?;
        // Any `lease_slot` / `install_signed` that checked `Staged` first is already inside the
        // lock; the ones that come later see `Claimed` and back off. Taking the lock here therefore
        // sees every slot that will ever exist.
        let taken: Vec<Option<ClaimedSlot>> = {
            let mut slots = self.lock();
            slots
                .iter_mut()
                .map(|s| s.take().map(|s| ClaimedSlot { lease: s.lease, spec: s.spec, signed: s.signed }))
                .collect()
        };
        self.closed.notify_one();
        Some(Claim { slots: taken })
    }

    pub fn mark_dispatched(&self) {
        let _ = self.state.compare_exchange(CLAIMED, DISPATCHED, Ordering::AcqRel, Ordering::Acquire);
    }

    /// Tear the batch down: `Staged -> to`, then release every lease. False if it already left `Staged`.
    fn terminate(&self, to: u8) -> bool {
        if self.state.compare_exchange(STAGED, to, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return false;
        }
        // Dropping the slots drops the leases, which hand the nonces back.
        let released: Vec<Slot> = self.lock().iter_mut().filter_map(Option::take).collect();
        drop(released);
        self.closed.notify_one();
        true
    }
}

// ── Book ─────────────────────────────────────────────────────────────────────

enum Entry {
    Live(Arc<StagedBatch>),
    /// Dispatched / cancelled / expired. Blocks a late `arm` of the same launch.
    Closed(Instant),
}

/// Outcome of [`StagingBook::arm`].
pub enum Armed {
    /// This batch is now the live one for its launch.
    New(Arc<StagedBatch>),
    /// A live batch already existed (duplicate arm); use it.
    Existing(Arc<StagedBatch>),
    /// The launch was already dispatched / cancelled: arming now would strand nonces.
    Refused,
}

/// Why a batch is being torn down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Teardown {
    Cancelled,
    Expired,
}

/// Staged batches by launch tx hash.
#[derive(Default)]
pub struct StagingBook {
    entries: Mutex<HashMap<B256, Entry>>,
    live: AtomicUsize,
    stats: Arc<PresignStats>,
}

impl StagingBook {
    fn lock(&self) -> MutexGuard<'_, HashMap<B256, Entry>> {
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn stats(&self) -> Arc<PresignStats> {
        self.stats.clone()
    }

    pub fn stats_ref(&self) -> &PresignStats {
        &self.stats
    }

    /// Number of live (`Staged`) batches: the lock-free "is there anything to look at" test used on
    /// the per-block path.
    pub fn live_count(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    fn purge_tombstones(entries: &mut HashMap<B256, Entry>, now: Instant) {
        entries.retain(|_, e| match e {
            Entry::Closed(at) => now.saturating_duration_since(*at) < TOMBSTONE_TTL,
            Entry::Live(_) => true,
        });
    }

    pub fn arm(&self, batch: Arc<StagedBatch>) -> Armed {
        let now = Instant::now();
        let mut entries = self.lock();
        Self::purge_tombstones(&mut entries, now);
        match entries.get(&batch.key) {
            Some(Entry::Live(existing)) => Armed::Existing(existing.clone()),
            Some(Entry::Closed(_)) => {
                PresignStats::bump(&self.stats.late_arm_refused);
                Armed::Refused
            }
            None => {
                entries.insert(batch.key, Entry::Live(batch.clone()));
                self.live.fetch_add(1, Ordering::AcqRel);
                PresignStats::bump(&self.stats.armed);
                Armed::New(batch)
            }
        }
    }

    pub fn live_batch(&self, key: B256) -> Option<Arc<StagedBatch>> {
        match self.lock().get(&key) {
            Some(Entry::Live(b)) => Some(b.clone()),
            _ => None,
        }
    }

    pub fn live_batches(&self) -> Vec<Arc<StagedBatch>> {
        self.lock()
            .values()
            .filter_map(|e| match e {
                Entry::Live(b) => Some(b.clone()),
                Entry::Closed(_) => None,
            })
            .collect()
    }

    /// Take the batch of `key` for dispatch. Whatever the answer, the launch is tombstoned: a late
    /// `arm` must not lease nonces for an entry that is already being sent.
    pub fn claim(&self, key: B256) -> Option<(Arc<StagedBatch>, Claim)> {
        let batch = {
            let mut entries = self.lock();
            let now = Instant::now();
            Self::purge_tombstones(&mut entries, now);
            match entries.insert(key, Entry::Closed(now)) {
                Some(Entry::Live(b)) => {
                    self.live.fetch_sub(1, Ordering::AcqRel);
                    Some(b)
                }
                _ => None,
            }
        };
        let batch = batch?;
        let claim = batch.claim()?;
        PresignStats::bump(&self.stats.claimed);
        Some((batch, claim))
    }

    /// Tear down the live batch of `key` (if any) and tombstone the launch. True if a live batch was
    /// torn down by this call.
    pub fn teardown(&self, key: B256, why: Teardown) -> bool {
        let batch = {
            let mut entries = self.lock();
            match entries.get(&key) {
                Some(Entry::Live(_)) | None => {}
                Some(Entry::Closed(_)) => return false,
            }
            match entries.insert(key, Entry::Closed(Instant::now())) {
                Some(Entry::Live(b)) => {
                    self.live.fetch_sub(1, Ordering::AcqRel);
                    Some(b)
                }
                _ => None,
            }
        };
        let Some(batch) = batch else { return false };
        let to = if why == Teardown::Expired { EXPIRED } else { CANCELLED };
        let won = batch.terminate(to);
        if won {
            PresignStats::bump(if why == Teardown::Expired { &self.stats.expired } else { &self.stats.cancelled });
        }
        won
    }

    /// Janitor entry point: tear `key` down only if it really is past its deadline.
    pub fn expire_if_due(&self, key: B256, now: Instant) -> bool {
        match self.live_batch(key) {
            Some(b) if b.is_expired(now) => self.teardown(key, Teardown::Expired),
            _ => false,
        }
    }

    /// Tear down every live batch (engine switched off, shutdown). Returns how many.
    pub fn teardown_all(&self, why: Teardown) -> usize {
        let keys: Vec<B256> = self.live_batches().iter().map(|b| b.key).collect();
        keys.into_iter().filter(|k| self.teardown(*k, why)).count()
    }
}
