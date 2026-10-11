//! Operator interface over Telegram.
//!
//! Layout:
//! * [`control`]: what the buttons do to the engine (testable without Telegram);
//! * [`screens`]: every text / keyboard, pure functions, HTML-safe by construction;
//! * [`outbox`]: engine → operator notifications (non-blocking enqueue, bounded retries);
//! * [`html`]: escaping, validation, plain-text fallback, length fitting;
//! * this file: the teloxide adapter, run on a thread of its own ([`start_control_plane`]).
//!
//! Isolation: polling, handlers, rendering, balance reads and the outbox all run on the
//! "telegram-io" OS thread with its own single-threaded tokio runtime. The engine runtime never
//! executes Telegram I/O and never waits for it: alerts are a `try_send`, and the control plane
//! hands engine work (nonce sync, re-signing, liquidation) to the engine runtime and only waits
//! for it on the Telegram side.

pub mod control;
pub mod emergency_controls;
pub mod html;
pub mod outbox;
pub mod screens;

pub use control::{Action, ControlPlane, EngineHandles, FeePreset};
pub use outbox::{Notifier, OutboxPolicy};

use crate::types::ExecutionMode;
use alloy::primitives::Address;
use anyhow::Result;
use futures_util::future::BoxFuture;
use outbox::{SendError, TelegramApi};
use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use teloxide::prelude::*;
use teloxide::types::{ChatId, InlineKeyboardButton, InlineKeyboardMarkup, MessageId, ParseMode, UpdateKind};
use teloxide::{ApiError, RequestError};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// User conversation state for handling interactive text inputs
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserState {
    Idle,
    AwaitingAddAddress,
    AwaitingMnemonic,
    AwaitingPrysmSize,
}

/// Escape text for Telegram `ParseMode::Html` (see [`html::escape`]).
pub fn html_escape(text: &str) -> String {
    html::escape(text)
}

/// An interactive reply waits at most this long on flood control, once.
const REPLY_MAX_RETRY_AFTER: Duration = Duration::from_secs(5);
/// Attempts per interactive reply (flood-control wait / plain-text fallback included).
const REPLY_ATTEMPTS: usize = 3;
/// Upper bound of one handler. Updates of a chat are handled in order; a handler that hung (a
/// stuck HTTP call) would otherwise stall that chat's queue until teloxide stops routing new
/// updates, the circuit breaker included.
const HANDLER_BUDGET: Duration = Duration::from_secs(25);

/// Where an interactive reply goes.
#[derive(Debug, Clone, Copy)]
enum Target {
    /// Edit the message the pressed button belongs to.
    Edit(MessageId),
    New,
}

/// Interactive Telegram controller (the adapter between teloxide and the [`ControlPlane`]).
///
/// It never touches engine state directly: every read and change goes through the control
/// plane, which runs it on the engine runtime.
#[derive(Clone)]
pub struct TelegramBot {
    bot: Bot,
    control: ControlPlane,
    user_states: Arc<RwLock<HashMap<i64, UserState>>>,
}

/// A message that is (most likely) a BIP-39 seed phrase: 12–24 words, at most two of them not
/// in the word list (typos), separated by spaces, commas or new lines, with or without "1." /
/// "1)" numbering. Deliberately generous: a false positive only deletes one operator message.
pub fn looks_like_mnemonic(text: &str) -> bool {
    let words: Vec<String> = text
        .split(|c: char| c.is_whitespace() || c == ',' || c == ';')
        .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase())
        .filter(|w| !w.is_empty() && !w.chars().all(|c| c.is_ascii_digit()))
        .collect();
    if !(12..=24).contains(&words.len()) {
        return false;
    }
    let known = words.iter().filter(|w| bip39::Language::English.find_word(w).is_some()).count();
    known + 2 >= words.len()
}

impl TelegramBot {
    pub fn new(bot: Bot, control: ControlPlane) -> Self {
        Self { bot, control, user_states: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn control(&self) -> &ControlPlane {
        &self.control
    }

    // ── keyboards (kept as associated functions for existing callers) ──

    pub fn main_menu_keyboard(is_running: bool) -> InlineKeyboardMarkup {
        screens::main_menu_keyboard(is_running, None)
    }

    pub fn models_keyboard() -> InlineKeyboardMarkup {
        screens::models_keyboard()
    }

    pub fn prysm_keyboard(is_enabled: bool) -> InlineKeyboardMarkup {
        screens::prysm_keyboard(is_enabled)
    }

    pub fn signals_keyboard() -> InlineKeyboardMarkup {
        screens::signals_keyboard()
    }

    pub fn treasury_keyboard() -> InlineKeyboardMarkup {
        screens::treasury_keyboard()
    }

    /// Settings keyboard with selection marks. `buffer_size == 0` means padding off; the fee
    /// preset is recognised from its priority floor.
    pub fn settings_keyboard(mode: ExecutionMode, sub_count: usize, prio_fee: f64, buffer_size: usize) -> InlineKeyboardMarkup {
        let preset = FeePreset::ALL.into_iter().find(|p| (p.gwei().0 - prio_fee).abs() < 0.2);
        screens::settings_keyboard_parts(mode, sub_count, preset, buffer_size > 0, None)
    }

    /// Only the configured operator chat may control the engine. Anyone can message a bot, and
    /// every handler below moves funds or replaces the wallet. Unset (0) denies everyone.
    fn is_authorized(&self, chat_id: i64) -> bool {
        let authorized = self.control.config().telegram_authorized_chat_id;
        authorized != 0 && chat_id == authorized
    }

    async fn set_state(&self, chat_id: i64, state: UserState) {
        self.user_states.write().await.insert(chat_id, state);
    }

    fn main_keyboard(&self) -> InlineKeyboardMarkup {
        // Two atomic loads: no engine lock.
        screens::main_menu_keyboard(self.control.config().is_engine_running(), self.control.breaker())
    }

    // ── sending ──

    /// HTML body that Telegram will accept, or its plain-text rendering.
    fn prepare(text: &str) -> (String, bool) {
        let fitted = html::fit(text, html::MAX_MESSAGE_CHARS);
        match html::validate(&fitted) {
            Ok(()) => (fitted, true),
            Err(err) => {
                error!("Screen is not valid Telegram HTML ({}): sending plain text", err);
                (html::truncate_plain(&html::to_plain(text), html::MAX_MESSAGE_CHARS), false)
            }
        }
    }

    async fn deliver(
        &self,
        chat_id: i64,
        target: Target,
        text: &str,
        keyboard: Option<&InlineKeyboardMarkup>,
    ) -> Result<(), RequestError> {
        let (mut body, mut as_html) = Self::prepare(text);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let sent = match target {
                Target::Edit(mid) => {
                    let mut req = self.bot.edit_message_text(ChatId(chat_id), mid, body.clone());
                    if as_html {
                        req = req.parse_mode(ParseMode::Html);
                    }
                    if let Some(k) = keyboard {
                        req = req.reply_markup(k.clone());
                    }
                    req.await.map(|_| ())
                }
                Target::New => {
                    let mut req = self.bot.send_message(ChatId(chat_id), body.clone());
                    if as_html {
                        req = req.parse_mode(ParseMode::Html);
                    }
                    if let Some(k) = keyboard {
                        req = req.reply_markup(k.clone());
                    }
                    req.await.map(|_| ())
                }
            };
            match sent {
                Ok(()) => return Ok(()),
                Err(RequestError::Api(ApiError::MessageNotModified)) => return Ok(()),
                Err(RequestError::Api(ApiError::CantParseEntities(e))) if as_html && attempt < REPLY_ATTEMPTS => {
                    warn!("Telegram refused the HTML of a reply ({}): resending as plain text", e);
                    body = html::truncate_plain(&html::to_plain(&body), html::MAX_MESSAGE_CHARS);
                    as_html = false;
                }
                Err(RequestError::RetryAfter(s))
                    if attempt < REPLY_ATTEMPTS && Duration::from_secs(u64::from(s.seconds())) <= REPLY_MAX_RETRY_AFTER =>
                {
                    tokio::time::sleep(Duration::from_secs(u64::from(s.seconds()))).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Show a screen: edit the button's message when there is one (a new message if that fails).
    async fn show(&self, chat_id: i64, edit: Option<MessageId>, text: &str, keyboard: Option<InlineKeyboardMarkup>) {
        if let Some(mid) = edit {
            match self.deliver(chat_id, Target::Edit(mid), text, keyboard.as_ref()).await {
                Ok(()) => return,
                Err(e) => warn!("Telegram edit failed ({}): sending a new message", e),
            }
        }
        if let Err(e) = self.deliver(chat_id, Target::New, text, keyboard.as_ref()).await {
            warn!("Telegram reply failed: {}", e);
        }
    }

    async fn show_error(&self, chat_id: i64, edit: Option<MessageId>, what: &str, err: &str) {
        let text = format!("⚠️ <b>{}</b>: {}", html::escape(what), html::escape(err));
        self.show(chat_id, edit, &text, Some(self.main_keyboard())).await;
    }

    /// Stop the button's spinner (with a short toast if given). Failures only cost the toast.
    async fn answer(&self, q: &CallbackQuery, toast: Option<String>) {
        let mut req = self.bot.answer_callback_query(q.id.clone());
        if let Some(t) = toast {
            req = req.text(t);
        }
        if let Err(e) = req.await {
            warn!("answerCallbackQuery failed: {}", e);
        }
    }

    // ── screens with data ──

    async fn show_dashboard(&self, chat_id: i64, edit: Option<MessageId>) {
        match self.control.snapshot(true).await {
            Ok(snap) => {
                let kb = screens::main_menu_keyboard(snap.engine_running, snap.breaker);
                self.show(chat_id, edit, &screens::dashboard(&snap), Some(kb)).await
            }
            Err(e) => self.show_error(chat_id, edit, "Dashboard tidak tersedia", &e).await,
        }
    }

    async fn show_settings(&self, chat_id: i64, edit: Option<MessageId>, report: Option<&control::ApplyReport>) {
        let mut text = String::new();
        if let Some(r) = report {
            text.push_str(&screens::apply_report(r));
            text.push('\n');
        }
        match self.control.settings().await {
            Ok(view) => {
                text.push_str(&screens::settings(&view));
                self.show(chat_id, edit, &text, Some(screens::settings_keyboard(&view))).await;
            }
            Err(e) => {
                text.push_str(&format!("⚠️ Setelan tidak terbaca: {}", html::escape(&e)));
                self.show(chat_id, edit, &text, Some(screens::back_keyboard("menu_main"))).await;
            }
        }
    }

    async fn show_prysm(&self, chat_id: i64, edit: Option<MessageId>, prefix: Option<String>) {
        match self.control.prysm_view().await {
            Ok(view) => {
                let text = match prefix {
                    Some(p) => format!("{}\n{}", p, screens::prysm_dashboard(&view)),
                    None => screens::prysm_dashboard(&view),
                };
                self.show(chat_id, edit, &text, Some(screens::prysm_keyboard(view.enabled))).await;
            }
            Err(e) => self.show_error(chat_id, edit, "Seri Prysm tidak terbaca", &e).await,
        }
    }

    async fn show_prysm_stats(&self, chat_id: i64, edit: Option<MessageId>) {
        match self.control.prysm_stats().await {
            Ok((counts, recent, max_mcap)) => {
                let text = screens::prysm_stats(&counts, &recent, max_mcap);
                self.show(chat_id, edit, &text, Some(screens::back_keyboard("menu_prysm"))).await;
            }
            Err(e) => self.show_error(chat_id, edit, "Statistik Prysm tidak terbaca", &e).await,
        }
    }

    async fn show_sweep(&self, chat_id: i64, edit: Option<MessageId>) {
        match self.control.open_positions().await {
            Ok(open) => self.show(chat_id, edit, &screens::sweep(&open), Some(self.main_keyboard())).await,
            Err(e) => self.show_error(chat_id, edit, "Posisi tidak terbaca", &e).await,
        }
    }

    /// Circuit breaker: latch + intake off inside `trigger_breaker` (two atomic stores, nothing
    /// awaited before them); the operator gets the "started" screen at once and the per-position
    /// report as a critical notice when the last sell is settled.
    async fn run_breaker(&self, chat_id: i64, edit: Option<MessageId>) {
        let outcome = self.control.trigger_breaker();
        let notifier = self.control.notifier().cloned();
        tokio::spawn(async move {
            let text = match outcome.await {
                Ok(o) => screens::breaker_report(&o),
                Err(e) => format!("❌ <b>Circuit breaker gagal menyelesaikan laporan</b>: {}", html::escape(&e.to_string())),
            };
            match notifier {
                Some(n) => {
                    if !n.notify_critical(chat_id, text) {
                        error!("Circuit breaker report could not be queued");
                    }
                }
                None => info!("Circuit breaker finished (no notifier)"),
            }
        });
        let open = self.control.open_positions().await.map(|p| p.len()).unwrap_or(0);
        self.show(chat_id, edit, &screens::breaker_started(open), Some(self.main_keyboard())).await;
    }

    // ── handlers ──

    /// An edited message only matters if it now holds a seed phrase: delete it.
    pub async fn handle_edited_message(&self, msg: Message) -> Result<()> {
        if !self.is_authorized(msg.chat.id.0) {
            return Ok(());
        }
        if looks_like_mnemonic(msg.text().unwrap_or_default()) {
            if let Err(e) = self.bot.delete_message(msg.chat.id, msg.id).await {
                warn!("Could not delete an edited seed phrase from the chat: {}", e);
            }
            let warning = "🔐 <b>Frasa seed terdeteksi (pesan diedit) dan dihapus.</b> Tidak ada yang diimpor.";
            self.show(msg.chat.id.0, None, warning, Some(screens::treasury_keyboard())).await;
        }
        Ok(())
    }

    /// Dispatch incoming Telegram message
    pub async fn handle_message(&self, msg: Message) -> Result<()> {
        let chat_id = msg.chat.id.0;
        if !self.is_authorized(chat_id) {
            warn!("Ignoring Telegram message from unauthorized chat {}", chat_id);
            return Ok(());
        }
        let text = msg.text().unwrap_or_default().trim().to_string();
        let command = text.split('@').next().unwrap_or_default().to_string();

        // The breaker never waits behind a pending conversation step.
        if command == "/panic" {
            self.set_state(chat_id, UserState::Idle).await;
            self.run_breaker(chat_id, None).await;
            return Ok(());
        }

        let state = self.user_states.read().await.get(&chat_id).cloned().unwrap_or(UserState::Idle);

        // A seed phrase must never stay in the chat history, whatever state the conversation is
        // in (the import step may have been reset by a restart, a menu press or /panic).
        if looks_like_mnemonic(&text) {
            if let Err(e) = self.bot.delete_message(msg.chat.id, msg.id).await {
                warn!("Could not delete a seed phrase from the chat: {}", e);
            }
            if state != UserState::AwaitingMnemonic {
                let warning = "🔐 <b>Frasa seed terdeteksi dan dihapus dari chat.</b>\nTidak ada yang diimpor. Untuk mengganti akun induk: Treasury → 📥 Impor Mnemonic.";
                self.show(chat_id, None, warning, Some(screens::treasury_keyboard())).await;
                return Ok(());
            }
        }

        match state {
            UserState::AwaitingAddAddress => {
                match Address::from_str(&text) {
                    Ok(addr) => {
                        let report = self.control.add_watchlist(addr).await;
                        self.set_state(chat_id, UserState::Idle).await;
                        let text = if report.aborted || report.pending {
                            screens::apply_report(&report)
                        } else {
                            format!("✅ Alamat <code>{:?}</code> ditambahkan ke watchlist.", addr)
                        };
                        self.show(chat_id, None, &text, Some(screens::signals_keyboard())).await;
                    }
                    Err(_) => {
                        self.show(chat_id, None, "❌ Alamat tidak valid. Kirim alamat EVM (<code>0x…</code>):", None).await;
                    }
                }
                return Ok(());
            }
            UserState::AwaitingMnemonic => {
                if !looks_like_mnemonic(&text) {
                    // Deleted as well: a malformed phrase is still mostly a phrase.
                    let _ = self.bot.delete_message(msg.chat.id, msg.id).await;
                }
                self.set_state(chat_id, UserState::Idle).await;
                let reply = if let Some(why) = self.control.wallet_replacement_blocked().await {
                    format!("❌ Mnemonic tidak diganti: {}", html::escape(&why))
                } else {
                    match self.control.replace_wallet(Some(text), chat_id).await {
                        Ok(_) => match self.control.master_address().await {
                            Ok(m) => format!("✅ Mnemonic diimpor.\n\n🔑 <b>Alamat Ayah baru:</b>\n<code>{:?}</code>", m),
                            Err(e) => format!("✅ Mnemonic diimpor, tetapi akun induk tidak terbaca: {}", html::escape(&e)),
                        },
                        // The parser's message could quote part of the phrase: never echo it.
                        Err(e) if e.starts_with("engine belum selesai") => format!("⏳ {}", html::escape(&e)),
                        Err(_) => {
                            warn!("Mnemonic import refused (detail withheld: it may quote the phrase)");
                            "❌ Mnemonic tidak diganti: frasa BIP-39 tidak valid, atau dompet sedang dipakai.".to_string()
                        }
                    }
                };
                self.show(chat_id, None, &reply, Some(screens::treasury_keyboard())).await;
                return Ok(());
            }
            UserState::AwaitingPrysmSize => {
                match text.parse::<f64>() {
                    Ok(size) if size.is_finite() && size > 0.0 => {
                        let report = self.control.set_prysm_size(size).await;
                        self.set_state(chat_id, UserState::Idle).await;
                        self.show_prysm(chat_id, None, Some(screens::apply_report(&report))).await;
                    }
                    _ => {
                        self.show(chat_id, None, "❌ Masukkan angka ETH lebih dari 0 (misal <code>0.02</code>):", None).await;
                    }
                }
                return Ok(());
            }
            UserState::Idle => {}
        }

        match command.as_str() {
            "/pause" | "/resume" => {
                let running = command == "/resume";
                let report = self.control.set_intake(running).await;
                let now = self.control.config().is_engine_running();
                self.show(chat_id, None, &screens::pause_result(&report, now), Some(self.main_keyboard())).await;
            }
            "/sweep" => self.show_sweep(chat_id, None).await,
            "/telemetry" => match self.control.snapshot(false).await {
                Ok(snap) => self.show(chat_id, None, &screens::telemetry(&snap), Some(screens::telemetry_keyboard())).await,
                Err(e) => self.show_error(chat_id, None, "Telemetri tidak tersedia", &e).await,
            },
            "/prysm" => self.show_prysm(chat_id, None, None).await,
            "/settings" => self.show_settings(chat_id, None, None).await,
            _ => self.show_dashboard(chat_id, None).await,
        }
        Ok(())
    }

    /// Dispatch incoming Callback Query from Inline Keyboard buttons
    pub async fn handle_callback(&self, q: CallbackQuery) -> Result<()> {
        let Some(message) = q.message.as_ref() else {
            self.answer(&q, None).await;
            return Ok(());
        };
        let chat_id = message.chat().id.0;
        if !self.is_authorized(chat_id) {
            warn!("Ignoring Telegram callback from unauthorized chat {}", chat_id);
            self.answer(&q, None).await;
            return Ok(());
        }
        let edit = Some(message.id());
        let data = q.data.clone().unwrap_or_default();

        // Breaker first: intake stops before any Telegram round trip (even the spinner answer).
        if data == "op_panic" {
            self.set_state(chat_id, UserState::Idle).await;
            self.run_breaker(chat_id, edit).await;
            self.answer(&q, Some("🚨 Circuit breaker aktif: intake berhenti, likuidasi berjalan".into())).await;
            return Ok(());
        }

        // Runtime parameters: exact whitelist, applied through the control plane.
        if let Some(action) = Action::parse(&data) {
            self.answer(&q, Some("⏳ Menerapkan…".into())).await;
            let report = self.control.apply(action).await;
            match action {
                Action::Intake(_) | Action::ReleaseBreaker(_) => {
                    let now = self.control.config().is_engine_running();
                    self.show(chat_id, edit, &screens::pause_result(&report, now), Some(self.main_keyboard())).await;
                }
                _ => self.show_settings(chat_id, edit, Some(&report)).await,
            }
            return Ok(());
        }

        // Navigation stops its spinner right away; the screen follows.
        self.answer(&q, None).await;
        match data.as_str() {
            "menu_main" => {
                self.set_state(chat_id, UserState::Idle).await;
                self.show_dashboard(chat_id, edit).await;
            }
            "toggle_engine" => {
                // Old dashboards carried a toggle without a target state: never act on it.
                let text = "ℹ️ Tombol ini dari menu lama dan tidak mengubah apa pun. Gunakan tombol Intake pada menu terbaru.";
                self.show(chat_id, edit, text, Some(self.main_keyboard())).await;
            }
            "menu_telemetry" => match self.control.snapshot(false).await {
                Ok(snap) => self.show(chat_id, edit, &screens::telemetry(&snap), Some(screens::telemetry_keyboard())).await,
                Err(e) => self.show_error(chat_id, edit, "Telemetri tidak tersedia", &e).await,
            },
            "menu_signals" => match self.control.watchlist().await {
                Ok(list) => self.show(chat_id, edit, &screens::signals_menu(list.len()), Some(screens::signals_keyboard())).await,
                Err(e) => self.show_error(chat_id, edit, "Watchlist tidak terbaca", &e).await,
            },
            "sig_add" => {
                self.set_state(chat_id, UserState::AwaitingAddAddress).await;
                let text = "➕ <b>TAMBAH ALAMAT PANTAUAN</b>\n\nKirim alamat dompet EVM (<code>0x…</code>) yang ingin dipantau:";
                self.show(chat_id, edit, text, None).await;
            }
            "sig_list" => match self.control.watchlist().await {
                Ok(list) => self.show(chat_id, edit, &screens::watchlist(&list), Some(screens::signals_keyboard())).await,
                Err(e) => self.show_error(chat_id, edit, "Watchlist tidak terbaca", &e).await,
            },
            "sig_del_menu" => match self.control.watchlist().await {
                Ok(list) => {
                    let mut buttons: Vec<Vec<InlineKeyboardButton>> = list
                        .iter()
                        .map(|addr| {
                            let full = format!("{:?}", addr);
                            vec![InlineKeyboardButton::callback(format!("❌ Hapus {}…", &full[..10]), format!("del_addr:{}", full))]
                        })
                        .collect();
                    buttons.push(vec![InlineKeyboardButton::callback("🔙 Kembali", "menu_signals")]);
                    self.show(chat_id, edit, "➖ <b>PILIH ALAMAT YANG AKAN DIHAPUS:</b>", Some(InlineKeyboardMarkup::new(buttons))).await;
                }
                Err(e) => self.show_error(chat_id, edit, "Watchlist tidak terbaca", &e).await,
            },
            s if s.starts_with("del_addr:") => {
                if let Ok(addr) = Address::from_str(&s["del_addr:".len()..]) {
                    let report = self.control.remove_watchlist(addr).await;
                    let text = if report.aborted || report.pending {
                        screens::apply_report(&report)
                    } else {
                        format!("✅ Alamat <code>{:?}</code> dihapus dari watchlist.", addr)
                    };
                    self.show(chat_id, edit, &text, Some(screens::signals_keyboard())).await;
                }
            }
            "menu_treasury" => match self.control.snapshot(true).await {
                Ok(snap) => self.show(chat_id, edit, &screens::treasury(&snap), Some(screens::treasury_keyboard())).await,
                Err(e) => self.show_error(chat_id, edit, "Treasury tidak terbaca", &e).await,
            },
            "tr_new_wallet" => {
                let blocked = self.control.wallet_replacement_blocked().await;
                let keyboard = if blocked.is_some() { screens::back_keyboard("menu_treasury") } else { screens::new_wallet_confirm_keyboard() };
                self.show(chat_id, edit, &screens::new_wallet_confirm(blocked.as_deref()), Some(keyboard)).await;
            }
            "tr_new_wallet_confirm" => {
                let text = match self.control.replace_wallet(None, chat_id).await {
                    Ok(phrase) => match self.control.master_address().await {
                        Ok(m) => screens::new_wallet_created(&m, &phrase),
                        Err(e) => format!("⚠️ Mnemonic dibuat, akun induk tidak terbaca: {}", html::escape(&e)),
                    },
                    Err(e) => format!("❌ Akun induk tidak diganti: {}", html::escape(&e)),
                };
                self.show(chat_id, edit, &text, Some(screens::treasury_keyboard())).await;
            }
            "tr_import_wallet" => match self.control.wallet_replacement_blocked().await {
                Some(why) => {
                    self.show(chat_id, edit, &screens::new_wallet_confirm(Some(&why)), Some(screens::back_keyboard("menu_treasury")))
                        .await
                }
                None => {
                    self.set_state(chat_id, UserState::AwaitingMnemonic).await;
                    let text = "📥 <b>IMPOR MNEMONIC</b>\n\nKirim 12 kata BIP-39 dipisah spasi (pesan Anda langsung dihapus dari chat):";
                    self.show(chat_id, edit, text, None).await;
                }
            },
            "tr_rebalance" => {
                self.show(chat_id, edit, "⛽ <b>Memproses rebalance saldo operasional…</b>\n<i>Hasilnya muncul di pesan ini.</i>", None).await;
                // Sends one tx per carrier and can take a while: off the chat's handler queue.
                let me = self.clone();
                tokio::spawn(async move {
                    let text = match me.control.rebalance().await {
                        Ok(results) => screens::rebalance_result(&results),
                        Err(e) => format!("❌ Rebalance gagal: {}", html::escape(&e)),
                    };
                    me.show(chat_id, edit, &text, Some(screens::treasury_keyboard())).await;
                });
            }
            "menu_settings" => self.show_settings(chat_id, edit, None).await,
            s if s.starts_with("cfg_tp:") => {
                let report = match s["cfg_tp:".len()..].parse::<f64>() {
                    Ok(tp) => self.control.set_take_profit(tp).await,
                    Err(_) => control::ApplyReport { aborted: true, ..Default::default() },
                };
                self.show_settings(chat_id, edit, Some(&report)).await;
            }
            s if s.starts_with("cfg_trail:") => {
                let report = match control::parse_trailing_callback(s) {
                    Some(control::TrailingCommand::Set(pct)) => self.control.set_trailing_stop(pct).await,
                    Some(control::TrailingCommand::ResetToEnv) => self.control.reset_trailing_stop().await,
                    None => control::ApplyReport { aborted: true, ..Default::default() },
                };
                self.show_settings(chat_id, edit, Some(&report)).await;
            }
            s if s.starts_with("cfg_exit:") => {
                let report = match s["cfg_exit:".len()..].parse::<u64>() {
                    Ok(secs) => self.control.set_time_decay(secs).await,
                    Err(_) => control::ApplyReport { aborted: true, ..Default::default() },
                };
                self.show_settings(chat_id, edit, Some(&report)).await;
            }
            s if s.starts_with("cfg_") => {
                // A state-changing callback outside the whitelist (old keyboard, forged data).
                let report = control::ApplyReport {
                    aborted: true,
                    warnings: vec!["Tombol tidak dikenal / kedaluwarsa: tidak ada yang diubah".into()],
                    ..Default::default()
                };
                self.show_settings(chat_id, edit, Some(&report)).await;
            }
            "menu_portfolio" => match self.control.snapshot(true).await {
                Ok(snap) => self.show(chat_id, edit, &screens::portfolio(&snap), Some(screens::back_keyboard("menu_main"))).await,
                Err(e) => self.show_error(chat_id, edit, "Portfolio tidak terbaca", &e).await,
            },
            "op_sweep" => self.show_sweep(chat_id, edit).await,
            "menu_models" => {
                let text = "🔬 <b>MODEL STRATEGI TRADING</b>\n═══════════════════════════\nPilih modul strategi untuk melihat konfigurasi, metrik, dan riwayat pola:";
                self.show(chat_id, edit, text, Some(screens::models_keyboard())).await;
            }
            "menu_prysm" => self.show_prysm(chat_id, edit, None).await,
            "prysm_toggle" => {
                let report = self.control.toggle_prysm().await;
                self.show_prysm(chat_id, edit, Some(screens::apply_report(&report))).await;
            }
            "prysm_set_size" => {
                self.set_state(chat_id, UserState::AwaitingPrysmSize).await;
                let text = "✏️ <b>UBAH MODAL ENTRI SERI PRYSM</b>\n\nKirim jumlah ETH per eksekusi (misal <code>0.01</code>):";
                self.show(chat_id, edit, text, None).await;
            }
            "prysm_toggle_exit" => {
                let report = self.control.toggle_prysm_exit().await;
                self.show_prysm(chat_id, edit, Some(screens::apply_report(&report))).await;
            }
            "prysm_stats" => self.show_prysm_stats(chat_id, edit).await,
            other => warn!("Unknown Telegram callback {:?}", other),
        }
        Ok(())
    }
}

/// Queue key for teloxide's dispatcher: updates of one chat are handled in order, except the
/// circuit breaker, which never waits behind a slow screen (balance reads, rebalance).
#[doc(hidden)]
pub fn route_key(update: &Update) -> Option<i64> {
    let breaker = match &update.kind {
        UpdateKind::CallbackQuery(q) => q.data.as_deref() == Some("op_panic"),
        UpdateKind::Message(m) => m.text().map(|t| t.trim().split('@').next().unwrap_or_default() == "/panic").unwrap_or(false),
        _ => false,
    };
    if breaker { None } else { update.chat().map(|c| c.id.0) }
}

/// Telegram send for the outbox, with errors mapped to the retry policy.
struct TeloxideApi {
    bot: Bot,
}

/// How the outbox should react to a failed send.
pub fn classify_send_error(e: &RequestError) -> SendError {
    match e {
        RequestError::RetryAfter(s) => SendError::RetryAfter(Duration::from_secs(u64::from(s.seconds()))),
        RequestError::Api(ApiError::CantParseEntities(m)) => SendError::Parse(m.clone()),
        RequestError::Network(e) => SendError::Transient(e.to_string()),
        RequestError::Io(e) => SendError::Transient(e.to_string()),
        RequestError::InvalidJson { .. } => SendError::Transient(e.to_string()),
        RequestError::Api(ApiError::Unknown(m)) if m.contains("can't parse entities") => SendError::Parse(m.clone()),
        RequestError::Api(ApiError::Unknown(m))
            if ["Internal Server Error", "Bad Gateway", "Gateway", "Service Unavailable", "restart"].iter().any(|k| m.contains(k)) =>
        {
            SendError::Transient(m.clone())
        }
        other => SendError::Fatal(other.to_string()),
    }
}

impl TelegramApi for TeloxideApi {
    fn send(&self, chat_id: i64, text: String, html: bool) -> BoxFuture<'_, Result<(), SendError>> {
        Box::pin(async move {
            let mut req = self.bot.send_message(ChatId(chat_id), text);
            if html {
                req = req.parse_mode(ParseMode::Html);
            }
            req.await.map(|_| ()).map_err(|e| classify_send_error(&e))
        })
    }
}

/// Run `make()` to completion on a new OS thread with a single-threaded tokio runtime of its own.
/// Nothing it does (blocking included) can occupy a worker of the caller's runtime.
pub fn spawn_io_thread<F, Fut>(name: &str, make: F) -> std::io::Result<std::thread::JoinHandle<()>>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let thread_name = name.to_string();
    std::thread::Builder::new().name(thread_name.clone()).spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => {
                error!("{}: could not build its runtime: {}", thread_name, e);
                return;
            }
        };
        rt.block_on(make());
    })
}

/// The running Telegram interface.
pub struct TelegramService {
    /// Engine-side alert handle (non-blocking).
    pub notifier: Notifier,
    pub thread: std::thread::JoinHandle<()>,
}

/// Start the bot (polling, handlers, outbox) on the "telegram-io" thread. `handles.engine` must
/// be the engine runtime: state changes are executed there.
pub fn start_control_plane(token: String, handles: EngineHandles, policy: OutboxPolicy) -> std::io::Result<TelegramService> {
    let (notifier, outbox_rx) = Notifier::channel(policy);
    let bot_notifier = notifier.clone();
    let thread = spawn_io_thread("telegram-io", move || async move {
        let bot = Bot::new(token);
        tokio::spawn(outbox::run_outbox(Arc::new(TeloxideApi { bot: bot.clone() }), outbox_rx));
        let control = ControlPlane::new(handles).with_notifier(bot_notifier);
        let tg = Arc::new(TelegramBot::new(bot.clone(), control));
        info!("Telegram control plane running on its own thread (polling, handlers, outbox)");

        let tg_msg = tg.clone();
        let tg_edit = tg.clone();
        let tg_cb = tg.clone();
        let handler = dptree::entry()
            .branch(Update::filter_message().endpoint(move |msg: Message| {
                let tg = tg_msg.clone();
                async move {
                    match tokio::time::timeout(HANDLER_BUDGET, tg.handle_message(msg)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => error!("Error handling message: {:#}", e),
                        Err(_) => error!("Message handler exceeded {:?}: abandoned", HANDLER_BUDGET),
                    }
                    respond(())
                }
            }))
            .branch(Update::filter_edited_message().endpoint(move |msg: Message| {
                let tg = tg_edit.clone();
                async move {
                    match tokio::time::timeout(HANDLER_BUDGET, tg.handle_edited_message(msg)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => error!("Error handling an edited message: {:#}", e),
                        Err(_) => error!("Edited-message handler exceeded {:?}: abandoned", HANDLER_BUDGET),
                    }
                    respond(())
                }
            }))
            .branch(Update::filter_callback_query().endpoint(move |q: CallbackQuery| {
                let tg = tg_cb.clone();
                async move {
                    match tokio::time::timeout(HANDLER_BUDGET, tg.handle_callback(q)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => error!("Error handling callback: {:#}", e),
                        Err(_) => error!("Callback handler exceeded {:?}: abandoned", HANDLER_BUDGET),
                    }
                    respond(())
                }
            }));
        teloxide::dispatching::Dispatcher::builder(bot, handler)
            .distribution_function(route_key)
            .build()
            .dispatch()
            .await;
        warn!("Telegram dispatcher stopped");
    })?;
    Ok(TelegramService { notifier, thread })
}
