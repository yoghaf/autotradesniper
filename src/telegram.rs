pub mod emergency_controls;

use crate::config::AppConfig;
use crate::risk::RiskManager;
use crate::types::{DispatchReport, ExecutionMode, Position, TradeSignal};
use crate::wallet::WalletManager;
use alloy::primitives::Address;
use anyhow::Result;
use emergency_controls::EmergencyControls;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use teloxide::prelude::*;
use teloxide::types::{ChatId, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode};
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

/// User conversation state for handling interactive text inputs
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserState {
    Idle,
    AwaitingAddAddress,
    AwaitingMnemonic,
    AwaitingPrysmSize,
}

/// Outgoing telemetry alert message payload for rate-limited async dispatch
#[derive(Debug, Clone)]
pub struct QueuedTelegramAlert {
    pub chat_id: i64,
    pub text: String,
}

/// Interactive Telegram Bot Controller with Non-Blocking State Machine & Rate Limiting
pub struct TelegramBot {
    bot: Bot,
    config: AppConfig,
    wallet_manager: WalletManager,
    risk_manager: Arc<RiskManager>,
    prysm_engine: Arc<crate::models::PrysmEngine>,
    positions: Arc<RwLock<Vec<Position>>>,
    user_states: Arc<RwLock<HashMap<i64, UserState>>>,
    alert_queue_tx: mpsc::Sender<QueuedTelegramAlert>,
}

impl TelegramBot {
    pub fn new(
        token: String,
        config: AppConfig,
        wallet_manager: WalletManager,
        risk_manager: Arc<RiskManager>,
        prysm_engine: Arc<crate::models::PrysmEngine>,
        positions: Arc<RwLock<Vec<Position>>>,
    ) -> Self {
        let bot = Bot::new(token);
        // Bounded channel to prevent memory bloat during on-chain activity spikes
        let (alert_queue_tx, alert_queue_rx) = mpsc::channel::<QueuedTelegramAlert>(500);

        // Spawn dedicated background rate-limited message consumer (1 msg/sec limit)
        let bot_clone = bot.clone();
        tokio::spawn(async move {
            Self::run_rate_limited_alert_worker(bot_clone, alert_queue_rx).await;
        });

        Self {
            bot,
            config,
            wallet_manager,
            risk_manager,
            prysm_engine,
            positions,
            user_states: Arc::new(RwLock::new(HashMap::new())),
            alert_queue_tx,
        }
    }

    /// Background worker that throttles messages to prevent Telegram HTTP 429 Rate Limits
    async fn run_rate_limited_alert_worker(
        bot: Bot,
        mut alert_rx: mpsc::Receiver<QueuedTelegramAlert>,
    ) {
        info!("📬 Telegram Rate-Limiting & Message Throttling worker active.");
        while let Some(alert) = alert_rx.recv().await {
            let res = bot
                .send_message(ChatId(alert.chat_id), alert.text)
                .parse_mode(ParseMode::Html)
                .await;

            if let Err(e) = res {
                warn!("Telegram send error (handled by throttler): {}", e);
            }

            // Enforce minimum 1000ms delay between consecutive Telegram messages
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    }

    /// Build Main Dashboard Inline Keyboard
    pub fn main_menu_keyboard(is_running: bool) -> InlineKeyboardMarkup {
        let toggle_engine_text = if is_running {
            "⚡ Bot Engine [ON 🟢]"
        } else {
            "⚡ Bot Engine [OFF 🔴]"
        };

        InlineKeyboardMarkup::new(vec![
            vec![
                InlineKeyboardButton::callback("🎯 Signal Accounts", "menu_signals"),
                InlineKeyboardButton::callback("💼 Treasury & Wallets", "menu_treasury"),
            ],
            vec![
                InlineKeyboardButton::callback("⚙️ Execution Settings", "menu_settings"),
                InlineKeyboardButton::callback(toggle_engine_text, "toggle_engine"),
            ],
            vec![
                InlineKeyboardButton::callback("📈 Portfolio & Status", "menu_portfolio"),
                InlineKeyboardButton::callback("🔬 Model Strategi", "menu_models"),
            ],
            vec![
                InlineKeyboardButton::callback("🚨 Panic Sell All", "op_panic"),
                InlineKeyboardButton::callback("🧹 Sweep Assets", "op_sweep"),
            ],
        ])
    }

    /// Build Strategy Models Sub-Menu Keyboard
    pub fn models_keyboard() -> InlineKeyboardMarkup {
        InlineKeyboardMarkup::new(vec![
            vec![
                InlineKeyboardButton::callback("⚡ Seri Prysm (Fast-Path Pattern)", "menu_prysm"),
            ],
            vec![
                InlineKeyboardButton::callback("🔙 Kembali ke Menu Utama", "menu_main"),
            ],
        ])
    }

    /// Build Prysm Series Dedicated Sub-Menu Keyboard
    pub fn prysm_keyboard(is_enabled: bool) -> InlineKeyboardMarkup {
        let toggle_text = if is_enabled {
            "🔴 NONAKTIFKAN MODEL"
        } else {
            "🟢 AKTIFKAN MODEL"
        };

        InlineKeyboardMarkup::new(vec![
            vec![
                InlineKeyboardButton::callback(toggle_text, "prysm_toggle"),
            ],
            vec![
                InlineKeyboardButton::callback("✏️ Ubah Modal (ETH)", "prysm_set_size"),
                InlineKeyboardButton::callback("🔄 Ganti Mode Exit", "prysm_toggle_exit"),
            ],
            vec![
                InlineKeyboardButton::callback("📊 Statistik & Riwayat Match", "prysm_stats"),
            ],
            vec![
                InlineKeyboardButton::callback("🔙 Kembali ke Menu Utama", "menu_main"),
            ],
        ])
    }

    /// Format Prysm Strategy Dashboard Summary Text
    pub async fn get_prysm_dashboard_text(&self) -> String {
        let is_enabled = self.config.is_prysm_enabled();
        let status_str = if is_enabled { "AKTIF 🟢" } else { "NONAKTIF 🔴" };
        let size_eth = *self.config.prysm_size_eth.read().await;
        let exit_mode = *self.config.prysm_exit_mode.read().await;
        let max_mcap = *self.config.prysm_max_mcap_usd.read().await;

        format!(
            "🔬 <b>STRATEGI: SERI PRYSM (FAST-PATH PATTERN)</b>\n\
             ═══════════════════════════\n\
             • <b>Status Model</b>   : [{}]\n\
             • <b>Modal Entri</b>    : <code>{:.4} ETH</code>\n\
             • <b>Mode Exit</b>      : {}\n\
             • <b>Pola Aktif</b>     : Pola A (Prysmcloud), Pola B (.lat), Pola C (Companion)\n\
             • <b>Max MCap Entri</b> : ${:.0} USD\n\
             • <b>Min Block Guard</b>: +3 Blok (Anti-Launch Tax Protection)\n\
             • <b>Max Usia Sinyal</b>: 15 Detik\n\
             • <b>Toleransi Slip</b> : 25.0%\n\
             ═══════════════════════════\n\
             <i>Sistem mengevaluasi metadata calldata peluncuran token secara mandiri.</i>",
            status_str,
            size_eth,
            exit_mode,
            max_mcap
        )
    }

    /// Format Prysm Match Statistics & History Text
    pub async fn get_prysm_stats_text(&self) -> String {
        let stats = self.prysm_engine.stats();
        let evaluated = stats.total_evaluated.load(Ordering::SeqCst);
        let a = stats.pattern_a_matches.load(Ordering::SeqCst);
        let b = stats.pattern_b_matches.load(Ordering::SeqCst);
        let c = stats.pattern_c_matches.load(Ordering::SeqCst);
        let executed = stats.total_executed.load(Ordering::SeqCst);
        let rej_stale = stats.rejected_staleness.load(Ordering::SeqCst);
        let rej_mcap = stats.rejected_mcap_ceiling.load(Ordering::SeqCst);

        let recents = stats.recent_matches.read().await;
        let mut recent_str = String::new();
        for (i, r) in recents.iter().enumerate() {
            let status = if r.guardrails_passed { "✅ Eksekusi" } else { "❌ Ditolak" };
            recent_str.push_str(&format!(
                "{}. <b>{}</b> - <code>{:?}</code> ({})\n   <i>{}</i>\n",
                i + 1,
                r.pattern,
                r.token_address,
                status,
                r.reason
            ));
        }
        if recent_str.is_empty() {
            recent_str = "<i>Belum ada riwayat kecocokan pola.</i>".to_string();
        }

        format!(
            "📊 <b>STATISTIK & RIWAYAT MATCH SERI PRYSM</b>\n\
             ═══════════════════════════\n\
             🔍 <b>Total Transaksi Dipindai:</b> {}\n\
             🎯 <b>Pola A (Prysmcloud Storage):</b> {} match\n\
             🌐 <b>Pola B (Domain .lat / 0.021 ETH):</b> {} match\n\
             👥 <b>Pola C (Companion Trade Signals):</b> {} match\n\n\
             🚀 <b>Total Order Tereksekusi:</b> {}\n\
             ⏱️ <b>Ditolak (Signal Staleness > 15s):</b> {}\n\
             📈 <b>Ditolak (Market Cap > $8k USD):</b> {}\n\
             ═══════════════════════════\n\
             <b>Riwayat Kecocokan Terakhir:</b>\n\
             {}\n\
             ═══════════════════════════",
            evaluated, a, b, c, executed, rej_stale, rej_mcap, recent_str
        )
    }

    /// Build Signal Accounts Sub-Menu Keyboard
    pub fn signals_keyboard() -> InlineKeyboardMarkup {
        InlineKeyboardMarkup::new(vec![
            vec![
                InlineKeyboardButton::callback("➕ Tambah Alamat", "sig_add"),
                InlineKeyboardButton::callback("➖ Hapus Alamat", "sig_del_menu"),
            ],
            vec![
                InlineKeyboardButton::callback("📋 Lihat Daftar Pantauan", "sig_list"),
            ],
            vec![
                InlineKeyboardButton::callback("🔙 Kembali ke Menu Utama", "menu_main"),
            ],
        ])
    }

    /// Build Treasury Manager Sub-Menu Keyboard
    pub fn treasury_keyboard() -> InlineKeyboardMarkup {
        InlineKeyboardMarkup::new(vec![
            vec![
                InlineKeyboardButton::callback("🔑 Buat Akun Induk Baru", "tr_new_wallet"),
                InlineKeyboardButton::callback("📥 Impor Frasa Mnemonic", "tr_import_wallet"),
            ],
            vec![
                InlineKeyboardButton::callback("👥 Atur 1 Sub-Akun", "tr_sub:1"),
                InlineKeyboardButton::callback("👥 Atur 2 Sub-Akun", "tr_sub:2"),
                InlineKeyboardButton::callback("👥 Atur 3 Sub-Akun", "tr_sub:3"),
            ],
            vec![
                InlineKeyboardButton::callback("⛽ Rebalance Saldo Operasional", "tr_rebalance"),
            ],
            vec![
                InlineKeyboardButton::callback("🔙 Kembali ke Menu Utama", "menu_main"),
            ],
        ])
    }

    /// Build Execution Settings Sub-Menu Keyboard
    pub fn settings_keyboard(mode: ExecutionMode) -> InlineKeyboardMarkup {
        let mode_btn_text = match mode {
            ExecutionMode::Single => "🔄 Mode: Single [Switch to Multi]",
            ExecutionMode::MultiAccount => "🔄 Mode: Multi [Switch to Single]",
        };

        InlineKeyboardMarkup::new(vec![
            vec![
                InlineKeyboardButton::callback(mode_btn_text, "cfg_toggle_mode"),
            ],
            vec![
                InlineKeyboardButton::callback("🎯 Target Profit: +25%", "cfg_tp:25.0"),
                InlineKeyboardButton::callback("🎯 Target Profit: +35%", "cfg_tp:35.0"),
                InlineKeyboardButton::callback("🎯 Target Profit: +50%", "cfg_tp:50.0"),
            ],
            vec![
                InlineKeyboardButton::callback("⏱ Exit Decay: 30s", "cfg_exit:30"),
                InlineKeyboardButton::callback("⏱ Exit Decay: 45s", "cfg_exit:45"),
                InlineKeyboardButton::callback("⏱ Exit Decay: 60s", "cfg_exit:60"),
            ],
            vec![
                InlineKeyboardButton::callback("📦 Calldata Buffer: 94.500b", "cfg_buffer_default"),
            ],
            vec![
                InlineKeyboardButton::callback("🔙 Kembali ke Menu Utama", "menu_main"),
            ],
        ])
    }

    /// Format Main Dashboard Summary Text (Clean HTML Format)
    pub async fn get_dashboard_text(&self) -> String {
        let is_running = self.config.is_engine_running();
        let status_emoji = if is_running { "🟢 ACTIVE" } else { "🔴 PAUSED" };
        let mode = *self.config.execution_mode.read().await;
        let buffer_size = *self.config.buffer_size.read().await;
        let gas_limit = *self.config.gas_limit.read().await;
        let trade_eth = *self.config.default_trade_eth.read().await;
        let tp = *self.config.target_profit_percent.read().await;
        let exit_secs = *self.config.time_decay_exit_seconds.read().await;
        let watchlist_count = self.config.watchlist_accounts.read().await.len();
        let sub_count = *self.config.sub_account_count.read().await;

        let master_acc = self.wallet_manager.get_master_account().await;
        let master_addr = master_acc
            .map(|a| format!("{:?}", a.address))
            .unwrap_or_else(|_| "Not configured".to_string());

        format!(
            "🤖 <b>AUTOTRADE SNIPER ENGINE DASHBOARD</b>\n\
             ═══════════════════════════\n\
             ⚡ <b>Engine Status:</b> {}\n\
             ⛓️ <b>Network:</b> Arbitrum Nitro / Robinhood Chain (ID: {})\n\
             \n\
             💼 <b>Master Account:</b>\n\
             <code>{}</code>\n\
             👥 <b>Satellite Sub-Accounts:</b> {} akun aktif\n\
             🎯 <b>Watchlist Monitored:</b> {} akun\n\
             \n\
             ⚙️ <b>Execution Parameters:</b>\n\
             • <b>Mode:</b> {}\n\
             • <b>Batch Calldata Buffer:</b> {} bytes (~{:.1} KB)\n\
             • <b>Static Gas Limit:</b> {} gas\n\
             • <b>Entry Trade:</b> {:.4} ETH\n\
             • <b>Take Profit Target:</b> +{:.1}%\n\
             • <b>Time-Decay Exit:</b> {} detik\n\
             \n\
             🛡️ <b>6-Layer Risk Architecture:</b> ARMED 🟢\n\
             • L1: Trailing Stop (+20% act, -15% peak) &amp; Hard SL (-20%)\n\
             • L2: Quiet Market (30s &lt;0.1 ETH) &amp; Inactivity Decay (25s)\n\
             • L3: Pre-Graduation Guard (90%) &amp; Secondary Pool Trail\n\
             • L4: Mempool Approval Anomaly Sensor (≥3 Approvals)\n\
             • L5: Watchdog Supervisor (5s interval, 600s max hold)\n\
             • L6: Operator Emergency Controls (/panic, /pause, /sweep)\n\
             ═══════════════════════════\n\
             <i>Gunakan tombol menu di bawah untuk mengelola sistem.</i>",
            status_emoji,
            self.config.chain_id,
            master_addr,
            sub_count,
            watchlist_count,
            mode,
            buffer_size,
            buffer_size as f64 / 1024.0,
            gas_limit,
            trade_eth,
            tp,
            exit_secs
        )
    }

    /// Dispatch incoming Telegram message
    pub async fn handle_message(&self, msg: Message) -> Result<()> {
        let chat_id = msg.chat.id.0;
        let text = msg.text().unwrap_or_default().trim();

        let current_state = {
            let states = self.user_states.read().await;
            states.get(&chat_id).cloned().unwrap_or(UserState::Idle)
        };

        match current_state {
            UserState::AwaitingAddAddress => {
                if let Ok(addr) = Address::from_str(text) {
                    self.config.add_watchlist_account(addr).await;
                    self.user_states.write().await.insert(chat_id, UserState::Idle);

                    self.bot
                        .send_message(
                            ChatId(chat_id),
                            format!("✅ Alamat <code>{}</code> berhasil ditambahkan ke Watchlist!", addr),
                        )
                        .parse_mode(ParseMode::Html)
                        .reply_markup(Self::signals_keyboard())
                        .await?;
                } else {
                    self.bot
                        .send_message(
                            ChatId(chat_id),
                            "❌ Format alamat tidak valid! Masukkan alamat EVM yang benar (misal: <code>0x...</code>):",
                        )
                        .parse_mode(ParseMode::Html)
                        .await?;
                }
                return Ok(());
            }
            UserState::AwaitingMnemonic => {
                match self.wallet_manager.set_mnemonic(text).await {
                    Ok(_) => {
                        self.user_states.write().await.insert(chat_id, UserState::Idle);
                        let master = self.wallet_manager.get_master_account().await?;
                        self.bot
                            .send_message(
                                ChatId(chat_id),
                                format!(
                                    "✅ Frasa Mnemonic berhasil diimpor!\n\n\
                                     🔑 <b>Alamat Master Baru:</b>\n<code>{:?}</code>",
                                    master.address
                                ),
                            )
                            .parse_mode(ParseMode::Html)
                            .reply_markup(Self::treasury_keyboard())
                            .await?;
                    }
                    Err(e) => {
                        self.bot
                            .send_message(
                                ChatId(chat_id),
                                format!("❌ Gagal mengimpor mnemonic: {}. Coba masukkan 12 kata kembali:", e),
                            )
                            .parse_mode(ParseMode::Html)
                            .await?;
                    }
                }
                return Ok(());
            }
            UserState::AwaitingPrysmSize => {
                if let Ok(size) = text.parse::<f64>() {
                    if size > 0.0 {
                        self.config.set_prysm_size_eth(size).await;
                        self.user_states.write().await.insert(chat_id, UserState::Idle);

                        let p_text = self.get_prysm_dashboard_text().await;
                        let is_enabled = self.config.is_prysm_enabled();
                        self.bot
                            .send_message(
                                ChatId(chat_id),
                                format!("✅ Modal entri Seri Prysm diubah menjadi <code>{:.4} ETH</code>!\n\n{}", size, p_text),
                            )
                            .parse_mode(ParseMode::Html)
                            .reply_markup(Self::prysm_keyboard(is_enabled))
                            .await?;
                    } else {
                        self.bot.send_message(ChatId(chat_id), "❌ Nilai modal harus lebih besar dari 0 ETH:").await?;
                    }
                } else {
                    self.bot.send_message(ChatId(chat_id), "❌ Format angka tidak valid! Masukkan angka desimal (misal: <code>0.02</code>):").parse_mode(ParseMode::Html).await?;
                }
                return Ok(());
            }
            UserState::Idle => {}
        }

        match text {
            "/start" | "/menu" => {
                let dashboard_text = self.get_dashboard_text().await;
                let is_running = self.config.is_engine_running();
                self.bot
                    .send_message(ChatId(chat_id), dashboard_text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await?;
            }
            "/prysm" => {
                let text = self.get_prysm_dashboard_text().await;
                let is_enabled = self.config.is_prysm_enabled();
                self.bot
                    .send_message(ChatId(chat_id), text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::prysm_keyboard(is_enabled))
                    .await?;
            }
            "/status" => {
                let text = self.get_dashboard_text().await;
                self.bot
                    .send_message(ChatId(chat_id), text)
                    .parse_mode(ParseMode::Html)
                    .await?;
            }
            "/panic" => {
                let alert_text = EmergencyControls::handle_panic(&self.risk_manager).await;
                let is_running = self.config.is_engine_running();
                self.bot
                    .send_message(ChatId(chat_id), alert_text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await?;
            }
            "/pause" => {
                let alert_text = EmergencyControls::handle_pause(&self.risk_manager).await;
                let is_running = self.config.is_engine_running();
                self.bot
                    .send_message(ChatId(chat_id), alert_text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await?;
            }
            "/sweep" => {
                let alert_text = EmergencyControls::handle_sweep(&self.risk_manager).await;
                let is_running = self.config.is_engine_running();
                self.bot
                    .send_message(ChatId(chat_id), alert_text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await?;
            }
            _ => {
                let dashboard_text = self.get_dashboard_text().await;
                let is_running = self.config.is_engine_running();
                self.bot
                    .send_message(ChatId(chat_id), dashboard_text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await?;
            }
        }

        Ok(())
    }

    /// Dispatch incoming Callback Query from Inline Keyboard buttons
    pub async fn handle_callback(&self, q: CallbackQuery) -> Result<()> {
        let data = q.data.unwrap_or_default();
        let message = match q.message {
            Some(m) => m,
            None => return Ok(()),
        };
        let chat_id = message.chat().id.0;
        let message_id = message.id();

        match data.as_str() {
            "menu_main" => {
                self.user_states.write().await.insert(chat_id, UserState::Idle);
                let text = self.get_dashboard_text().await;
                let is_running = self.config.is_engine_running();
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await;
            }
            "toggle_engine" => {
                let is_now_running = self.config.toggle_engine().await;
                let text = self.get_dashboard_text().await;
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::main_menu_keyboard(is_now_running))
                    .await;
            }
            "menu_signals" => {
                let watchlist = self.config.watchlist_accounts.read().await;
                let text = format!(
                    "🎯 <b>SIGNAL ACCOUNTS MANAGER</b>\n\
                     ═══════════════════════════\n\
                     Saat ini ada <b>{}</b> alamat akun pantauan aktif.\n\
                     Pilih aksi di bawah:",
                    watchlist.len()
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::signals_keyboard())
                    .await;
            }
            "sig_add" => {
                self.user_states.write().await.insert(chat_id, UserState::AwaitingAddAddress);
                let text = "➕ <b>TAMBAH ALAMAT PANTAUAN</b>\n\n\
                            Kirimkan alamat dompet EVM (format <code>0x...</code>) yang ingin dipantau transaksinya:";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .await;
            }
            "sig_list" => {
                let watchlist = self.config.watchlist_accounts.read().await;
                let mut list_str = String::new();
                for (i, addr) in watchlist.iter().enumerate() {
                    list_str.push_str(&format!("{}. <code>{}</code>\n", i + 1, addr));
                }
                if list_str.is_empty() {
                    list_str = "<i>Belum ada alamat dalam daftar pantauan.</i>".to_string();
                }

                let text = format!(
                    "📋 <b>DAFTAR AKUN PANTAUAN (WATCHLIST)</b>\n\
                     ═══════════════════════════\n\
                     {}\n\
                     ═══════════════════════════",
                    list_str
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::signals_keyboard())
                    .await;
            }
            "sig_del_menu" => {
                let watchlist = self.config.watchlist_accounts.read().await;
                let mut buttons = Vec::new();
                for addr in watchlist.iter() {
                    let short_addr = format!("{:?}", addr);
                    let label = format!("❌ Hapus {}", &short_addr[0..10]);
                    let callback_data = format!("del_addr:{}", short_addr);
                    buttons.push(vec![InlineKeyboardButton::callback(label, callback_data)]);
                }
                buttons.push(vec![InlineKeyboardButton::callback("🔙 Kembali", "menu_signals")]);

                let text = "➖ <b>PILIH ALAMAT YANG AKAN DIHAPUS:</b>";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(InlineKeyboardMarkup::new(buttons))
                    .await;
            }
            s if s.starts_with("del_addr:") => {
                let addr_str = &s["del_addr:".len()..];
                if let Ok(addr) = Address::from_str(addr_str) {
                    self.config.remove_watchlist_account(&addr).await;
                    let text = format!("✅ Alamat <code>{}</code> berhasil dihapus dari Watchlist!", addr);
                    let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                        .parse_mode(ParseMode::Html)
                        .reply_markup(Self::signals_keyboard())
                        .await;
                }
            }
            "menu_treasury" => {
                let summaries = self.wallet_manager.get_treasury_summary().await?;
                let mut summary_text = String::new();
                for s in summaries {
                    summary_text.push_str(&format!(
                        "• <b>{}</b>\n  Path: <code>{}</code>\n  Alamat: <code>{}</code>\n  Saldo: <code>{:.4} ETH</code>\n\n",
                        s.role, s.derivation_path, s.address, s.balance_eth
                    ));
                }

                let text = format!(
                    "💼 <b>TREASURY & WALLETS MANAGER</b>\n\
                     ═══════════════════════════\n\
                     {}\
                     ═══════════════════════════",
                    summary_text
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::treasury_keyboard())
                    .await;
            }
            "tr_new_wallet" => {
                let new_phrase = self.wallet_manager.generate_new_mnemonic().await?;
                let master = self.wallet_manager.get_master_account().await?;
                let text = format!(
                    "🔑 <b>AKUN INDUK BARU BERHASIL DIBUAT!</b>\n\
                     ═══════════════════════════\n\
                     <b>Alamat Master:</b>\n<code>{:?}</code>\n\n\
                     <b>12-Word Mnemonic Recovery Phrase:</b>\n\
                     <code>{}</code>\n\n\
                     ⚠️ <i>SIMPAN FRASA INI DENGAN AMAN! Seluruh sub-akun diturunkan dari frasa ini.</i>",
                    master.address, new_phrase
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::treasury_keyboard())
                    .await;
            }
            "tr_import_wallet" => {
                self.user_states.write().await.insert(chat_id, UserState::AwaitingMnemonic);
                let text = "📥 <b>IMPOR FRASA MNEMONIC</b>\n\n\
                            Kirimkan 12 kata pemulihan BIP-39 dipisahkan dengan spasi:";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .await;
            }
            s if s.starts_with("tr_sub:") => {
                let count_str = &s["tr_sub:".len()..];
                if let Ok(count) = count_str.parse::<usize>() {
                    *self.config.sub_account_count.write().await = count;
                    let _ = self.config.save_persisted_state().await;
                    let text = format!("👥 Jumlah Sub-Akun Satelit diatur ke <b>{}</b> akun.", count);
                    let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                        .parse_mode(ParseMode::Html)
                        .reply_markup(Self::treasury_keyboard())
                        .await;
                }
            }
            "tr_rebalance" => {
                let _ = self.bot.edit_message_text(
                    ChatId(chat_id),
                    message_id,
                    "⛽ <b>Memproses Rebalance Saldo Operasional...</b>",
                ).parse_mode(ParseMode::Html).await;

                let rebalances = self.wallet_manager.rebalance_gas().await?;
                let mut res_str = String::new();
                for r in rebalances {
                    let status_icon = if r.success { "✅" } else { "❌" };
                    res_str.push_str(&format!(
                        "{} Alamat <code>{}</code>: Alokasi <code>{:.4} ETH</code> (TX: <code>{}</code>)\n",
                        status_icon, r.sub_account_address, r.amount_eth, r.tx_hash
                    ));
                }
                if res_str.is_empty() {
                    res_str = "Seluruh Sub-Akun sudah memiliki saldo di atas ambang batas.".to_string();
                }

                let text = format!(
                    "⛽ <b>HASIL REBALANCE OPERASIONAL</b>\n\
                     ═══════════════════════════\n\
                     {}\n\
                     ═══════════════════════════",
                    res_str
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::treasury_keyboard())
                    .await;
            }
            "menu_settings" => {
                let mode = *self.config.execution_mode.read().await;
                let text = "⚙️ <b>EXECUTION SETTINGS</b>\n\
                            ═══════════════════════════\n\
                            Sesuaikan parameter eksekusi, target profit, dan stop-loss waktu di bawah:";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::settings_keyboard(mode))
                    .await;
            }
            "cfg_toggle_mode" => {
                let mut mode_lock = self.config.execution_mode.write().await;
                *mode_lock = match *mode_lock {
                    ExecutionMode::Single => ExecutionMode::MultiAccount,
                    ExecutionMode::MultiAccount => ExecutionMode::Single,
                };
                let new_mode = *mode_lock;
                drop(mode_lock);
                let _ = self.config.save_persisted_state().await;

                let text = format!("🔄 Mode eksekusi diubah menjadi <b>{}</b>", new_mode);
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::settings_keyboard(new_mode))
                    .await;
            }
            s if s.starts_with("cfg_tp:") => {
                let tp_str = &s["cfg_tp:".len()..];
                if let Ok(tp) = tp_str.parse::<f64>() {
                    *self.config.target_profit_percent.write().await = tp;
                    let _ = self.config.save_persisted_state().await;
                    let mode = *self.config.execution_mode.read().await;
                    let text = format!("🎯 Target Profit diatur ke <b>+{}%</b>", tp);
                    let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                        .parse_mode(ParseMode::Html)
                        .reply_markup(Self::settings_keyboard(mode))
                        .await;
                }
            }
            s if s.starts_with("cfg_exit:") => {
                let exit_str = &s["cfg_exit:".len()..];
                if let Ok(exit_secs) = exit_str.parse::<u64>() {
                    *self.config.time_decay_exit_seconds.write().await = exit_secs;
                    let _ = self.config.save_persisted_state().await;
                    let mode = *self.config.execution_mode.read().await;
                    let text = format!("⏱ Time-Decay Exit limit diatur ke <b>{} detik</b>", exit_secs);
                    let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                        .parse_mode(ParseMode::Html)
                        .reply_markup(Self::settings_keyboard(mode))
                        .await;
                }
            }
            "cfg_buffer_default" => {
                *self.config.buffer_size.write().await = 94500;
                let _ = self.config.save_persisted_state().await;
                let mode = *self.config.execution_mode.read().await;
                let text = "📦 Calldata buffer dikalibrasi ke <b>94.500 bytes</b> (~94.5 KB) sesuai kuota batch L2.";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::settings_keyboard(mode))
                    .await;
            }
            "menu_portfolio" => {
                let summaries = self.wallet_manager.get_treasury_summary().await?;
                let mut wallet_str = String::new();
                let mut total_eth = 0.0;
                for w in summaries {
                    total_eth += w.balance_eth;
                    wallet_str.push_str(&format!(
                        "• <b>{}</b>: <code>{:.4} ETH</code>\n",
                        w.role, w.balance_eth
                    ));
                }

                let pos_lock = self.positions.read().await;
                let open_pos_count = pos_lock.iter().filter(|p| p.is_open).count();
                let closed_pos_count = pos_lock.iter().filter(|p| !p.is_open).count();

                let mut pnl_str = String::new();
                for p in pos_lock.iter().rev().take(5) {
                    let status = if p.is_open { "🟢 OPEN" } else { "⚪ CLOSED" };
                    let sign = if p.pnl_percent >= 0.0 { "+" } else { "" };
                    pnl_str.push_str(&format!(
                        "[{}] <b>{}</b>: {}{:.1}% ({})\n",
                        status,
                        p.token_symbol,
                        sign,
                        p.pnl_percent,
                        p.exit_reason.as_deref().unwrap_or("Running")
                    ));
                }
                if pnl_str.is_empty() {
                    pnl_str = "<i>Belum ada riwayat posisi.</i>".to_string();
                }

                let text = format!(
                    "📈 <b>PORTFOLIO & STATUS PnL</b>\n\
                     ═══════════════════════════\n\
                     💰 <b>Total Saldo Treasury:</b> <code>{:.4} ETH</code>\n\n\
                     <b>Distribusi Saldo:</b>\n\
                     {}\n\
                     📊 <b>Statistik Posisi:</b>\n\
                     • Open Positions: {}\n\
                     • Closed Trades: {}\n\n\
                     <b>Riwayat PnL Terakhir:</b>\n\
                     {}\n\
                     ═══════════════════════════",
                    total_eth, wallet_str, open_pos_count, closed_pos_count, pnl_str
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(InlineKeyboardMarkup::new(vec![vec![
                        InlineKeyboardButton::callback("🔙 Kembali ke Menu Utama", "menu_main"),
                    ]]))
                    .await;
            }
            "op_panic" => {
                let text = EmergencyControls::handle_panic(&self.risk_manager).await;
                let is_running = self.config.is_engine_running();
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await;
            }
            "op_sweep" => {
                let text = EmergencyControls::handle_sweep(&self.risk_manager).await;
                let is_running = self.config.is_engine_running();
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await;
            }
            "menu_models" => {
                let text = "🔬 <b>MODEL STRATEGI TRADING</b>\n\
                            ═══════════════════════════\n\
                            Pilih modul strategi mandiri di bawah untuk melihat konfigurasi, \
                            status metrik, dan riwayat pola:";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::models_keyboard())
                    .await;
            }
            "menu_prysm" => {
                let text = self.get_prysm_dashboard_text().await;
                let is_enabled = self.config.is_prysm_enabled();
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::prysm_keyboard(is_enabled))
                    .await;
            }
            "prysm_toggle" => {
                let is_now_enabled = self.config.toggle_prysm_strategy().await;
                let text = self.get_prysm_dashboard_text().await;
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::prysm_keyboard(is_now_enabled))
                    .await;
            }
            "prysm_set_size" => {
                self.user_states.write().await.insert(chat_id, UserState::AwaitingPrysmSize);
                let text = "✏️ <b>UBAH MODAL ENTRI SERI PRYSM</b>\n\n\
                            Kirimkan jumlah ETH untuk setiap eksekusi Seri Prysm (misal: <code>0.01</code> atau <code>0.025</code>):";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .await;
            }
            "prysm_toggle_exit" => {
                let _ = self.config.toggle_prysm_exit_mode().await;
                let text = self.get_prysm_dashboard_text().await;
                let is_enabled = self.config.is_prysm_enabled();
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(Self::prysm_keyboard(is_enabled))
                    .await;
            }
            "prysm_stats" => {
                let text = self.get_prysm_stats_text().await;
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::Html)
                    .reply_markup(InlineKeyboardMarkup::new(vec![vec![
                        InlineKeyboardButton::callback("🔙 Kembali ke Seri Prysm", "menu_prysm"),
                    ]]))
                    .await;
            }
            _ => {}
        }

        Ok(())
    }

    /// Non-blocking alert enqueue: Takes < 100ns and never halts the execution engine
    pub async fn enqueue_alert(&self, chat_id: i64, text: String) {
        if chat_id == 0 {
            return;
        }
        let _ = self.alert_queue_tx.try_send(QueuedTelegramAlert { chat_id, text });
    }

    /// Broadcast live alert when on-chain signal is detected (Non-blocking)
    pub async fn broadcast_signal_detected(&self, target_chat: i64, signal: &TradeSignal) {
        let text = format!(
            "🔔 <b>SINYAL TERDETEKSI!</b>\n\
             ═══════════════════════════\n\
             👤 <b>Akun Pantauan:</b>\n<code>{:?}</code>\n\n\
             🪙 <b>Target Token:</b>\n<code>{:?}</code>\n\n\
             🏊 <b>Pool / Router:</b>\n<code>{:?}</code>\n\n\
             💵 <b>Entry Amount:</b> <code>{} wei</code>\n\
             ⛓️ <b>Blok Sequencer:</b> <code>#{}</code>\n\
             ═══════════════════════════\n\
             <i>Mengeksekusi batch sequence calldata buffer...</i>",
            signal.monitored_account,
            signal.target_token,
            signal.pool_or_router,
            signal.entry_amount_wei,
            signal.block_number
        );
        self.enqueue_alert(target_chat, text).await;
    }

    /// Broadcast live alert when transaction dispatch succeeds (Non-blocking)
    pub async fn broadcast_dispatch_success(&self, target_chat: i64, report: &DispatchReport) {
        let mut tx_str = String::new();
        for (i, hash) in report.tx_hashes.iter().enumerate() {
            let role = report.account_roles.get(i).map(|s| s.as_str()).unwrap_or("Account");
            tx_str.push_str(&format!("• <b>{}</b>:\n  <code>{}</code>\n", role, hash));
        }

        let text = format!(
            "🚀 <b>DISPATCH BERHASIL!</b>\n\
             ═══════════════════════════\n\
             ⛓️ <b>Dieksekusi pada Blok:</b> <code>#{}</code>\n\
             ⚙️ <b>Mode:</b> {}\n\
             📦 <b>Calldata Buffer Size:</b> <code>{} bytes</code>\n\
             ⛽ <b>Gas Limit:</b> <code>{}</code> gas\n\
             ⚡ <b>Latensi Eksekusi:</b> <code>{} µs</code>\n\n\
             📝 <b>Daftar Transaksi:</b>\n\
             {}\
             ═══════════════════════════",
            report.block_number,
            report.execution_mode,
            report.calldata_size_bytes,
            report.gas_limit,
            report.latency_micros,
            tx_str
        );
        self.enqueue_alert(target_chat, text).await;
    }

    /// Broadcast live alert when position is liquidated or target profit reached (Non-blocking)
    pub async fn broadcast_position_realized(&self, target_chat: i64, pos: &Position) {
        let sign = if pos.pnl_percent >= 0.0 { "+" } else { "" };
        let reason = pos.exit_reason.as_deref().unwrap_or("Manual Close");
        let tx_hash = pos.exit_tx_hash.as_deref().unwrap_or("0x0");

        let text = format!(
            "📊 <b>REALISASI POSISI!</b>\n\
             ═══════════════════════════\n\
             🪙 <b>Token:</b> {}\n\
             📈 <b>Hasil PnL:</b> <code>{sign}{:.1}%</code>\n\
             💵 <b>Modal Entri:</b> <code>{:.4} ETH</code>\n\
             💰 <b>Nilai Keluar:</b> <code>{:.4} ETH</code>\n\
             📌 <b>Alasan Keluar:</b> {}\n\
             📝 <b>TX Exit (Calldata standar ~164 byte):</b>\n<code>{}</code>\n\
             ═══════════════════════════",
            pos.token_symbol,
            pos.pnl_percent,
            pos.entry_eth_amount,
            pos.current_eth_value,
            reason,
            tx_hash
        );
        self.enqueue_alert(target_chat, text).await;
    }
}
