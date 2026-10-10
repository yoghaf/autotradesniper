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
        positions: Arc<RwLock<Vec<Position>>>,
    ) -> Self {
        let bot = Bot::new(token);
        // Bounded channel to prevent memory bloat during on-chain activity spikes
        let (alert_queue_tx, alert_queue_rx) = mpsc::channel::<QueuedTelegramAlert>(500);

        let bot_clone = bot.clone();
        // Spawn independent async rate-limiting worker (1 msg/sec token bucket)
        tokio::spawn(async move {
            Self::run_rate_limited_alert_worker(bot_clone, alert_queue_rx).await;
        });

        Self {
            bot,
            config,
            wallet_manager,
            risk_manager,
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
                .parse_mode(ParseMode::MarkdownV2)
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
            ],
            vec![
                InlineKeyboardButton::callback("🚨 Panic Sell All", "op_panic"),
                InlineKeyboardButton::callback("🧹 Sweep Assets", "op_sweep"),
            ],
        ])
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
                InlineKeyboardButton::callback("🎯 TP: +25%", "cfg_tp:25"),
                InlineKeyboardButton::callback("🎯 TP: +35%", "cfg_tp:35"),
                InlineKeyboardButton::callback("🎯 TP: +50%", "cfg_tp:50"),
            ],
            vec![
                InlineKeyboardButton::callback("⏱ Exit: 30s", "cfg_exit:30"),
                InlineKeyboardButton::callback("⏱ Exit: 45s", "cfg_exit:45"),
                InlineKeyboardButton::callback("⏱ Exit: 60s", "cfg_exit:60"),
            ],
            vec![
                InlineKeyboardButton::callback("📦 Calibrate Buffer (94.5 KB)", "cfg_buffer_default"),
            ],
            vec![
                InlineKeyboardButton::callback("🔙 Kembali ke Menu Utama", "menu_main"),
            ],
        ])
    }

    /// Format Main Dashboard Summary Text
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
            "🤖 *AUTOTRADE SNIPER ENGINE DASHBOARD*\n\
             ═══════════════════════════\n\
             ⚡ *Engine Status:* {}\n\
             ⛓️ *Network:* Arbitrum Nitro / Robinhood Chain (ID: {})\n\
             \n\
             💼 *Master Account:*\n\
             `{}`\n\
             👥 *Satellite Sub-Accounts:* {} akun aktif\n\
             🎯 *Watchlist Monitored:* {} akun\n\
             \n\
             ⚙️ *Execution Parameters:*\n\
             • *Mode:* {}\n\
             • *Batch Calldata Buffer:* {} bytes (~{:.1} KB)\n\
             • *Static Gas Limit:* {} gas\n\
             • *Entry Trade:* {:.4} ETH\n\
             • *Take Profit Target:* +{:.1}%\n\
             • *Time-Decay Exit:* {} detik\n\
             \n\
             🛡️ *6-Layer Risk Architecture:* ARMED 🟢\n\
             • L1: Trailing Stop (+20% act, -15% peak) & Hard SL (-20%)\n\
             • L2: Quiet Market (30s <0.1 ETH) & Inactivity Decay (25s)\n\
             • L3: Pre-Graduation Guard (90%) & Secondary Pool Trail\n\
             • L4: Mempool Approval Anomaly Sensor (≥3 Approvals)\n\
             • L5: Watchdog Supervisor (5s interval, 600s max hold)\n\
             • L6: Operator Emergency Controls (/panic, /pause, /sweep)\n\
             ═══════════════════════════\n\
             _Gunakan tombol menu di bawah untuk mengelola sistem._",
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
                            format!("✅ Alamat `{}` berhasil ditambahkan ke Watchlist!", addr),
                        )
                        .parse_mode(ParseMode::MarkdownV2)
                        .reply_markup(Self::signals_keyboard())
                        .await?;
                } else {
                    self.bot
                        .send_message(
                            ChatId(chat_id),
                            "❌ Format alamat tidak valid! Masukkan alamat EVM yang benar (misal: `0x...`):",
                        )
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
                                     🔑 *Alamat Master Baru:*\n`{:?}`",
                                    master.address
                                ),
                            )
                            .parse_mode(ParseMode::MarkdownV2)
                            .reply_markup(Self::treasury_keyboard())
                            .await?;
                    }
                    Err(e) => {
                        self.bot
                            .send_message(
                                ChatId(chat_id),
                                format!("❌ Gagal mengimpor mnemonic: {}. Coba masukkan 12 kata kembali:", e),
                            )
                            .await?;
                    }
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
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await?;
            }
            "/status" => {
                let text = self.get_dashboard_text().await;
                self.bot
                    .send_message(ChatId(chat_id), text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .await?;
            }
            "/panic" => {
                let alert_text = EmergencyControls::handle_panic(&self.risk_manager).await;
                let is_running = self.config.is_engine_running();
                self.bot
                    .send_message(ChatId(chat_id), alert_text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await?;
            }
            "/pause" => {
                let alert_text = EmergencyControls::handle_pause(&self.risk_manager).await;
                let is_running = self.config.is_engine_running();
                self.bot
                    .send_message(ChatId(chat_id), alert_text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await?;
            }
            "/sweep" => {
                let alert_text = EmergencyControls::handle_sweep(&self.risk_manager).await;
                let is_running = self.config.is_engine_running();
                self.bot
                    .send_message(ChatId(chat_id), alert_text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await?;
            }
            _ => {
                let dashboard_text = self.get_dashboard_text().await;
                let is_running = self.config.is_engine_running();
                self.bot
                    .send_message(ChatId(chat_id), dashboard_text)
                    .parse_mode(ParseMode::MarkdownV2)
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
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await;
            }
            "toggle_engine" => {
                let is_now_running = self.config.toggle_engine().await;
                let text = self.get_dashboard_text().await;
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::main_menu_keyboard(is_now_running))
                    .await;
            }
            "menu_signals" => {
                let watchlist = self.config.watchlist_accounts.read().await;
                let text = format!(
                    "🎯 *SIGNAL ACCOUNTS MANAGER*\n\
                     ═══════════════════════════\n\
                     Saat ini ada *{}* alamat akun pantauan aktif.\n\
                     Pilih aksi di bawah:",
                    watchlist.len()
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::signals_keyboard())
                    .await;
            }
            "sig_add" => {
                self.user_states.write().await.insert(chat_id, UserState::AwaitingAddAddress);
                let text = "➕ *TAMBAH ALAMAT PANTAUAN*\n\n\
                            Kirimkan alamat dompet EVM (format `0x...`) yang ingin dipantau transaksinya:";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .await;
            }
            "sig_list" => {
                let watchlist = self.config.watchlist_accounts.read().await;
                let mut list_str = String::new();
                for (i, addr) in watchlist.iter().enumerate() {
                    list_str.push_str(&format!("{}\\. `{}`\n", i + 1, addr));
                }
                if list_str.is_empty() {
                    list_str = "_Belum ada alamat dalam daftar pantauan\\._".to_string();
                }

                let text = format!(
                    "📋 *DAFTAR AKUN PANTAUAN (WATCHLIST)*\n\
                     ═══════════════════════════\n\
                     {}\n\
                     ═══════════════════════════",
                    list_str
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
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

                let text = "➖ *PILIH ALAMAT YANG AKAN DIHAPUS:*";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(InlineKeyboardMarkup::new(buttons))
                    .await;
            }
            s if s.starts_with("del_addr:") => {
                let addr_str = &s["del_addr:".len()..];
                if let Ok(addr) = Address::from_str(addr_str) {
                    self.config.remove_watchlist_account(&addr).await;
                    let text = format!("✅ Alamat `{}` berhasil dihapus dari Watchlist!", addr);
                    let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                        .parse_mode(ParseMode::MarkdownV2)
                        .reply_markup(Self::signals_keyboard())
                        .await;
                }
            }
            "menu_treasury" => {
                let summaries = self.wallet_manager.get_treasury_summary().await?;
                let mut summary_text = String::new();
                for s in summaries {
                    summary_text.push_str(&format!(
                        "• *{}*\n  Path: `{}`\n  Alamat: `{}`\n  Saldo: `{:.4} ETH`\n\n",
                        s.role, s.derivation_path, s.address, s.balance_eth
                    ));
                }

                let text = format!(
                    "💼 *TREASURY & WALLETS MANAGER*\n\
                     ═══════════════════════════\n\
                     {}\
                     ═══════════════════════════",
                    summary_text
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::treasury_keyboard())
                    .await;
            }
            "tr_new_wallet" => {
                let new_phrase = self.wallet_manager.generate_new_mnemonic().await?;
                let master = self.wallet_manager.get_master_account().await?;
                let text = format!(
                    "🔑 *AKUN INDUK BARU BERHASIL DIBUAT!*\n\
                     ═══════════════════════════\n\
                     *Alamat Master:*\n`{:?}`\n\n\
                     *12\\-Word Mnemonic Recovery Phrase:*\n\
                     `{}`\n\n\
                     ⚠️ _SIMPAN FRASA INI DENGAN AMAN! Seluruh sub-akun diturunkan dari frasa ini\\._",
                    master.address, new_phrase
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::treasury_keyboard())
                    .await;
            }
            "tr_import_wallet" => {
                self.user_states.write().await.insert(chat_id, UserState::AwaitingMnemonic);
                let text = "📥 *IMPOR FRASA MNEMONIC*\n\n\
                            Kirimkan 12 kata pemulihan BIP\\-39 dipisahkan dengan spasi:";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .await;
            }
            s if s.starts_with("tr_sub:") => {
                let count_str = &s["tr_sub:".len()..];
                if let Ok(count) = count_str.parse::<usize>() {
                    *self.config.sub_account_count.write().await = count;
                    let _ = self.config.save_persisted_state().await;
                    let text = format!("👥 Jumlah Sub\\-Akun Satelit diatur ke *{}* akun\\.", count);
                    let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                        .parse_mode(ParseMode::MarkdownV2)
                        .reply_markup(Self::treasury_keyboard())
                        .await;
                }
            }
            "tr_rebalance" => {
                let _ = self.bot.edit_message_text(
                    ChatId(chat_id),
                    message_id,
                    "⛽ *Memproses Rebalance Saldo Operasional...*",
                ).parse_mode(ParseMode::MarkdownV2).await;

                let rebalances = self.wallet_manager.rebalance_gas().await?;
                let mut res_str = String::new();
                for r in rebalances {
                    let status_icon = if r.success { "✅" } else { "❌" };
                    res_str.push_str(&format!(
                        "{} Alamat `{}`: Alokasi `{:.4} ETH` (TX: `{}`)\n",
                        status_icon, r.sub_account_address, r.amount_eth, r.tx_hash
                    ));
                }
                if res_str.is_empty() {
                    res_str = "Seluruh Sub\\-Akun sudah memiliki saldo di atas ambang batas\\.".to_string();
                }

                let text = format!(
                    "⛽ *HASIL REBALANCE OPERASIONAL*\n\
                     ═══════════════════════════\n\
                     {}\n\
                     ═══════════════════════════",
                    res_str
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::treasury_keyboard())
                    .await;
            }
            "menu_settings" => {
                let mode = *self.config.execution_mode.read().await;
                let text = "⚙️ *EXECUTION SETTINGS*\n\
                            ═══════════════════════════\n\
                            Sesuaikan parameter eksekusi, target profit, dan stop-loss waktu di bawah:";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
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

                let text = format!("🔄 Mode eksekusi diubah menjadi *{}*", new_mode);
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::settings_keyboard(new_mode))
                    .await;
            }
            s if s.starts_with("cfg_tp:") => {
                let tp_str = &s["cfg_tp:".len()..];
                if let Ok(tp) = tp_str.parse::<f64>() {
                    *self.config.target_profit_percent.write().await = tp;
                    let _ = self.config.save_persisted_state().await;
                    let mode = *self.config.execution_mode.read().await;
                    let text = format!("🎯 Target Profit diatur ke *\\+{}%*", tp);
                    let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                        .parse_mode(ParseMode::MarkdownV2)
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
                    let text = format!("⏱ Time\\-Decay Exit limit diatur ke *{} detik*", exit_secs);
                    let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                        .parse_mode(ParseMode::MarkdownV2)
                        .reply_markup(Self::settings_keyboard(mode))
                        .await;
                }
            }
            "cfg_buffer_default" => {
                *self.config.buffer_size.write().await = 94500;
                let _ = self.config.save_persisted_state().await;
                let mode = *self.config.execution_mode.read().await;
                let text = "📦 Calldata buffer dikalibrasi ke *94\\.500 bytes* \\(~94\\.5 KB\\) sesuai kuota batch L2\\.";
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
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
                        "• *{}*: `{:.4} ETH`\n",
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
                        "[{}] *{}*: {}{:.1}% ({})\n",
                        status,
                        p.token_symbol,
                        sign,
                        p.pnl_percent,
                        p.exit_reason.as_deref().unwrap_or("Running")
                    ));
                }
                if pnl_str.is_empty() {
                    pnl_str = "_Belum ada riwayat posisi\\._".to_string();
                }

                let text = format!(
                    "📈 *PORTFOLIO & STATUS PnL*\n\
                     ═══════════════════════════\n\
                     💰 *Total Saldo Treasury:* `{:.4} ETH`\n\n\
                     *Distribusi Saldo:*\n\
                     {}\n\
                     📊 *Statistik Posisi:*\n\
                     • Open Positions: {}\n\
                     • Closed Trades: {}\n\n\
                     *Riwayat PnL Terakhir:*\n\
                     {}\n\
                     ═══════════════════════════",
                    total_eth, wallet_str, open_pos_count, closed_pos_count, pnl_str
                );
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(InlineKeyboardMarkup::new(vec![vec![
                        InlineKeyboardButton::callback("🔙 Kembali ke Menu Utama", "menu_main"),
                    ]]))
                    .await;
            }
            "op_panic" => {
                let text = EmergencyControls::handle_panic(&self.risk_manager).await;
                let is_running = self.config.is_engine_running();
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::main_menu_keyboard(is_running))
                    .await;
            }
            "op_sweep" => {
                let text = EmergencyControls::handle_sweep(&self.risk_manager).await;
                let is_running = self.config.is_engine_running();
                let _ = self.bot.edit_message_text(ChatId(chat_id), message_id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .reply_markup(Self::main_menu_keyboard(is_running))
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
            "🔔 *SINYAL TERDETEKSI!*\n\
             ═══════════════════════════\n\
             👤 *Akun Pantauan:*\n`{:?}`\n\n\
             🪙 *Target Token:*\n`{:?}`\n\n\
             🏊 *Pool / Router:*\n`{:?}`\n\n\
             💵 *Entry Amount:* `{} wei`\n\
             ⛓️ *Blok Sequencer:* `#{}`\n\
             ═══════════════════════════\n\
             _Mengeksekusi batch sequence calldata buffer..._",
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
            tx_str.push_str(&format!("• *{}*:\n  `{}`\n", role, hash));
        }

        let text = format!(
            "🚀 *DISPATCH BERHASIL!*\n\
             ═══════════════════════════\n\
             ⛓️ *Dieksekusi pada Blok:* `#{}`\n\
             ⚙️ *Mode:* {}\n\
             📦 *Calldata Buffer Size:* `{} bytes`\n\
             ⛽ *Gas Limit:* `{}` gas\n\
             ⚡ *Latensi Eksekusi:* `{} µs`\n\n\
             📝 *Daftar Transaksi:*\n\
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
            "📊 *REALISASI POSISI!*\n\
             ═══════════════════════════\n\
             🪙 *Token:* {}\n\
             📈 *Hasil PnL:* `{sign}{:.1}%`\n\
             💵 *Modal Entri:* `{:.4} ETH`\n\
             💰 *Nilai Keluar:* `{:.4} ETH`\n\
             📌 *Alasan Keluar:* {}\n\
             📝 *TX Exit (Calldata standar ~164 byte):*\n`{}`\n\
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
