use crate::risk::RiskManager;
use std::sync::Arc;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};

/// Lapis 6: Operator Emergency Controls Interface
/// Provides direct rapid-response mechanisms for human operators via Telegram
pub struct EmergencyControls;

impl EmergencyControls {
    /// Handler for `/panic` command & `op_panic` inline button
    /// Liquidates all open positions instantaneously and stops signal intake
    pub async fn handle_panic(risk_manager: &Arc<RiskManager>) -> String {
        let liquidated_count = risk_manager.panic_sell_all().await;
        format!(
            "🚨 *EMERGENCY PANIC SELL ACTIVATED\\!*\n\
             ═══════════════════════════\n\
             ⚡ *Status:* Engine intake PAUSED \\[🔴\\]\n\
             🎯 *Posisi Dilikuidasi:* {} posisi aktif\n\
             📦 *Payload:* Calldata standar ~164 byte \\(Zero padding\\)\n\
             🛡️ *Slippage Darurat:* 50\\.0%\n\
             ═══════════════════════════\n\
             _Seluruh posisi terbuka telah ditutup seketika\\. Intake sinyal dinonaktifkan\\._",
            liquidated_count
        )
    }

    /// Handler for `/pause` command
    /// Freezes new signal intake while keeping open positions guarded by Layers 1-5
    pub async fn handle_pause(risk_manager: &Arc<RiskManager>) -> String {
        let is_running = risk_manager.pause_engine().await;
        if !is_running {
            "⏸ *ENGINE INTAKE PAUSED\\!*\n\
             ═══════════════════════════\n\
             ⚡ *Status:* Signal intake BEKU \\[🔴\\]\n\
             🛡️ *Supervisi Risiko:* Tetap AKTIF \\(Lapis 1\\-5 terus mengawal posisi terbuka\\)\n\
             ═══════════════════════════\n\
             _Tidak ada transaksi beli baru yang akan dieksekusi\\._"
                .to_string()
        } else {
            "▶️ *ENGINE INTAKE RESUMED\\!*\n\
             ═══════════════════════════\n\
             ⚡ *Status:* Signal intake AKTIF \\[🟢\\]\n\
             ═══════════════════════════\n\
             _Sistem siap mereplikasi sinyal baru dari watchlist\\._"
                .to_string()
        }
    }

    /// Handler for `/sweep` command & `op_sweep` inline button
    /// Sells remaining tokens back to ETH across Master and Satellite accounts
    pub async fn handle_sweep(risk_manager: &Arc<RiskManager>) -> String {
        let swept_tokens = risk_manager.sweep_assets().await;
        let details = if swept_tokens.is_empty() {
            "_Tidak ada saldo token residual yang terdeteksi\\._".to_string()
        } else {
            swept_tokens
                .iter()
                .map(|t| format!("• `{}`", t.replace('-', "\\-").replace('_', "\\_")))
                .collect::<Vec<_>>()
                .join("\n")
        };

        format!(
            "🧹 *EMERGENCY ASSET SWEEP EXECUTED\\!*\n\
             ═══════════════════════════\n\
             💼 *Target:* Master & Deterministic Satellite Sub\\-Accounts\n\
             🪙 *Token Dikonversi ke ETH:*\n{}\n\
             ═══════════════════════════\n\
             _Saldo residual telah dilikuidasi kembali ke Native ETH\\._",
            details
        )
    }

    /// Inline keyboard for emergency controls confirmation or direct actions
    pub fn emergency_keyboard() -> InlineKeyboardMarkup {
        InlineKeyboardMarkup::new(vec![
            vec![
                InlineKeyboardButton::callback("🚨 Konfirmasi Panic Sell", "op_panic"),
                InlineKeyboardButton::callback("🧹 Konfirmasi Sweep", "op_sweep"),
            ],
            vec![
                InlineKeyboardButton::callback("🔙 Kembali ke Menu Utama", "menu_main"),
            ],
        ])
    }
}
