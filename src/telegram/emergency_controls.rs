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
            "🚨 <b>EMERGENCY PANIC ACTIVATED!</b>\n\
             ═══════════════════════════\n\
             ⚡ <b>Status:</b> Engine intake PAUSED [🔴]\n\
             🎯 <b>Posisi Ditandai Tutup:</b> {} posisi aktif\n\
             ⚠️ <b>Eksekusi:</b> SIMULASI, belum ada transaksi jual on-chain\n\
             ═══════════════════════════\n\
             <i>Intake sinyal dinonaktifkan. Token masih berada di dompet: jual manual sampai executor exit tersedia.</i>",
            liquidated_count
        )
    }

    /// Handler for `/pause` command
    /// Freezes new signal intake while keeping open positions guarded by Layers 1-5
    pub async fn handle_pause(risk_manager: &Arc<RiskManager>) -> String {
        let is_running = risk_manager.pause_engine().await;
        if !is_running {
            "⏸ <b>ENGINE INTAKE PAUSED!</b>\n\
             ═══════════════════════════\n\
             ⚡ <b>Status:</b> Signal intake BEKU [🔴]\n\
             🛡️ <b>Supervisi Risiko:</b> Tetap AKTIF (Lapis 1-5 terus mengawal posisi terbuka)\n\
             ═══════════════════════════\n\
             <i>Tidak ada transaksi beli baru yang akan dieksekusi.</i>"
                .to_string()
        } else {
            "▶️ <b>ENGINE INTAKE RESUMED!</b>\n\
             ═══════════════════════════\n\
             ⚡ <b>Status:</b> Signal intake AKTIF [🟢]\n\
             ═══════════════════════════\n\
             <i>Sistem siap mereplikasi sinyal baru dari watchlist.</i>"
                .to_string()
        }
    }

    /// Handler for `/sweep` command & `op_sweep` inline button
    /// Sells remaining tokens back to ETH across Master and Satellite accounts
    pub async fn handle_sweep(risk_manager: &Arc<RiskManager>) -> String {
        let swept_tokens = risk_manager.sweep_assets().await;
        let details = if swept_tokens.is_empty() {
            "<i>Tidak ada saldo token residual yang terdeteksi.</i>".to_string()
        } else {
            swept_tokens
                .iter()
                .map(|t| format!("• <code>{}</code>", t))
                .collect::<Vec<_>>()
                .join("\n")
        };

        format!(
            "🧹 <b>EMERGENCY ASSET SWEEP (SIMULASI)</b>\n\
             ═══════════════════════════\n\
             💼 <b>Target:</b> Master & Deterministic Satellite Sub-Accounts\n\
             🪙 <b>Posisi Tercatat (belum dijual):</b>\n{}\n\
             ═══════════════════════════\n\
             <i>SIMULASI: sweep on-chain belum diimplementasikan, tidak ada token yang dijual.</i>",
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
