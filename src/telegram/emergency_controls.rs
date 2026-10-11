use crate::risk::RiskManager;
use crate::telegram::control::BreakerOutcome;
use crate::telegram::screens;
use std::sync::Arc;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};

/// Lapis 6: Operator Emergency Controls.
///
/// The Telegram bot drives these through [`crate::telegram::ControlPlane`] (which also drops queued
/// Prysm orders and staged pre-signed batches and never makes the operator wait for the sells).
/// These helpers are for callers that hold only the risk manager.
pub struct EmergencyControls;

impl EmergencyControls {
    /// Stop intake and liquidate every open position through the receipt-confirmed exit; returns
    /// the per-position report once every sell has been settled.
    pub async fn handle_panic(risk_manager: &Arc<RiskManager>) -> String {
        let report = risk_manager.panic_sell_all_report().await;
        screens::breaker_report(&BreakerOutcome { report, ..BreakerOutcome::default() })
    }

    /// Idempotent pause: freezes new entries while open positions stay supervised (Layers 1-5).
    pub async fn handle_pause(risk_manager: &Arc<RiskManager>) -> String {
        let was_running = !risk_manager.pause_engine().await;
        let report = crate::telegram::control::ApplyReport {
            unchanged: !was_running,
            changes: vec![if was_running { "Engine intake → PAUSE".into() } else { "Engine intake tetap PAUSE".into() }],
            ..Default::default()
        };
        screens::pause_result(&report, false)
    }

    /// Residual-token sweep is not implemented: says so and lists what is held.
    pub async fn handle_sweep(risk_manager: &Arc<RiskManager>) -> String {
        let positions = risk_manager.open_positions_snapshot().await;
        screens::sweep(&positions)
    }

    /// Inline keyboard for emergency controls
    pub fn emergency_keyboard() -> InlineKeyboardMarkup {
        InlineKeyboardMarkup::new(vec![
            vec![
                InlineKeyboardButton::callback("🚨 Panic Sell", "op_panic"),
                InlineKeyboardButton::callback("🧹 Sweep", "op_sweep"),
            ],
            vec![InlineKeyboardButton::callback("🔙 Kembali ke Menu Utama", "menu_main")],
        ])
    }
}
