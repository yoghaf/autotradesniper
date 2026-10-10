use crate::risk::types::{ExitReason, ExitSignal, PositionTracker, RiskConfig};
use alloy::primitives::Address;
use std::str::FromStr;
use std::time::Instant;

/// Layer 5: Fault-Tolerant Watchdog Supervisor (Evaluated every 5 seconds)
pub struct WatchdogSupervisor;

impl WatchdogSupervisor {
    /// Evaluate position for extreme drawdown, max hold duration, or quote timeout
    pub fn evaluate_position(
        tracker: &PositionTracker,
        now: Instant,
        config: &RiskConfig,
    ) -> Option<ExitSignal> {
        let token_addr = Address::from_str(&tracker.position.token_address).ok()?;
        let pool_addr = Address::from_str(&tracker.position.pool_address).ok()?;

        // 1. Watchdog Drawdown Ekstrem (`watchdog_rugi`): >= 30% loss
        if tracker.position.pnl_percent <= -config.watchdog_loss_percent {
            return Some(ExitSignal {
                position_id: tracker.position.id.clone(),
                token_address: token_addr,
                pool_address: pool_addr,
                reason: ExitReason::WatchdogDrawdown,
                reason_description: format!(
                    "🛡️ WATCHDOG EXTREME LOSS TRIGGERED: Drawdown {:.2}% >= {:.2}% emergency threshold! Unconditional liquidation.",
                    tracker.position.pnl_percent, config.watchdog_loss_percent
                ),
                is_emergency: true,
                slippage_percent: 50.0,
            });
        }

        // 2. Watchdog Durasi Maksimal (`watchdog_waktu`): Max hold 600 seconds (10 minutes)
        let elapsed_secs = tracker.entry_instant.elapsed().as_secs();
        if elapsed_secs >= config.max_hold_seconds {
            return Some(ExitSignal {
                position_id: tracker.position.id.clone(),
                token_address: token_addr,
                pool_address: pool_addr,
                reason: ExitReason::WatchdogTimeout,
                reason_description: format!(
                    "🛡️ WATCHDOG MAX HOLD TIMEOUT TRIGGERED: Position held for {}s >= {}s limit (10 minutes)! Auto-liquidating.",
                    elapsed_secs, config.max_hold_seconds
                ),
                is_emergency: false,
                slippage_percent: 5.0,
            });
        }

        // 3. Watchdog Kegagalan Kuotasi Pool (`pool_timeout`): Secondary pool quote failure for 30s
        if tracker.is_migrated_to_pool {
            let quote_gap_secs = now.duration_since(tracker.last_pool_quote_instant).as_secs();
            if quote_gap_secs >= config.pool_quote_timeout_seconds {
                return Some(ExitSignal {
                    position_id: tracker.position.id.clone(),
                    token_address: token_addr,
                    pool_address: pool_addr,
                    reason: ExitReason::PoolQuoteTimeout,
                    reason_description: format!(
                        "🛡️ WATCHDOG QUOTE TIMEOUT TRIGGERED: Secondary pool quote unavailable for {}s >= {}s limit! Capital safety exit.",
                        quote_gap_secs, config.pool_quote_timeout_seconds
                    ),
                    is_emergency: false,
                    slippage_percent: 5.0,
                });
            }
        }

        None
    }
}
