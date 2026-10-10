use crate::risk::types::{ExitReason, ExitSignal, PositionTracker, RiskConfig};
use alloy::primitives::Address;
use std::str::FromStr;
use std::time::Instant;

/// Layer 3: Curve Graduation Transition & Secondary Pool Migration
pub struct MigrationRules;

impl MigrationRules {
    /// Evaluate curve graduation capacity and secondary pool price quote
    pub fn evaluate(
        tracker: &mut PositionTracker,
        curve_capacity_pct: f64,
        pool_quote_price: Option<f64>,
        now: Instant,
        config: &RiskConfig,
    ) -> Option<ExitSignal> {
        tracker.curve_capacity_percent = curve_capacity_pct;

        let token_addr = Address::from_str(&tracker.position.token_address).ok()?;
        let pool_addr = Address::from_str(&tracker.position.pool_address).ok()?;

        // 1. Pre-Graduation Guard (`pre_grad`): Exit before 100% liquidity migration cliff
        if config.grad_guard_enabled && !tracker.is_migrated_to_pool {
            if curve_capacity_pct >= config.grad_guard_capacity_percent {
                return Some(ExitSignal {
                    position_id: tracker.position.id.clone(),
                    token_address: token_addr,
                    pool_address: pool_addr,
                    reason: ExitReason::PreGraduationGuard,
                    reason_description: format!(
                        "Pre-Graduation Guard Triggered: Curve capacity {:.1}% >= {:.1}% threshold (Safeguard pre-migration)",
                        curve_capacity_pct, config.grad_guard_capacity_percent
                    ),
                    is_emergency: false,
                    slippage_percent: 5.0,
                });
            }
        }

        // 2. Secondary Pool Trailing (`pool_exit`): Monitor position after migrating to Uniswap v4
        if config.pool_trail_enabled && tracker.is_migrated_to_pool {
            if let Some(quote_price) = pool_quote_price {
                tracker.last_pool_quote_instant = now;
                tracker.current_price_eth = quote_price;

                let entry_price = tracker.entry_price_eth;
                if entry_price > 0.0 {
                    let pnl_pct = ((quote_price - entry_price) / entry_price) * 100.0;
                    tracker.position.pnl_percent = pnl_pct;

                    // Hard Stop-Loss in secondary pool
                    if pnl_pct <= -config.stop_loss_percent {
                        return Some(ExitSignal {
                            position_id: tracker.position.id.clone(),
                            token_address: token_addr,
                            pool_address: pool_addr,
                            reason: ExitReason::SecondaryPoolTrailing,
                            reason_description: format!(
                                "Secondary Pool Stop-Loss Triggered: {:.2}% <= -{:.2}% threshold",
                                pnl_pct, config.stop_loss_percent
                            ),
                            is_emergency: false,
                            slippage_percent: 5.0,
                        });
                    }

                    // Trailing activation
                    if !tracker.high_watermark_activated && pnl_pct >= config.trail_activate_percent {
                        tracker.high_watermark_activated = true;
                        tracker.highest_price_eth = quote_price;
                    }

                    if tracker.high_watermark_activated {
                        if quote_price > tracker.highest_price_eth {
                            tracker.highest_price_eth = quote_price;
                        }
                        let peak = tracker.highest_price_eth;
                        let drawdown_from_peak = ((peak - quote_price) / peak) * 100.0;
                        if drawdown_from_peak >= config.trail_distance_percent {
                            return Some(ExitSignal {
                                position_id: tracker.position.id.clone(),
                                token_address: token_addr,
                                pool_address: pool_addr,
                                reason: ExitReason::SecondaryPoolTrailing,
                                reason_description: format!(
                                    "Secondary Pool Trailing Triggered: {:.2}% pullback from peak, PnL: +{:.2}%",
                                    drawdown_from_peak, pnl_pct
                                ),
                                is_emergency: false,
                                slippage_percent: 3.0,
                            });
                        }
                    }
                }
            }
        }

        None
    }
}
