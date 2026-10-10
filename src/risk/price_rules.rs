use crate::risk::types::{ExitReason, ExitSignal, PositionTracker, RiskConfig};
use alloy::primitives::Address;
use std::str::FromStr;

/// Layer 1: Spot Price Rules & High-Watermark Trailing
pub struct PriceRules;

impl PriceRules {
    /// Evaluate spot price on new block confirmation
    /// Checks adaptive trailing stop and hard stop-loss
    pub fn evaluate(
        tracker: &mut PositionTracker,
        current_spot_price: f64,
        config: &RiskConfig,
    ) -> Option<ExitSignal> {
        tracker.current_price_eth = current_spot_price;

        let entry_price = tracker.entry_price_eth;
        if entry_price <= 0.0 {
            return None;
        }

        // Calculate gain/loss relative to entry price in percentage
        let pnl_pct = ((current_spot_price - entry_price) / entry_price) * 100.0;
        tracker.position.pnl_percent = pnl_pct;
        tracker.position.current_eth_value = tracker.position.entry_eth_amount * (1.0 + (pnl_pct / 100.0)).max(0.0);

        let token_addr = Address::from_str(&tracker.position.token_address).ok()?;
        let pool_addr = Address::from_str(&tracker.position.pool_address).ok()?;

        // Prysm Series Strategy Dedicated Target Exit Mode Evaluation
        if tracker.is_prysm_strategy && tracker.prysm_exit_mode == Some(crate::types::PrysmExitMode::Target) {
            // Update current market cap based on price change
            if tracker.prysm_entry_mcap_usd > 0.0 {
                tracker.current_mcap_usd = tracker.prysm_entry_mcap_usd * (1.0 + (pnl_pct / 100.0)).max(0.0);
            }

            // Target Exit Rule 1: Take-Profit at Market Cap >= $10,000 USD
            if tracker.current_mcap_usd >= 10000.0 {
                return Some(ExitSignal {
                    position_id: tracker.position.id.clone(),
                    token_address: token_addr,
                    pool_address: pool_addr,
                    reason: ExitReason::PrysmTargetMcap,
                    reason_description: format!(
                        "Prysm Target Take-Profit Reached: Market Cap ${:.2} >= $10,000.00 USD (PnL: +{:.2}%)",
                        tracker.current_mcap_usd, pnl_pct
                    ),
                    is_emergency: false,
                    slippage_percent: 5.0,
                });
            }

            // Target Exit Rule 2: Strict Stop-Loss at -30% Deficit
            if pnl_pct <= -30.0 {
                return Some(ExitSignal {
                    position_id: tracker.position.id.clone(),
                    token_address: token_addr,
                    pool_address: pool_addr,
                    reason: ExitReason::PrysmTargetStopLoss,
                    reason_description: format!(
                        "Prysm Target Strict Stop-Loss Hit: {:.2}% <= -30.00% Deficit Threshold",
                        pnl_pct
                    ),
                    is_emergency: false,
                    slippage_percent: 5.0,
                });
            }

            return None;
        }

        // 1. Hard Stop-Loss Rule (-20% from entry)
        if pnl_pct <= -config.stop_loss_percent {
            return Some(ExitSignal {
                position_id: tracker.position.id.clone(),
                token_address: token_addr,
                pool_address: pool_addr,
                reason: ExitReason::StopLoss,
                reason_description: format!(
                    "Hard Stop-Loss Hit: {:.2}% <= -{:.2}% threshold",
                    pnl_pct, config.stop_loss_percent
                ),
                is_emergency: false,
                slippage_percent: 5.0,
            });
        }

        // 2. High-Watermark Trailing Logic
        // Activation: Profit reaches >= +20%
        if !tracker.high_watermark_activated && pnl_pct >= config.trail_activate_percent {
            tracker.high_watermark_activated = true;
            tracker.highest_price_eth = current_spot_price;
        }

        if tracker.high_watermark_activated {
            // Track highest watermark peak
            if current_spot_price > tracker.highest_price_eth {
                tracker.highest_price_eth = current_spot_price;
            }

            // Calculate pullback drawdown from peak: (Peak - Current) / Peak
            let peak_price = tracker.highest_price_eth;
            let drawdown_from_peak_pct = ((peak_price - current_spot_price) / peak_price) * 100.0;

            // Liquidation: Pullback reaches >= 15% from peak
            if drawdown_from_peak_pct >= config.trail_distance_percent {
                return Some(ExitSignal {
                    position_id: tracker.position.id.clone(),
                    token_address: token_addr,
                    pool_address: pool_addr,
                    reason: ExitReason::TrailingStop,
                    reason_description: format!(
                        "Trailing Stop Triggered: {:.2}% pullback from peak ({:.6} ETH -> {:.6} ETH), Realized PnL: +{:.2}%",
                        drawdown_from_peak_pct, peak_price, current_spot_price, pnl_pct
                    ),
                    is_emergency: false,
                    slippage_percent: 3.0,
                });
            }
        }

        None
    }
}
