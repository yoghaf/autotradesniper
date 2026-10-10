use crate::risk::types::{ExitReason, ExitSignal, InflowEvent, PositionTracker, RiskConfig};
use alloy::primitives::Address;
use std::str::FromStr;
use std::time::Instant;

/// Layer 2: Capital Flow Heuristics & Market Momentum
pub struct FlowRules;

impl FlowRules {
    /// Ingest outside capital inflow transaction into tracker
    pub fn record_inflow(tracker: &mut PositionTracker, event: InflowEvent) {
        if !event.is_deployer_or_initial {
            tracker.outside_inflow_eth += event.amount_eth;
            tracker.last_outside_inflow_instant = event.timestamp;
        }
    }

    /// Evaluate flow conditions (Quiet Market and Inactivity Decay)
    pub fn evaluate(
        tracker: &mut PositionTracker,
        now: Instant,
        config: &RiskConfig,
    ) -> Option<ExitSignal> {
        let elapsed_secs = tracker.entry_instant.elapsed().as_secs();

        let token_addr = Address::from_str(&tracker.position.token_address).ok()?;
        let pool_addr = Address::from_str(&tracker.position.pool_address).ok()?;

        // 1. Quiet Market Evaluation (`sepi`): Evaluated at 30 seconds after entry
        if !tracker.quiet_evaluated && elapsed_secs >= config.quiet_exit_seconds {
            tracker.quiet_evaluated = true;
            if tracker.outside_inflow_eth < config.quiet_min_inflow_eth {
                return Some(ExitSignal {
                    position_id: tracker.position.id.clone(),
                    token_address: token_addr,
                    pool_address: pool_addr,
                    reason: ExitReason::QuietMarket,
                    reason_description: format!(
                        "Quiet Market Triggered at {}s: Third-party inflow {:.4} ETH < {:.4} ETH threshold",
                        elapsed_secs, tracker.outside_inflow_eth, config.quiet_min_inflow_eth
                    ),
                    is_emergency: false,
                    slippage_percent: 5.0,
                });
            }
        }

        // 2. Inactivity Decay Filter (`idle`): No outside buy for 25 consecutive seconds
        let idle_elapsed_secs = now.duration_since(tracker.last_outside_inflow_instant).as_secs();
        if idle_elapsed_secs >= config.idle_timeout_seconds {
            return Some(ExitSignal {
                position_id: tracker.position.id.clone(),
                token_address: token_addr,
                pool_address: pool_addr,
                reason: ExitReason::InactivityDecay,
                reason_description: format!(
                    "Inactivity Decay Triggered: No outside inflow for {}s >= {}s limit",
                    idle_elapsed_secs, config.idle_timeout_seconds
                ),
                is_emergency: false,
                slippage_percent: 5.0,
            });
        }

        None
    }
}
