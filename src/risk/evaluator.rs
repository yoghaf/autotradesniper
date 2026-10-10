use crate::risk::flow_rules::FlowRules;
use crate::risk::migration_rules::MigrationRules;
use crate::risk::price_rules::PriceRules;
use crate::risk::types::{BlockEvent, ExitSignal, PositionTracker, RiskConfig};
use std::time::Instant;

/// Non-blocking Multi-Layer Risk Evaluator
pub struct RiskEvaluator;

impl RiskEvaluator {
    /// Non-blocking evaluation triggered on every end-of-block event
    /// Evaluates Layer 1 (Price Rules), Layer 2 (Flow Rules), and Layer 3 (Migration Rules)
    pub fn evaluate_block_event(
        tracker: &mut PositionTracker,
        block_event: &BlockEvent,
        pool_quote_price: Option<f64>,
        now: Instant,
        config: &RiskConfig,
    ) -> Option<ExitSignal> {
        // 1. Evaluate Layer 1: Spot Price Rules & High-Watermark Trailing
        if let Some(signal) = PriceRules::evaluate(tracker, block_event.spot_price_eth, config) {
            return Some(signal);
        }

        // 2. Evaluate Layer 2: Flow Rules & Inactivity Decay
        if let Some(signal) = FlowRules::evaluate(tracker, now, config) {
            return Some(signal);
        }

        // 3. Evaluate Layer 3: Graduation & Secondary Pool Migration Rules
        if let Some(signal) = MigrationRules::evaluate(
            tracker,
            block_event.curve_capacity_percent,
            pool_quote_price,
            now,
            config,
        ) {
            return Some(signal);
        }

        None
    }
}
