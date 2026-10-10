use crate::risk::types::{ApprovalEvent, ExitReason, ExitSignal, PositionTracker, RiskConfig};
use alloy::primitives::Address;
use std::str::FromStr;

/// Layer 4: Mempool Anomaly Sensor & Fast Pre-Emptive Liquidation
pub struct AnomalySensor;

impl AnomalySensor {
    /// Ingest approval event from WebSocket mempool feed
    /// Detects concurrent token approval spikes from multiple holders
    pub fn inspect_approval(
        tracker: &mut PositionTracker,
        event: ApprovalEvent,
        config: &RiskConfig,
    ) -> Option<ExitSignal> {
        let token_addr = Address::from_str(&tracker.position.token_address).ok()?;
        if event.token != token_addr {
            return None;
        }

        let pool_addr = Address::from_str(&tracker.position.pool_address).ok()?;

        // Record distinct approving holder address
        tracker.approval_accounts.insert(event.owner);

        // Danger Condition: >= 3 distinct holders approving token concurrently
        if tracker.approval_accounts.len() >= config.approval_anomaly_threshold {
            return Some(ExitSignal {
                position_id: tracker.position.id.clone(),
                token_address: token_addr,
                pool_address: pool_addr,
                reason: ExitReason::ApprovalAnomaly,
                reason_description: format!(
                    "🚨 MEMPOOL APPROVAL ANOMALY DETECTED: {} distinct early holders approved curve router! Pre-empting supply dump.",
                    tracker.approval_accounts.len()
                ),
                // Emergency flag triggers dedicated fast pre-emption runner on separate thread
                is_emergency: true,
                // Emergency slippage tolerance (90%) ensures immediate front-run execution
                slippage_percent: config.emergency_slippage_percent,
            });
        }

        None
    }
}
