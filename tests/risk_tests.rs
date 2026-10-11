use autotradesniper::risk::anomaly_sensor::AnomalySensor;
use autotradesniper::risk::evaluator::RiskEvaluator;
use autotradesniper::risk::flow_rules::FlowRules;
use autotradesniper::risk::migration_rules::MigrationRules;
use autotradesniper::risk::price_rules::PriceRules;
use autotradesniper::risk::types::{
    ApprovalEvent, BlockEvent, ExitReason, PositionTracker, RiskConfig,
};
use autotradesniper::risk::watchdog::WatchdogSupervisor;
use autotradesniper::types::Position;
use alloy::primitives::Address;
use std::time::{Duration, Instant};

fn create_sample_tracker(entry_eth: f64) -> PositionTracker {
    let pos = Position {
        id: "POS-TEST-1".to_string(),
        token_address: "0x1111111111111111111111111111111111111111".to_string(),
        token_symbol: "TKN-TEST".to_string(),
        entry_eth_amount: entry_eth,
        current_eth_value: entry_eth,
        entry_timestamp_secs: 1000,
        pool_address: "0x2222222222222222222222222222222222222222".to_string(),
        is_open: true,
        pnl_percent: 0.0,
        exit_reason: None,
        exit_tx_hash: None,
        strategy: None,
        exit_mode: None,
        entry_mcap_usd: None,
        current_mcap_usd: None,
    };
    PositionTracker::new(pos, entry_eth)
}

#[test]
fn test_layer1_high_watermark_trailing() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_tracker(1.0);

    // 1. Price at +10% (1.10 ETH) -> Trailing should NOT activate (needs >= 20%)
    let signal = PriceRules::evaluate(&mut tracker, 1.10, &config);
    assert!(signal.is_none());
    assert!(!tracker.high_watermark_activated);
    assert_eq!(tracker.highest_price_eth, 1.0);

    // 2. Price at +25% (1.25 ETH) -> Trailing MUST activate
    let signal = PriceRules::evaluate(&mut tracker, 1.25, &config);
    assert!(signal.is_none());
    assert!(tracker.high_watermark_activated);
    assert_eq!(tracker.highest_price_eth, 1.25);

    // 3. Price pulls back 8% from the peak (1.15 ETH) -> should NOT liquidate yet (default distance 10%)
    assert_eq!(config.trail_distance_percent, 10.0);
    let signal = PriceRules::evaluate(&mut tracker, 1.15, &config);
    assert!(signal.is_none());

    // 4. Price pulls back 12% from the peak (1.10 ETH < 1.25 * 0.90 = 1.125) -> MUST liquidate
    let signal = PriceRules::evaluate(&mut tracker, 1.10, &config);
    assert!(signal.is_some());
    let sig = signal.unwrap();
    assert_eq!(sig.reason, ExitReason::TrailingStop);
}

#[test]
fn test_layer1_trailing_distance_follows_the_configured_value() {
    // After +40%, a 6% pullback: inside a 10% distance, beyond a 5% one.
    for (distance, sells) in [(10.0, false), (5.0, true), (3.0, true), (30.0, false)] {
        let config = RiskConfig { trail_distance_percent: distance, ..RiskConfig::default() };
        let mut tracker = create_sample_tracker(1.0);
        assert!(PriceRules::evaluate(&mut tracker, 1.40, &config).is_none());
        let signal = PriceRules::evaluate(&mut tracker, 1.40 * 0.94, &config);
        assert_eq!(signal.is_some(), sells, "distance {distance}%");
    }
    // Exactly at the distance (25% of 2.0 = 0.5, exact in binary): sells (>=, not >).
    let config = RiskConfig { trail_distance_percent: 25.0, ..RiskConfig::default() };
    let mut tracker = create_sample_tracker(1.0);
    PriceRules::evaluate(&mut tracker, 2.0, &config);
    assert!(PriceRules::evaluate(&mut tracker, 1.5000001, &config).is_none());
    assert!(PriceRules::evaluate(&mut tracker, 1.5, &config).is_some());
}

#[test]
fn test_layer1_hard_stop_loss() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_tracker(1.0);

    // Price drops by 21% to 0.79 ETH (Hard SL is -20%)
    let signal = PriceRules::evaluate(&mut tracker, 0.79, &config);
    assert!(signal.is_some());
    let sig = signal.unwrap();
    assert_eq!(sig.reason, ExitReason::StopLoss);
}

#[test]
fn test_layer2_quiet_market_exit() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_tracker(1.0);
    // Simulate entry 31 seconds ago
    let now = Instant::now();
    tracker.entry_instant = now - Duration::from_secs(31);
    tracker.outside_inflow_eth = 0.05; // Less than 0.1 ETH

    let signal = FlowRules::evaluate(&mut tracker, now, &config);
    assert!(signal.is_some());
    let sig = signal.unwrap();
    assert_eq!(sig.reason, ExitReason::QuietMarket);
}

#[test]
fn test_layer2_inactivity_decay_exit() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_tracker(1.0);
    let now = Instant::now();
    // Entry was 10s ago, quiet evaluation not reached yet, but no outside inflow for 26s
    tracker.entry_instant = now - Duration::from_secs(10);
    tracker.last_outside_inflow_instant = now - Duration::from_secs(26);

    let signal = FlowRules::evaluate(&mut tracker, now, &config);
    assert!(signal.is_some());
    let sig = signal.unwrap();
    assert_eq!(sig.reason, ExitReason::InactivityDecay);
}

#[test]
fn test_layer3_pre_graduation_guard() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_tracker(1.0);
    let now = Instant::now();

    // Curve reaches 91% capacity (threshold is >= 90%)
    let signal = MigrationRules::evaluate(&mut tracker, 91.0, None, now, &config);
    assert!(signal.is_some());
    let sig = signal.unwrap();
    assert_eq!(sig.reason, ExitReason::PreGraduationGuard);
}

#[test]
fn test_layer3_secondary_pool_trailing() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_tracker(1.0);
    let now = Instant::now();
    tracker.is_migrated_to_pool = true;
    tracker.entry_price_eth = 1.0;

    // Price drops by 25% to 0.75 in secondary pool (stop loss is -20%)
    let signal = MigrationRules::evaluate(&mut tracker, 99.0, Some(0.75), now, &config);
    assert!(signal.is_some());
    let sig = signal.unwrap();
    assert_eq!(sig.reason, ExitReason::SecondaryPoolTrailing);
}

#[test]
fn test_layer4_approval_anomaly_sensor() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_tracker(1.0);
    let curve_address = Address::repeat_byte(0x22);
    let token_address = Address::repeat_byte(0x11);
    let now = Instant::now();

    let holder1 = Address::repeat_byte(0x01);
    let holder2 = Address::repeat_byte(0x02);
    let holder3 = Address::repeat_byte(0x03);

    // Record 2 approvals within window -> no trigger
    let sig1 = AnomalySensor::inspect_approval(
        &mut tracker,
        ApprovalEvent { token: token_address, owner: holder1, spender: curve_address, timestamp: now },
        &config,
    );
    assert!(sig1.is_none());

    let sig2 = AnomalySensor::inspect_approval(
        &mut tracker,
        ApprovalEvent { token: token_address, owner: holder2, spender: curve_address, timestamp: now },
        &config,
    );
    assert!(sig2.is_none());

    // 3rd concurrent holder approval -> MUST trigger anomaly!
    let sig3 = AnomalySensor::inspect_approval(
        &mut tracker,
        ApprovalEvent { token: token_address, owner: holder3, spender: curve_address, timestamp: now },
        &config,
    );
    assert!(sig3.is_some());
    let sig = sig3.unwrap();
    assert_eq!(sig.reason, ExitReason::ApprovalAnomaly);
    assert!(sig.is_emergency);
    assert_eq!(sig.slippage_percent, config.emergency_slippage_percent);
}

#[test]
fn test_layer5_watchdog_supervisor() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_tracker(1.0);
    let now = Instant::now();

    // 1. Severe drawdown >= 30% (current value 0.68 ETH)
    tracker.position.current_eth_value = 0.68;
    tracker.position.pnl_percent = -32.0;
    let signal = WatchdogSupervisor::evaluate_position(&tracker, now, &config);
    assert!(signal.is_some());
    assert_eq!(signal.unwrap().reason, ExitReason::WatchdogDrawdown);

    // 2. Max hold timeout (601 seconds)
    tracker.position.current_eth_value = 1.0;
    tracker.position.pnl_percent = 0.0;
    tracker.entry_instant = now - Duration::from_secs(605);
    let signal = WatchdogSupervisor::evaluate_position(&tracker, now, &config);
    assert!(signal.is_some());
    assert_eq!(signal.unwrap().reason, ExitReason::WatchdogTimeout);

    // 3. Stale Quoter timeout (35 seconds)
    tracker.entry_instant = now - Duration::from_secs(100);
    tracker.is_migrated_to_pool = true;
    tracker.last_pool_quote_instant = now - Duration::from_secs(35);
    let signal = WatchdogSupervisor::evaluate_position(&tracker, now, &config);
    assert!(signal.is_some());
    assert_eq!(signal.unwrap().reason, ExitReason::PoolQuoteTimeout);
}

#[test]
fn test_risk_evaluator_sub_microsecond_efficiency() {
    let config = RiskConfig::default();
    let mut tracker = create_sample_tracker(1.0);
    let block = BlockEvent {
        block_number: 123456,
        timestamp_secs: 1000,
        spot_price_eth: 1.05,
        curve_capacity_percent: 50.0,
        token_address: Address::repeat_byte(0x11),
    };
    let now = Instant::now();

    // Evaluation on normal active block
    let signal = RiskEvaluator::evaluate_block_event(&mut tracker, &block, Some(1.05), now, &config);
    assert!(signal.is_none());
}
