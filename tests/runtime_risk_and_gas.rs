//! Regressions for three operator decisions:
//! 1. the trailing-stop distance is configurable (`TRAILING_STOP_PERCENT`, Telegram) within 3-30%,
//!    default 10%, persisted only once set at runtime;
//! 2. the total buy tax (fee + creator tax + snipe tax) is capped at 99%;
//! 3. padded transactions are sized for the EIP-7623 calldata floor on their first attempt.

use autotradesniper::config::{clamp_trailing_stop_percent, parse_trailing_stop_percent, AppConfig, RuntimeState};
use autotradesniper::dispatcher::{EntryCtx, PresignPolicy, TxSpec};
use autotradesniper::payload::{self, CalldataPayloadBuilder};
use autotradesniper::risk::types::{RiskConfig, DEFAULT_TRAILING_STOP_PERCENT, MAX_TRAILING_STOP_PERCENT, MIN_TRAILING_STOP_PERCENT};
use autotradesniper::telegram::control::{parse_trailing_callback, TrailingCommand, TRAILING_CHOICES};
use alloy::primitives::{Address, Bytes, U256};

// ── 1. Trailing-stop distance ───────────────────────────────────────────────

#[test]
fn trailing_stop_env_value_is_parsed_defaulted_and_clamped() {
    assert_eq!((DEFAULT_TRAILING_STOP_PERCENT, MIN_TRAILING_STOP_PERCENT, MAX_TRAILING_STOP_PERCENT), (10.0, 3.0, 30.0));
    assert_eq!(RiskConfig::default().trail_distance_percent, 10.0, "the risk engine's own default matches");
    // Missing or blank: the default, silently.
    assert_eq!(parse_trailing_stop_percent(None), (10.0, None));
    assert_eq!(parse_trailing_stop_percent(Some("  ")), (10.0, None));
    // In range: taken as written (a trailing '%' is tolerated).
    for (raw, want) in [("7.5", 7.5), ("3", 3.0), ("30.0", 30.0), (" 12 ", 12.0), ("8%", 8.0)] {
        assert_eq!(parse_trailing_stop_percent(Some(raw)), (want, None), "{raw:?}");
    }
    // Out of range: clamped, with a warning.
    for (raw, want) in [("1", 3.0), ("2.99", 3.0), ("45", 30.0), ("-5", 3.0)] {
        let (got, warning) = parse_trailing_stop_percent(Some(raw));
        assert_eq!(got, want, "{raw:?}");
        assert!(warning.is_some_and(|w| w.contains("outside")), "{raw:?}");
    }
    // Not a number: the default, with a warning.
    for raw in ["abc", "NaN", "inf", "1e999", "10,5"] {
        let (got, warning) = parse_trailing_stop_percent(Some(raw));
        assert_eq!(got, 10.0, "{raw:?}");
        assert!(warning.is_some_and(|w| w.contains("not a number")), "{raw:?}");
    }
    assert_eq!(clamp_trailing_stop_percent(f64::NAN), None);
    assert_eq!(clamp_trailing_stop_percent(100.0), Some(30.0));
}

#[tokio::test]
async fn a_runtime_trailing_stop_is_validated_persisted_and_resettable() {
    let config = AppConfig::default();
    let env = config.trailing_stop_from_env;
    assert_eq!(config.trailing_stop().await, (env, false));

    // Refused outside 3-30 or when not finite: nothing changes, nothing is persisted.
    for bad in [2.9, 30.1, f64::NAN, f64::INFINITY, -10.0] {
        assert!(config.set_trailing_stop_percent(bad).await.is_err(), "{bad}");
    }
    assert_eq!(config.trailing_stop().await, (env, false));

    // Accepted: previous value back, flagged as a runtime override, written to the state file.
    assert_eq!(config.set_trailing_stop_percent(7.0).await, Ok(env));
    assert_eq!(config.trailing_stop().await, (7.0, true));
    let saved: RuntimeState = serde_json::from_str(&std::fs::read_to_string(config.state_path()).unwrap()).unwrap();
    assert_eq!(saved.trailing_stop_percent, Some(7.0));

    // A restart restores it over `.env` (another config loading the same state).
    let restarted = AppConfig::default();
    std::fs::copy(config.state_path(), restarted.state_path()).unwrap();
    restarted.load_persisted_state().unwrap();
    assert_eq!(restarted.trailing_stop().await, (7.0, true));

    // An out-of-range value written into the file by hand is clamped on load.
    let mut edited = saved.clone();
    edited.trailing_stop_percent = Some(80.0);
    std::fs::write(restarted.state_path(), serde_json::to_vec(&edited).unwrap()).unwrap();
    restarted.load_persisted_state().unwrap();
    assert_eq!(restarted.trailing_stop().await, (30.0, true));

    // Reset: back to `.env`, and the state file stops carrying a value.
    assert_eq!(config.reset_trailing_stop_percent().await, 7.0);
    assert_eq!(config.trailing_stop().await, (env, false));
    let saved: RuntimeState = serde_json::from_str(&std::fs::read_to_string(config.state_path()).unwrap()).unwrap();
    assert_eq!(saved.trailing_stop_percent, None);
    assert!(!std::fs::read_to_string(config.state_path()).unwrap().contains("trailing_stop_percent"));
}

#[tokio::test]
async fn other_runtime_changes_never_freeze_the_env_trailing_stop_into_the_state_file() {
    let config = AppConfig::default();
    // Pressing the button of the distance already in force changes nothing, not even its source.
    let env = config.trailing_stop_from_env;
    assert_eq!(config.set_trailing_stop_percent(env).await, Ok(env));
    assert_eq!(config.trailing_stop().await, (env, false));
    config.set_fee_preset(1.0, 5.0).await;
    config.set_carrier_count(3).await;
    let saved: RuntimeState = serde_json::from_str(&std::fs::read_to_string(config.state_path()).unwrap()).unwrap();
    assert_eq!(saved.trailing_stop_percent, None, ".env keeps deciding until the distance is set at runtime");

    // A state file written before this setting existed loads, and leaves `.env` in force.
    let mut legacy = serde_json::to_value(&saved).unwrap();
    legacy.as_object_mut().unwrap().remove("trailing_stop_percent");
    let restarted = AppConfig::default();
    std::fs::write(restarted.state_path(), serde_json::to_vec(&legacy).unwrap()).unwrap();
    restarted.load_persisted_state().unwrap();
    assert_eq!(restarted.trailing_stop().await, (restarted.trailing_stop_from_env, false));
}

#[test]
fn trailing_buttons_parse_to_exactly_one_command() {
    assert_eq!(parse_trailing_callback("cfg_trail:env"), Some(TrailingCommand::ResetToEnv));
    for p in TRAILING_CHOICES {
        assert_eq!(parse_trailing_callback(&format!("cfg_trail:{p}")), Some(TrailingCommand::Set(f64::from(p))));
    }
    assert_eq!(parse_trailing_callback("cfg_trail:7.5"), Some(TrailingCommand::Set(7.5)));
    for bad in ["cfg_trail:", "cfg_trail:x", "cfg_trail:NaN", "cfg_trail:inf", "cfg_trailx", "cfg_tp:10", "cfg_trail:env2"] {
        assert_eq!(parse_trailing_callback(bad), None, "{bad:?}");
    }
}

// ── 3. Gas sizing for the EIP-7623 calldata floor ───────────────────────────

fn carrier_spec(l1: u64) -> TxSpec {
    let padding = payload::TARGET_CALLDATA_BUFFER_SIZE;
    TxSpec {
        chain_id: 4663,
        to: Address::repeat_byte(0x0c),
        value: U256::ZERO,
        input: Bytes::from(vec![0u8; padding]),
        gas_limit: CalldataPayloadBuilder::carrier_gas_limit(padding, payload::STATIC_GAS_LIMIT, l1),
        max_fee_per_gas: 10_000_000_000,
        max_priority_fee_per_gas: 2_500_000_000,
        floor_with_l1: CalldataPayloadBuilder::calldata_floor_gas(&[], padding) + CalldataPayloadBuilder::l1_margin(l1),
        retry_rate_limited: false,
        gas_floor_without_l1: CalldataPayloadBuilder::gas_needed_without_l1(&[], padding, payload::CARRIER_GAS_RESERVE),
        configured_gas_limit: payload::STATIC_GAS_LIMIT,
        l1_component_gas: l1,
        swap: Vec::new(),
    }
}

#[test]
fn a_94_5_kb_carrier_clears_the_calldata_floor_on_its_first_attempt() {
    let padding = payload::TARGET_CALLDATA_BUFFER_SIZE;
    let floor = CalldataPayloadBuilder::calldata_floor_gas(&[], padding);
    assert_eq!(floor, 21_000 + 10 * 94_500);
    // Needed without L1: the floor (966,000), not intrinsic + reserve (420,000).
    assert_eq!(CalldataPayloadBuilder::gas_needed_without_l1(&[], padding, payload::CARRIER_GAS_RESERVE), floor);
    for l1 in [0u64, 50_000, 200_000] {
        let spec = carrier_spec(l1);
        let l1_used = if l1 == 0 { payload::L1_COMPONENT_FALLBACK_GAS } else { l1 };
        assert!(spec.gas_limit >= floor + l1_used, "L1 {l1}: limit {} covers floor + L1", spec.gas_limit);
        assert!(spec.gas_limit >= spec.floor_with_l1, "an escalated retry would not be higher: never needed");
    }
    assert_eq!(carrier_spec(50_000).gas_limit, 1_041_000, "~1.05M with a 50k L1 component");
    // A small unpadded tx (an exit sell, a copy without padding) is unaffected.
    let sell = CalldataPayloadBuilder::build_pons_sell_payload(U256::from(1u64), U256::ZERO, Address::ZERO);
    assert!(CalldataPayloadBuilder::calldata_floor_gas(&sell, 0) < CalldataPayloadBuilder::intrinsic_gas(&sell, 0) + payload::EXECUTION_GAS_RESERVE);
    assert_eq!(
        CalldataPayloadBuilder::gas_limit(&sell, 0, 0, 50_000),
        CalldataPayloadBuilder::intrinsic_gas(&sell, 0) + payload::EXECUTION_GAS_RESERVE + 75_000
    );
}

#[test]
fn a_refreshed_or_revalidated_presigned_packet_keeps_covering_the_floor() {
    let spec = carrier_spec(50_000);
    let floor = CalldataPayloadBuilder::calldata_floor_gas(&[], payload::TARGET_CALLDATA_BUFFER_SIZE);
    let policy = PresignPolicy::default();
    // L1 moved up: the re-sized spec covers floor + 1.5x the new L1.
    let ctx = EntryCtx {
        chain_id: 4663,
        buffer_size: payload::TARGET_CALLDATA_BUFFER_SIZE,
        carriers_enabled: true,
        configured_gas_limit: payload::STATIC_GAS_LIMIT,
        l1_component_gas: 120_000,
        priority_fee_wei: spec.max_priority_fee_per_gas,
        max_fee_wei: spec.max_fee_per_gas,
    };
    let refreshed = spec.refreshed(&ctx);
    assert_eq!(refreshed.gas_limit, floor + 180_000);
    // The held packet stays valid while L1 grows into its 0.5x slack, and is re-signed beyond it:
    // the check is against floor + L1, not against intrinsic + L1.
    assert!(spec.fee_gas_check(spec.max_fee_per_gas, spec.max_priority_fee_per_gas, 75_000, &policy).is_ok());
    assert!(spec.fee_gas_check(spec.max_fee_per_gas, spec.max_priority_fee_per_gas, 76_000, &policy).is_err());
}
