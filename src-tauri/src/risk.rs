use crate::domain::*;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EntryDirectionConstraint {
    #[default]
    Any,
    LongOnly,
    ShortOnly,
    Conflicting,
}

pub fn explicit_direction_constraint(objective: &str) -> EntryDirectionConstraint {
    let normalized = objective.to_ascii_lowercase();
    let long_only = [
        "long only",
        "only long",
        "do not short",
        "don't short",
        "never short",
        "no short",
        "avoid short",
    ]
    .iter()
    .any(|cue| normalized.contains(cue));
    let short_only = [
        "short only",
        "only short",
        "do not long",
        "don't long",
        "never long",
        "no long",
        "avoid long",
    ]
    .iter()
    .any(|cue| normalized.contains(cue));
    match (long_only, short_only) {
        (true, true) => EntryDirectionConstraint::Conflicting,
        (true, false) => EntryDirectionConstraint::LongOnly,
        (false, true) => EntryDirectionConstraint::ShortOnly,
        (false, false) => EntryDirectionConstraint::Any,
    }
}

pub struct EntryRiskInput<'a> {
    pub run_id: &'a str,
    pub loop_id: &'a str,
    pub decision_id: &'a str,
    pub action: Jev1Action,
    pub direction_constraint: EntryDirectionConstraint,
    pub confidence: f64,
    pub signal_at: DateTime<Utc>,
    pub instrument: &'a str,
    pub reference_price: f64,
    pub allocated_fraction: f64,
    pub current_total_exposure: f64,
    /// When present, all capital and notional comparisons use this live
    /// deposit-currency snapshot and its instrument-specific conversion rate.
    pub broker_risk_snapshot: Option<&'a crate::ports::BrokerRiskSnapshot>,
    pub broker_risk_snapshot_error: Option<&'a str>,
    pub allow_static_risk_policy: bool,
    pub open_positions_in_loop: usize,
    pub duplicate: bool,
    pub minimum_confidence: f64,
    pub broker_volume_minimum: Option<f64>,
    pub broker_volume_step: Option<f64>,
    /// Fixed unit configured for one or more explicitly mapped Demo symbols.
    /// This keeps demo order sizing bounded when no account-risk snapshot is
    /// available; the broker still validates the submitted quantity.
    pub demo_fixed_quantity: Option<f64>,
    /// A single human-approved Demo cycle may round a positive sub-step size
    /// up to one configured quantity step when broker metadata is unavailable.
    pub allow_demo_minimum_step: bool,
}

pub struct CloseRiskInput<'a> {
    pub run_id: &'a str,
    pub loop_id: &'a str,
    pub decision_id: &'a str,
    pub direction: &'a str,
    pub control: &'a PositionControlRecord,
    pub signal_at: DateTime<Utc>,
    pub duplicate: bool,
    pub stop_triggered: bool,
    pub reference_price: f64,
}

#[derive(Clone)]
pub struct DeterministicRiskEngine {
    pub config: RiskPolicyConfig,
}

impl DeterministicRiskEngine {
    pub fn new(config: RiskPolicyConfig) -> Self {
        Self { config }
    }

    pub fn evaluate_entry(&self, input: EntryRiskInput<'_>) -> (OrderRecord, GuardrailDecision) {
        let now = Utc::now();
        let price = input.reference_price;
        let snapshot = input.broker_risk_snapshot;
        let static_currency_matches =
            instrument_quote_asset(input.instrument).is_some_and(|currency| {
                currency.eq_ignore_ascii_case(&self.config.paper_account_currency)
            });
        let static_policy_applies = input.allow_static_risk_policy
            && snapshot.is_none()
            && input.broker_risk_snapshot_error.is_none()
            && static_currency_matches;
        let demo_fixed_quantity = input
            .demo_fixed_quantity
            .filter(|quantity| quantity.is_finite() && *quantity > 0.0);
        let demo_fixed_policy_applies = snapshot.is_none()
            && demo_fixed_quantity.is_some()
            && static_currency_matches
            && input.broker_risk_snapshot_error.is_none();
        let snapshot_is_fresh = snapshot.is_some_and(|snapshot| {
            crate::freshness::is_fresh(now, snapshot.observed_at, 15)
                && snapshot.equity.is_finite()
                && snapshot.equity > 0.0
                && snapshot.free_margin.is_finite()
                && snapshot.free_margin >= 0.0
                && snapshot.account_open_exposure.is_finite()
                && snapshot.account_open_exposure >= 0.0
                && !snapshot.deposit_asset_id.trim().is_empty()
                && is_supported_currency_code(&snapshot.deposit_currency_code)
        });
        let quote_to_deposit = snapshot
            .filter(|_| snapshot_is_fresh)
            .and_then(|snapshot| {
                let normalized_instrument = normalize_instrument(input.instrument);
                let mut rates = snapshot
                    .quote_to_deposit
                    .iter()
                    .filter(|(instrument, _)| {
                        normalize_instrument(instrument) == normalized_instrument
                    })
                    .map(|(_, rate)| *rate);
                let first = rates.next()?;
                rates
                    .all(|rate| {
                        rate.is_finite()
                            && rate > 0.0
                            && (rate - first).abs() <= f64::EPSILON * first.abs().max(1.0)
                    })
                    .then_some(first)
            })
            .filter(|rate| rate.is_finite() && *rate > 0.0)
            .or_else(|| {
                (static_policy_applies || demo_fixed_policy_applies).then_some(1.0)
            });
        let risk_capital = snapshot
            .filter(|_| snapshot_is_fresh)
            .map(|snapshot| snapshot.equity)
            .or_else(|| static_policy_applies.then_some(self.config.paper_account_capital))
            .or_else(|| {
                demo_fixed_policy_applies.then(|| {
                    demo_fixed_quantity.unwrap_or_default() * price * 1.001
                        / self.config.max_total_exposure_fraction.max(f64::EPSILON)
                })
            })
            .unwrap_or(0.0);
        let loop_capital = risk_capital * input.allocated_fraction;
        let mut desired_account_notional = loop_capital * self.config.max_position_fraction_of_loop;
        if let Some(snapshot) = snapshot.filter(|_| snapshot_is_fresh) {
            // Without a broker-provided margin estimator, never size a single
            // order above currently available free margin.
            desired_account_notional = desired_account_notional.min(snapshot.free_margin);
        }
        let account_limit = risk_capital * self.config.max_total_exposure_fraction;
        let remaining_exposure = (account_limit - input.current_total_exposure).max(0.0);
        let desired_account_notional = desired_account_notional.min(remaining_exposure);
        let notional = quote_to_deposit
            .filter(|rate| *rate > 0.0)
            .map(|rate| desired_account_notional / rate)
            .unwrap_or(0.0);
        let raw_quantity = if price > 0.0 { notional / price } else { 0.0 };
        let mut quantity = demo_fixed_quantity.unwrap_or_else(|| {
            match (input.broker_volume_minimum, input.broker_volume_step) {
                (Some(minimum), Some(step)) => floor_step_from_minimum(raw_quantity, minimum, step),
                _ => floor_step(raw_quantity, self.config.quantity_step),
            }
        });
        if input.allow_demo_minimum_step
            && input.broker_volume_minimum.is_none()
            && input.broker_volume_step.is_none()
            && raw_quantity.is_finite()
            && raw_quantity > 0.0
            && quantity <= 0.0
            && self.config.quantity_step.is_finite()
            && self.config.quantity_step > 0.0
        {
            quantity = self.config.quantity_step;
        }
        let actual_account_notional = quantity * price * quote_to_deposit.unwrap_or(0.0);
        let side = match input.action {
            Jev1Action::Long => "BUY",
            Jev1Action::Short => "SELL",
            Jev1Action::NoTrade => "NO_TRADE",
        };
        let stop_loss_price = match input.action {
            Jev1Action::Long => {
                Some(price * (1.0 - self.config.stop_loss_basis_points as f64 / 10_000.0))
            }
            Jev1Action::Short => {
                Some(price * (1.0 + self.config.stop_loss_basis_points as f64 / 10_000.0))
            }
            Jev1Action::NoTrade => None,
        };
        let mut idempotency_key = idempotency_key(
            input.run_id,
            input.loop_id,
            input.decision_id,
            input.instrument,
            side,
            input.signal_at,
            self.config.duplicate_order_window_seconds,
        );
        let mut reasons = Vec::new();
        if matches!(input.action, Jev1Action::NoTrade) {
            reasons.push("model selected NO TRADE".into());
        }
        match (input.direction_constraint, &input.action) {
            (EntryDirectionConstraint::LongOnly, Jev1Action::Short) => {
                reasons.push(
                    "short entry conflicts with the user's explicit long-only instruction".into(),
                );
            }
            (EntryDirectionConstraint::ShortOnly, Jev1Action::Long) => {
                reasons.push(
                    "long entry conflicts with the user's explicit short-only instruction".into(),
                );
            }
            (EntryDirectionConstraint::Conflicting, Jev1Action::Long | Jev1Action::Short) => {
                reasons
                    .push("user instructions contain conflicting explicit entry directions".into());
            }
            _ => {}
        }
        if input.confidence < input.minimum_confidence {
            reasons.push(format!(
                "confidence {} below configured threshold {}",
                input.confidence, input.minimum_confidence
            ));
        }
        if (now - input.signal_at).num_seconds() > self.config.max_signal_age_seconds as i64 {
            reasons.push("signal exceeded configured maximum age".into());
        }
        if input.duplicate {
            reasons.push("duplicate idempotency key already exists".into());
        }
        if input.allocated_fraction <= 0.0 {
            reasons.push("loop has no capital allocation".into());
        }
        if !snapshot_is_fresh && !static_policy_applies && !demo_fixed_policy_applies {
            reasons.push(input.broker_risk_snapshot_error.map(str::to_owned).unwrap_or_else(|| {
                if input.allow_static_risk_policy && snapshot.is_none() && !static_currency_matches {
                    return format!(
        "static paper capital is denominated in {}, but {} has no matching recognized quote asset",
                        self.config.paper_account_currency, input.instrument
                    );
                }
                "fresh broker equity and free-margin snapshot is unavailable; static paper capital cannot size this broker".into()
            }));
        }
        if snapshot_is_fresh && quote_to_deposit.is_none() {
            reasons.push(format!(
                "no fresh quote-to-deposit conversion is available for {}",
                input.instrument
            ));
        }
        if let (Some(quantity), Some(minimum), Some(step)) = (
            demo_fixed_quantity,
            input.broker_volume_minimum,
            input.broker_volume_step,
        ) {
            if quantity + f64::EPSILON < minimum
                || !step.is_finite()
                || step <= 0.0
                || ((quantity - minimum) / step - ((quantity - minimum) / step).round()).abs()
                    > 1e-6
            {
                reasons.push(
                    "configured Demo fixed quantity does not match broker minimum and increment"
                        .into(),
                );
            }
        }
        if input.open_positions_in_loop >= self.config.max_open_positions_per_loop {
            reasons.push("loop reached maximum open-position count".into());
        }
        if !price.is_finite() || price <= 0.0 {
            reasons.push("reference price is invalid".into());
        }
        if actual_account_notional < self.config.minimum_order_notional || quantity <= 0.0 {
            reasons.push("order is below minimum notional or quantity".into());
        }
        if input.allow_demo_minimum_step
            && snapshot_is_fresh
            && quantity > raw_quantity
            && actual_account_notional > snapshot.unwrap().free_margin
        {
            reasons.push("one-step Demo quantity exceeds the verified free-margin estimate".into());
        }
        if demo_fixed_quantity.is_some()
            && snapshot_is_fresh
            && actual_account_notional > snapshot.unwrap().free_margin
        {
            reasons.push("configured Demo fixed quantity exceeds verified free margin".into());
        }
        if input.current_total_exposure + actual_account_notional > account_limit + f64::EPSILON {
            reasons.push("order would exceed hard account exposure limit".into());
        }
        let accepted = reasons.is_empty();
        let event_id = Uuid::new_v4().to_string();
        let order_id = Uuid::new_v4().to_string();
        if input.duplicate {
            idempotency_key = format!("{idempotency_key}:rejected:{order_id}");
        }
        let order = OrderRecord {
            id: order_id,
            run_id: input.run_id.into(),
            loop_id: input.loop_id.into(),
            decision_id: input.decision_id.into(),
            idempotency_key,
            order_kind: "market_entry".into(),
            instrument: input.instrument.into(),
            side: side.into(),
            quantity,
            reference_price: price,
            notional: quantity * price,
            stop_loss_price,
            signal_at: input.signal_at,
            status: if accepted { "approved" } else { "rejected" }.into(),
            rejection_reasons: reasons.clone(),
            created_by_event_id: event_id,
            created_at: now,
        };
        let decision = GuardrailDecision {
            accepted,
            effective_action: if accepted {
                input.action
            } else {
                Jev1Action::NoTrade
            },
            reason: if accepted {
                "all configured deterministic checks passed".into()
            } else {
                reasons.join("; ")
            },
            order_id: order.id.clone(),
            checks: vec![
                "confidence".into(),
                "no_trade_conversion".into(),
                "signal_freshness".into(),
                "duplicate_order".into(),
                "loop_allocation".into(),
                "position_count".into(),
                "account_exposure".into(),
                "quantity_step".into(),
                "stop_loss".into(),
            ],
        };
        (order, decision)
    }

    pub fn stop_triggered(
        &self,
        control: &PositionControlRecord,
        direction: &str,
        price: f64,
    ) -> bool {
        if direction.to_ascii_lowercase().contains("short") {
            price >= control.stop_loss_price
        } else {
            price <= control.stop_loss_price
        }
    }

    pub fn evaluate_close(&self, input: CloseRiskInput<'_>) -> (OrderRecord, GuardrailDecision) {
        let now = Utc::now();
        let side = if input.direction.to_ascii_lowercase().contains("short") {
            "BUY"
        } else {
            "SELL"
        };
        let mut key = idempotency_key(
            input.run_id,
            input.loop_id,
            input.decision_id,
            &input.control.instrument,
            side,
            input.signal_at,
            self.config.duplicate_order_window_seconds,
        );
        let mut reasons = Vec::new();
        if input.duplicate {
            reasons.push("duplicate idempotency key already exists".into());
        }
        if input.control.quantity <= 0.0 || input.control.notional <= 0.0 {
            reasons.push("position control has invalid quantity or notional".into());
        }
        if !input.reference_price.is_finite() || input.reference_price <= 0.0 {
            reasons.push("reference price is invalid".into());
        }
        if !input.stop_triggered
            && (now - input.signal_at).num_seconds() > self.config.max_signal_age_seconds as i64
        {
            reasons.push("close signal exceeded configured maximum age".into());
        }
        let accepted = reasons.is_empty();
        let event_id = Uuid::new_v4().to_string();
        let order_id = Uuid::new_v4().to_string();
        if input.duplicate {
            key = format!("{key}:rejected:{order_id}");
        }
        let order = OrderRecord {
            id: order_id,
            run_id: input.run_id.into(),
            loop_id: input.loop_id.into(),
            decision_id: input.decision_id.into(),
            idempotency_key: key,
            order_kind: if input.stop_triggered {
                "stop_close"
            } else {
                "market_close"
            }
            .into(),
            instrument: input.control.instrument.clone(),
            side: side.into(),
            quantity: input.control.quantity,
            reference_price: input.reference_price,
            notional: input.control.quantity * input.reference_price,
            stop_loss_price: None,
            signal_at: input.signal_at,
            status: if accepted { "approved" } else { "rejected" }.into(),
            rejection_reasons: reasons.clone(),
            created_by_event_id: event_id,
            created_at: now,
        };
        let gate = GuardrailDecision {
            accepted,
            effective_action: Jev1Action::NoTrade,
            reason: if accepted {
                if input.stop_triggered {
                    "deterministic stop-loss close approved".into()
                } else {
                    "deterministic close checks passed".into()
                }
            } else {
                reasons.join("; ")
            },
            order_id: order.id.clone(),
            checks: vec![
                "duplicate_order".into(),
                "signal_freshness".into(),
                "position_control".into(),
                "stop_loss".into(),
            ],
        };
        (order, gate)
    }

    pub fn retry_state(
        &self,
        loop_id: &str,
        previous_failures: u32,
        error: String,
        event_id: String,
    ) -> LoopFailureState {
        let failures = previous_failures.saturating_add(1);
        let delay = self
            .config
            .retry_base_seconds
            .saturating_mul(2u64.saturating_pow(failures.saturating_sub(1)));
        let paused = failures >= self.config.max_consecutive_failures;
        LoopFailureState {
            loop_id: loop_id.into(),
            consecutive_failures: failures,
            next_retry_at: (!paused).then(|| Utc::now() + chrono::Duration::seconds(delay as i64)),
            paused,
            last_error: Some(error),
            updated_by_event_id: event_id,
            updated_at: Utc::now(),
        }
    }
}

fn floor_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return 0.0;
    }
    (value / step).floor() * step
}

fn floor_step_from_minimum(value: f64, minimum: f64, step: f64) -> f64 {
    if !value.is_finite()
        || !minimum.is_finite()
        || !step.is_finite()
        || value < minimum
        || minimum <= 0.0
        || step <= 0.0
    {
        return 0.0;
    }
    let increments = ((value - minimum) / step).floor();
    let rounded = minimum + increments * step;
    if rounded.is_finite() && rounded >= minimum {
        rounded
    } else {
        0.0
    }
}

fn normalize_instrument(instrument: &str) -> String {
    instrument
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_uppercase)
        .collect()
}

pub fn instrument_quote_asset(instrument: &str) -> Option<String> {
    let normalized = normalize_instrument(instrument);
    ["USDT", "USDC", "BTC", "ETH"]
        .into_iter()
        .find(|suffix| normalized.ends_with(suffix))
        .map(str::to_owned)
        .or_else(|| {
            let suffix = normalized.get(normalized.len().checked_sub(3)?..)?;
            is_supported_currency_code(suffix).then(|| suffix.to_owned())
        })
}

pub fn is_supported_currency_code(currency: &str) -> bool {
    matches!(
        currency.to_ascii_uppercase().as_str(),
        "USD"
            | "EUR"
            | "GBP"
            | "AUD"
            | "NZD"
            | "CAD"
            | "CHF"
            | "JPY"
            | "CNY"
            | "HKD"
            | "SGD"
            | "MXN"
            | "ZAR"
            | "NOK"
            | "SEK"
            | "DKK"
            | "PLN"
            | "CZK"
            | "HUF"
            | "TRY"
            | "ILS"
            | "KRW"
            | "INR"
            | "BRL"
            | "BTC"
            | "ETH"
            | "USDT"
            | "USDC"
    )
}

pub fn idempotency_key(
    run_id: &str,
    loop_id: &str,
    decision_id: &str,
    instrument: &str,
    side: &str,
    signal_at: DateTime<Utc>,
    window_seconds: u64,
) -> String {
    let window = window_seconds.max(1) as i64;
    let bucket = signal_at.timestamp().div_euclid(window);
    let digest = Sha256::digest(
        format!("{run_id}|{loop_id}|{decision_id}|{instrument}|{side}|{bucket}").as_bytes(),
    );
    format!("{digest:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn input<'a>(action: Jev1Action, signal_at: DateTime<Utc>) -> EntryRiskInput<'a> {
        EntryRiskInput {
            run_id: "run",
            loop_id: "loop",
            decision_id: "decision",
            action,
            direction_constraint: EntryDirectionConstraint::Any,
            confidence: 0.9,
            signal_at,
            instrument: "BTC-USD",
            reference_price: 100.0,
            allocated_fraction: 0.5,
            current_total_exposure: 0.0,
            broker_risk_snapshot: None,
            broker_risk_snapshot_error: None,
            allow_static_risk_policy: true,
            open_positions_in_loop: 0,
            duplicate: false,
            minimum_confidence: 0.65,
            broker_volume_minimum: None,
            broker_volume_step: None,
            demo_fixed_quantity: None,
            allow_demo_minimum_step: false,
        }
    }

    #[test]
    fn confidence_and_no_trade_are_deterministic_rejections() {
        let engine = DeterministicRiskEngine::new(RiskPolicyConfig::default());
        let mut low = input(Jev1Action::Long, Utc::now());
        low.confidence = 0.64;
        let (order, gate) = engine.evaluate_entry(low);
        assert!(!gate.accepted);
        assert!(matches!(gate.effective_action, Jev1Action::NoTrade));
        assert_eq!(order.status, "rejected");

        let (_, no_trade) = engine.evaluate_entry(input(Jev1Action::NoTrade, Utc::now()));
        assert!(!no_trade.accepted);
    }

    #[test]
    fn explicit_user_direction_is_enforced_by_risk_gate() {
        let engine = DeterministicRiskEngine::new(RiskPolicyConfig::default());
        let objective = "Open one LONG BTCUSD position. Do not short.";
        let constraint = explicit_direction_constraint(objective);
        assert_eq!(constraint, EntryDirectionConstraint::LongOnly);

        let mut conflicting = input(Jev1Action::Short, Utc::now());
        conflicting.direction_constraint = constraint;
        let (order, gate) = engine.evaluate_entry(conflicting);
        assert!(!gate.accepted);
        assert_eq!(order.status, "rejected");
        assert!(order
            .rejection_reasons
            .iter()
            .any(|reason| reason.contains("explicit long-only")));

        let mut matching = input(Jev1Action::Long, Utc::now());
        matching.direction_constraint = constraint;
        assert!(engine.evaluate_entry(matching).1.accepted);
        assert_eq!(
            explicit_direction_constraint("Only short positions; do not go long."),
            EntryDirectionConstraint::ShortOnly
        );
        assert_eq!(
            explicit_direction_constraint("Long only and short only"),
            EntryDirectionConstraint::Conflicting
        );
    }

    #[test]
    fn human_verified_broker_minimum_and_increment_control_sizing() {
        assert_eq!(floor_step_from_minimum(0.019, 0.01, 0.01), 0.01);
        assert_eq!(floor_step_from_minimum(0.02, 0.01, 0.01), 0.02);
        assert_eq!(floor_step_from_minimum(0.25, 0.05, 0.1), 0.25);
        assert!((floor_step_from_minimum(0.249, 0.05, 0.1) - 0.15).abs() < 1e-12);
        assert_eq!(floor_step_from_minimum(0.049, 0.05, 0.1), 0.0);
    }

    #[test]
    fn human_approved_demo_cycle_rounds_up_to_one_configured_step_only() {
        let mut policy = RiskPolicyConfig::default();
        policy.quantity_step = 0.01;
        let engine = DeterministicRiskEngine::new(policy);
        let snapshot = crate::ports::BrokerRiskSnapshot {
            account_id: "demo".into(),
            environment: "demo".into(),
            equity: 1_000.0,
            free_margin: 1_000.0,
            deposit_asset_id: "USD".into(),
            deposit_currency_code: "USD".into(),
            observed_at: Utc::now(),
            account_open_exposure: 0.0,
            quote_to_deposit: std::collections::HashMap::from([("BTC-USD".into(), 1.0)]),
        };
        let mut approved = input(Jev1Action::Long, Utc::now());
        approved.instrument = "BTC-USD";
        approved.reference_price = 80_000.0;
        approved.allocated_fraction = 0.5;
        approved.broker_risk_snapshot = Some(&snapshot);
        approved.allow_demo_minimum_step = true;
        let (order, gate) = engine.evaluate_entry(approved);
        assert!(gate.accepted, "{}", gate.reason);
        assert_eq!(order.quantity, 0.01);
        assert_eq!(order.notional, 800.0);

        let mut outside_demo_approval = input(Jev1Action::Long, Utc::now());
        outside_demo_approval.instrument = "BTC-USD";
        outside_demo_approval.reference_price = 80_000.0;
        outside_demo_approval.allocated_fraction = 0.5;
        outside_demo_approval.broker_risk_snapshot = Some(&snapshot);
        let (order, gate) = engine.evaluate_entry(outside_demo_approval);
        assert!(!gate.accepted);
        assert_eq!(order.quantity, 0.0);
    }

    #[test]
    fn stale_duplicate_and_account_constraints_reject_orders() {
        let engine = DeterministicRiskEngine::new(RiskPolicyConfig::default());
        let (_, stale) =
            engine.evaluate_entry(input(Jev1Action::Long, Utc::now() - Duration::seconds(301)));
        assert!(!stale.accepted);

        let mut duplicate = input(Jev1Action::Long, Utc::now());
        duplicate.duplicate = true;
        assert!(!engine.evaluate_entry(duplicate).1.accepted);

        let mut capped = input(Jev1Action::Long, Utc::now());
        capped.current_total_exposure = 80_000.0;
        assert!(!engine.evaluate_entry(capped).1.accepted);
    }

    #[test]
    fn sizing_and_stops_come_only_from_policy_and_allocation() {
        let engine = DeterministicRiskEngine::new(RiskPolicyConfig::default());
        let (long, gate) = engine.evaluate_entry(input(Jev1Action::Long, Utc::now()));
        assert!(gate.accepted);
        assert_eq!(long.notional, 25_000.0);
        assert_eq!(long.quantity, 250.0);
        assert_eq!(long.stop_loss_price, Some(98.0));

        let (short, _) = engine.evaluate_entry(input(Jev1Action::Short, Utc::now()));
        assert_eq!(short.stop_loss_price, Some(102.0));
    }

    #[test]
    fn stop_and_failure_recovery_rules_are_reproducible() {
        let engine = DeterministicRiskEngine::new(RiskPolicyConfig::default());
        let control = PositionControlRecord {
            position_id: "position".into(),
            order_id: "order".into(),
            instrument: "BTC-USD".into(),
            quantity: 1.0,
            entry_price: 100.0,
            notional: 100.0,
            stop_loss_price: 98.0,
            created_by_event_id: "event".into(),
            created_at: Utc::now(),
        };
        assert!(engine.stop_triggered(&control, "Long", 98.0));
        assert!(!engine.stop_triggered(&control, "Long", 99.0));

        let first = engine.retry_state("loop", 0, "down".into(), "e1".into());
        assert_eq!(first.consecutive_failures, 1);
        assert!(!first.paused);
        let final_state = engine.retry_state("loop", 2, "down".into(), "e3".into());
        assert_eq!(final_state.consecutive_failures, 3);
        assert!(final_state.paused);
        assert!(final_state.next_retry_at.is_none());
    }
}
