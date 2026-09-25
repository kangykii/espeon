use crate::domain::*;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub struct EntryRiskInput<'a> {
    pub run_id: &'a str,
    pub loop_id: &'a str,
    pub decision_id: &'a str,
    pub action: Jev1Action,
    pub confidence: f64,
    pub signal_at: DateTime<Utc>,
    pub instrument: &'a str,
    pub reference_price: f64,
    pub allocated_fraction: f64,
    pub current_total_exposure: f64,
    pub open_positions_in_loop: usize,
    pub duplicate: bool,
    pub minimum_confidence: f64,
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
        let loop_capital = self.config.paper_account_capital * input.allocated_fraction;
        let desired_notional = loop_capital * self.config.max_position_fraction_of_loop;
        let account_limit =
            self.config.paper_account_capital * self.config.max_total_exposure_fraction;
        let remaining_exposure = (account_limit - input.current_total_exposure).max(0.0);
        let notional = desired_notional.min(remaining_exposure);
        let raw_quantity = if price > 0.0 { notional / price } else { 0.0 };
        let quantity = floor_step(raw_quantity, self.config.quantity_step);
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
        if input.open_positions_in_loop >= self.config.max_open_positions_per_loop {
            reasons.push("loop reached maximum open-position count".into());
        }
        if !price.is_finite() || price <= 0.0 {
            reasons.push("reference price is invalid".into());
        }
        if notional < self.config.minimum_order_notional || quantity <= 0.0 {
            reasons.push("order is below minimum notional or quantity".into());
        }
        if input.current_total_exposure + notional > account_limit + f64::EPSILON {
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
            confidence: 0.9,
            signal_at,
            instrument: "BTC-USD",
            reference_price: 100.0,
            allocated_fraction: 0.5,
            current_total_exposure: 0.0,
            open_positions_in_loop: 0,
            duplicate: false,
            minimum_confidence: 0.65,
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
