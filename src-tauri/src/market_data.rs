use crate::domain::*;
use crate::ports::MarketDataProvider;
use anyhow::{bail, Context, Result};
use chrono::{TimeZone, Utc};
use native_tls::{TlsConnector, TlsStream};
use parking_lot::Mutex;
use prost::Message;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
enum EvalValue {
    Number(f64),
    Boolean(bool),
}

impl EvalValue {
    fn number(self) -> Result<f64> {
        match self {
            Self::Number(value) if value.is_finite() => Ok(value),
            Self::Number(_) => bail!("formula produced a non-finite number"),
            Self::Boolean(_) => bail!("formula expected a number but produced a boolean"),
        }
    }

    fn boolean(self) -> Result<bool> {
        match self {
            Self::Boolean(value) => Ok(value),
            Self::Number(_) => bail!("formula expected a boolean but produced a number"),
        }
    }

    fn json(self) -> Value {
        match self {
            Self::Number(value) => json!(value),
            Self::Boolean(value) => json!(value),
        }
    }
}

pub fn default_live_context_spec(instrument: &str) -> LiveContextSpec {
    let period = MarketDataPeriod::M5;
    let maximum_age_seconds = (period.seconds() * 2) as u64;
    LiveContextSpec {
        id: Uuid::new_v4().to_string(),
        version: 1,
        instrument: instrument.to_owned(),
        series_sources: HashMap::new(),
        fields: vec![
            LiveContextFieldSpec {
                field_id: "current_price".into(),
                label: "Current midpoint".into(),
                value_type: LiveValueType::Number,
                required: true,
                maximum_age_seconds: 15,
                expression: LiveExpression::CurrentMid,
                description: "Current executable-context midpoint from the live FIX quote.".into(),
            },
            LiveContextFieldSpec {
                field_id: "current_spread".into(),
                label: "Current spread".into(),
                value_type: LiveValueType::Number,
                required: true,
                maximum_age_seconds: 15,
                expression: LiveExpression::CurrentSpread,
                description: "Current ask minus bid.".into(),
            },
            LiveContextFieldSpec {
                field_id: "prior_30m_range".into(),
                label: "Prior completed 30-minute range".into(),
                value_type: LiveValueType::Number,
                required: true,
                maximum_age_seconds,
                expression: LiveExpression::Subtract {
                    left: Box::new(LiveExpression::RollingMax {
                        period: period.clone(),
                        column: "high".into(),
                        window: 6,
                    }),
                    right: Box::new(LiveExpression::RollingMin {
                        period: period.clone(),
                        column: "low".into(),
                        window: 6,
                    }),
                },
                description: "High-low range of the last six completed five-minute candles.".into(),
            },
            LiveContextFieldSpec {
                field_id: "price_vs_prior_range_high".into(),
                label: "Price versus prior range high".into(),
                value_type: LiveValueType::Number,
                required: true,
                maximum_age_seconds,
                expression: LiveExpression::Subtract {
                    left: Box::new(LiveExpression::CurrentMid),
                    right: Box::new(LiveExpression::RollingMax {
                        period: period.clone(),
                        column: "high".into(),
                        window: 6,
                    }),
                },
                description: "Live midpoint distance from the completed 30-minute range high."
                    .into(),
            },
            LiveContextFieldSpec {
                field_id: "five_minute_return".into(),
                label: "Last completed candle return".into(),
                value_type: LiveValueType::Number,
                required: true,
                maximum_age_seconds,
                expression: LiveExpression::PercentChange {
                    period: period.clone(),
                    column: "close".into(),
                    lag: 1,
                },
                description: "Close-to-close return over the latest completed five-minute candle."
                    .into(),
            },
            LiveContextFieldSpec {
                field_id: "rsi_14".into(),
                label: "14-period RSI".into(),
                value_type: LiveValueType::Number,
                required: true,
                maximum_age_seconds,
                expression: LiveExpression::Rsi { period, window: 14 },
                description: "RSI calculated from completed five-minute closes.".into(),
            },
        ],
        created_at: Utc::now(),
    }
}

pub fn requirements(spec: &LiveContextSpec) -> Result<Vec<SeriesRequirement>> {
    let mut required = BTreeMap::<i64, (MarketDataPeriod, usize)>::new();
    let mut field_ids = std::collections::HashSet::new();
    if spec.instrument.trim().is_empty() || spec.fields.is_empty() {
        bail!("live context specification requires an instrument and at least one field");
    }
    for field in &spec.fields {
        if field.field_id.trim().is_empty() || !field_ids.insert(field.field_id.clone()) {
            bail!("live context field IDs must be non-empty and unique");
        }
        if expression_type(&field.expression)? != field.value_type {
            bail!(
                "field {} declares a value type that does not match its formula",
                field.field_id
            );
        }
        collect_requirements(&field.expression, &mut required)?;
    }
    for source in spec.series_sources.values() {
        if !matches!(
            source.as_str(),
            "twelve-data-rest" | "ctrader-fix-price-only"
        ) {
            bail!("live context selected an unsupported candle source");
        }
    }
    Ok(required
        .into_values()
        .map(|(period, bars)| SeriesRequirement {
            source: spec.series_sources.get(&format!("{:?}", period)).cloned(),
            period,
            bars: bars.min(1_000),
        })
        .collect())
}

fn expression_type(expression: &LiveExpression) -> Result<LiveValueType> {
    let require = |value: &LiveExpression, expected: LiveValueType| -> Result<()> {
        if expression_type(value)? != expected {
            bail!("formula operand has the wrong type");
        }
        Ok(())
    };
    match expression {
        LiveExpression::GreaterThan { left, right } | LiveExpression::LessThan { left, right } => {
            require(left, LiveValueType::Number)?;
            require(right, LiveValueType::Number)?;
            Ok(LiveValueType::Boolean)
        }
        LiveExpression::And { args } | LiveExpression::Or { args } => {
            if args.is_empty() {
                bail!("boolean formula requires at least one operand");
            }
            for arg in args {
                require(arg, LiveValueType::Boolean)?;
            }
            Ok(LiveValueType::Boolean)
        }
        LiveExpression::Not { value } => {
            require(value, LiveValueType::Boolean)?;
            Ok(LiveValueType::Boolean)
        }
        LiveExpression::Crossover { left, right, .. }
        | LiveExpression::CrossUnder { left, right, .. } => {
            require(left, LiveValueType::Number)?;
            require(right, LiveValueType::Number)?;
            Ok(LiveValueType::Boolean)
        }
        LiveExpression::Add { args } | LiveExpression::Multiply { args } => {
            if args.is_empty() {
                bail!("arithmetic formula requires at least one operand");
            }
            for arg in args {
                require(arg, LiveValueType::Number)?;
            }
            Ok(LiveValueType::Number)
        }
        LiveExpression::Subtract { left, right } => {
            require(left, LiveValueType::Number)?;
            require(right, LiveValueType::Number)?;
            Ok(LiveValueType::Number)
        }
        LiveExpression::Divide {
            numerator,
            denominator,
        } => {
            require(numerator, LiveValueType::Number)?;
            require(denominator, LiveValueType::Number)?;
            Ok(LiveValueType::Number)
        }
        LiveExpression::RangePosition { value, low, high } => {
            require(value, LiveValueType::Number)?;
            require(low, LiveValueType::Number)?;
            require(high, LiveValueType::Number)?;
            Ok(LiveValueType::Number)
        }
        _ => Ok(LiveValueType::Number),
    }
}

fn collect_requirements(
    expression: &LiveExpression,
    required: &mut BTreeMap<i64, (MarketDataPeriod, usize)>,
) -> Result<()> {
    let mut add = |period: &MarketDataPeriod, bars: usize| -> Result<()> {
        if bars == 0 || bars > 1_000 {
            bail!("formula lookback must be between 1 and 1000 bars");
        }
        let entry = required
            .entry(period.seconds())
            .or_insert_with(|| (period.clone(), 0));
        entry.1 = entry.1.max(bars);
        Ok(())
    };
    match expression {
        LiveExpression::Series { period, lag, .. } => add(period, lag + 1)?,
        LiveExpression::Change { period, lag, .. }
        | LiveExpression::PercentChange { period, lag, .. } => add(period, lag + 1)?,
        LiveExpression::RollingMin { period, window, .. }
        | LiveExpression::RollingMax { period, window, .. }
        | LiveExpression::RollingMean { period, window, .. }
        | LiveExpression::RollingSum { period, window, .. }
        | LiveExpression::RollingStdDev { period, window, .. }
        | LiveExpression::Ema { period, window, .. } => add(period, *window)?,
        LiveExpression::Rsi { period, window } | LiveExpression::Atr { period, window } => {
            add(period, window + 1)?
        }
        LiveExpression::TrueRange { period, lag } => add(period, lag + 2)?,
        LiveExpression::Crossover {
            left,
            right,
            period,
        }
        | LiveExpression::CrossUnder {
            left,
            right,
            period,
        } => {
            add(period, 2)?;
            collect_requirements(left, required)?;
            collect_requirements(right, required)?;
        }
        LiveExpression::Add { args }
        | LiveExpression::Multiply { args }
        | LiveExpression::And { args }
        | LiveExpression::Or { args } => {
            for arg in args {
                collect_requirements(arg, required)?;
            }
        }
        LiveExpression::Subtract { left, right }
        | LiveExpression::Divide {
            numerator: left,
            denominator: right,
        }
        | LiveExpression::GreaterThan { left, right }
        | LiveExpression::LessThan { left, right } => {
            collect_requirements(left, required)?;
            collect_requirements(right, required)?;
        }
        LiveExpression::Not { value } => collect_requirements(value, required)?,
        LiveExpression::RangePosition { value, low, high } => {
            collect_requirements(value, required)?;
            collect_requirements(low, required)?;
            collect_requirements(high, required)?;
        }
        LiveExpression::Constant { .. }
        | LiveExpression::CurrentBid
        | LiveExpression::CurrentAsk
        | LiveExpression::CurrentMid
        | LiveExpression::CurrentSpread => {}
    }
    Ok(())
}

pub fn resolve_snapshot(
    run_id: &str,
    loop_id: &str,
    thesis_version_id: &str,
    context_version_id: &str,
    spec: &LiveContextSpec,
    market: MarketDataSnapshot,
) -> Result<ResolvedContextSnapshot> {
    let now = Utc::now();
    if market.quote.symbol != spec.instrument {
        bail!("quote symbol does not match live context specification");
    }
    if !market.quote.bid.is_finite()
        || !market.quote.ask.is_finite()
        || market.quote.bid <= 0.0
        || market.quote.ask < market.quote.bid
    {
        bail!("live quote is invalid");
    }
    if !crate::freshness::is_not_far_future(now, market.quote.received_at) {
        bail!("quote timestamp is in the future beyond the allowed clock skew");
    }
    let candle_map = candle_map(&market.candles)?;
    let mut fields = Vec::with_capacity(spec.fields.len());
    for field in &spec.fields {
        let value = eval(&field.expression, &market.quote, &candle_map, 0)
            .with_context(|| format!("field {} could not resolve", field.field_id));
        let value = match value {
            Ok(value) => value,
            Err(_error) if !field.required => continue,
            Err(error) => return Err(error),
        };
        match (&field.value_type, value) {
            (LiveValueType::Number, EvalValue::Number(_))
            | (LiveValueType::Boolean, EvalValue::Boolean(_)) => {}
            _ => bail!("field {} produced the wrong value type", field.field_id),
        }
        let uses_quote = expression_uses_quote(&field.expression);
        let periods = expression_periods(&field.expression);
        let mut ids = Vec::new();
        let mut provenance = Vec::new();
        let mut observed_at = now;
        if uses_quote {
            // Quote freshness is independent of candle-period freshness. A
            // model cannot make a stale quote acceptable by mixing it with a
            // longer-period candle formula.
            let quote_max_age_seconds = field.maximum_age_seconds.min(15);
            if !crate::freshness::is_fresh(now, market.quote.received_at, quote_max_age_seconds) {
                let quote_age = crate::freshness::age_seconds(now, market.quote.received_at);
                bail!(
                    "field {} requires a fresh quote; age={:.3}s max={}s",
                    field.field_id,
                    quote_age,
                    quote_max_age_seconds
                );
            }
            observed_at = market.quote.received_at;
            ids.push(market.quote.id.clone());
            provenance.push(market.quote.provenance.clone());
        }
        if !periods.is_empty() {
            for period in &periods {
                let period_candles: Vec<&Candle> = market
                    .candles
                    .iter()
                    .filter(|candle| &candle.period == period)
                    .collect();
                let latest_close = period_candles
                    .iter()
                    .map(|candle| {
                        candle.open_time + chrono::Duration::seconds(candle.period.seconds())
                    })
                    .max()
                    .with_context(|| {
                        format!("formula has no completed {period:?} candle observations")
                    })?;
                // Mixed quote+candle formulas refresh with each new quote, so
                // their candle inputs use the period's normal two-bar bound;
                // maximumAgeSeconds applies to the quote/output freshness.
                // Candle-only formulas retain the model's stricter declared
                // freshness, capped at two bars for the selected period.
                let candle_max_age_seconds = if uses_quote {
                    (period.seconds().max(1) as u64).saturating_mul(2)
                } else {
                    field
                        .maximum_age_seconds
                        .min((period.seconds().max(1) as u64).saturating_mul(2))
                };
                if !crate::freshness::is_fresh(now, latest_close, candle_max_age_seconds) {
                    let age = crate::freshness::age_seconds(now, latest_close);
                    bail!(
                        "field {} requires fresh completed candles; period={period:?} age={:.3}s max={}s",
                        field.field_id,
                        age,
                        candle_max_age_seconds
                    );
                }
                if !uses_quote && latest_close < observed_at {
                    observed_at = latest_close;
                }
                ids.extend(period_candles.iter().map(|candle| candle.id.clone()));
                ids.extend(
                    period_candles
                        .iter()
                        .flat_map(|candle| candle.source_observation_ids.iter().cloned()),
                );
                provenance.extend(
                    period_candles
                        .iter()
                        .map(|candle| candle.provenance.clone()),
                );
            }
        }
        ids.sort();
        ids.dedup();
        provenance.sort();
        provenance.dedup();
        fields.push(ResolvedLiveField {
            field_id: field.field_id.clone(),
            label: field.label.clone(),
            value: value.json(),
            value_type: field.value_type.clone(),
            formula: field.expression.clone(),
            observed_at,
            source_observation_ids: ids,
            provenance,
        });
    }
    Ok(ResolvedContextSnapshot {
        id: Uuid::new_v4().to_string(),
        run_id: run_id.into(),
        loop_id: loop_id.into(),
        thesis_version_id: thesis_version_id.into(),
        context_version_id: context_version_id.into(),
        live_context_spec_id: spec.id.clone(),
        live_context_spec_version: spec.version,
        quote: market.quote,
        candles: market.candles,
        fields,
        freshness_state: "fresh".into(),
        quality_state: market.quality_state,
        resolved_at: now,
        created_by_event_id: Uuid::new_v4().to_string(),
    })
}

fn candle_map(candles: &[Candle]) -> Result<HashMap<MarketDataPeriod, Vec<Candle>>> {
    let mut map = HashMap::new();
    for candle in candles {
        if !candle.closed {
            bail!("partial candle {} cannot enter Jev context", candle.id);
        }
        if ![candle.open, candle.high, candle.low, candle.close]
            .iter()
            .all(|v| v.is_finite())
            || candle.low > candle.high
        {
            bail!("candle {} is invalid", candle.id);
        }
        map.entry(candle.period.clone())
            .or_insert_with(Vec::new)
            .push(candle.clone());
    }
    for values in map.values_mut() {
        values.sort_by_key(|candle| candle.open_time);
    }
    Ok(map)
}

fn series<'a>(
    map: &'a HashMap<MarketDataPeriod, Vec<Candle>>,
    period: &MarketDataPeriod,
) -> Result<&'a [Candle]> {
    map.get(period)
        .map(Vec::as_slice)
        .context("required candle series is missing")
}

fn column(candle: &Candle, name: &str) -> Result<f64> {
    match name.to_ascii_lowercase().as_str() {
        "open" => Ok(candle.open),
        "high" => Ok(candle.high),
        "low" => Ok(candle.low),
        "close" => Ok(candle.close),
        "volume" | "provider_volume" => candle
            .provider_volume
            .context("provider trading volume is unavailable for this bar"),
        "tick_volume" => {
            if candle.volume_kind.as_deref() == Some("broker_tick_volume")
                || candle.volume_kind.as_deref() == Some("simulated_tick_volume")
                || (candle.volume_kind.is_none()
                    && (candle.provenance.contains("ctrader-open-api")
                        || candle.provenance.contains("simulated")))
            {
                Ok(candle.tick_volume as f64)
            } else {
                bail!("broker tick volume is unavailable for this bar")
            }
        }
        _ => bail!("unsupported candle column {name}"),
    }
}

fn at(values: &[Candle], lag: usize, offset: usize) -> Result<&Candle> {
    values
        .get(
            values
                .len()
                .checked_sub(1 + lag + offset)
                .context("insufficient candle history")?,
        )
        .context("insufficient candle history")
}

fn window_values(
    map: &HashMap<MarketDataPeriod, Vec<Candle>>,
    period: &MarketDataPeriod,
    name: &str,
    window: usize,
    offset: usize,
) -> Result<Vec<f64>> {
    if window == 0 {
        bail!("rolling window cannot be zero");
    }
    let values = series(map, period)?;
    let end = values
        .len()
        .checked_sub(offset)
        .context("insufficient candle history")?;
    let start = end
        .checked_sub(window)
        .context("insufficient candle history")?;
    values[start..end]
        .iter()
        .map(|candle| column(candle, name))
        .collect()
}

fn eval(
    expression: &LiveExpression,
    quote: &QuoteSnapshot,
    map: &HashMap<MarketDataPeriod, Vec<Candle>>,
    offset: usize,
) -> Result<EvalValue> {
    let number = |value: f64| {
        if value.is_finite() {
            Ok(EvalValue::Number(value))
        } else {
            bail!("formula produced a non-finite value")
        }
    };
    match expression {
        LiveExpression::Constant { value } => number(*value),
        LiveExpression::CurrentBid => number(quote.bid),
        LiveExpression::CurrentAsk => number(quote.ask),
        LiveExpression::CurrentMid => number(quote.mid),
        LiveExpression::CurrentSpread => number(quote.spread),
        LiveExpression::Series {
            period,
            column: name,
            lag,
        } => number(column(at(series(map, period)?, *lag, offset)?, name)?),
        LiveExpression::Add { args } => number(args.iter().try_fold(0.0, |sum, arg| {
            Ok::<_, anyhow::Error>(sum + eval(arg, quote, map, offset)?.number()?)
        })?),
        LiveExpression::Multiply { args } => {
            number(args.iter().try_fold(1.0, |product, arg| {
                Ok::<_, anyhow::Error>(product * eval(arg, quote, map, offset)?.number()?)
            })?)
        }
        LiveExpression::Subtract { left, right } => number(
            eval(left, quote, map, offset)?.number()?
                - eval(right, quote, map, offset)?.number()?,
        ),
        LiveExpression::Divide {
            numerator,
            denominator,
        } => {
            let d = eval(denominator, quote, map, offset)?.number()?;
            if d.abs() <= f64::EPSILON {
                bail!("division by zero");
            }
            number(eval(numerator, quote, map, offset)?.number()? / d)
        }
        LiveExpression::GreaterThan { left, right } => Ok(EvalValue::Boolean(
            eval(left, quote, map, offset)?.number()?
                > eval(right, quote, map, offset)?.number()?,
        )),
        LiveExpression::LessThan { left, right } => Ok(EvalValue::Boolean(
            eval(left, quote, map, offset)?.number()?
                < eval(right, quote, map, offset)?.number()?,
        )),
        LiveExpression::And { args } => {
            for arg in args {
                if !eval(arg, quote, map, offset)?.boolean()? {
                    return Ok(EvalValue::Boolean(false));
                }
            }
            Ok(EvalValue::Boolean(true))
        }
        LiveExpression::Or { args } => {
            for arg in args {
                if eval(arg, quote, map, offset)?.boolean()? {
                    return Ok(EvalValue::Boolean(true));
                }
            }
            Ok(EvalValue::Boolean(false))
        }
        LiveExpression::Not { value } => Ok(EvalValue::Boolean(
            !eval(value, quote, map, offset)?.boolean()?,
        )),
        LiveExpression::Change {
            period,
            column: name,
            lag,
        } => {
            let values = series(map, period)?;
            number(column(at(values, 0, offset)?, name)? - column(at(values, *lag, offset)?, name)?)
        }
        LiveExpression::PercentChange {
            period,
            column: name,
            lag,
        } => {
            let values = series(map, period)?;
            let old = column(at(values, *lag, offset)?, name)?;
            if old.abs() <= f64::EPSILON {
                bail!("percentage change divides by zero");
            }
            number(column(at(values, 0, offset)?, name)? / old - 1.0)
        }
        LiveExpression::RollingMin {
            period,
            column: name,
            window,
        } => number(
            window_values(map, period, name, *window, offset)?
                .into_iter()
                .fold(f64::INFINITY, f64::min),
        ),
        LiveExpression::RollingMax {
            period,
            column: name,
            window,
        } => number(
            window_values(map, period, name, *window, offset)?
                .into_iter()
                .fold(f64::NEG_INFINITY, f64::max),
        ),
        LiveExpression::RollingSum {
            period,
            column: name,
            window,
        } => number(
            window_values(map, period, name, *window, offset)?
                .into_iter()
                .sum(),
        ),
        LiveExpression::RollingMean {
            period,
            column: name,
            window,
        } => {
            let v = window_values(map, period, name, *window, offset)?;
            number(v.iter().sum::<f64>() / v.len() as f64)
        }
        LiveExpression::RollingStdDev {
            period,
            column: name,
            window,
        } => {
            let v = window_values(map, period, name, *window, offset)?;
            let mean = v.iter().sum::<f64>() / v.len() as f64;
            number((v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / v.len() as f64).sqrt())
        }
        LiveExpression::Ema {
            period,
            column: name,
            window,
        } => {
            let v = window_values(map, period, name, *window, offset)?;
            let alpha = 2.0 / (*window as f64 + 1.0);
            let mut ema = v[0];
            for x in &v[1..] {
                ema = alpha * x + (1.0 - alpha) * ema;
            }
            number(ema)
        }
        LiveExpression::Rsi { period, window } => {
            let closes = window_values(map, period, "close", window + 1, offset)?;
            let (mut gain, mut loss) = (0.0, 0.0);
            for pair in closes.windows(2) {
                let d = pair[1] - pair[0];
                if d >= 0.0 {
                    gain += d
                } else {
                    loss -= d
                }
            }
            if loss <= f64::EPSILON {
                number(100.0)
            } else {
                number(100.0 - (100.0 / (1.0 + gain / loss)))
            }
        }
        LiveExpression::TrueRange { period, lag } => {
            let values = series(map, period)?;
            let current = at(values, *lag, offset)?;
            let previous = at(values, lag + 1, offset)?;
            number(
                (current.high - current.low)
                    .max((current.high - previous.close).abs())
                    .max((current.low - previous.close).abs()),
            )
        }
        LiveExpression::Atr { period, window } => {
            let values = series(map, period)?;
            let mut sum = 0.0;
            for lag in 0..*window {
                let c = at(values, lag, offset)?;
                let p = at(values, lag + 1, offset)?;
                sum += (c.high - c.low)
                    .max((c.high - p.close).abs())
                    .max((c.low - p.close).abs());
            }
            number(sum / *window as f64)
        }
        LiveExpression::Crossover { left, right, .. } => Ok(EvalValue::Boolean(
            eval(left, quote, map, offset)?.number()?
                > eval(right, quote, map, offset)?.number()?
                && eval(left, quote, map, offset + 1)?.number()?
                    <= eval(right, quote, map, offset + 1)?.number()?,
        )),
        LiveExpression::CrossUnder { left, right, .. } => Ok(EvalValue::Boolean(
            eval(left, quote, map, offset)?.number()?
                < eval(right, quote, map, offset)?.number()?
                && eval(left, quote, map, offset + 1)?.number()?
                    >= eval(right, quote, map, offset + 1)?.number()?,
        )),
        LiveExpression::RangePosition { value, low, high } => {
            let low = eval(low, quote, map, offset)?.number()?;
            let high = eval(high, quote, map, offset)?.number()?;
            if (high - low).abs() <= f64::EPSILON {
                bail!("range position has a zero-width range");
            }
            number((eval(value, quote, map, offset)?.number()? - low) / (high - low))
        }
    }
}

fn expression_uses_quote(expression: &LiveExpression) -> bool {
    match expression {
        LiveExpression::CurrentBid
        | LiveExpression::CurrentAsk
        | LiveExpression::CurrentMid
        | LiveExpression::CurrentSpread => true,
        LiveExpression::Add { args }
        | LiveExpression::Multiply { args }
        | LiveExpression::And { args }
        | LiveExpression::Or { args } => args.iter().any(expression_uses_quote),
        LiveExpression::Subtract { left, right }
        | LiveExpression::GreaterThan { left, right }
        | LiveExpression::LessThan { left, right } => {
            expression_uses_quote(left) || expression_uses_quote(right)
        }
        LiveExpression::Divide {
            numerator,
            denominator,
        } => expression_uses_quote(numerator) || expression_uses_quote(denominator),
        LiveExpression::Not { value } => expression_uses_quote(value),
        LiveExpression::Crossover { left, right, .. }
        | LiveExpression::CrossUnder { left, right, .. } => {
            expression_uses_quote(left) || expression_uses_quote(right)
        }
        LiveExpression::RangePosition { value, low, high } => {
            expression_uses_quote(value)
                || expression_uses_quote(low)
                || expression_uses_quote(high)
        }
        _ => false,
    }
}

fn expression_periods(expression: &LiveExpression) -> Vec<MarketDataPeriod> {
    let mut required = BTreeMap::new();
    let _ = collect_requirements(expression, &mut required);
    required.into_values().map(|(period, _)| period).collect()
}

#[derive(Clone)]
pub struct CTraderOpenApiConfig {
    host: String,
    port: u16,
    client_id: String,
    client_secret: String,
    access_token: String,
    account_id: i64,
    symbol_map: HashMap<String, i64>,
    history_depth: usize,
    quote_max_age_seconds: u64,
    timeout: Duration,
}

impl CTraderOpenApiConfig {
    pub fn load(project_root: &Path) -> Result<Self> {
        dotenvy::from_path_override(project_root.join(".env")).ok();
        let required = |name: &str| -> Result<String> {
            let value = env::var(name).with_context(|| format!("missing {name}"))?;
            if value.trim().is_empty() || value.starts_with("REQUIRED_") {
                bail!("{name} is required for live Jev candle context");
            }
            Ok(value)
        };
        let environment =
            env::var("CTRADER_OPEN_API_ENVIRONMENT").unwrap_or_else(|_| "demo".into());
        let host = env::var("CTRADER_OPEN_API_HOST").unwrap_or_else(|_| {
            if environment.eq_ignore_ascii_case("live") {
                "live.ctraderapi.com".into()
            } else {
                "demo.ctraderapi.com".into()
            }
        });
        let client_id = required("CTRADER_OPEN_API_CLIENT_ID")?;
        let client_secret = required("CTRADER_OPEN_API_CLIENT_SECRET")?;
        let access_token = env::var("CTRADER_OPEN_API_ACCESS_TOKEN").unwrap_or_default();
        let access_token =
            if access_token.trim().is_empty() || access_token.starts_with("REQUIRED_") {
                let refresh_token = required("CTRADER_OPEN_API_REFRESH_TOKEN")?;
                let response = reqwest::blocking::Client::new()
                    .get("https://openapi.ctrader.com/apps/token")
                    .query(&[
                        ("grant_type", "refresh_token"),
                        ("refresh_token", refresh_token.as_str()),
                        ("client_id", client_id.as_str()),
                        ("client_secret", client_secret.as_str()),
                    ])
                    .send()
                    .context("refresh cTrader Open API access token")?;
                let status = response.status();
                let body: serde_json::Value = response
                    .json()
                    .context("decode cTrader token refresh response")?;
                if !status.is_success() {
                    bail!("cTrader token refresh failed with HTTP {status}: {body}");
                }
                body.get("accessToken")
                    .or_else(|| body.get("access_token"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .context("cTrader token refresh omitted access token")?
            } else {
                access_token
            };
        let symbol_map = required("CTRADER_OPEN_API_SYMBOL_MAP")?
            .split(',')
            .map(|entry| {
                let (name, id) = entry.split_once(':').with_context(|| {
                    format!("invalid CTRADER_OPEN_API_SYMBOL_MAP entry {entry}")
                })?;
                Ok((
                    name.trim()
                        .to_ascii_uppercase()
                        .replace(['/', '-', '_'], ""),
                    id.trim().parse::<i64>()?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let history_depth = env::var("CTRADER_MARKET_HISTORY_DEPTH")
            .ok()
            .map(|v| v.parse())
            .transpose()?
            .unwrap_or(250usize)
            .clamp(20, 1_000);
        Ok(Self {
            host,
            port: env::var("CTRADER_OPEN_API_PORT")
                .ok()
                .map(|v| v.parse())
                .transpose()?
                .unwrap_or(5035),
            client_id,
            client_secret,
            access_token,
            account_id: required("CTRADER_OPEN_API_ACCOUNT_ID")?
                .parse()
                .context("CTRADER_OPEN_API_ACCOUNT_ID must be an integer")?,
            symbol_map,
            history_depth,
            quote_max_age_seconds: env::var("CTRADER_MARKET_QUOTE_MAX_AGE_SECONDS")
                .ok()
                .map(|v| v.parse())
                .transpose()?
                .unwrap_or(15),
            timeout: Duration::from_secs(
                env::var("CTRADER_OPEN_API_TIMEOUT_SECONDS")
                    .ok()
                    .map(|v| v.parse())
                    .transpose()?
                    .unwrap_or(15),
            ),
        })
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoMessageEnvelope {
    #[prost(uint32, tag = "1")]
    payload_type: u32,
    #[prost(bytes = "vec", optional, tag = "2")]
    payload: Option<Vec<u8>>,
    #[prost(string, optional, tag = "3")]
    client_msg_id: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct ApplicationAuthReq {
    #[prost(string, tag = "2")]
    client_id: String,
    #[prost(string, tag = "3")]
    client_secret: String,
}

#[derive(Clone, PartialEq, Message)]
struct AccountAuthReq {
    #[prost(int64, tag = "2")]
    account_id: i64,
    #[prost(string, tag = "3")]
    access_token: String,
}

#[derive(Clone, PartialEq, Message)]
struct GetTrendbarsReq {
    #[prost(int64, tag = "2")]
    account_id: i64,
    #[prost(int64, optional, tag = "3")]
    from_timestamp: Option<i64>,
    #[prost(int64, optional, tag = "4")]
    to_timestamp: Option<i64>,
    #[prost(enumeration = "TrendbarPeriod", tag = "5")]
    period: i32,
    #[prost(int64, tag = "6")]
    symbol_id: i64,
    #[prost(uint32, optional, tag = "7")]
    count: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
struct GetTrendbarsRes {
    #[prost(int64, tag = "2")]
    account_id: i64,
    #[prost(enumeration = "TrendbarPeriod", tag = "3")]
    period: i32,
    #[prost(message, repeated, tag = "5")]
    trendbar: Vec<ProtoTrendbar>,
    #[prost(int64, optional, tag = "6")]
    symbol_id: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoTrendbar {
    #[prost(int64, tag = "3")]
    volume: i64,
    #[prost(enumeration = "TrendbarPeriod", optional, tag = "4")]
    period: Option<i32>,
    #[prost(int64, optional, tag = "5")]
    low: Option<i64>,
    #[prost(uint64, optional, tag = "6")]
    delta_open: Option<u64>,
    #[prost(uint64, optional, tag = "7")]
    delta_close: Option<u64>,
    #[prost(uint64, optional, tag = "8")]
    delta_high: Option<u64>,
    #[prost(uint32, optional, tag = "9")]
    utc_timestamp_minutes: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum TrendbarPeriod {
    M1 = 1,
    M5 = 5,
    M15 = 7,
    M30 = 8,
    H1 = 9,
    H4 = 10,
    D1 = 12,
}

fn proto_period(period: &MarketDataPeriod) -> TrendbarPeriod {
    match period {
        MarketDataPeriod::M1 => TrendbarPeriod::M1,
        MarketDataPeriod::M5 => TrendbarPeriod::M5,
        MarketDataPeriod::M15 => TrendbarPeriod::M15,
        MarketDataPeriod::M30 => TrendbarPeriod::M30,
        MarketDataPeriod::H1 => TrendbarPeriod::H1,
        MarketDataPeriod::H4 => TrendbarPeriod::H4,
        MarketDataPeriod::D1 => TrendbarPeriod::D1,
    }
}

struct OpenApiConnection {
    stream: TlsStream<TcpStream>,
}

impl OpenApiConnection {
    fn connect(config: &CTraderOpenApiConfig) -> Result<Self> {
        let address = (config.host.as_str(), config.port)
            .to_socket_addrs()?
            .next()
            .context("cTrader Open API host did not resolve")?;
        let tcp = TcpStream::connect_timeout(&address, config.timeout)?;
        tcp.set_read_timeout(Some(config.timeout))?;
        tcp.set_write_timeout(Some(config.timeout))?;
        tcp.set_nodelay(true)?;
        let stream = TlsConnector::new()?.connect(&config.host, tcp)?;
        let mut connection = Self { stream };
        connection.request(
            2100,
            &ApplicationAuthReq {
                client_id: config.client_id.clone(),
                client_secret: config.client_secret.clone(),
            },
            2101,
        )?;
        connection.request(
            2102,
            &AccountAuthReq {
                account_id: config.account_id,
                access_token: config.access_token.clone(),
            },
            2103,
        )?;
        Ok(connection)
    }

    fn send<M: Message>(
        &mut self,
        payload_type: u32,
        payload: &M,
        client_msg_id: String,
    ) -> Result<()> {
        let envelope = ProtoMessageEnvelope {
            payload_type,
            payload: Some(payload.encode_to_vec()),
            client_msg_id: Some(client_msg_id),
        }
        .encode_to_vec();
        let length = u32::try_from(envelope.len())
            .context("Open API message too large")?
            .to_be_bytes();
        self.stream.write_all(&length)?;
        self.stream.write_all(&envelope)?;
        self.stream.flush()?;
        Ok(())
    }

    fn receive(&mut self) -> Result<ProtoMessageEnvelope> {
        let mut length = [0u8; 4];
        self.stream.read_exact(&mut length)?;
        let length = u32::from_be_bytes(length) as usize;
        if length > 8 * 1024 * 1024 {
            bail!("cTrader Open API frame exceeded 8 MiB");
        }
        let mut body = vec![0u8; length];
        self.stream.read_exact(&mut body)?;
        Ok(ProtoMessageEnvelope::decode(body.as_slice())?)
    }

    fn request<M: Message>(
        &mut self,
        payload_type: u32,
        payload: &M,
        expected: u32,
    ) -> Result<ProtoMessageEnvelope> {
        let id = Uuid::new_v4().to_string();
        self.send(payload_type, payload, id.clone())?;
        loop {
            let response = self.receive()?;
            if response.payload_type == 2142 {
                bail!("cTrader Open API rejected request {payload_type}");
            }
            if response.payload_type == expected && response.client_msg_id.as_deref() == Some(&id) {
                return Ok(response);
            }
        }
    }
}

pub struct HybridCTraderMarketDataProvider {
    quote_feed: crate::ctrader_fix::CTraderFixQuoteFeed,
    open_api: CTraderOpenApiConfig,
    cache: Mutex<HashMap<(String, MarketDataPeriod), Vec<Candle>>>,
    last_history_request: Mutex<Option<Instant>>,
}

impl HybridCTraderMarketDataProvider {
    pub fn new(
        fix: crate::ctrader_fix::CTraderFixConfig,
        open_api: CTraderOpenApiConfig,
    ) -> Result<Self> {
        Ok(Self {
            quote_feed: crate::ctrader_fix::CTraderFixQuoteFeed::start(fix)?,
            open_api,
            cache: Mutex::new(HashMap::new()),
            last_history_request: Mutex::new(None),
        })
    }

    fn history(
        &self,
        instrument: &str,
        period: &MarketDataPeriod,
        count: usize,
    ) -> Result<Vec<Candle>> {
        let normalized = instrument.to_ascii_uppercase().replace(['/', '-', '_'], "");
        let now = Utc::now();
        if let Some(cached) = self
            .cache
            .lock()
            .get(&(normalized.clone(), period.clone()))
            .cloned()
        {
            let latest_close = cached
                .last()
                .map(|c| c.open_time + chrono::Duration::seconds(period.seconds()));
            if cached.len() >= count
                && latest_close
                    .map(|t| crate::freshness::is_fresh(now, t, (period.seconds() * 2) as u64))
                    .unwrap_or(false)
            {
                return Ok(cached
                    .into_iter()
                    .rev()
                    .take(count)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect());
            }
        }
        {
            let mut last = self.last_history_request.lock();
            if let Some(previous) = *last {
                let minimum = Duration::from_millis(220);
                if previous.elapsed() < minimum {
                    std::thread::sleep(minimum - previous.elapsed());
                }
            }
            *last = Some(Instant::now());
        }
        let symbol_id =
            *self.open_api.symbol_map.get(&normalized).with_context(|| {
                format!("CTRADER_OPEN_API_SYMBOL_MAP has no {normalized} entry")
            })?;
        let requested = count.max(self.open_api.history_depth).min(1_000);
        let to = (now.timestamp().div_euclid(period.seconds()) * period.seconds()) * 1_000;
        let from = to - (requested as i64 + 2) * period.seconds() * 1_000;
        let mut connection = OpenApiConnection::connect(&self.open_api)?;
        let response = connection.request(
            2137,
            &GetTrendbarsReq {
                account_id: self.open_api.account_id,
                from_timestamp: Some(from),
                to_timestamp: Some(to - 1),
                period: proto_period(period) as i32,
                symbol_id,
                count: Some(requested as u32),
            },
            2138,
        )?;
        let payload = response
            .payload
            .context("cTrader trendbar response omitted payload")?;
        let decoded = GetTrendbarsRes::decode(payload.as_slice())?;
        let mut candles = Vec::new();
        for bar in decoded.trendbar {
            let low = bar.low.context("trendbar omitted low")? as f64 / 100_000.0;
            let minute = bar
                .utc_timestamp_minutes
                .context("trendbar omitted UTC timestamp")? as i64;
            candles.push(Candle {
                id: format!("ctrader-openapi-{symbol_id}-{}-{minute}", period.seconds()),
                symbol: normalized.clone(),
                period: period.clone(),
                open_time: Utc
                    .timestamp_opt(minute * 60, 0)
                    .single()
                    .context("invalid trendbar timestamp")?,
                open: low + bar.delta_open.unwrap_or(0) as f64 / 100_000.0,
                high: low + bar.delta_high.unwrap_or(0) as f64 / 100_000.0,
                low,
                close: low + bar.delta_close.unwrap_or(0) as f64 / 100_000.0,
                tick_volume: bar.volume.max(0) as u64,
                provider_volume: None,
                volume_kind: Some("broker_tick_volume".into()),
                received_at: Some(now),
                source_observation_ids: Vec::new(),
                closed: true,
                provenance: "ctrader-open-api-historical-trendbar".into(),
            });
        }
        candles.sort_by_key(|c| c.open_time);
        candles.retain(|c| c.open_time + chrono::Duration::seconds(period.seconds()) <= now);
        if candles.len() < count {
            bail!(
                "cTrader Open API returned {} completed {:?} bars; {} required",
                candles.len(),
                period,
                count
            );
        }
        self.cache
            .lock()
            .insert((normalized, candles[0].period.clone()), candles.clone());
        Ok(candles
            .into_iter()
            .rev()
            .take(count)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect())
    }

    fn merge_completed_fix_bars(
        &self,
        instrument: &str,
        period: &MarketDataPeriod,
        mut candles: Vec<Candle>,
        count: usize,
    ) -> Result<Vec<Candle>> {
        let seconds = period.seconds();
        let since = candles
            .last()
            .map(|candle| candle.open_time + chrono::Duration::seconds(seconds))
            .unwrap_or_else(|| Utc::now() - chrono::Duration::seconds(seconds * count as i64));
        let quotes = self.quote_feed.quotes_since(instrument, since);
        let now = Utc::now();
        let mut grouped = BTreeMap::<i64, Vec<QuoteSnapshot>>::new();
        for quote in quotes {
            let bucket = quote.source_timestamp.timestamp().div_euclid(seconds) * seconds;
            if bucket + seconds <= now.timestamp() {
                grouped.entry(bucket).or_default().push(quote);
            }
        }
        for (bucket, mut quotes) in grouped {
            quotes.sort_by_key(|quote| quote.source_timestamp);
            let first = quotes.first().context("empty FIX quote bucket")?;
            let last = quotes.last().context("empty FIX quote bucket")?;
            let high = quotes
                .iter()
                .map(|quote| quote.mid)
                .fold(f64::NEG_INFINITY, f64::max);
            let low = quotes
                .iter()
                .map(|quote| quote.mid)
                .fold(f64::INFINITY, f64::min);
            let open_time = Utc
                .timestamp_opt(bucket, 0)
                .single()
                .context("invalid FIX candle time")?;
            let id = format!("ctrader-fix-{}-{}-{bucket}", instrument, seconds);
            candles.retain(|candle| candle.open_time != open_time);
            candles.push(Candle {
                id,
                symbol: instrument.into(),
                period: period.clone(),
                open_time,
                open: first.mid,
                high,
                low,
                close: last.mid,
                tick_volume: 0,
                provider_volume: None,
                volume_kind: Some("unavailable".into()),
                received_at: Some(now),
                source_observation_ids: quotes.iter().map(|quote| quote.id.clone()).collect(),
                closed: true,
                provenance: "ctrader-fix-locally-aggregated-completed-bar".into(),
            });
        }
        candles.sort_by_key(|candle| candle.open_time);
        candles.dedup_by_key(|candle| candle.open_time);
        if candles.len() < count {
            bail!(
                "only {} completed {:?} bars available after FIX aggregation; {} required",
                candles.len(),
                period,
                count
            );
        }
        let normalized = instrument.to_ascii_uppercase().replace(['/', '-', '_'], "");
        self.cache
            .lock()
            .insert((normalized, period.clone()), candles.clone());
        Ok(candles
            .into_iter()
            .rev()
            .take(count)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect())
    }
}

impl MarketDataProvider for HybridCTraderMarketDataProvider {
    fn snapshot(&self, request: &MarketDataRequest) -> Result<MarketDataSnapshot> {
        let quote = self.quote_feed.quote(&request.instrument)?;
        let now = Utc::now();
        let maximum_age = request
            .quote_max_age_seconds
            .min(self.open_api.quote_max_age_seconds);
        if !crate::freshness::is_fresh(now, quote.received_at, maximum_age) {
            let age = crate::freshness::age_seconds(now, quote.received_at);
            bail!("live FIX quote is stale or future dated: age={age:.3}s max={maximum_age}s");
        }
        let mut candles = Vec::new();
        for series in &request.series {
            let history = self.history(&request.instrument, &series.period, series.bars)?;
            candles.extend(self.merge_completed_fix_bars(
                &request.instrument,
                &series.period,
                history,
                series.bars,
            )?);
        }
        Ok(MarketDataSnapshot {
            quote,
            candles,
            captured_at: Utc::now(),
            quality_state: "legacy-open-api".into(),
        })
    }
}

pub struct UnavailableMarketDataProvider(pub String);
impl MarketDataProvider for UnavailableMarketDataProvider {
    fn snapshot(&self, _: &MarketDataRequest) -> Result<MarketDataSnapshot> {
        bail!("{}", self.0)
    }
}

pub struct SimulatedMarketDataProvider {
    sequence: Mutex<u64>,
}

impl SimulatedMarketDataProvider {
    pub fn new() -> Self {
        Self {
            sequence: Mutex::new(0),
        }
    }
}

impl Default for SimulatedMarketDataProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketDataProvider for SimulatedMarketDataProvider {
    fn is_simulated(&self) -> bool {
        true
    }

    fn snapshot(&self, request: &MarketDataRequest) -> Result<MarketDataSnapshot> {
        let mut sequence = self.sequence.lock();
        *sequence += 1;
        let now = Utc::now();
        let mid = 100.0 + sequence.saturating_sub(1) as f64 * 0.1;
        let quote = QuoteSnapshot {
            id: Uuid::new_v4().to_string(),
            symbol: request.instrument.clone(),
            bid: mid - 0.05,
            ask: mid + 0.05,
            mid,
            spread: 0.1,
            source_timestamp: now,
            received_at: now,
            provenance: "simulated-live-feed".into(),
        };
        let mut candles = Vec::new();
        for requirement in &request.series {
            let seconds = requirement.period.seconds();
            let aligned = now.timestamp().div_euclid(seconds) * seconds;
            for index in (0..requirement.bars.max(20)).rev() {
                let ts = aligned - seconds * (index as i64 + 1);
                let base = mid + (ts.rem_euclid(97) as f64 - 48.0) * 0.01;
                candles.push(Candle {
                    id: format!("sim-{}-{}-{ts}", request.instrument, seconds),
                    symbol: request.instrument.clone(),
                    period: requirement.period.clone(),
                    open_time: Utc
                        .timestamp_opt(ts, 0)
                        .single()
                        .context("invalid simulated candle time")?,
                    open: base - 0.04,
                    high: base + 0.12,
                    low: base - 0.11,
                    close: base + 0.03,
                    tick_volume: 100 + (ts.rem_euclid(23) as u64),
                    provider_volume: None,
                    volume_kind: Some("simulated_tick_volume".into()),
                    received_at: Some(now),
                    source_observation_ids: Vec::new(),
                    closed: true,
                    provenance: "simulated-completed-bars".into(),
                });
            }
        }
        Ok(MarketDataSnapshot {
            quote,
            candles,
            captured_at: now,
            quality_state: "simulated".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_changes_without_changing_the_spec() {
        let provider = SimulatedMarketDataProvider::new();
        let spec = default_live_context_spec("BTCUSD");
        let req = MarketDataRequest {
            instrument: "BTCUSD".into(),
            series: requirements(&spec).unwrap(),
            quote_max_age_seconds: 15,
        };
        let a =
            resolve_snapshot("r", "l", "t", "c", &spec, provider.snapshot(&req).unwrap()).unwrap();
        let b =
            resolve_snapshot("r", "l", "t", "c", &spec, provider.snapshot(&req).unwrap()).unwrap();
        assert_ne!(a.quote.id, b.quote.id);
        assert_ne!(a.quote.mid, b.quote.mid);
        assert_eq!(a.live_context_spec_id, b.live_context_spec_id);
    }

    #[test]
    fn partial_bars_are_rejected() {
        let provider = SimulatedMarketDataProvider::new();
        let spec = default_live_context_spec("BTCUSD");
        let req = MarketDataRequest {
            instrument: "BTCUSD".into(),
            series: requirements(&spec).unwrap(),
            quote_max_age_seconds: 15,
        };
        let mut market = provider.snapshot(&req).unwrap();
        market.candles[0].closed = false;
        assert!(resolve_snapshot("r", "l", "t", "c", &spec, market).is_err());
    }

    #[test]
    fn stale_quotes_and_division_by_zero_fail_resolution() {
        let provider = SimulatedMarketDataProvider::new();
        let mut spec = default_live_context_spec("BTCUSD");
        let req = MarketDataRequest {
            instrument: "BTCUSD".into(),
            series: requirements(&spec).unwrap(),
            quote_max_age_seconds: 15,
        };
        let mut stale = provider.snapshot(&req).unwrap();
        stale.quote.received_at = Utc::now() - chrono::Duration::seconds(16);
        assert!(resolve_snapshot("r", "l", "t", "c", &spec, stale)
            .unwrap_err()
            .to_string()
            .contains("fresh quote"));

        spec.fields = vec![LiveContextFieldSpec {
            field_id: "bad".into(),
            label: "bad".into(),
            value_type: LiveValueType::Number,
            required: true,
            maximum_age_seconds: 15,
            expression: LiveExpression::Divide {
                numerator: Box::new(LiveExpression::Constant { value: 1.0 }),
                denominator: Box::new(LiveExpression::Constant { value: 0.0 }),
            },
            description: "test".into(),
        }];
        let req = MarketDataRequest {
            instrument: "BTCUSD".into(),
            series: requirements(&spec).unwrap(),
            quote_max_age_seconds: 15,
        };
        assert!(
            resolve_snapshot("r", "l", "t", "c", &spec, provider.snapshot(&req).unwrap())
                .unwrap_err()
                .to_string()
                .contains("field bad")
        );
    }

    #[test]
    fn mixed_quote_candle_formula_uses_source_specific_freshness() {
        let provider = SimulatedMarketDataProvider::new();
        let mut spec = default_live_context_spec("BTCUSD");
        spec.fields = vec![LiveContextFieldSpec {
            field_id: "quote_distance_from_close".into(),
            label: "Quote distance from last close".into(),
            value_type: LiveValueType::Number,
            required: true,
            maximum_age_seconds: 15,
            expression: LiveExpression::Subtract {
                left: Box::new(LiveExpression::CurrentMid),
                right: Box::new(LiveExpression::Series {
                    period: MarketDataPeriod::M15,
                    column: "close".into(),
                    lag: 0,
                }),
            },
            description: "Fresh quote compared with the latest completed M15 close.".into(),
        }];
        let request = MarketDataRequest {
            instrument: "BTCUSD".into(),
            series: requirements(&spec).unwrap(),
            quote_max_age_seconds: 15,
        };
        let mut market = provider.snapshot(&request).unwrap();
        let candle = market.candles.first_mut().unwrap();
        candle.open_time = Utc::now() - chrono::Duration::minutes(17);
        assert!(resolve_snapshot("r", "l", "t", "c", &spec, market).is_ok());
    }

    #[test]
    fn future_quote_and_candle_timestamps_are_rejected() {
        let provider = SimulatedMarketDataProvider::new();
        let spec = default_live_context_spec("BTCUSD");
        let request = MarketDataRequest {
            instrument: "BTCUSD".into(),
            series: requirements(&spec).unwrap(),
            quote_max_age_seconds: 15,
        };

        let mut future_quote = provider.snapshot(&request).unwrap();
        future_quote.quote.received_at = Utc::now() + chrono::Duration::seconds(5);
        assert!(resolve_snapshot("r", "l", "t", "c", &spec, future_quote)
            .unwrap_err()
            .to_string()
            .contains("timestamp is in the future"));

        let mut future_candle = provider.snapshot(&request).unwrap();
        future_candle.candles.last_mut().unwrap().open_time =
            Utc::now() + chrono::Duration::seconds(5);
        assert!(resolve_snapshot("r", "l", "t", "c", &spec, future_candle)
            .unwrap_err()
            .to_string()
            .contains("requires fresh completed candles"));
    }

    #[test]
    fn formula_validation_rejects_type_mismatch_before_market_access() {
        let mut spec = default_live_context_spec("BTCUSD");
        spec.fields[0].value_type = LiveValueType::Boolean;
        assert!(requirements(&spec)
            .unwrap_err()
            .to_string()
            .contains("value type"));
    }

    #[test]
    fn strict_transport_expression_with_null_placeholders_deserializes() {
        let expression: LiveExpression = serde_json::from_value(json!({
            "op":"current_mid","value":null,"period":null,"column":null,"lag":null,
            "window":null,"args":[],"left":null,"right":null,"numerator":null,
            "denominator":null,"low":null,"high":null
        }))
        .unwrap();
        assert!(matches!(expression, LiveExpression::CurrentMid));
    }
}
