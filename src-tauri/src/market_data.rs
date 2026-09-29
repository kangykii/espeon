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
use std::path::{Path, PathBuf};
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
            "twelve-data-rest" | "ctrader-fix-price-only" | "ctrader-open-api"
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
    token_state: std::sync::Arc<Mutex<OpenApiTokenState>>,
    project_root: PathBuf,
    account_id: Option<i64>,
    mcp_account_login: Option<i64>,
    environment: String,
    symbol_map: std::sync::Arc<Mutex<HashMap<String, i64>>>,
    history_depth: usize,
    quote_max_age_seconds: u64,
    timeout: Duration,
}

struct OpenApiTokenState {
    access_token: String,
    refresh_token: String,
    expires_at: Option<i64>,
    refresh_retry_after: Option<i64>,
}

impl CTraderOpenApiConfig {
    /// Loads Open API settings only when the integration has been configured.
    /// Missing credentials are a normal optional-source state, not a startup error.
    pub fn load_optional(project_root: &Path) -> Result<Option<Self>> {
        dotenvy::from_path_override(project_root.join(".env")).ok();
        let configured = [
            "CTRADER_OPEN_API_CLIENT_ID",
            "CTRADER_OPEN_API_CLIENT_SECRET",
        ]
        .iter()
        .all(|key| env::var(key).is_ok_and(|value| !value.trim().is_empty()));
        let has_token = [
            "CTRADER_OPEN_API_ACCESS_TOKEN",
            "CTRADER_OPEN_API_REFRESH_TOKEN",
        ]
        .iter()
        .any(|key| {
            env::var(key)
                .is_ok_and(|value| !value.trim().is_empty() && !value.starts_with("REQUIRED_"))
        });
        if !configured || !has_token {
            return Ok(None);
        }
        match Self::load(project_root) {
            Ok(config) => Ok(Some(config)),
            Err(error) => {
                eprintln!("cTrader Open API market-data source is disabled: {error:#}");
                Ok(None)
            }
        }
    }

    pub fn load(project_root: &Path) -> Result<Self> {
        dotenvy::from_path_override(project_root.join(".env")).ok();
        let required = |name: &str| -> Result<String> {
            let value = env::var(name).with_context(|| format!("missing {name}"))?;
            if value.trim().is_empty() || value.starts_with("REQUIRED_") {
                bail!("{name} is required for live Jev candle context");
            }
            Ok(value)
        };
        let environment = env::var("CTRADER_OPEN_API_ENVIRONMENT")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| env::var("CTRADER_MCP_ENVIRONMENT").ok())
            .unwrap_or_else(|| "demo".into());
        let host = env::var("CTRADER_OPEN_API_HOST").unwrap_or_else(|_| {
            if environment.eq_ignore_ascii_case("live") {
                "live.ctraderapi.com".into()
            } else {
                "demo.ctraderapi.com".into()
            }
        });
        let client_id = required("CTRADER_OPEN_API_CLIENT_ID")?;
        let client_secret = required("CTRADER_OPEN_API_CLIENT_SECRET")?;
        let mut access_token = env::var("CTRADER_OPEN_API_ACCESS_TOKEN")
            .unwrap_or_default()
            .trim()
            .to_owned();
        let mut refresh_token = env::var("CTRADER_OPEN_API_REFRESH_TOKEN")
            .unwrap_or_default()
            .trim()
            .to_owned();
        let mut token_expiry = env::var("CTRADER_OPEN_API_ACCESS_TOKEN_EXPIRES_AT")
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok());
        let refresh_is_configured =
            !refresh_token.is_empty() && !refresh_token.starts_with("REQUIRED_");
        let refresh_is_due = access_token.is_empty()
            || access_token.starts_with("REQUIRED_")
            || token_expiry.map_or(true, |expiry| expiry <= Utc::now().timestamp() + 120);
        let mut refresh_retry_after = None;
        if refresh_is_configured && refresh_is_due {
            match refresh_open_api_tokens(project_root, &client_id, &client_secret, &refresh_token)
            {
                Ok(tokens) => {
                    access_token = tokens.access_token;
                    refresh_token = tokens.refresh_token;
                    token_expiry = tokens.expires_at;
                }
                Err(error)
                    if !access_token.trim().is_empty()
                        && !access_token.starts_with("REQUIRED_")
                        && token_expiry.map_or(true, |expiry| expiry > Utc::now().timestamp()) =>
                {
                    eprintln!("cTrader Open API token refresh failed; retaining the unexpired access token: {error:#}");
                    refresh_retry_after = Some(Utc::now().timestamp() + 60);
                }
                Err(error) => return Err(error).context("refresh cTrader Open API access token"),
            }
        }
        if access_token.trim().is_empty() || access_token.starts_with("REQUIRED_") {
            bail!("CTRADER_OPEN_API_ACCESS_TOKEN or CTRADER_OPEN_API_REFRESH_TOKEN is required");
        }
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
            token_state: std::sync::Arc::new(Mutex::new(OpenApiTokenState {
                access_token,
                refresh_token,
                expires_at: token_expiry,
                refresh_retry_after,
            })),
            project_root: project_root.to_path_buf(),
            account_id: env::var("CTRADER_OPEN_API_ACCOUNT_ID")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(|value| {
                    value
                        .parse()
                        .context("cTrader Open API account ID must be an integer")
                })
                .transpose()?,
            mcp_account_login: env::var("CTRADER_MCP_ACCOUNT_ID")
                .ok()
                .and_then(|value| value.trim().parse().ok()),
            environment,
            symbol_map: std::sync::Arc::new(Mutex::new(HashMap::new())),
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

    fn access_token(&self) -> Result<String> {
        let mut state = self.token_state.lock();
        let now = Utc::now().timestamp();
        let refresh_due = state.expires_at.map_or(true, |expiry| expiry <= now + 120);
        if refresh_due {
            let current_token_still_unexpired =
                state.expires_at.map_or(true, |expiry| expiry > now);
            if current_token_still_unexpired
                && state
                    .refresh_retry_after
                    .is_some_and(|retry_after| retry_after > now)
            {
                return Ok(state.access_token.clone());
            }
            if state.refresh_token.trim().is_empty() || state.refresh_token.starts_with("REQUIRED_")
            {
                bail!(
                    "cTrader Open API access token expires soon and no refresh token is configured"
                );
            }
            match refresh_open_api_tokens(
                &self.project_root,
                &self.client_id,
                &self.client_secret,
                &state.refresh_token,
            ) {
                Ok(next) => *state = next,
                Err(error) if current_token_still_unexpired => {
                    eprintln!("cTrader Open API token refresh failed; retaining the unexpired access token: {error:#}");
                    state.refresh_retry_after = Some(now + 60);
                }
                Err(error) => {
                    return Err(error).context("refresh expired cTrader Open API access token");
                }
            }
        }
        Ok(state.access_token.clone())
    }
}

fn refresh_open_api_tokens(
    project_root: &Path,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<OpenApiTokenState> {
    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("create cTrader token refresh client")?
        .get("https://openapi.ctrader.com/apps/token")
        .query(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ])
        .send()
        .context("send cTrader token refresh request")?;
    let status = response.status();
    let body: Value = response
        .json()
        .context("decode cTrader token refresh response")?;
    if !status.is_success() {
        let error_code = body
            .get("errorCode")
            .and_then(Value::as_str)
            .unwrap_or("unknown error");
        bail!("cTrader token refresh failed with HTTP {status} ({error_code})");
    }
    let next_access = body
        .get("accessToken")
        .or_else(|| body.get("access_token"))
        .and_then(Value::as_str)
        .filter(|token| !token.trim().is_empty())
        .context("cTrader token refresh omitted access token")?;
    let next_refresh = body
        .get("refreshToken")
        .or_else(|| body.get("refresh_token"))
        .and_then(Value::as_str)
        .filter(|token| !token.trim().is_empty())
        .context("cTrader token refresh omitted rotated refresh token")?;
    let lifetime_seconds = body
        .get("expiresIn")
        .or_else(|| body.get("expires_in"))
        .and_then(Value::as_i64)
        .filter(|seconds| *seconds > 0)
        .unwrap_or(2_628_000);
    let expires_at = Utc::now().timestamp().saturating_add(lifetime_seconds);
    crate::config::persist_open_api_tokens(project_root, next_access, next_refresh, expires_at)
        .context("persist rotated cTrader Open API token pair and expiry")?;
    Ok(OpenApiTokenState {
        access_token: next_access.to_owned(),
        refresh_token: next_refresh.to_owned(),
        expires_at: Some(expires_at),
        refresh_retry_after: None,
    })
}

impl CTraderOpenApiConfig {
    pub fn volume_rules(
        &self,
        instruments: &[String],
    ) -> Result<HashMap<String, OpenApiVolumeRules>> {
        let mut connection = OpenApiConnection::connect(self)?;
        let mut rules = HashMap::new();
        for instrument in instruments {
            let normalized = instrument.to_ascii_uppercase().replace(['/', '-', '_'], "");
            let symbol_id = connection.symbol_id(&normalized)?;
            let response = connection.request(
                2112,
                &SymbolByIdReq {
                    account_id: connection.account_id,
                    symbol_id: vec![symbol_id],
                },
                2113,
            )?;
            let payload = response
                .payload
                .context("cTrader symbol-details response omitted payload")?;
            let details = SymbolByIdRes::decode(payload.as_slice())?;
            let mut matching = details
                .symbol
                .into_iter()
                .filter(|item| item.symbol_id == symbol_id);
            let symbol = matching
                .next()
                .context("cTrader Open API returned no details for the selected symbol")?;
            if matching.next().is_some() {
                bail!("cTrader Open API returned duplicate details for symbol {normalized}");
            }
            if symbol.symbol_name.as_deref().is_some_and(|name| {
                name.to_ascii_uppercase().replace(['/', '-', '_'], "") != normalized
            }) {
                bail!("cTrader Open API symbol ID resolved to a different symbol name");
            }
            let min = symbol
                .min_volume
                .context("cTrader symbol metadata omitted minVolume")?;
            let step = symbol
                .step_volume
                .context("cTrader symbol metadata omitted stepVolume")?;
            let lot = symbol
                .lot_size
                .context("cTrader symbol metadata omitted lotSize")?;
            if min <= 0 || step <= 0 || lot <= 0 {
                bail!("cTrader symbol metadata contains non-positive volume values");
            }
            let maximum = symbol.max_volume.map(|value| value as f64 / lot as f64);
            if maximum.is_some_and(|value| !value.is_finite() || value < min as f64 / lot as f64) {
                bail!("cTrader symbol metadata has an invalid maxVolume");
            }
            rules.insert(
                normalized,
                OpenApiVolumeRules {
                    minimum_lots: min as f64 / lot as f64,
                    step_lots: step as f64 / lot as f64,
                    maximum_lots: maximum,
                },
            );
        }
        Ok(rules)
    }

    pub fn history(
        &self,
        instrument: &str,
        period: &MarketDataPeriod,
        count: usize,
    ) -> Result<Vec<Candle>> {
        let normalized = instrument.to_ascii_uppercase().replace(['/', '-', '_'], "");
        let now = Utc::now();
        let requested = count.max(self.history_depth).min(1_000);
        let to = (now.timestamp().div_euclid(period.seconds()) * period.seconds()) * 1_000;
        let from = to - (requested as i64 + 2) * period.seconds() * 1_000;
        let mut connection = OpenApiConnection::connect(self)?;
        let symbol_id = connection.symbol_id(&normalized)?;
        let response = connection.request(
            2137,
            &GetTrendbarsReq {
                account_id: connection.account_id,
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
        let mut candles = decoded
            .trendbar
            .into_iter()
            .map(|bar| {
                let low = bar.low.unwrap_or_default() as f64 / 100_000.0;
                let minute = bar.utc_timestamp_minutes.unwrap_or_default() as i64;
                Candle {
                    id: format!("ctrader-openapi-{symbol_id}-{}-{minute}", period.seconds()),
                    symbol: normalized.clone(),
                    period: period.clone(),
                    open_time: Utc.timestamp_opt(minute * 60, 0).single().unwrap_or(now),
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
                }
            })
            .collect::<Vec<_>>();
        candles.sort_by_key(|candle| candle.open_time);
        candles
            .retain(|candle| candle.open_time + chrono::Duration::seconds(period.seconds()) <= now);
        if candles.len() < count {
            bail!(
                "cTrader Open API returned {} completed {:?} bars; {} required",
                candles.len(),
                period,
                count
            );
        }
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
struct GetAccountsByAccessTokenReq {
    #[prost(string, tag = "2")]
    access_token: String,
}

#[derive(Clone, PartialEq, Message)]
struct GetAccountsByAccessTokenRes {
    #[prost(message, repeated, tag = "4")]
    account: Vec<CTraderAccount>,
}

#[derive(Clone, PartialEq, Message)]
struct CTraderAccount {
    #[prost(int64, tag = "1")]
    account_id: i64,
    #[prost(bool, optional, tag = "2")]
    is_live: Option<bool>,
    #[prost(int64, optional, tag = "3")]
    trader_login: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct SymbolsListReq {
    #[prost(int64, tag = "2")]
    account_id: i64,
    #[prost(bool, optional, tag = "3")]
    include_archived_symbols: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
struct SymbolsListRes {
    #[prost(message, repeated, tag = "3")]
    symbol: Vec<LightSymbol>,
}

#[derive(Clone, PartialEq, Message)]
struct LightSymbol {
    #[prost(int64, tag = "1")]
    symbol_id: i64,
    #[prost(string, optional, tag = "2")]
    symbol_name: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct SymbolByIdReq {
    #[prost(int64, tag = "2")]
    account_id: i64,
    #[prost(int64, repeated, tag = "3")]
    symbol_id: Vec<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct SymbolByIdRes {
    #[prost(int64, tag = "2")]
    account_id: i64,
    #[prost(message, repeated, tag = "3")]
    symbol: Vec<ProtoSymbol>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoSymbol {
    #[prost(int64, tag = "1")]
    symbol_id: i64,
    #[prost(string, optional, tag = "2")]
    symbol_name: Option<String>,
    #[prost(int64, optional, tag = "9")]
    max_volume: Option<i64>,
    #[prost(int64, optional, tag = "10")]
    min_volume: Option<i64>,
    #[prost(int64, optional, tag = "11")]
    step_volume: Option<i64>,
    #[prost(int64, optional, tag = "30")]
    lot_size: Option<i64>,
}

#[derive(Clone, Copy, Debug)]
pub struct OpenApiVolumeRules {
    pub minimum_lots: f64,
    pub step_lots: f64,
    pub maximum_lots: Option<f64>,
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
    account_id: i64,
    symbol_map: HashMap<String, i64>,
}

impl OpenApiConnection {
    fn connect(config: &CTraderOpenApiConfig) -> Result<Self> {
        let access_token = config.access_token()?;
        let address = (config.host.as_str(), config.port)
            .to_socket_addrs()?
            .next()
            .context("cTrader Open API host did not resolve")?;
        let tcp = TcpStream::connect_timeout(&address, config.timeout)?;
        tcp.set_read_timeout(Some(config.timeout))?;
        tcp.set_write_timeout(Some(config.timeout))?;
        tcp.set_nodelay(true)?;
        let stream = TlsConnector::new()?.connect(&config.host, tcp)?;
        let mut connection = Self {
            stream,
            account_id: 0,
            symbol_map: HashMap::new(),
        };
        connection.request(
            2100,
            &ApplicationAuthReq {
                client_id: config.client_id.clone(),
                client_secret: config.client_secret.clone(),
            },
            2101,
        )?;

        let account_list = connection.request(
            2149,
            &GetAccountsByAccessTokenReq {
                access_token: access_token.clone(),
            },
            2150,
        )?;
        let account_list_payload = account_list
            .payload
            .context("cTrader account-list response omitted payload")?;
        let authorized: GetAccountsByAccessTokenRes =
            Message::decode(account_list_payload.as_slice())?;
        let requested_live = config.environment.eq_ignore_ascii_case("live");
        let account_id = if let Some(account_id) = config.account_id {
            let record = authorized
                .account
                .iter()
                .find(|account| account.account_id == account_id)
                .context("configured Open API account is not authorized by the saved token")?;
            if record
                .is_live
                .is_some_and(|is_live| is_live != requested_live)
            {
                bail!("configured Open API account environment does not match cTrader environment");
            }
            account_id
        } else {
            let matching = authorized
                .account
                .iter()
                .filter(|account| account.is_live == Some(requested_live))
                .collect::<Vec<_>>();
            match matching.as_slice() {
                [account] => account.account_id,
                [] => bail!(
                    "Open API token has no authorized {0} account",
                    config.environment
                ),
                _ => {
                    let matching_login = config.mcp_account_login.and_then(|login| {
                        let by_login = matching
                            .iter()
                            .filter(|account| account.trader_login == Some(login))
                            .collect::<Vec<_>>();
                        if by_login.len() == 1 {
                            Some(by_login[0].account_id)
                        } else {
                            None
                        }
                    });
                    matching_login.with_context(|| {
                        format!(
                            "Open API token has multiple authorized {} accounts and none uniquely matches the cTrader login; set CTRADER_OPEN_API_ACCOUNT_ID",
                            config.environment
                        )
                    })?
                }
            }
        };
        connection.account_id = account_id;
        connection.request(
            2102,
            &AccountAuthReq {
                account_id,
                access_token,
            },
            2103,
        )?;
        let cached_symbols = config.symbol_map.lock().clone();
        if cached_symbols.is_empty() {
            let symbols = connection.request(
                2114,
                &SymbolsListReq {
                    account_id,
                    include_archived_symbols: Some(false),
                },
                2115,
            )?;
            let symbols_payload = symbols
                .payload
                .context("cTrader symbol-list response omitted payload")?;
            let symbols: SymbolsListRes = Message::decode(symbols_payload.as_slice())?;
            for symbol in symbols.symbol {
                if let Some(name) = symbol.symbol_name {
                    let normalized = name.to_ascii_uppercase().replace(['/', '-', '_'], "");
                    connection.symbol_map.insert(normalized, symbol.symbol_id);
                }
            }
            *config.symbol_map.lock() = connection.symbol_map.clone();
        } else {
            connection.symbol_map = cached_symbols;
        }
        Ok(connection)
    }

    fn symbol_id(&self, instrument: &str) -> Result<i64> {
        self.symbol_map.get(instrument).copied().with_context(|| {
            format!("cTrader Open API account does not expose symbol {instrument}")
        })
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
        let requested = count.max(self.open_api.history_depth).min(1_000);
        let to = (now.timestamp().div_euclid(period.seconds()) * period.seconds()) * 1_000;
        let from = to - (requested as i64 + 2) * period.seconds() * 1_000;
        let mut connection = OpenApiConnection::connect(&self.open_api)?;
        let symbol_id = connection.symbol_id(&normalized)?;
        let response = connection.request(
            2137,
            &GetTrendbarsReq {
                account_id: connection.account_id,
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
