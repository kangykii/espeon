//! Local, source-labelled market observations. Network workers never run under the harness lock.
use crate::ctrader_fix::{CTraderFixConfig, CTraderFixQuoteFeed};
use crate::domain::{
    Candle, IntegrationStatus, MarketDataPeriod, MarketDataRequest, MarketDataSnapshot,
    QuoteSnapshot,
};
use crate::ports::MarketDataProvider;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use parking_lot::Mutex;
use rusqlite::{params, Connection};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tungstenite::{connect, Message};

fn timestamp_is_fresh(
    now: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    maximum_age_seconds: u64,
) -> bool {
    crate::freshness::is_fresh(now, observed_at, maximum_age_seconds)
}

#[derive(Clone)]
struct TwelveConfig {
    key: String,
    symbols: HashMap<String, String>,
    history_depth: usize,
    rest_interval: Duration,
}

impl TwelveConfig {
    fn load(root: &Path) -> Result<Option<Self>> {
        dotenvy::from_path_override(root.join(".env")).ok();
        let key = env::var("TWELVE_DATA_API_KEY").unwrap_or_default();
        if key.is_empty() || key.starts_with("REQUIRED_") {
            return Ok(None);
        }
        let mut symbols = HashMap::new();
        for mapping in env::var("TWELVE_DATA_SYMBOL_MAP")
            .unwrap_or_default()
            .split(',')
        {
            if mapping.trim().is_empty() {
                continue;
            }
            let (broker, provider) = mapping
                .split_once(':')
                .context("invalid Twelve Data symbol mapping")?;
            if broker.trim().is_empty() || provider.trim().is_empty() {
                bail!("Twelve Data symbol mappings require both broker and provider symbols");
            }
            symbols.insert(normalize(broker), provider.trim().to_owned());
        }
        if symbols.is_empty() {
            bail!("TWELVE_DATA_SYMBOL_MAP is required when Twelve Data is enabled");
        }
        Ok(Some(Self {
            key,
            symbols,
            history_depth: env::var("TWELVE_DATA_HISTORY_DEPTH")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(250)
                .clamp(30, 1_000),
            rest_interval: Duration::from_secs(
                env::var("TWELVE_DATA_REST_INTERVAL_SECONDS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(65)
                    .max(60),
            ),
        }))
    }
}

fn normalize(symbol: &str) -> String {
    symbol.to_ascii_uppercase().replace(['/', '-', '_'], "")
}

#[derive(Default)]
struct CacheState {
    fix_quote: HashMap<String, QuoteSnapshot>,
    external_price: HashMap<String, (String, f64, DateTime<Utc>, DateTime<Utc>)>,
    bars: HashMap<(String, String, i64), Candle>,
    provisional: HashMap<(String, i64), Candle>,
    health: HashMap<String, String>,
    health_events: Vec<(String, String)>,
    rest_credits_left: Option<u64>,
}

struct Cache {
    state: Mutex<CacheState>,
    db: Mutex<Connection>,
    divergence: HashMap<String, f64>,
    observations_written: Mutex<u64>,
    last_forced_refresh: Mutex<HashMap<String, Instant>>,
}

impl Cache {
    fn set_health(&self, service: &str, detail: String) {
        let mut state = self.state.lock();
        if state.health.get(service) != Some(&detail) {
            state.health.insert(service.into(), detail.clone());
            state.health_events.push((service.into(), detail));
            if state.health_events.len() > 1_000 {
                state.health_events.remove(0);
            }
        }
    }
    fn open(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root)?;
        let conn = Connection::open(root.join("market_context_cache.sqlite"))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=3000;")?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS observations (id TEXT PRIMARY KEY, symbol TEXT NOT NULL, source TEXT NOT NULL, kind TEXT NOT NULL, observed_at TEXT NOT NULL, received_at TEXT NOT NULL, payload TEXT NOT NULL, freshness_limit_seconds INTEGER NOT NULL DEFAULT 0, quality_state TEXT NOT NULL DEFAULT 'legacy'); CREATE INDEX IF NOT EXISTS observations_lookup ON observations(symbol, source, kind, observed_at);")?;
        let columns = conn
            .prepare("SELECT name FROM pragma_table_info('observations')")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if !columns.iter().any(|name| name == "freshness_limit_seconds") {
            conn.execute("ALTER TABLE observations ADD COLUMN freshness_limit_seconds INTEGER NOT NULL DEFAULT 0", [])?;
        }
        if !columns.iter().any(|name| name == "quality_state") {
            conn.execute(
                "ALTER TABLE observations ADD COLUMN quality_state TEXT NOT NULL DEFAULT 'legacy'",
                [],
            )?;
        }
        let mut state = CacheState::default();
        {
            let mut stmt = conn.prepare("SELECT payload FROM observations WHERE kind='completed_bar' ORDER BY observed_at DESC, rowid DESC LIMIT 10000")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            for row in rows {
                if let Ok(bar) = serde_json::from_str::<Candle>(&row?) {
                    state
                        .bars
                        .entry((
                            normalize(&bar.symbol),
                            bar.provenance.clone(),
                            bar.open_time.timestamp(),
                        ))
                        .or_insert(bar);
                }
            }
        }
        let mut divergence = HashMap::new();
        for item in env::var("MARKET_PRICE_DIVERGENCE_LIMITS")
            .unwrap_or_default()
            .split(',')
        {
            if let Some((symbol, limit)) = item.split_once(':') {
                if let Ok(limit) = limit.trim().parse::<f64>() {
                    divergence.insert(normalize(symbol), limit);
                }
            }
        }
        Ok(Self {
            state: Mutex::new(state),
            db: Mutex::new(conn),
            divergence,
            observations_written: Mutex::new(0),
            last_forced_refresh: Mutex::new(HashMap::new()),
        })
    }

    fn insert_bar(&self, mut bar: Candle) -> Result<()> {
        if !valid_bar(&bar, Utc::now()) {
            bail!("invalid or future market bar");
        }
        let fingerprint = serde_json::json!({"symbol":bar.symbol,"source":bar.provenance,"openTime":bar.open_time,
            "open":bar.open,"high":bar.high,"low":bar.low,"close":bar.close,
            "providerVolume":bar.provider_volume,"volumeKind":bar.volume_kind,
            "sourceObservationIds":bar.source_observation_ids});
        let hash = Sha256::digest(fingerprint.to_string().as_bytes());
        let digest = format!("{hash:x}");
        bar.id = format!("{}-{}", bar.id, &digest[..16]);
        let source = bar.provenance.clone();
        let quality = if source == "twelve-data-rest" {
            "provider-confirmed"
        } else {
            "fix-price-sampled"
        };
        self.db.lock().execute("INSERT OR IGNORE INTO observations(id,symbol,source,kind,observed_at,received_at,payload,freshness_limit_seconds,quality_state) VALUES(?1,?2,?3,'completed_bar',?4,?5,?6,?7,?8)",
            params![bar.id, normalize(&bar.symbol), source, (bar.open_time + chrono::Duration::seconds(bar.period.seconds())).to_rfc3339(),
                bar.received_at.unwrap_or_else(Utc::now).to_rfc3339(), serde_json::to_string(&bar)?, bar.period.seconds() * 2, quality])?;
        self.state.lock().bars.insert(
            (
                normalize(&bar.symbol),
                bar.provenance.clone(),
                bar.open_time.timestamp(),
            ),
            bar,
        );
        Ok(())
    }

    fn insert_fix_quote(&self, quote: QuoteSnapshot) -> Result<()> {
        let now = Utc::now();
        if !crate::freshness::is_not_far_future(now, quote.source_timestamp)
            || !crate::freshness::is_not_far_future(quote.received_at, quote.source_timestamp)
            || quote.ask < quote.bid
            || !quote.mid.is_finite()
        {
            bail!("invalid FIX quote timestamp or price");
        }
        self.state
            .lock()
            .fix_quote
            .insert(normalize(&quote.symbol), quote);
        Ok(())
    }

    fn save_observation(
        &self,
        id: &str,
        symbol: &str,
        source: &str,
        kind: &str,
        observed: DateTime<Utc>,
        received: DateTime<Utc>,
        freshness_limit_seconds: i64,
        quality_state: &str,
        payload: &Value,
    ) -> Result<()> {
        let conn = self.db.lock();
        conn.execute("INSERT OR IGNORE INTO observations(id,symbol,source,kind,observed_at,received_at,payload,freshness_limit_seconds,quality_state) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![id, normalize(symbol), source, kind, observed.to_rfc3339(), received.to_rfc3339(), payload.to_string(), freshness_limit_seconds, quality_state])?;
        let mut written = self.observations_written.lock();
        *written += 1;
        if *written >= 1_000 {
            conn.execute("DELETE FROM observations WHERE kind IN ('fix_quote','external_price') AND observed_at < ?1", [ (Utc::now() - chrono::Duration::hours(24)).to_rfc3339() ])?;
            *written = 0;
        }
        Ok(())
    }

    fn check_divergence(&self, symbol: &str, fix: f64, external: f64) -> Result<()> {
        let normalized = normalize(symbol);
        let limit = self
            .divergence
            .get(&normalized)
            .copied()
            .or_else(|| {
                if normalized.ends_with("USD")
                    && ["BTC", "ETH", "SOL"]
                        .iter()
                        .any(|p| normalized.starts_with(p))
                {
                    Some(0.01)
                } else if normalized.len() == 6 {
                    Some(0.002)
                } else {
                    None
                }
            })
            .context("market price divergence limit must be configured for this instrument")?;
        if fix <= 0.0 || external <= 0.0 || ((fix - external) / fix).abs() > limit {
            bail!("external market price diverges from executable FIX midpoint beyond configured limit");
        }
        Ok(())
    }
}

fn valid_bar(bar: &Candle, now: DateTime<Utc>) -> bool {
    bar.closed
        && bar.open_time.timestamp().rem_euclid(bar.period.seconds()) == 0
        && crate::freshness::is_not_far_future(
            now,
            bar.open_time + chrono::Duration::seconds(bar.period.seconds()),
        )
        && [bar.open, bar.high, bar.low, bar.close]
            .iter()
            .all(|v| v.is_finite() && *v > 0.0)
        && bar.low <= bar.open.min(bar.close)
        && bar.high >= bar.open.max(bar.close)
}

pub struct MarketContextCacheProvider {
    cache: Arc<Cache>,
    feed: Option<CTraderFixQuoteFeed>,
    twelve: Option<TwelveConfig>,
    open_api: Option<crate::market_data::CTraderOpenApiConfig>,
    rest_throttle: Option<Arc<Mutex<Instant>>>,
}

impl MarketContextCacheProvider {
    pub fn start(runtime_root: &Path, config_root: &Path, fix: CTraderFixConfig) -> Result<Self> {
        let feed = CTraderFixQuoteFeed::start(fix.clone())?;
        Self::start_with_feed(runtime_root, config_root, fix, feed)
    }

    pub fn start_with_feed(
        runtime_root: &Path,
        config_root: &Path,
        fix: CTraderFixConfig,
        feed: CTraderFixQuoteFeed,
    ) -> Result<Self> {
        let twelve = TwelveConfig::load(config_root)?;
        let open_api = crate::market_data::CTraderOpenApiConfig::load_optional(config_root)?;
        let cache = Arc::new(Cache::open(runtime_root)?);
        if open_api.is_none() {
            cache.set_health(
                "openapi",
                "credentials incomplete or invalid; optional source disabled, Twelve Data and FIX fallbacks remain available".into(),
            );
        }
        let symbols = CTraderFixQuoteFeed::configured_symbols(&fix);
        for symbol in &symbols {
            let symbol = symbol.clone();
            let worker_cache = Arc::clone(&cache);
            let feed = feed.clone();
            spawn_supervised(
                format!("market-fix-cache-{symbol}"),
                Arc::clone(&cache),
                move || fix_worker(Arc::clone(&worker_cache), feed.clone(), symbol.clone()),
            )?;
        }
        let rest_throttle = twelve
            .as_ref()
            .map(|_| Arc::new(Mutex::new(Instant::now() - Duration::from_secs(2))));
        if let Some(config) = twelve.clone() {
            for (symbol, provider_symbol) in config.symbols.clone() {
                let cache_rest = Arc::clone(&cache);
                let config_rest = config.clone();
                let rest_symbol = symbol.clone();
                let rest_provider = provider_symbol.clone();
                let throttle = Arc::clone(rest_throttle.as_ref().expect("configured throttle"));
                spawn_supervised(
                    format!("market-rest-{symbol}"),
                    Arc::clone(&cache),
                    move || {
                        rest_worker(
                            Arc::clone(&cache_rest),
                            config_rest.clone(),
                            rest_symbol.clone(),
                            rest_provider.clone(),
                            Arc::clone(&throttle),
                        )
                    },
                )?;
            }
            let cache_ws = Arc::clone(&cache);
            spawn_supervised(
                "market-twelve-websocket".into(),
                Arc::clone(&cache),
                move || websocket_worker(Arc::clone(&cache_ws), config.clone()),
            )?;
        }
        Ok(Self {
            cache,
            feed: Some(feed),
            twelve,
            open_api,
            rest_throttle,
        })
    }
}

fn spawn_supervised(
    name: String,
    cache: Arc<Cache>,
    worker: impl Fn() + Send + 'static,
) -> Result<()> {
    thread::Builder::new()
        .name(name.clone())
        .spawn(move || loop {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(&worker));
            cache.set_health(
                &format!("supervisor:{name}"),
                if result.is_err() {
                    "worker panicked; restarting after backoff".into()
                } else {
                    "worker exited; restarting after backoff".into()
                },
            );
            thread::sleep(Duration::from_secs(5));
        })?;
    Ok(())
}

impl MarketDataProvider for MarketContextCacheProvider {
    fn refresh(&self, request: &MarketDataRequest) -> Result<()> {
        let symbol = normalize(&request.instrument);
        let quote_stale = self
            .cache
            .state
            .lock()
            .fix_quote
            .get(&symbol)
            .is_none_or(|quote| {
                !timestamp_is_fresh(
                    Utc::now(),
                    quote.source_timestamp,
                    request.quote_max_age_seconds.min(15),
                )
            });
        if quote_stale {
            if let Some(feed) = &self.feed {
                feed.wait_for_fresh_quote(
                    &symbol,
                    request.quote_max_age_seconds.min(15),
                    Duration::from_secs(2),
                )?;
            }
        }
        if let Some(config) = &self.open_api {
            for series in request
                .series
                .iter()
                .filter(|series| series.source.as_deref() == Some("ctrader-open-api"))
            {
                let bars = config.history(&symbol, &series.period, series.bars)?;
                for bar in bars {
                    self.cache.insert_bar(bar)?;
                }
                self.cache.set_health(
                    &format!("openapi:{symbol}"),
                    "historical trendbars available".into(),
                );
            }
        }
        let Some(config) = &self.twelve else {
            return Ok(());
        };
        if request.series.is_empty()
            || request.series.iter().all(|series| {
                matches!(
                    series.source.as_deref(),
                    Some("ctrader-fix-price-only" | "ctrader-open-api")
                )
            })
        {
            return Ok(());
        }
        let Some(provider_symbol) = config.symbols.get(&symbol) else {
            return Ok(());
        };
        if self.cache.state.lock().rest_credits_left == Some(0) {
            bail!("Twelve Data REST credit budget is exhausted");
        }
        {
            let mut last = self.cache.last_forced_refresh.lock();
            if last
                .get(&symbol)
                .is_some_and(|at| at.elapsed() < Duration::from_secs(10))
            {
                return Ok(());
            }
            last.insert(symbol.clone(), Instant::now());
        }
        let throttle = self
            .rest_throttle
            .as_ref()
            .context("REST throttle not configured")?;
        {
            let mut last = throttle.lock();
            let remaining = Duration::from_secs(2).saturating_sub(last.elapsed());
            if !remaining.is_zero() {
                thread::sleep(remaining);
            }
            *last = Instant::now();
        }
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let (bars, credits) = fetch_minutes(&client, config, &symbol, provider_symbol)?;
        for bar in bars {
            self.cache.insert_bar(bar)?;
        }
        self.cache.state.lock().rest_credits_left = credits;
        self.cache.set_health(
            &format!("rest:{symbol}"),
            "one-shot context repair completed".into(),
        );
        Ok(())
    }
    fn drain_health_events(&self) -> Vec<(String, String)> {
        std::mem::take(&mut self.cache.state.lock().health_events)
    }

    fn health_statuses(&self) -> Vec<IntegrationStatus> {
        let state = self.cache.state.lock();
        let now = Utc::now();
        state
            .health
            .iter()
            .map(|(service, detail)| IntegrationStatus {
                id: format!("market-{service}"),
                label: service.clone(),
                state: if let Some(symbol) = service.strip_prefix("fix:") {
                    if state
                        .fix_quote
                        .get(symbol)
                        .is_some_and(|quote| timestamp_is_fresh(now, quote.source_timestamp, 15))
                        && self
                            .feed
                            .as_ref()
                            .is_none_or(|feed| feed.is_healthy(symbol))
                        && detail == "healthy"
                    {
                        "connected"
                    } else {
                        "degraded"
                    }
                } else if let Some(symbol) = service.strip_prefix("ws:") {
                    if state
                        .external_price
                        .get(symbol)
                        .is_some_and(|(_, _, observed, received)| {
                            timestamp_is_fresh(now, *observed, 30)
                                && timestamp_is_fresh(now, *received, 30)
                        })
                        && detail == "subscribed"
                    {
                        "connected"
                    } else {
                        "degraded"
                    }
                } else if detail == "healthy" {
                    "connected"
                } else {
                    "degraded"
                }
                .into(),
                detail: if let Some(symbol) = service.strip_prefix("fix:") {
                    format!(
                        "{detail}; {}",
                        self.feed
                            .as_ref()
                            .map(|feed| feed.metrics(symbol))
                            .unwrap_or_default()
                    )
                } else if let Some(symbol) = service.strip_prefix("ws:") {
                    format!(
                        "{detail}; last external update {}",
                        state
                            .external_price
                            .get(symbol)
                            .map(|(_, _, observed, _)| observed.to_rfc3339())
                            .unwrap_or_else(|| "never".into())
                    )
                } else {
                    detail.clone()
                },
            })
            .collect()
    }

    fn snapshot(&self, request: &MarketDataRequest) -> Result<MarketDataSnapshot> {
        let symbol = normalize(&request.instrument);
        let now = Utc::now();
        let state = self.cache.state.lock();
        let quote = state
            .fix_quote
            .get(&symbol)
            .cloned()
            .context("fresh FIX quote not yet cached")?;
        if self
            .feed
            .as_ref()
            .is_some_and(|feed| !feed.is_healthy(&symbol))
        {
            bail!("FIX price session is disconnected or unhealthy for {symbol}");
        }
        if !timestamp_is_fresh(
            now,
            quote.source_timestamp,
            request.quote_max_age_seconds.min(15),
        ) {
            let age = now
                .signed_duration_since(quote.source_timestamp)
                .num_milliseconds() as f64
                / 1_000.0;
            bail!("FIX quote stale or future dated: age={age:.3}s");
        }
        if let Some((_, price, observed, received)) = state.external_price.get(&symbol) {
            if timestamp_is_fresh(now, *observed, 30) && timestamp_is_fresh(now, *received, 30) {
                self.cache.check_divergence(&symbol, quote.mid, *price)?;
            }
        }
        let mut selected = Vec::new();
        for requirement in &request.series {
            let source = requirement.source.as_deref().unwrap_or("auto");
            let mut candidate = None;
            let choices = if source == "auto" {
                vec![
                    "ctrader-open-api",
                    "twelve-data-rest",
                    "ctrader-fix-price-only",
                ]
            } else {
                vec![source]
            };
            let mut progress = Vec::new();
            for choice in choices {
                let bars = if choice == "ctrader-open-api" {
                    series_from_period(&state, &symbol, &requirement.period, requirement.bars, now)
                } else {
                    series_from_minutes(
                        &state,
                        &symbol,
                        choice,
                        &requirement.period,
                        requirement.bars,
                        now,
                    )
                };
                if bars.len() >= requirement.bars {
                    candidate = Some(bars);
                    break;
                }
                progress.push(format!("{choice} {}/{}", bars.len(), requirement.bars));
            }
            let bars = candidate.with_context(|| format!(
                "warming up: need {} complete fresh {:?} bars from permitted source {source}; latest contiguous bars: {}",
                requirement.bars, requirement.period, progress.join(", ")))?;
            selected.extend(bars);
        }
        if selected
            .iter()
            .any(|bar| bar.provenance == "twelve-data-rest")
        {
            let fresh_stream =
                state
                    .external_price
                    .get(&symbol)
                    .filter(|(_, _, observed, received)| {
                        timestamp_is_fresh(now, *observed, 30)
                            && timestamp_is_fresh(now, *received, 30)
                    });
            if fresh_stream.is_none() {
                let last_complete_minute = now.timestamp().div_euclid(60) * 60 - 60;
                let aligned = (0..5).find_map(|offset| {
                    let minute = last_complete_minute - offset * 60;
                    let external =
                        state
                            .bars
                            .get(&(symbol.clone(), "twelve-data-rest".into(), minute))?;
                    let broker = state.bars.get(&(
                        symbol.clone(),
                        "ctrader-fix-price-only".into(),
                        minute,
                    ))?;
                    Some((external.close, broker.close))
                });
                let (external, broker) = aligned.context(
                    "Twelve Data context lacks a recent time-aligned FIX price comparison",
                )?;
                self.cache.check_divergence(&symbol, broker, external)?;
            }
        }
        let quality_state = if selected
            .iter()
            .any(|bar| bar.provenance == "ctrader-fix-price-only")
        {
            "fix-price-only"
        } else if selected
            .iter()
            .any(|bar| bar.provenance == "ctrader-open-api-historical-trendbar")
        {
            "ctrader-open-api-historical"
        } else if selected
            .iter()
            .any(|bar| bar.provenance == "twelve-data-rest")
        {
            if state
                .external_price
                .get(&symbol)
                .is_some_and(|(_, _, observed, received)| {
                    timestamp_is_fresh(now, *observed, 30) && timestamp_is_fresh(now, *received, 30)
                })
            {
                "rest-confirmed-with-websocket"
            } else {
                "rest-confirmed-stream-degraded"
            }
        } else {
            "fix-quote-only"
        };
        drop(state);
        self.cache.save_observation(
            &quote.id,
            &quote.symbol,
            &quote.provenance,
            "fix_quote",
            quote.source_timestamp,
            quote.received_at,
            15,
            "broker-executable",
            &serde_json::to_value(&quote)?,
        )?;
        Ok(MarketDataSnapshot {
            quote,
            candles: selected,
            captured_at: now,
            quality_state: quality_state.into(),
        })
    }
}

fn series_from_period(
    state: &CacheState,
    symbol: &str,
    period: &MarketDataPeriod,
    count: usize,
    now: DateTime<Utc>,
) -> Vec<Candle> {
    let mut bars = state
        .bars
        .iter()
        .filter(|((bar_symbol, source, _), bar)| {
            bar_symbol == symbol
                && source == "ctrader-open-api-historical-trendbar"
                && &bar.period == period
                && valid_bar(bar, now)
        })
        .map(|(_, bar)| bar.clone())
        .collect::<Vec<_>>();
    bars.sort_by_key(|bar| bar.open_time);
    bars.into_iter()
        .rev()
        .take(count)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

fn series_from_minutes(
    state: &CacheState,
    symbol: &str,
    source: &str,
    period: &MarketDataPeriod,
    count: usize,
    now: DateTime<Utc>,
) -> Vec<Candle> {
    let seconds = period.seconds();
    let current = now.timestamp().div_euclid(seconds) * seconds;
    let mut out = Vec::new();
    for index in 1..=count.min(1_000) {
        let start = current - index as i64 * seconds;
        let mut minutes = Vec::new();
        for minute in (start..start + seconds).step_by(60) {
            let Some(bar) = state
                .bars
                .get(&(symbol.to_owned(), source.to_owned(), minute))
            else {
                return out.into_iter().rev().collect();
            };
            if !valid_bar(bar, now) {
                return out.into_iter().rev().collect();
            }
            minutes.push(bar);
        }
        if minutes.len() != (seconds / 60) as usize {
            return out.into_iter().rev().collect();
        }
        let first = minutes[0];
        let last = minutes[minutes.len() - 1];
        let volume = if minutes
            .iter()
            .all(|bar| bar.volume_kind.as_deref() == Some("provider_volume"))
        {
            Some(minutes.iter().filter_map(|bar| bar.provider_volume).sum())
        } else {
            None
        };
        out.push(Candle {
            id: format!("{source}-{symbol}-{seconds}-{start}"),
            symbol: symbol.to_owned(),
            period: period.clone(),
            open_time: Utc
                .timestamp_opt(start, 0)
                .single()
                .expect("aligned timestamp"),
            open: first.open,
            high: minutes
                .iter()
                .map(|bar| bar.high)
                .fold(f64::NEG_INFINITY, f64::max),
            low: minutes
                .iter()
                .map(|bar| bar.low)
                .fold(f64::INFINITY, f64::min),
            close: last.close,
            tick_volume: 0,
            provider_volume: volume,
            volume_kind: Some(
                if volume.is_some() {
                    "provider_volume"
                } else {
                    "unavailable"
                }
                .into(),
            ),
            received_at: Some(last.received_at.unwrap_or(now)),
            closed: true,
            source_observation_ids: minutes
                .iter()
                .flat_map(|bar| {
                    std::iter::once(bar.id.clone())
                        .chain(bar.source_observation_ids.iter().cloned())
                })
                .collect(),
            provenance: source.to_owned(),
        });
    }
    out.into_iter().rev().collect()
}

fn fix_worker(cache: Arc<Cache>, feed: CTraderFixQuoteFeed, symbol: String) {
    let mut seen = std::collections::HashSet::<String>::new();
    let mut pending: Option<(i64, Vec<QuoteSnapshot>)> = None;
    let mut generation = 0;
    loop {
        let current_generation = feed.generation(&symbol);
        if current_generation != generation {
            generation = current_generation;
            pending = None;
            seen.clear();
            cache.state.lock().fix_quote.remove(&symbol);
        }
        let mut quotes = feed.quotes_since(&symbol, Utc::now() - chrono::Duration::minutes(5));
        quotes.sort_by_key(|quote| quote.source_timestamp);
        for quote in quotes {
            if !seen.insert(quote.id.clone()) {
                continue;
            }
            if let Err(error) = cache.insert_fix_quote(quote.clone()) {
                cache.set_health(&format!("fix:{symbol}"), error.to_string());
                continue;
            }
            let minute = quote.source_timestamp.timestamp().div_euclid(60) * 60;
            match &mut pending {
                Some((start, minute_quotes)) if *start == minute => minute_quotes.push(quote),
                Some((start, minute_quotes)) => {
                    if minute == *start + 60 {
                        if let Some(bar) = completed_fix_minute(&symbol, *start, minute_quotes) {
                            let _ = cache.insert_bar(bar);
                        }
                    }
                    pending = Some((minute, vec![quote]));
                }
                None => pending = Some((minute, vec![quote])),
            }
        }
        if seen.len() > 20_000 {
            seen.clear();
        }
        let stale = cache
            .state
            .lock()
            .fix_quote
            .get(&symbol)
            .is_none_or(|quote| !timestamp_is_fresh(Utc::now(), quote.source_timestamp, 15));
        if stale {
            pending = None;
            cache.set_health(
                &format!("fix:{symbol}"),
                "stale quote or disconnected".into(),
            );
        } else {
            cache.set_health(&format!("fix:{symbol}"), "healthy".into());
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn completed_fix_minute(symbol: &str, start: i64, quotes: &[QuoteSnapshot]) -> Option<Candle> {
    let first = quotes.first()?;
    let last = quotes.last()?;
    let open_time = Utc.timestamp_opt(start, 0).single()?;
    if first.source_timestamp > open_time + chrono::Duration::seconds(15)
        || last.source_timestamp < open_time + chrono::Duration::seconds(45)
    {
        return None;
    }
    if quotes.windows(2).any(|pair| {
        pair[1]
            .source_timestamp
            .signed_duration_since(pair[0].source_timestamp)
            .num_milliseconds()
            > 30_000
    }) {
        return None;
    }
    Some(Candle {
        id: format!("ctrader-fix-price-only-{symbol}-60-{start}"),
        symbol: symbol.into(),
        period: MarketDataPeriod::M1,
        open_time,
        open: first.mid,
        high: quotes
            .iter()
            .map(|q| q.mid)
            .fold(f64::NEG_INFINITY, f64::max),
        low: quotes.iter().map(|q| q.mid).fold(f64::INFINITY, f64::min),
        close: last.mid,
        tick_volume: 0,
        provider_volume: None,
        volume_kind: Some("unavailable".into()),
        received_at: Some(Utc::now()),
        closed: true,
        source_observation_ids: quotes.iter().map(|quote| quote.id.clone()).collect(),
        provenance: "ctrader-fix-price-only".into(),
    })
}

fn rest_worker(
    cache: Arc<Cache>,
    config: TwelveConfig,
    symbol: String,
    provider_symbol: String,
    throttle: Arc<Mutex<Instant>>,
) {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("REST client");
    let mut failures = 0u32;
    loop {
        if cache.state.lock().rest_credits_left == Some(0) {
            cache.set_health(
                &format!("rest:{symbol}"),
                "REST credits exhausted; waiting for quota reset".into(),
            );
            thread::sleep(Duration::from_secs(900));
            cache.state.lock().rest_credits_left = None;
        }
        {
            let mut last = throttle.lock();
            let remaining = Duration::from_secs(2).saturating_sub(last.elapsed());
            if !remaining.is_zero() {
                thread::sleep(remaining);
            }
            *last = Instant::now();
        }
        let mut credits_exhausted = false;
        match fetch_minutes(&client, &config, &symbol, &provider_symbol) {
            Ok((bars, credits)) => {
                for bar in bars {
                    if let Err(error) = cache.insert_bar(bar) {
                        cache.set_health(&format!("rest:{symbol}"), error.to_string());
                    }
                }
                let mut state = cache.state.lock();
                state.rest_credits_left = credits;
                credits_exhausted = credits == Some(0);
                drop(state);
                cache.set_health(&format!("rest:{symbol}"), "healthy".into());
                failures = 0;
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                cache.set_health(&format!("rest:{symbol}"), error.to_string());
            }
        }
        let backoff = if credits_exhausted {
            900
        } else {
            config
                .rest_interval
                .as_secs()
                .saturating_mul(1 << failures.min(3))
        };
        thread::sleep(Duration::from_secs(backoff.min(900)));
    }
}

fn fetch_minutes(
    client: &reqwest::blocking::Client,
    config: &TwelveConfig,
    symbol: &str,
    provider_symbol: &str,
) -> Result<(Vec<Candle>, Option<u64>)> {
    let response = client
        .get("https://api.twelvedata.com/time_series")
        .query(&[
            ("symbol", provider_symbol),
            ("interval", "1min"),
            ("outputsize", &config.history_depth.to_string()),
            ("timezone", "UTC"),
            ("apikey", config.key.as_str()),
        ])
        .send()
        .map_err(|_| anyhow::anyhow!("Twelve Data REST request failed"))?;
    let credits = response
        .headers()
        .get("api-credits-left")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    let status = response.status();
    if !status.is_success() {
        bail!("Twelve Data REST returned HTTP {status}");
    }
    let body: Value = response
        .json()
        .context("decode Twelve Data REST response")?;
    if body.get("status").and_then(Value::as_str) == Some("error") {
        bail!("Twelve Data REST rejected time_series request");
    }
    let now = Utc::now();
    let mut bars = Vec::new();
    for value in body
        .get("values")
        .and_then(Value::as_array)
        .context("Twelve Data response omitted values")?
    {
        let read = |name: &str| -> Result<f64> {
            Ok(value
                .get(name)
                .and_then(Value::as_str)
                .with_context(|| format!("missing {name}"))?
                .parse()?)
        };
        let time = value
            .get("datetime")
            .and_then(Value::as_str)
            .context("missing UTC datetime")?;
        let open_time = NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M:%S")?.and_utc();
        if open_time + chrono::Duration::minutes(1) > now {
            continue;
        }
        let volume = value
            .get("volume")
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0);
        let bar = Candle {
            id: format!("twelve-data-rest-{symbol}-60-{}", open_time.timestamp()),
            symbol: symbol.into(),
            period: MarketDataPeriod::M1,
            open_time,
            open: read("open")?,
            high: read("high")?,
            low: read("low")?,
            close: read("close")?,
            tick_volume: 0,
            provider_volume: volume,
            volume_kind: Some(
                if volume.is_some() {
                    "provider_volume"
                } else {
                    "unavailable"
                }
                .into(),
            ),
            received_at: Some(now),
            closed: true,
            provenance: "twelve-data-rest".into(),
            source_observation_ids: Vec::new(),
        };
        if valid_bar(&bar, now) {
            bars.push(bar);
        }
    }
    Ok((bars, credits))
}

fn websocket_worker(cache: Arc<Cache>, config: TwelveConfig) {
    let mut failures = 0u32;
    loop {
        match websocket_session(&cache, &config) {
            Ok(()) => failures = 0,
            Err(error) => {
                failures = failures.saturating_add(1);
                for symbol in config.symbols.keys() {
                    cache.set_health(&format!("ws:{symbol}"), error.to_string());
                }
            }
        }
        thread::sleep(Duration::from_secs((1u64 << failures.min(5)).min(30)));
    }
}

fn websocket_session(cache: &Cache, config: &TwelveConfig) -> Result<()> {
    let url = format!(
        "wss://ws.twelvedata.com/v1/quotes/price?apikey={}",
        config.key
    );
    let (mut ws, _) = connect(url.as_str())
        .map_err(|_| anyhow::anyhow!("Twelve Data WebSocket connection failed"))?;
    match ws.get_mut() {
        tungstenite::stream::MaybeTlsStream::Plain(stream) => {
            stream.set_read_timeout(Some(Duration::from_secs(10)))?
        }
        tungstenite::stream::MaybeTlsStream::NativeTls(stream) => stream
            .get_mut()
            .set_read_timeout(Some(Duration::from_secs(10)))?,
        _ => {}
    }
    let symbols = config
        .symbols
        .values()
        .cloned()
        .collect::<Vec<_>>()
        .join(",");
    ws.send(Message::Text(
        serde_json::json!({"action":"subscribe","params":{"symbols":symbols}})
            .to_string()
            .into(),
    ))?;
    let mut acknowledged = std::collections::HashSet::<String>::new();
    let mut last_message = std::time::Instant::now();
    loop {
        let message = match ws.read() {
            Ok(message) => message,
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if last_message.elapsed() > Duration::from_secs(45) {
                    bail!("Twelve Data WebSocket heartbeat timed out");
                }
                for symbol in config.symbols.keys() {
                    let stale = cache.state.lock().external_price.get(symbol).is_none_or(
                        |(_, _, observed, received)| {
                            !timestamp_is_fresh(Utc::now(), *observed, 30)
                                || !timestamp_is_fresh(Utc::now(), *received, 30)
                        },
                    );
                    if stale {
                        cache.set_health(
                            &format!("ws:{symbol}"),
                            "subscribed but price updates stale".into(),
                        );
                    }
                }
                ws.send(Message::Text(r#"{"action":"heartbeat"}"#.into()))?;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        last_message = std::time::Instant::now();
        let Value::Object(item) = serde_json::from_str::<Value>(&message.to_text()?)? else {
            continue;
        };
        match item.get("event").and_then(Value::as_str).unwrap_or("") {
            "subscribe-status" => {
                let success = item
                    .get("success")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for entry in success {
                    if let Some(provider) = entry.get("symbol").and_then(Value::as_str) {
                        acknowledged.insert(provider.to_owned());
                        for (symbol, mapped) in &config.symbols {
                            if mapped == provider {
                                cache.set_health(&format!("ws:{symbol}"), "subscribed".into());
                            }
                        }
                    }
                }
                for entry in item
                    .get("fails")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let Some(provider) = entry.get("symbol").and_then(Value::as_str) {
                        for (symbol, mapped) in &config.symbols {
                            if mapped == provider {
                                cache.set_health(
                                    &format!("ws:{symbol}"),
                                    "subscription rejected or quota exhausted".into(),
                                );
                            }
                        }
                    }
                }
            }
            "price" => {
                let Some(provider_symbol) = item.get("symbol").and_then(Value::as_str) else {
                    continue;
                };
                if !acknowledged.contains(provider_symbol) {
                    continue;
                }
                let Some((symbol, _)) = config
                    .symbols
                    .iter()
                    .find(|(_, mapped)| mapped.as_str() == provider_symbol)
                else {
                    continue;
                };
                let Some(price) = item.get("price").and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                }) else {
                    continue;
                };
                let Some(observed) = item
                    .get("timestamp")
                    .and_then(Value::as_i64)
                    .and_then(|ts| Utc.timestamp_opt(ts, 0).single())
                else {
                    continue;
                };
                if !crate::freshness::is_not_far_future(Utc::now(), observed)
                    || price <= 0.0
                    || !price.is_finite()
                {
                    continue;
                }
                if cache
                    .state
                    .lock()
                    .external_price
                    .get(symbol)
                    .is_some_and(|(_, _, previous, _)| observed < *previous)
                {
                    continue;
                }
                let received = Utc::now();
                let observation_id = uuid::Uuid::new_v4().to_string();
                cache.save_observation(&observation_id, symbol, "twelve-data-websocket", "external_price", observed, received,
                    30, "provider-stream-provisional",
                    &serde_json::json!({"providerSymbol":provider_symbol,"price":price,"observedAt":observed,"receivedAt":received}))?;
                let minute = observed.timestamp().div_euclid(60) * 60;
                let mut state = cache.state.lock();
                state
                    .external_price
                    .insert(symbol.into(), (observation_id, price, observed, received));
                let provisional = state
                    .provisional
                    .entry((symbol.into(), minute))
                    .or_insert_with(|| Candle {
                        id: format!("twelve-data-provisional-{symbol}-{minute}"),
                        symbol: symbol.into(),
                        period: MarketDataPeriod::M1,
                        open_time: Utc
                            .timestamp_opt(minute, 0)
                            .single()
                            .expect("aligned timestamp"),
                        open: price,
                        high: price,
                        low: price,
                        close: price,
                        tick_volume: 0,
                        provider_volume: None,
                        volume_kind: Some("unavailable".into()),
                        received_at: Some(Utc::now()),
                        closed: false,
                        provenance: "twelve-data-websocket-provisional".into(),
                        source_observation_ids: Vec::new(),
                    });
                provisional.high = provisional.high.max(price);
                provisional.low = provisional.low.min(price);
                provisional.close = price;
                state
                    .provisional
                    .retain(|(_, start), _| *start >= minute - 120);
                drop(state);
                cache.set_health(&format!("ws:{symbol}"), "subscribed".into());
            }
            "heartbeat" => {}
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_freshness_uses_both_future_and_stale_boundaries() {
        let now = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
        assert!(timestamp_is_fresh(
            now,
            now + chrono::Duration::milliseconds(1_000),
            15
        ));
        assert!(!timestamp_is_fresh(
            now,
            now + chrono::Duration::milliseconds(1_001),
            15
        ));
        assert!(timestamp_is_fresh(
            now,
            now - chrono::Duration::milliseconds(15_000),
            15
        ));
        assert!(!timestamp_is_fresh(
            now,
            now - chrono::Duration::milliseconds(15_001),
            15
        ));
    }

    #[test]
    fn missing_minute_never_becomes_complete_bar() {
        let state = CacheState::default();
        assert!(series_from_minutes(
            &state,
            "BTCUSD",
            "ctrader-fix-price-only",
            &MarketDataPeriod::M5,
            1,
            Utc::now()
        )
        .is_empty());
    }
    #[test]
    fn fix_quote_count_is_not_volume() {
        let start = Utc::now().timestamp().div_euclid(60) * 60 - 60;
        let make = |second: i64| QuoteSnapshot {
            id: second.to_string(),
            symbol: "BTCUSD".into(),
            bid: 99.0,
            ask: 101.0,
            mid: 100.0,
            spread: 2.0,
            source_timestamp: Utc.timestamp_opt(start + second, 0).single().unwrap(),
            received_at: Utc::now(),
            provenance: "fix".into(),
        };
        let bar = completed_fix_minute("BTCUSD", start, &[make(1), make(25), make(50)]).unwrap();
        assert_eq!(bar.tick_volume, 0);
        assert_eq!(bar.volume_kind.as_deref(), Some("unavailable"));
    }

    #[test]
    fn fix_minute_coverage_and_tick_gaps_use_exact_millisecond_bounds() {
        let start = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
        let make = |millis: i64| QuoteSnapshot {
            id: millis.to_string(),
            symbol: "BTCUSD".into(),
            bid: 99.0,
            ask: 101.0,
            mid: 100.0,
            spread: 2.0,
            source_timestamp: start + chrono::Duration::milliseconds(millis),
            received_at: start + chrono::Duration::seconds(60),
            provenance: "fix".into(),
        };

        assert!(completed_fix_minute(
            "BTCUSD",
            start.timestamp(),
            &[make(15_000), make(30_000), make(45_000)]
        )
        .is_some());
        assert!(completed_fix_minute(
            "BTCUSD",
            start.timestamp(),
            &[make(15_001), make(30_000), make(45_000)]
        )
        .is_none());
        assert!(completed_fix_minute(
            "BTCUSD",
            start.timestamp(),
            &[make(15_000), make(30_000), make(44_999)]
        )
        .is_none());
        assert!(completed_fix_minute(
            "BTCUSD",
            start.timestamp(),
            &[make(1_000), make(31_000), make(50_000)]
        )
        .is_some());
        assert!(completed_fix_minute(
            "BTCUSD",
            start.timestamp(),
            &[make(1_000), make(31_001), make(50_000)]
        )
        .is_none());
    }

    #[test]
    fn confirmed_minutes_aggregate_with_provenance_and_real_volume() {
        let now = Utc::now();
        let end = now.timestamp().div_euclid(300) * 300;
        let mut state = CacheState::default();
        for i in 0..5 {
            let open = end - 300 + i * 60;
            let bar = Candle {
                id: format!("minute-{i}"),
                symbol: "BTCUSD".into(),
                period: MarketDataPeriod::M1,
                open_time: Utc.timestamp_opt(open, 0).single().unwrap(),
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                tick_volume: 0,
                provider_volume: Some(10.0),
                volume_kind: Some("provider_volume".into()),
                received_at: Some(now),
                source_observation_ids: Vec::new(),
                closed: true,
                provenance: "twelve-data-rest".into(),
            };
            state
                .bars
                .insert(("BTCUSD".into(), "twelve-data-rest".into(), open), bar);
        }
        let bars = series_from_minutes(
            &state,
            "BTCUSD",
            "twelve-data-rest",
            &MarketDataPeriod::M5,
            1,
            now,
        );
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].provider_volume, Some(50.0));
        assert_eq!(bars[0].source_observation_ids.len(), 5);
        state
            .bars
            .remove(&("BTCUSD".into(), "twelve-data-rest".into(), end - 180));
        assert!(series_from_minutes(
            &state,
            "BTCUSD",
            "twelve-data-rest",
            &MarketDataPeriod::M5,
            1,
            now
        )
        .is_empty());
    }

    #[test]
    fn restart_cache_never_uses_old_quote_as_live() {
        let temp = tempfile::tempdir().unwrap();
        let cache = Cache::open(temp.path()).unwrap();
        let now = Utc::now();
        let quote = QuoteSnapshot {
            id: "q".into(),
            symbol: "BTCUSD".into(),
            bid: 99.0,
            ask: 101.0,
            mid: 100.0,
            spread: 2.0,
            source_timestamp: now - chrono::Duration::minutes(1),
            received_at: now,
            provenance: "fix".into(),
        };
        cache.insert_fix_quote(quote).unwrap();
        let provider = MarketContextCacheProvider {
            cache: Arc::new(cache),
            feed: None,
            twelve: None,
            open_api: None,
            rest_throttle: None,
        };
        assert!(provider
            .snapshot(&MarketDataRequest {
                instrument: "BTCUSD".into(),
                series: Vec::new(),
                quote_max_age_seconds: 15
            })
            .is_err());
        drop(provider);
        let restored = MarketContextCacheProvider {
            cache: Arc::new(Cache::open(temp.path()).unwrap()),
            feed: None,
            twelve: None,
            open_api: None,
            rest_throttle: None,
        };
        assert!(restored
            .snapshot(&MarketDataRequest {
                instrument: "BTCUSD".into(),
                series: Vec::new(),
                quote_max_age_seconds: 15
            })
            .is_err());
    }

    #[test]
    fn revised_rest_bar_preserves_the_original_observation_for_replay() {
        let temp = tempfile::tempdir().unwrap();
        let cache = Cache::open(temp.path()).unwrap();
        let start = Utc::now().timestamp().div_euclid(60) * 60 - 60;
        let mut bar = Candle {
            id: format!("rest-BTCUSD-{start}"),
            symbol: "BTCUSD".into(),
            period: MarketDataPeriod::M1,
            open_time: Utc.timestamp_opt(start, 0).single().unwrap(),
            open: 100.0,
            high: 102.0,
            low: 99.0,
            close: 101.0,
            tick_volume: 0,
            provider_volume: Some(12.0),
            volume_kind: Some("provider_volume".into()),
            received_at: Some(Utc::now()),
            source_observation_ids: Vec::new(),
            closed: true,
            provenance: "twelve-data-rest".into(),
        };
        cache.insert_bar(bar.clone()).unwrap();
        let first_id = cache
            .state
            .lock()
            .bars
            .get(&("BTCUSD".into(), "twelve-data-rest".into(), start))
            .unwrap()
            .id
            .clone();
        bar.close = 101.5;
        cache.insert_bar(bar).unwrap();
        let latest_id = cache
            .state
            .lock()
            .bars
            .get(&("BTCUSD".into(), "twelve-data-rest".into(), start))
            .unwrap()
            .id
            .clone();
        assert_ne!(first_id, latest_id);
        let count: i64 = cache
            .db
            .lock()
            .query_row(
                "SELECT count(*) FROM observations WHERE id IN (?1,?2)",
                params![first_id, latest_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
        drop(cache);
        let restored = Cache::open(temp.path()).unwrap();
        assert_eq!(
            restored
                .state
                .lock()
                .bars
                .get(&("BTCUSD".into(), "twelve-data-rest".into(), start))
                .unwrap()
                .id,
            latest_id
        );
    }

    #[test]
    fn unused_fix_ticks_stay_in_memory_but_selected_quote_is_persisted() {
        let temp = tempfile::tempdir().unwrap();
        let cache = Arc::new(Cache::open(temp.path()).unwrap());
        let now = Utc::now();
        cache
            .insert_fix_quote(QuoteSnapshot {
                id: "selected-quote".into(),
                symbol: "BTCUSD".into(),
                bid: 99.0,
                ask: 101.0,
                mid: 100.0,
                spread: 2.0,
                source_timestamp: now,
                received_at: now,
                provenance: "ctrader-fix".into(),
            })
            .unwrap();
        let count = || -> i64 {
            cache
                .db
                .lock()
                .query_row("SELECT count(*) FROM observations", [], |row| row.get(0))
                .unwrap()
        };
        assert_eq!(count(), 0);
        let provider = MarketContextCacheProvider {
            cache: Arc::clone(&cache),
            feed: None,
            twelve: None,
            open_api: None,
            rest_throttle: None,
        };
        assert_eq!(
            provider
                .snapshot(&MarketDataRequest {
                    instrument: "BTCUSD".into(),
                    series: Vec::new(),
                    quote_max_age_seconds: 15
                })
                .unwrap()
                .quote
                .id,
            "selected-quote"
        );
        assert_eq!(count(), 1);
    }
}
