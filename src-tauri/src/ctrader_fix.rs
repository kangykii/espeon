use crate::domain::*;
use crate::ports::ExecutionBroker;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use native_tls::{TlsConnector, TlsStream};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const SOH: u8 = 1;

fn timestamp_is_fresh(
    now: chrono::DateTime<Utc>,
    observed_at: chrono::DateTime<Utc>,
    maximum_age_seconds: u64,
) -> bool {
    crate::freshness::is_fresh(now, observed_at, maximum_age_seconds)
}

#[derive(Clone)]
struct FixEndpoint {
    host: String,
    port: u16,
    ssl: bool,
    username: String,
    password: String,
    sender_comp_id: String,
    sender_sub_id: String,
    target_comp_id: String,
    target_sub_id: String,
}

#[derive(Clone)]
pub struct CTraderFixConfig {
    trade: FixEndpoint,
    price: FixEndpoint,
    symbol_map: HashMap<String, String>,
    account_id: Option<String>,
    environment: Option<String>,
    demo_fixed_quantities: HashMap<String, f64>,
    heartbeat_seconds: u64,
    timeout_seconds: u64,
    reconnect_attempts: u32,
}

#[derive(Clone)]
pub struct CTraderFixQuoteFeed {
    latest: Arc<Mutex<HashMap<String, QuoteSnapshot>>>,
    recent: Arc<Mutex<HashMap<String, VecDeque<QuoteSnapshot>>>>,
    errors: Arc<Mutex<HashMap<String, String>>>,
    generations: Arc<Mutex<HashMap<String, u64>>>,
    last_message: Arc<Mutex<HashMap<String, chrono::DateTime<Utc>>>>,
    last_heartbeat: Arc<Mutex<HashMap<String, chrono::DateTime<Utc>>>>,
    last_sequence: Arc<Mutex<HashMap<String, u64>>>,
    timeout: Duration,
}

impl CTraderFixQuoteFeed {
    pub fn start(config: CTraderFixConfig) -> Result<Self> {
        let latest = Arc::new(Mutex::new(HashMap::new()));
        let recent = Arc::new(Mutex::new(HashMap::new()));
        let errors = Arc::new(Mutex::new(HashMap::new()));
        let generations = Arc::new(Mutex::new(HashMap::new()));
        let last_message = Arc::new(Mutex::new(HashMap::new()));
        let last_heartbeat = Arc::new(Mutex::new(HashMap::new()));
        let last_sequence = Arc::new(Mutex::new(HashMap::new()));
        for (instrument, symbol) in config.symbol_map.clone() {
            let endpoint = config.price.clone();
            let latest_state = Arc::clone(&latest);
            let error_state = Arc::clone(&errors);
            let recent_state = Arc::clone(&recent);
            let generation_state = Arc::clone(&generations);
            let message_state = Arc::clone(&last_message);
            let heartbeat_state = Arc::clone(&last_heartbeat);
            let sequence_state = Arc::clone(&last_sequence);
            let heartbeat = config.heartbeat_seconds;
            let timeout =
                Duration::from_secs(config.timeout_seconds.max(heartbeat.saturating_mul(2)));
            let reconnect_attempts = config.reconnect_attempts.max(1);
            thread::Builder::new()
                .name(format!("ctrader-fix-price-{instrument}"))
                .spawn(move || loop {
                    *generation_state
                        .lock()
                        .entry(instrument.clone())
                        .or_default() += 1;
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        stream_quotes(
                            &instrument,
                            &symbol,
                            &endpoint,
                            heartbeat,
                            timeout,
                            &latest_state,
                            &recent_state,
                            &message_state,
                            &heartbeat_state,
                            &sequence_state,
                            &error_state,
                        )
                    })) {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            latest_state.lock().remove(&instrument);
                            error_state
                                .lock()
                                .insert(instrument.clone(), error.to_string());
                        }
                        Err(_) => {
                            latest_state.lock().remove(&instrument);
                            error_state.lock().insert(
                                instrument.clone(),
                                "FIX price worker panicked; reconnecting".into(),
                            );
                        }
                    }
                    let backoff = reconnect_attempts.min(30) as u64;
                    thread::sleep(Duration::from_secs(backoff));
                })
                .context("start persistent cTrader FIX price worker")?;
        }
        Ok(Self {
            latest,
            recent,
            errors,
            generations,
            last_message,
            last_heartbeat,
            last_sequence,
            timeout: Duration::from_secs(
                config
                    .timeout_seconds
                    .max(config.heartbeat_seconds.saturating_mul(2)),
            ),
        })
    }

    pub fn configured_symbols(config: &CTraderFixConfig) -> Vec<String> {
        config.symbol_map.keys().cloned().collect()
    }

    pub fn generation(&self, instrument: &str) -> u64 {
        self.generations
            .lock()
            .get(&normalize_symbol(instrument))
            .copied()
            .unwrap_or(0)
    }

    pub fn metrics(&self, instrument: &str) -> String {
        let key = normalize_symbol(instrument);
        let age = |at: Option<chrono::DateTime<Utc>>| {
            at.map(|at| {
                Utc::now()
                    .signed_duration_since(at)
                    .num_seconds()
                    .to_string()
            })
            .unwrap_or_else(|| "never".into())
        };
        let quote = self.latest.lock().get(&key).cloned();
        format!("quote_age={}s message_age={}s heartbeat_age={}s sequence={} reconnects={} last_error={}",
            age(quote.map(|q| q.source_timestamp)), age(self.last_message.lock().get(&key).copied()),
            age(self.last_heartbeat.lock().get(&key).copied()),
            self.last_sequence.lock().get(&key).copied().unwrap_or(0), self.generation(&key).saturating_sub(1),
            self.errors.lock().get(&key).cloned().unwrap_or_else(|| "none".into()))
    }

    pub fn is_healthy(&self, instrument: &str) -> bool {
        let key = normalize_symbol(instrument);
        if self.errors.lock().contains_key(&key) || self.last_sequence.lock().get(&key).is_none() {
            return false;
        }
        let now = Utc::now();
        self.latest
            .lock()
            .get(&key)
            .is_some_and(|quote| timestamp_is_fresh(now, quote.source_timestamp, 15))
            && self
                .last_message
                .lock()
                .get(&key)
                .is_some_and(|at| timestamp_is_fresh(now, *at, self.timeout.as_secs()))
    }

    pub fn quote(&self, instrument: &str) -> Result<QuoteSnapshot> {
        let normalized = normalize_symbol(instrument);
        let deadline = std::time::Instant::now() + self.timeout;
        loop {
            if let Some(quote) = self.latest.lock().get(&normalized).cloned() {
                return Ok(quote);
            }
            if std::time::Instant::now() >= deadline {
                let detail = self
                    .errors
                    .lock()
                    .get(&normalized)
                    .cloned()
                    .unwrap_or_else(|| "no quote received before timeout".into());
                bail!("persistent cTrader FIX price feed unavailable for {normalized}: {detail}");
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn wait_for_fresh_quote(
        &self,
        instrument: &str,
        max_age_seconds: u64,
        timeout: Duration,
    ) -> Result<()> {
        let key = normalize_symbol(instrument);
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.latest.lock().get(&key).is_some_and(|quote| {
                timestamp_is_fresh(Utc::now(), quote.source_timestamp, max_age_seconds)
            }) {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                bail!("FIX quote did not refresh within bounded wait");
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn quotes_since(
        &self,
        instrument: &str,
        since: chrono::DateTime<Utc>,
    ) -> Vec<QuoteSnapshot> {
        self.recent
            .lock()
            .get(&normalize_symbol(instrument))
            .map(|quotes| {
                quotes
                    .iter()
                    .filter(|quote| quote.received_at >= since)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn stream_quotes(
    instrument: &str,
    symbol: &str,
    endpoint: &FixEndpoint,
    heartbeat: u64,
    timeout: Duration,
    latest: &Arc<Mutex<HashMap<String, QuoteSnapshot>>>,
    recent: &Arc<Mutex<HashMap<String, VecDeque<QuoteSnapshot>>>>,
    last_message: &Arc<Mutex<HashMap<String, chrono::DateTime<Utc>>>>,
    last_heartbeat: &Arc<Mutex<HashMap<String, chrono::DateTime<Utc>>>>,
    last_sequence: &Arc<Mutex<HashMap<String, u64>>>,
    errors: &Arc<Mutex<HashMap<String, String>>>,
) -> Result<()> {
    latest.lock().remove(instrument);
    recent.lock().remove(instrument);
    last_message.lock().remove(instrument);
    last_heartbeat.lock().remove(instrument);
    last_sequence.lock().remove(instrument);
    let mut session = FixSession::connect(endpoint, heartbeat, timeout)?;
    session.send("V", &market_data_request_fields(symbol))?;
    let mut previous: Option<QuoteSnapshot> = None;
    loop {
        let response = session.read_message()?;
        last_message.lock().insert(instrument.into(), Utc::now());
        if let Some(sequence) = value(&response, "34").and_then(|v| v.parse().ok()) {
            last_sequence.lock().insert(instrument.into(), sequence);
        }
        match value(&response, "35") {
            Some("W") | Some("X") => {
                if value(&response, "55").is_some_and(|received| received != symbol) {
                    bail!(
                        "cTrader FIX quote symbol differs from configured mapping for {instrument}"
                    );
                }
                let (message_bid, message_ask) = market_data_prices(&response);
                let bid = message_bid.or_else(|| previous.as_ref().map(|quote| quote.bid));
                let ask = message_ask.or_else(|| previous.as_ref().map(|quote| quote.ask));
                if let (Some(bid), Some(ask)) = (bid, ask) {
                    if ask < bid {
                        continue;
                    }
                    let now = Utc::now();
                    let Some(source_timestamp) = value(&response, "52")
                        .and_then(|value| {
                            chrono::NaiveDateTime::parse_from_str(value, "%Y%m%d-%H:%M:%S%.f").ok()
                        })
                        .map(|value| value.and_utc())
                    else {
                        continue;
                    };
                    if !crate::freshness::is_not_far_future(now, source_timestamp)
                        || previous
                            .as_ref()
                            .is_some_and(|quote| source_timestamp < quote.source_timestamp)
                    {
                        continue;
                    }
                    let mid = (bid + ask) / 2.0;
                    let quote = QuoteSnapshot {
                        id: Uuid::new_v4().to_string(),
                        symbol: instrument.to_owned(),
                        bid,
                        ask,
                        mid,
                        spread: ask - bid,
                        source_timestamp,
                        received_at: now,
                        provenance: "ctrader-fix-persistent-price-session".into(),
                    };
                    latest.lock().insert(instrument.to_owned(), quote.clone());
                    errors.lock().remove(instrument);
                    previous = Some(quote.clone());
                    let mut recent = recent.lock();
                    let queue = recent.entry(instrument.to_owned()).or_default();
                    queue.push_back(quote);
                    while queue.len() > 10_000 {
                        queue.pop_front();
                    }
                }
            }
            Some("j") => bail!(
                "cTrader streaming price request rejected: {}",
                value(&response, "58").unwrap_or("unknown")
            ),
            Some("Y") => bail!("{}", market_data_rejection(&response)),
            Some("0") => {
                last_heartbeat.lock().insert(instrument.into(), Utc::now());
                session.send("0", &[])?;
            }
            Some("1") => session.send(
                "0",
                &value(&response, "112")
                    .map(|id| vec![("112", id.to_owned())])
                    .unwrap_or_default(),
            )?,
            Some("5") => bail!("cTrader ended persistent price session"),
            _ => {}
        }
    }
}

fn market_data_request_fields(symbol: &str) -> Vec<(&'static str, String)> {
    vec![
        ("262", Uuid::new_v4().to_string()),
        ("263", "1".into()),
        ("264", "1".into()),
        ("265", "1".into()),
        ("146", "1".into()),
        ("55", symbol.to_owned()),
        ("267", "2".into()),
        ("269", "0".into()),
        ("269", "1".into()),
    ]
}

fn market_data_rejection(fields: &[(String, String)]) -> String {
    let reason = match value(fields, "281") {
        Some("0") => "unknown symbol",
        Some("4") => "unsupported subscription type",
        Some("5") => "unsupported market depth",
        Some(_) | None => "unspecified rejection",
    };
    format!(
        "cTrader market-data request rejected ({reason}): {}",
        value(fields, "58").unwrap_or("no broker detail")
    )
}

fn market_data_prices(fields: &[(String, String)]) -> (Option<f64>, Option<f64>) {
    let mut side = None;
    let mut bid = None;
    let mut ask = None;
    for (tag, entry) in fields {
        if tag == "269" {
            side = Some(entry.as_str());
        } else if tag == "270" {
            let Ok(price) = entry.parse::<f64>() else {
                continue;
            };
            match side {
                Some("0") => bid = Some(price),
                Some("1") => ask = Some(price),
                _ => {}
            }
        }
    }
    (bid, ask)
}

impl CTraderFixConfig {
    pub fn validate_mcp_identity(&self) -> Result<()> {
        let account = required("CTRADER_MCP_ACCOUNT_ID")?.trim().to_owned();
        let environment = required("CTRADER_MCP_ENVIRONMENT")?.trim().to_owned();
        if self.account_id.as_deref() != Some(account.as_str())
            || !self
                .environment
                .as_deref()
                .is_some_and(|configured| configured.eq_ignore_ascii_case(&environment))
        {
            bail!("FIX configuration was loaded under a different MCP account or environment");
        }
        for endpoint in [&self.price, &self.trade] {
            let parts: Vec<_> = endpoint.sender_comp_id.split('.').collect();
            if parts.len() < 3
                || !parts[0].eq_ignore_ascii_case(&environment)
                || parts.last() != Some(&account.as_str())
                || endpoint.username != account
            {
                bail!("FIX session identity differs from configured MCP account or environment");
            }
        }
        Ok(())
    }

    pub fn load(project_root: &Path) -> Result<Self> {
        let env_path = project_root.join(".env");
        dotenvy::from_path_override(&env_path)
            .with_context(|| format!("parse cTrader settings from {}", env_path.display()))?;
        let account_id = env::var("CTRADER_MCP_ACCOUNT_ID")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        let environment = env::var("CTRADER_MCP_ENVIRONMENT")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        let trade = endpoint("TRADE")?;
        let price = endpoint("PRICE")?;
        let mut symbol_map = HashMap::new();
        let mut configured_ids = HashSet::new();
        for entry in required("CTRADER_FIX_SYMBOL_MAP")?.split(',') {
            let (name, id) = entry
                .split_once(':')
                .with_context(|| format!("invalid CTRADER_FIX_SYMBOL_MAP entry {entry}"))?;
            let normalized_name = normalize_symbol(name);
            let raw_id = id.trim();
            if normalized_name.is_empty() || raw_id.is_empty() {
                bail!("CTRADER_FIX_SYMBOL_MAP contains a blank symbol name or FIX ID");
            }
            let id = canonical_fix_symbol_id(raw_id).with_context(|| {
                format!("CTRADER_FIX_SYMBOL_MAP FIX ID for {normalized_name} is invalid")
            })?;
            if symbol_map.contains_key(&normalized_name) {
                bail!(
                    "CTRADER_FIX_SYMBOL_MAP contains duplicate normalized symbol {normalized_name}"
                );
            }
            if !configured_ids.insert(id.clone()) {
                bail!("CTRADER_FIX_SYMBOL_MAP contains duplicate FIX symbol ID {id}");
            }
            symbol_map.insert(normalized_name, id);
        }
        if symbol_map.is_empty() {
            bail!("CTRADER_FIX_SYMBOL_MAP must contain at least one SYMBOL:FIX_ID mapping");
        }
        let mut demo_fixed_quantities = HashMap::new();
        if environment
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("demo"))
        {
            if let Ok(configured) = env::var("CTRADER_FIX_DEMO_FIXED_QUANTITY_MAP") {
                for entry in configured.split(',').filter(|entry| !entry.trim().is_empty()) {
                    let (name, raw_quantity) = entry.split_once(':').with_context(|| {
                        format!("invalid CTRADER_FIX_DEMO_FIXED_QUANTITY_MAP entry {entry}")
                    })?;
                    let normalized_name = normalize_symbol(name);
                    let quantity: f64 = raw_quantity.trim().parse().with_context(|| {
                        format!("invalid Demo fixed quantity for {normalized_name}")
                    })?;
                    if !symbol_map.contains_key(&normalized_name)
                        || !quantity.is_finite()
                        || quantity <= 0.0
                        || ((quantity * 100.0).round() - quantity * 100.0).abs() > 1e-7
                        || demo_fixed_quantities
                            .insert(normalized_name.clone(), quantity)
                            .is_some()
                    {
                        bail!("CTRADER_FIX_DEMO_FIXED_QUANTITY_MAP has an invalid, duplicate, or unmapped entry for {normalized_name}");
                    }
                }
            }
        }
        Ok(Self {
            trade,
            price,
            symbol_map,
            account_id,
            environment,
            demo_fixed_quantities,
            heartbeat_seconds: optional_u64("CTRADER_FIX_HEARTBEAT_SECONDS", 30)?,
            timeout_seconds: optional_u64("CTRADER_FIX_TIMEOUT_SECONDS", 15)?,
            reconnect_attempts: optional_u64("CTRADER_FIX_RECONNECT_ATTEMPTS", 3)? as u32,
        })
    }
}

fn endpoint(kind: &str) -> Result<FixEndpoint> {
    let prefix = format!("CTRADER_FIX_{kind}_");
    let identity_field = |suffix: &str| -> Result<String> {
        Ok(required(&format!("{prefix}{suffix}"))?.trim().to_owned())
    };
    Ok(FixEndpoint {
        host: required(&format!("{prefix}HOST"))?,
        port: required(&format!("{prefix}PORT"))?
            .parse()
            .with_context(|| format!("{prefix}PORT must be a TCP port"))?,
        ssl: optional_bool(&format!("{prefix}SSL"), false)?,
        username: identity_field("USERNAME")?,
        password: required(&format!("{prefix}PASSWORD"))?,
        sender_comp_id: identity_field("SENDER_COMP_ID")?,
        sender_sub_id: identity_field("SENDER_SUB_ID")?,
        target_comp_id: env::var(format!("{prefix}TARGET_COMP_ID"))
            .unwrap_or_else(|_| "CSERVER".into())
            .trim()
            .to_owned(),
        target_sub_id: identity_field("TARGET_SUB_ID")?,
    })
}

fn required(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("missing {name}"))?;
    if value.trim().is_empty() || value.starts_with("REQUIRED_") {
        bail!("{name} is required when brokerAdapter=ctrader-fix");
    }
    Ok(value)
}

fn optional_u64(name: &str, default: u64) -> Result<u64> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse()
                .with_context(|| format!("{name} must be an integer"))
        })
        .unwrap_or(Ok(default))
}

fn optional_bool(name: &str, default: bool) -> Result<bool> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| match value.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            _ => bail!("{name} must be true or false"),
        })
        .unwrap_or(Ok(default))
}

enum FixStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl Read for FixStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for FixStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

struct FixSession {
    endpoint: FixEndpoint,
    stream: FixStream,
    sequence: u64,
    next_inbound_sequence: Option<u64>,
    read_buffer: Vec<u8>,
}

impl FixSession {
    fn connect(endpoint: &FixEndpoint, heartbeat: u64, timeout: Duration) -> Result<Self> {
        let host = endpoint.host.clone();
        let port = endpoint.port;
        let (resolved_tx, resolved_rx) = std::sync::mpsc::sync_channel(1);
        thread::Builder::new()
            .name("ctrader-fix-dns".into())
            .spawn(move || {
                let resolved = (host.as_str(), port)
                    .to_socket_addrs()
                    .map(|addresses| addresses.collect::<Vec<_>>());
                let _ = resolved_tx.send(resolved);
            })
            .context("start cTrader FIX host resolver")?;
        let address = resolved_rx
            .recv_timeout(timeout)
            .context("cTrader FIX host resolution timed out")??
            .into_iter()
            .next()
            .context("cTrader FIX host did not resolve")?;
        let tcp = TcpStream::connect_timeout(&address, timeout)?;
        tcp.set_read_timeout(Some(timeout))?;
        tcp.set_write_timeout(Some(timeout))?;
        tcp.set_nodelay(true)?;
        let stream = if endpoint.ssl {
            FixStream::Tls(TlsConnector::new()?.connect(&endpoint.host, tcp)?)
        } else {
            FixStream::Plain(tcp)
        };
        let mut session = Self {
            endpoint: endpoint.clone(),
            stream,
            sequence: 1,
            next_inbound_sequence: None,
            read_buffer: Vec::new(),
        };
        session.send(
            "A",
            &[
                ("98", "0".into()),
                ("108", heartbeat.to_string()),
                ("141", "Y".into()),
                ("553", endpoint.username.clone()),
                ("554", endpoint.password.clone()),
            ],
        )?;
        let response = session.read_message()?;
        match value(&response, "35") {
            Some("A") => Ok(session),
            Some("5") => bail!(
                "cTrader FIX logon rejected: {}",
                value(&response, "58").unwrap_or("unknown")
            ),
            other => bail!("unexpected cTrader FIX logon response {other:?}"),
        }
    }

    fn send(&mut self, message_type: &str, fields: &[(&str, String)]) -> Result<()> {
        let message = encode(&self.endpoint, self.sequence, message_type, fields);
        self.sequence += 1;
        self.stream.write_all(&message)?;
        self.stream.flush()?;
        Ok(())
    }

    fn read_message(&mut self) -> Result<Vec<(String, String)>> {
        loop {
            if let Some(end) = message_end(&self.read_buffer) {
                let bytes: Vec<u8> = self.read_buffer.drain(..end).collect();
                let fields = parse(&bytes)?;
                if let Some(actual) = value(&fields, "34").and_then(|s| s.parse::<u64>().ok()) {
                    if let Some(expected) = self.next_inbound_sequence {
                        if actual != expected {
                            bail!("cTrader FIX inbound sequence mismatch: expected {expected}, got {actual}");
                        }
                    }
                    self.next_inbound_sequence = Some(actual + 1);
                } else {
                    bail!("cTrader FIX message omitted valid sequence number");
                }
                return Ok(fields);
            }
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk)?;
            if read == 0 {
                bail!("cTrader FIX connection closed before a complete response");
            }
            self.read_buffer.extend_from_slice(&chunk[..read]);
        }
    }
}

pub struct CTraderFixBroker {
    config: CTraderFixConfig,
    operation_lock: Mutex<()>,
    entry_validation:
        Option<std::result::Result<crate::mcp_context::McpExecutionValidator, String>>,
    manual_entry_limits: Mutex<HashMap<String, (Instant, f64, f64)>>,
    demo_unverified_volume_grants: Mutex<HashMap<String, Instant>>,
    #[cfg(test)]
    test_entry_validation_bypass: bool,
}

impl CTraderFixBroker {
    pub fn new(config: CTraderFixConfig) -> Self {
        Self {
            config,
            operation_lock: Mutex::new(()),
            entry_validation: None,
            manual_entry_limits: Mutex::new(HashMap::new()),
            demo_unverified_volume_grants: Mutex::new(HashMap::new()),
            #[cfg(test)]
            test_entry_validation_bypass: false,
        }
    }

    pub fn with_entry_validation(
        config: CTraderFixConfig,
        validation: Result<crate::mcp_context::McpExecutionValidator>,
    ) -> Self {
        let mut broker = Self::new(config);
        broker.entry_validation = Some(validation.map_err(|error| error.to_string()));
        broker
    }

    #[cfg(test)]
    fn with_test_entry_validation_bypass(config: CTraderFixConfig) -> Self {
        let mut broker = Self::new(config);
        broker.test_entry_validation_bypass = true;
        broker
    }

    fn symbol_id(&self, instrument: &str) -> Result<&str> {
        self.config
            .symbol_map
            .get(&normalize_symbol(instrument))
            .map(String::as_str)
            .with_context(|| format!("no cTrader FIX symbol ID configured for {instrument}"))
    }

    fn instrument_name(&self, symbol_id: &str) -> Result<String> {
        let symbol_id = canonical_fix_symbol_id(symbol_id)
            .context("cTrader returned an invalid FIX symbol ID")?;
        self.config
            .symbol_map
            .iter()
            .find(|(_, configured_id)| *configured_id == &symbol_id)
            .map(|(instrument, _)| instrument.clone())
            .with_context(|| format!("cTrader returned unmapped position symbol ID {symbol_id}"))
    }

    fn session(&self, endpoint: &FixEndpoint) -> Result<FixSession> {
        let timeout = Duration::from_secs(self.config.timeout_seconds.max(1));
        let mut last = None;
        for _ in 0..self.config.reconnect_attempts.max(1) {
            match FixSession::connect(endpoint, self.config.heartbeat_seconds, timeout) {
                Ok(session) => return Ok(session),
                Err(error) => last = Some(error),
            }
        }
        Err(last.context("cTrader FIX connection attempts exhausted")?)
    }

    fn submit(&self, order: &OrderRecord, position_id: Option<&str>) -> Result<ExecutionReceipt> {
        let _guard = self.operation_lock.lock();
        let symbol = self.symbol_id(&order.instrument)?.to_owned();
        let mut session = self.session(&self.config.trade)?;
        validate_fix_symbol(&mut session, &symbol, &order.instrument)?;
        let client_id = order.id.clone();
        let mut fields = vec![
            ("11", client_id),
            ("55", symbol.clone()),
            ("54", if order.side == "BUY" { "1" } else { "2" }.into()),
            ("60", fix_time()),
            ("40", "1".into()),
            ("38", quantity(order.quantity)),
            ("59", "3".into()),
        ];
        if let Some(position_id) = position_id {
            fields.push(("721", position_id.into()));
        }
        if let Some(stop) = order.stop_loss_price {
            fields.push(("1002", stop.to_string()));
        }
        session.send("D", &fields)?;
        loop {
            let response = session.read_message()?;
            match value(&response, "35") {
                Some("8") => {
                    ensure_unique_fix_fields(
                        &response,
                        &["11", "37", "38", "39", "150", "55", "54", "721", "14", "6"],
                    )?;
                    validate_order_report_identity(order, &response, &symbol, position_id)?;
                    let status = value(&response, "39").unwrap_or("");
                    let exec_type = value(&response, "150").unwrap_or("");
                    if (status == "8" && exec_type == "F")
                        || ((status == "1" || status == "2") && exec_type == "8")
                    {
                        bail!("contradictory FIX execution status and type: {response:?}");
                    }
                    if value(&response, "55").is_some_and(|report_symbol| report_symbol != symbol) {
                        bail!("FIX execution report symbol differs from submitted symbol: {response:?}");
                    }
                    let expected_side = if order.side == "BUY" { "1" } else { "2" };
                    if value(&response, "54")
                        .is_some_and(|report_side| report_side != expected_side)
                    {
                        bail!(
                            "FIX execution report side differs from submitted side: {response:?}"
                        );
                    }
                    if let (Some(expected_position), Some(reported_position)) =
                        (position_id, value(&response, "721"))
                    {
                        if reported_position.trim() != expected_position.trim() {
                            bail!("FIX execution report position ID differs from the position being closed: {response:?}");
                        }
                    }
                    if status == "8" || exec_type == "8" {
                        return receipt(order, &response, "rejected");
                    }
                    if status == "2" {
                        return receipt(order, &response, "filled");
                    }
                    if status == "1" || exec_type == "F" {
                        let cumulative_fill = parse_positive_fix_number(&response, "14")?;
                        let fill_status =
                            if cumulative_fill + ORDER_FILL_QUANTITY_TOLERANCE >= order.quantity {
                                "filled"
                            } else {
                                "partially-filled"
                            };
                        return receipt(order, &response, fill_status);
                    }
                }
                Some("j") | Some("3") | Some("9") => {
                    return receipt(order, &response, "rejected");
                }
                Some("0") => session.send("0", &[])?,
                Some("1") => session.send(
                    "0",
                    &value(&response, "112")
                        .map(|id| vec![("112", id.to_owned())])
                        .unwrap_or_default(),
                )?,
                _ => {}
            }
        }
    }
}

fn validate_fix_symbol(
    session: &mut FixSession,
    configured_id: &str,
    instrument: &str,
) -> Result<()> {
    let request_id = Uuid::new_v4().to_string();
    session.send(
        "x",
        &[
            ("320", request_id.clone()),
            ("559", "0".into()),
            ("55", configured_id.into()),
        ],
    )?;
    loop {
        let response = session.read_message()?;
        match value(&response, "35") {
            Some("y") if value(&response, "320") == Some(request_id.as_str()) => {
                validate_security_list_response(&response, configured_id, instrument)?;
                return Ok(());
            }
            Some("j") => bail!("broker rejected FIX security-list validation"),
            Some("0") => session.send("0", &[])?,
            Some("1") => session.send(
                "0",
                &value(&response, "112")
                    .map(|id| vec![("112", id.to_owned())])
                    .unwrap_or_default(),
            )?,
            _ => {}
        }
    }
}

fn validate_security_list_response(
    response: &[(String, String)],
    configured_id: &str,
    instrument: &str,
) -> Result<()> {
    ensure_unique_fix_fields(response, &["320", "322", "560", "146"])?;
    if value(response, "322")
        .filter(|id| !id.trim().is_empty())
        .is_none()
    {
        bail!("broker security list omitted SecurityResponseID");
    }
    if value(response, "560") != Some("0") {
        bail!("broker could not validate FIX symbol mapping");
    }
    let declared_groups = value(response, "146")
        .map(|count| {
            count
                .parse::<usize>()
                .context("broker security list has an invalid NoRelatedSym group count")
        })
        .transpose()?;
    let mut groups = Vec::<(String, Option<String>)>::new();
    let mut current: Option<(String, Option<String>, bool)> = None;
    for (tag, field) in response {
        match tag.as_str() {
            "55" => {
                if let Some(group) = current.take() {
                    groups.push((group.0, group.1));
                }
                current = Some((canonical_fix_symbol_id(field)?, None, false));
            }
            "1007" => {
                let group = current
                    .as_mut()
                    .context("broker security-list symbol name appeared before its ID")?;
                if group.1.replace(field.clone()).is_some() {
                    bail!("broker security-list group contains duplicate SymbolName fields");
                }
            }
            "1008" => {
                let group = current
                    .as_mut()
                    .context("broker security-list symbol digits appeared before its ID")?;
                if group.2 {
                    bail!("broker security-list group contains duplicate SymbolDigits fields");
                }
                let digits: u8 = field
                    .parse()
                    .context("broker security-list SymbolDigits is invalid")?;
                if digits > 5 {
                    bail!("broker security-list SymbolDigits is out of range");
                }
                group.2 = true;
            }
            _ => {}
        }
    }
    if let Some(group) = current {
        groups.push((group.0, group.1));
    }
    if declared_groups.is_some_and(|count| count != groups.len()) {
        bail!("broker security-list group count does not match NoRelatedSym");
    }
    let mut seen_ids = HashSet::new();
    if groups.iter().any(|(id, _)| !seen_ids.insert(id)) {
        bail!("broker security list contains duplicate symbol IDs");
    }
    let matching_groups = groups
        .iter()
        .filter(|(id, _)| id == configured_id)
        .collect::<Vec<_>>();
    if matching_groups.len() != 1 {
        bail!("broker security list did not return exactly one matching FIX symbol ID");
    }
    let name = matching_groups[0]
        .1
        .as_deref()
        .context("broker security-list matching group omitted SymbolName")?;
    if normalize_symbol(name) != normalize_symbol(instrument) {
        bail!("broker FIX symbol mapping differs from intended instrument {instrument}");
    }
    Ok(())
}

impl ExecutionBroker for CTraderFixBroker {
    fn risk_account_identity(&self) -> Option<crate::ports::BrokerAccountIdentity> {
        Some(crate::ports::BrokerAccountIdentity {
            account_id: self.config.account_id.clone()?,
            environment: self.config.environment.clone()?,
        })
    }

    fn demo_fixed_entry_quantity(&self, instrument: &str) -> Option<f64> {
        if !self
            .config
            .environment
            .as_deref()
            .is_some_and(|environment| environment.eq_ignore_ascii_case("demo"))
        {
            return None;
        }
        self.config
            .demo_fixed_quantities
            .get(&normalize_symbol(instrument))
            .copied()
    }

    fn execute(&self, request: &TradeRequest) -> Result<ExecutionReceipt> {
        validate_order(&request.order, "market_entry")?;
        let manual_limits = self
            .manual_entry_limits
            .lock()
            .remove(&normalize_symbol(&request.order.instrument))
            .filter(|(approved_at, _, _)| approved_at.elapsed() <= Duration::from_secs(120));
        let demo_volume_grant = self
            .demo_unverified_volume_grants
            .lock()
            .remove(&normalize_symbol(&request.order.instrument))
            .is_some_and(|approved_at| approved_at.elapsed() <= Duration::from_secs(120));
        let broker_validation = match &self.entry_validation {
            Some(Ok(validator)) => {
                validator.validate_entry(&request.order.instrument, request.order.quantity)
            }
            Some(Err(reason)) => Err(anyhow::anyhow!("{reason}")),
            None => {
                #[cfg(test)]
                if self.test_entry_validation_bypass {
                    // Explicit test-only escape hatch for the FIX socket fixture.
                    Ok(())
                } else {
                    Err(anyhow::anyhow!(
                        "broker account and volume validation unavailable"
                    ))
                }
                #[cfg(not(test))]
                Err(anyhow::anyhow!(
                    "broker account and volume validation unavailable"
                ))
            }
        };
        if let Err(error) = broker_validation {
            if let Some((_, minimum, step)) = manual_limits {
                validate_manual_entry_limits(
                    &request.order.instrument,
                    request.order.quantity,
                    minimum,
                    step,
                )?;
            } else if !demo_volume_grant {
                return Err(error).context(
                    "new FIX entries require broker volume metadata or a one-cycle Demo approval",
                );
            }
        }
        let mut receipt = self.submit(&request.order, None)?;
        receipt.created_by_event_id = request.execution_event_id.clone();
        Ok(receipt)
    }

    fn approve_manual_entry_limits(&self, instrument: &str, minimum: f64, step: f64) -> Result<()> {
        if !minimum.is_finite() || minimum <= 0.0 || !step.is_finite() || step <= 0.0 {
            bail!("human-verified broker minimum and increment must be positive finite quantities");
        }
        if self.symbol_id(instrument).is_err() {
            bail!("cannot approve manual limits for an unmapped FIX symbol {instrument}");
        }
        self.manual_entry_limits.lock().insert(
            normalize_symbol(instrument),
            (Instant::now(), minimum, step),
        );
        Ok(())
    }

    fn approve_demo_entry_without_volume_metadata(&self, instrument: &str) -> Result<()> {
        if !self
            .config
            .environment
            .as_deref()
            .is_some_and(|environment| environment.trim().eq_ignore_ascii_case("demo"))
        {
            bail!("unverified-volume approval is available only for a configured Demo account");
        }
        if self.symbol_id(instrument).is_err() {
            bail!("cannot approve an unmapped FIX symbol {instrument}");
        }
        self.demo_unverified_volume_grants
            .lock()
            .insert(normalize_symbol(instrument), Instant::now());
        Ok(())
    }

    fn clear_demo_entry_without_volume_metadata(&self, instrument: &str) {
        self.demo_unverified_volume_grants
            .lock()
            .remove(&normalize_symbol(instrument));
    }

    fn close(
        &self,
        position: &PositionRecord,
        order: &OrderRecord,
        _caused_by_decision_id: &str,
        execution_event_id: &str,
    ) -> Result<ExecutionReceipt> {
        if order.order_kind != "market_close" && order.order_kind != "stop_close" {
            bail!("cTrader close requires an approved deterministic close order");
        }
        validate_order(order, &order.order_kind)?;
        let mut receipt = self.submit(
            order,
            Some(
                position
                    .broker_position_id
                    .as_deref()
                    .context("local position has no reconciliable cTrader position ID")?,
            ),
        )?;
        receipt.created_by_event_id = execution_event_id.into();
        Ok(receipt)
    }

    fn resolve_order_status(
        &self,
        order: &OrderRecord,
        broker_position_id: Option<&str>,
    ) -> Result<ExecutionReceipt> {
        let broker_position_id = broker_position_id
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .context(
                "cannot resolve a lifecycle close without its reconciled cTrader position ID",
            )?;
        let _guard = self.operation_lock.lock();
        let symbol = self.symbol_id(&order.instrument)?.to_owned();
        let mut session = self.session(&self.config.trade)?;
        session.send(
            "H",
            &[
                ("11", order.id.clone()),
                ("54", if order.side == "BUY" { "1" } else { "2" }.into()),
            ],
        )?;
        loop {
            let response = session.read_message()?;
            match value(&response, "35") {
                Some("8") => {
                    validate_order_report_identity(
                        order,
                        &response,
                        &symbol,
                        Some(broker_position_id),
                    )?;
                    if value(&response, "55")
                        .is_some_and(|reported_symbol| reported_symbol != symbol)
                    {
                        bail!("cTrader FIX order-status response references a different symbol: {response:?}");
                    }
                    if value(&response, "54").is_some_and(|reported_side| {
                        reported_side != if order.side == "BUY" { "1" } else { "2" }
                    }) {
                        bail!("cTrader FIX order-status response references a different side: {response:?}");
                    }
                    return order_status_receipt(order, &response);
                }
                Some("j") => bail!(
                    "cTrader FIX rejected order-status request for {}: {response:?}",
                    order.id
                ),
                Some("0") => session.send("0", &[])?,
                Some("1") => session.send(
                    "0",
                    &value(&response, "112")
                        .map(|id| vec![("112", id.to_owned())])
                        .unwrap_or_default(),
                )?,
                _ => {}
            }
        }
    }

    fn reference_price(&self, instrument: &str) -> Result<Option<f64>> {
        let _guard = self.operation_lock.lock();
        let symbol = self.symbol_id(instrument)?.to_owned();
        let mut session = self.session(&self.config.price)?;
        session.send("V", &market_data_request_fields(&symbol))?;
        loop {
            let response = session.read_message()?;
            match value(&response, "35") {
                Some("W") | Some("X") => {
                    let mut side = None;
                    let mut bid = None;
                    let mut ask = None;
                    for (tag, entry) in &response {
                        if tag == "269" {
                            side = Some(entry.as_str());
                        } else if tag == "270" {
                            let price: f64 = entry.parse()?;
                            match side {
                                Some("0") => bid = Some(price),
                                Some("1") => ask = Some(price),
                                _ => {}
                            }
                        }
                    }
                    return Ok(match (bid, ask) {
                        (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
                        (Some(price), None) | (None, Some(price)) => Some(price),
                        _ => None,
                    });
                }
                Some("Y") => bail!("{}", market_data_rejection(&response)),
                Some("j") => bail!(
                    "cTrader price request rejected: {}",
                    value(&response, "58").unwrap_or("unknown")
                ),
                Some("0") => session.send("0", &[])?,
                _ => {}
            }
        }
    }

    fn supports_instrument(&self, instrument: &str) -> Result<bool> {
        Ok(self.symbol_id(instrument).is_ok())
    }

    fn reconcile(&self) -> Result<BrokerSnapshot> {
        let _guard = self.operation_lock.lock();
        let mut session = self.session(&self.config.trade)?;
        let request_id = Uuid::new_v4().to_string();
        session.send("AN", &[("710", request_id.clone())])?;
        let mut positions = Vec::new();
        let mut expected_reports = None;
        let response_deadline =
            Instant::now() + Duration::from_secs(self.config.timeout_seconds.max(1));
        loop {
            if Instant::now() >= response_deadline {
                bail!(
                    "cTrader FIX position reconciliation exceeded its {}-second response deadline",
                    self.config.timeout_seconds.max(1)
                );
            }
            let response = session.read_message()?;
            match value(&response, "35") {
                Some("AP") => {
                    ensure_unique_fix_fields(
                        &response,
                        &["710", "721", "727", "728", "55", "702", "704", "705", "730"],
                    )?;
                    let response_request_id = value(&response, "710")
                        .context("cTrader position report omitted PosReqID (tag 710)")?;
                    if response_request_id != request_id {
                        bail!("cTrader position report PosReqID does not match the outstanding request");
                    }
                    let result = value(&response, "728")
                        .context("cTrader position report omitted PosReqResult (tag 728)")?;
                    let total: usize = value(&response, "727")
                        .context("cTrader position report omitted TotalNumPosReports (tag 727)")?
                        .parse()
                        .context(
                            "cTrader position report has invalid TotalNumPosReports (tag 727)",
                        )?;
                    if result == "2" {
                        if total != 0
                            || !positions.is_empty()
                            || ["721", "55", "702", "704", "705", "730"]
                                .iter()
                                .any(|tag| value(&response, tag).is_some())
                        {
                            bail!("cTrader no-positions response contradicts the reported position count");
                        }
                        break;
                    }
                    if result != "0" {
                        bail!("cTrader position report has unsupported PosReqResult (tag 728): {result}");
                    }
                    if total == 0 {
                        bail!(
                            "cTrader valid position response reported zero total position reports"
                        );
                    }
                    if value(&response, "702").is_some_and(|count| count != "1") {
                        bail!(
                            "cTrader valid position report has inconsistent NoPositions (tag 702)"
                        );
                    }
                    match expected_reports {
                        Some(expected) if expected != total => {
                            bail!("cTrader position reports disagree on TotalNumPosReports")
                        }
                        None => expected_reports = Some(total),
                        _ => {}
                    }
                    if positions.len() >= total {
                        bail!("cTrader returned more position reports than declared");
                    }
                    let broker_position_id = value(&response, "721")
                        .filter(|id| !id.trim().is_empty())
                        .context("cTrader valid position report omitted PosMaintRptID (tag 721)")?
                        .to_owned();
                    let symbol_id = value(&response, "55")
                        .filter(|symbol| !symbol.trim().is_empty())
                        .context("cTrader valid position report omitted Symbol (tag 55)")?;
                    let long = parse_optional_nonnegative_fix_number(&response, "704")?;
                    let short = parse_optional_nonnegative_fix_number(&response, "705")?;
                    if long > 0.0 && short > 0.0 {
                        bail!("cTrader position report contains both long and short open volume");
                    }
                    let (side, quantity) = if long > 0.0 {
                        ("BUY", long)
                    } else if short > 0.0 {
                        ("SELL", short)
                    } else {
                        bail!("cTrader position report has no positive open volume")
                    };
                    let average_price = value(&response, "730")
                        .map(|_| parse_positive_fix_number(&response, "730"))
                        .transpose()?;
                    positions.push(BrokerPosition {
                        broker_position_id,
                        instrument: self.instrument_name(symbol_id)?,
                        side: side.into(),
                        quantity,
                        average_price,
                    });
                    if positions.len() == total {
                        break;
                    }
                }
                Some("j") => bail!(
                    "cTrader reconciliation rejected: {}",
                    value(&response, "58").unwrap_or("unknown")
                ),
                Some("0") => session.send("0", &[])?,
                _ => {}
            }
        }
        if expected_reports.is_some_and(|total| total != positions.len()) {
            bail!("cTrader position reconciliation ended before all declared reports arrived");
        }
        Ok(BrokerSnapshot {
            adapter: "ctrader-fix".into(),
            connected: true,
            complete: true,
            positions,
            observed_at: Utc::now(),
        })
    }
}

fn validate_order(order: &OrderRecord, kind: &str) -> Result<()> {
    if order.status != "approved" || order.order_kind != kind || order.quantity <= 0.0 {
        bail!("cTrader submission requires a matching approved deterministic order");
    }
    if !order.quantity.is_finite()
        || ((order.quantity * 100.0).round() - order.quantity * 100.0).abs() > 1e-7
    {
        bail!("cTrader FIX order quantity exceeds the protocol's 0.01 maximum precision");
    }
    Ok(())
}

fn validate_manual_entry_limits(
    instrument: &str,
    quantity: f64,
    minimum: f64,
    step: f64,
) -> Result<()> {
    if quantity + 1e-9 < minimum
        || ((quantity - minimum) / step - ((quantity - minimum) / step).round()).abs() > 1e-6
    {
        bail!("human-verified FIX quantity does not meet the supplied {instrument} minimum and increment");
    }
    Ok(())
}

fn parse_positive_fix_number(message: &[(String, String)], tag: &str) -> Result<f64> {
    let raw = value(message, tag)
        .with_context(|| format!("FIX execution report missing tag {tag}: {message:?}"))?;
    let parsed = parse_fix_decimal(raw)
        .with_context(|| format!("invalid FIX decimal tag {tag}: {message:?}"))?;
    if !parsed.is_finite() || parsed <= 0.0 {
        bail!("non-positive or non-finite FIX numeric tag {tag}: {message:?}");
    }
    Ok(parsed)
}

fn parse_optional_nonnegative_fix_number(message: &[(String, String)], tag: &str) -> Result<f64> {
    let Some(raw) = value(message, tag) else {
        return Ok(0.0);
    };
    let parsed = parse_fix_decimal(raw)
        .with_context(|| format!("invalid FIX decimal tag {tag}: {message:?}"))?;
    if !parsed.is_finite() || parsed < 0.0 {
        bail!("negative or non-finite FIX numeric tag {tag}: {message:?}");
    }
    Ok(parsed)
}

fn parse_fix_decimal(raw: &str) -> Result<f64> {
    let mut decimal_points = 0;
    let mut digits_before_decimal = 0;
    let mut digits_after_decimal = 0;
    for byte in raw.bytes() {
        match byte {
            b'0'..=b'9' if decimal_points == 0 => digits_before_decimal += 1,
            b'0'..=b'9' => digits_after_decimal += 1,
            b'.' if decimal_points == 0 => decimal_points += 1,
            _ => bail!("FIX decimal contains a non-decimal character"),
        }
    }
    if digits_before_decimal == 0 || (decimal_points == 1 && digits_after_decimal == 0) {
        bail!("FIX decimal is missing digits around its decimal point");
    }
    let parsed = raw.parse::<f64>().context("FIX decimal is out of range")?;
    if !parsed.is_finite() {
        bail!("FIX decimal is not finite");
    }
    Ok(parsed)
}

fn ensure_unique_fix_fields(message: &[(String, String)], tags: &[&str]) -> Result<()> {
    for tag in tags {
        let count = message.iter().filter(|(field, _)| field == tag).count();
        if count > 1 {
            bail!("FIX message contains duplicate singleton tag {tag}");
        }
    }
    Ok(())
}

fn receipt(
    order: &OrderRecord,
    message: &[(String, String)],
    status: &str,
) -> Result<ExecutionReceipt> {
    let filled_quantity = if status == "rejected" {
        match value(message, "14") {
            Some(raw) => raw
                .parse::<f64>()
                .context("invalid rejected FIX cumulative fill")?,
            None => 0.0,
        }
    } else {
        parse_positive_fix_number(message, "14")?
    };
    if !filled_quantity.is_finite()
        || filled_quantity < 0.0
        || filled_quantity > order.quantity + ORDER_FILL_QUANTITY_TOLERANCE
    {
        bail!("FIX cumulative fill is inconsistent with submitted quantity: {message:?}");
    }
    if status == "filled" && filled_quantity + ORDER_FILL_QUANTITY_TOLERANCE < order.quantity {
        bail!("FIX reports a filled order with less than the requested cumulative quantity: {message:?}");
    }
    let effective_status = if status == "rejected" && filled_quantity > 0.0 {
        "partially-filled"
    } else {
        status
    };
    let broker_reference = match value(message, "37") {
        Some(reference) if !reference.is_empty() => reference.to_owned(),
        _ if effective_status == "rejected" => "unavailable".into(),
        _ => bail!("filled FIX execution report omitted broker order reference: {message:?}"),
    };
    let broker_position_id = value(message, "721")
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned);
    if effective_status != "rejected"
        && order.order_kind == "market_entry"
        && broker_position_id.is_none()
    {
        bail!("entry FIX execution report omitted a nonblank broker position ID: {message:?}");
    }
    let average_price = if effective_status == "rejected" {
        None
    } else {
        Some(parse_positive_fix_number(message, "6")?)
    };
    Ok(ExecutionReceipt {
        execution_id: Uuid::new_v4().to_string(),
        run_id: order.run_id.clone(),
        loop_id: order.loop_id.clone(),
        caused_by_decision_id: order.decision_id.clone(),
        action: if order.side == "BUY" {
            Jev1Action::Long
        } else {
            Jev1Action::Short
        },
        execution_kind: if order.order_kind == "market_entry" {
            "open"
        } else {
            "close"
        }
        .into(),
        status: effective_status.into(),
        broker_reference,
        broker_position_id,
        filled_quantity,
        average_price,
        rejection_reason: (status == "rejected").then(|| {
            value(message, "58")
                .unwrap_or("broker rejected order")
                .to_owned()
        }),
        raw_fix_report: Some(message.to_vec()),
        created_by_event_id: order.created_by_event_id.clone(),
        executed_at: Utc::now(),
    })
}

fn validate_order_report_identity(
    order: &OrderRecord,
    message: &[(String, String)],
    expected_symbol: &str,
    expected_position_id: Option<&str>,
) -> Result<()> {
    let client_id = value(message, "11").context(
        "FIX ExecutionReport omitted ClOrdID(11); refusing to correlate an order response without its client order ID",
    )?;
    if client_id != order.id {
        bail!("FIX execution report client order ID does not match submitted order: {message:?}");
    }
    if value(message, "55").is_some_and(|symbol| symbol != expected_symbol) {
        bail!("FIX execution report symbol differs from submitted symbol: {message:?}");
    }
    let expected_side = if order.side == "BUY" { "1" } else { "2" };
    if value(message, "54").is_some_and(|side| side != expected_side) {
        bail!("FIX execution report side differs from submitted side: {message:?}");
    }
    if let (Some(expected), Some(reported)) = (expected_position_id, value(message, "721")) {
        if reported.trim() != expected.trim() {
            bail!(
                "FIX execution report position ID differs from the expected position: {message:?}"
            );
        }
    }
    Ok(())
}

fn order_status_receipt(
    order: &OrderRecord,
    message: &[(String, String)],
) -> Result<ExecutionReceipt> {
    ensure_unique_fix_fields(
        message,
        &["11", "37", "38", "39", "55", "54", "721", "14", "6"],
    )?;
    for tag in ["37", "39"] {
        if value(message, tag).is_none_or(|value| value.trim().is_empty()) {
            bail!(
                "cTrader FIX order status omitted required identity/status tag {tag}: {message:?}"
            );
        }
    }
    let raw_filled = value(message, "14")
        .map(|raw| {
            raw.parse::<f64>()
                .context("cTrader FIX order status has invalid CumQty")
        })
        .transpose()?
        .unwrap_or(0.0);
    if !raw_filled.is_finite()
        || raw_filled < 0.0
        || raw_filled > order.quantity + ORDER_FILL_QUANTITY_TOLERANCE
    {
        bail!("cTrader FIX order status has an inconsistent CumQty: {message:?}");
    }
    let status = match value(message, "39") {
        Some("0" | "1") => "pending",
        Some("2") => "filled",
        Some("4") => "cancelled",
        Some("8") => "rejected",
        Some("C") => "expired",
        other => {
            bail!("cTrader FIX order status has an unsupported OrdStatus {other:?}: {message:?}")
        }
    };
    if status == "filled" && raw_filled + ORDER_FILL_QUANTITY_TOLERANCE < order.quantity {
        bail!(
            "cTrader FIX reports a filled order with less than the requested CumQty: {message:?}"
        );
    }
    let broker_reference = value(message, "37")
        .filter(|reference| !reference.is_empty())
        .unwrap_or("unavailable")
        .to_owned();
    Ok(ExecutionReceipt {
        execution_id: Uuid::new_v4().to_string(),
        run_id: order.run_id.clone(),
        loop_id: order.loop_id.clone(),
        caused_by_decision_id: order.decision_id.clone(),
        action: if order.side == "BUY" {
            Jev1Action::Long
        } else {
            Jev1Action::Short
        },
        execution_kind: "close-order-status".into(),
        status: status.into(),
        broker_reference,
        broker_position_id: value(message, "721")
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_owned),
        filled_quantity: raw_filled,
        average_price: if raw_filled > 0.0 {
            Some(parse_positive_fix_number(message, "6")?)
        } else {
            None
        },
        rejection_reason: (status == "rejected").then(|| {
            value(message, "58")
                .unwrap_or("cTrader rejected the order")
                .to_owned()
        }),
        raw_fix_report: Some(message.to_vec()),
        created_by_event_id: order.created_by_event_id.clone(),
        executed_at: Utc::now(),
    })
}

fn encode(
    endpoint: &FixEndpoint,
    sequence: u64,
    message_type: &str,
    fields: &[(&str, String)],
) -> Vec<u8> {
    let mut body = format!(
        "35={message_type}\x0149={}\x0156={}\x0134={sequence}\x0152={}\x0150={}\x0157={}\x01",
        endpoint.sender_comp_id,
        endpoint.target_comp_id,
        fix_time(),
        endpoint.sender_sub_id,
        endpoint.target_sub_id
    )
    .into_bytes();
    for (tag, value) in fields {
        body.extend_from_slice(format!("{tag}={value}\x01").as_bytes());
    }
    let mut message = format!("8=FIX.4.4\x019={}\x01", body.len()).into_bytes();
    message.extend(body);
    let checksum: u32 = message.iter().map(|byte| *byte as u32).sum::<u32>() % 256;
    message.extend_from_slice(format!("10={checksum:03}\x01").as_bytes());
    message
}

fn message_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == [SOH, b'1', b'0', b'='])
        .and_then(|start| {
            buffer[start + 4..]
                .iter()
                .position(|byte| *byte == SOH)
                .map(|end| start + 5 + end)
        })
}

fn parse(bytes: &[u8]) -> Result<Vec<(String, String)>> {
    let fields = bytes
        .split(|byte| *byte == SOH)
        .filter(|field| !field.is_empty())
        .map(|field| {
            let text = std::str::from_utf8(field)?;
            let (tag, value) = text.split_once('=').context("malformed FIX field")?;
            Ok((tag.to_owned(), value.to_owned()))
        })
        .collect::<Result<Vec<_>>>()?;
    if value(&fields, "8") != Some("FIX.4.4") {
        bail!("unexpected FIX version");
    }
    Ok(fields)
}

fn value<'a>(fields: &'a [(String, String)], tag: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|(key, _)| key == tag)
        .map(|(_, value)| value.as_str())
}

fn fix_time() -> String {
    Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string()
}

fn quantity(value: f64) -> String {
    format!("{value:.8}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}

fn normalize_symbol(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase()
}

fn canonical_fix_symbol_id(value: &str) -> Result<String> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("FIX symbol ID must contain decimal digits only");
    }
    let id = value
        .parse::<u64>()
        .context("FIX symbol ID is out of range")?;
    if id == 0 {
        bail!("FIX symbol ID must be positive");
    }
    Ok(id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn human_verified_volume_limits_require_minimum_and_step() {
        assert!(validate_manual_entry_limits("BTCUSD", 0.01, 0.01, 0.01).is_ok());
        assert!(validate_manual_entry_limits("BTCUSD", 0.02, 0.01, 0.01).is_ok());
        assert!(validate_manual_entry_limits("BTCUSD", 0.015, 0.01, 0.01).is_err());
        assert!(validate_manual_entry_limits("BTCUSD", 0.005, 0.01, 0.01).is_err());
    }

    #[test]
    fn fix_freshness_uses_exact_future_and_maximum_age_bounds() {
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
    fn fix_encoder_sets_body_length_and_checksum() {
        let endpoint = FixEndpoint {
            host: "127.0.0.1".into(),
            port: 1,
            ssl: false,
            username: "1".into(),
            password: "p".into(),
            sender_comp_id: "demo.broker.1".into(),
            sender_sub_id: "TRADE".into(),
            target_comp_id: "CSERVER".into(),
            target_sub_id: "TRADE".into(),
        };
        let message = encode(&endpoint, 1, "0", &[]);
        let fields = parse(&message).unwrap();
        assert_eq!(value(&fields, "35"), Some("0"));
        assert!(value(&fields, "10").unwrap().len() == 3);
    }

    #[test]
    fn parser_preserves_repeating_market_data_fields() {
        let endpoint = FixEndpoint {
            host: "127.0.0.1".into(),
            port: 1,
            ssl: false,
            username: "1".into(),
            password: "p".into(),
            sender_comp_id: "demo.broker.1".into(),
            sender_sub_id: "PRICE".into(),
            target_comp_id: "CSERVER".into(),
            target_sub_id: "PRICE".into(),
        };
        let message = encode(
            &endpoint,
            2,
            "W",
            &[
                ("269", "0".into()),
                ("270", "99".into()),
                ("269", "1".into()),
                ("270", "101".into()),
            ],
        );
        let fields = parse(&message).unwrap();
        assert_eq!(fields.iter().filter(|(tag, _)| tag == "269").count(), 2);
    }

    #[test]
    fn fix_order_precision_rejects_sub_cent_lot_increment() {
        let order = OrderRecord {
            id: "o".into(),
            run_id: "r".into(),
            loop_id: "l".into(),
            decision_id: "d".into(),
            instrument: "BTCUSD".into(),
            side: "BUY".into(),
            quantity: 0.0001,
            reference_price: 100.0,
            notional: 0.01,
            stop_loss_price: None,
            order_kind: "market_entry".into(),
            status: "approved".into(),
            rejection_reasons: Vec::new(),
            idempotency_key: "k".into(),
            signal_at: Utc::now(),
            created_by_event_id: "e".into(),
            created_at: Utc::now(),
        };
        assert!(validate_order(&order, "market_entry").is_err());
    }

    #[test]
    fn human_verified_volume_fallback_runs_quote_fill_and_reconciliation_over_a_fix_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            for expected in ["V", "D", "AN"] {
                let (mut stream, _) = listener.accept().unwrap();
                let logon = read_one(&mut stream);
                assert_eq!(value(&parse(&logon).unwrap(), "35"), Some("A"));
                let response_endpoint = fixture_endpoint(port, "SERVER");
                stream
                    .write_all(&encode(&response_endpoint, 1, "A", &[]))
                    .unwrap();
                if expected == "D" {
                    let security_request = parse(&read_one(&mut stream)).unwrap();
                    assert_eq!(value(&security_request, "35"), Some("x"));
                    stream
                        .write_all(&encode(
                            &response_endpoint,
                            2,
                            "y",
                            &[
                                ("320", value(&security_request, "320").unwrap().into()),
                                ("322", "security-response-1".into()),
                                ("560", "0".into()),
                                ("146", "1".into()),
                                ("55", "1".into()),
                                ("1007", "BTCUSD".into()),
                            ],
                        ))
                        .unwrap();
                }
                let request = read_one(&mut stream);
                let request_fields = parse(&request).unwrap();
                assert_eq!(value(&request_fields, "35"), Some(expected));
                let response = match expected {
                    "V" => encode(
                        &response_endpoint,
                        2,
                        "W",
                        &[
                            ("269", "0".into()),
                            ("270", "99".into()),
                            ("269", "1".into()),
                            ("270", "101".into()),
                        ],
                    ),
                    "D" => encode(
                        &response_endpoint,
                        3,
                        "8",
                        &[
                            ("11", value(&parse(&request).unwrap(), "11").unwrap().into()),
                            ("37", "broker-order-1".into()),
                            ("721", "position-1".into()),
                            ("39", "1".into()),
                            ("150", "F".into()),
                            ("14", "1".into()),
                            ("6", "100".into()),
                        ],
                    ),
                    _ => encode(
                        &response_endpoint,
                        2,
                        "AP",
                        &[
                            ("710", value(&request_fields, "710").unwrap().into()),
                            ("728", "0".into()),
                            ("727", "1".into()),
                            ("702", "1".into()),
                            ("721", "position-1".into()),
                            ("55", "1".into()),
                            ("704", "2".into()),
                            ("705", "0".into()),
                            ("730", "100".into()),
                        ],
                    ),
                };
                stream.write_all(&response).unwrap();
            }
        });

        let endpoint = fixture_endpoint(port, "TRADE");
        let config = CTraderFixConfig {
            trade: endpoint.clone(),
            price: endpoint,
            symbol_map: [("BTCUSD".into(), "1".into())].into(),
            account_id: Some("demo-account".into()),
            environment: Some("demo".into()),
            demo_fixed_quantities: HashMap::new(),
            heartbeat_seconds: 30,
            timeout_seconds: 2,
            reconnect_attempts: 1,
        };
        let broker = CTraderFixBroker::with_entry_validation(
            config,
            Err(anyhow::anyhow!("cTrader MCP endpoint returned HTTP 404")),
        );
        broker
            .approve_manual_entry_limits("BTCUSD", 1.0, 1.0)
            .unwrap();
        assert_eq!(broker.reference_price("BTCUSD").unwrap(), Some(100.0));
        let order = fixture_order();
        let receipt = broker
            .execute(&TradeRequest {
                run_id: order.run_id.clone(),
                loop_id: order.loop_id.clone(),
                decision_id: order.decision_id.clone(),
                execution_event_id: "execution-event".into(),
                action: Jev1Action::Long,
                confidence: 0.9,
                order,
            })
            .unwrap();
        assert_eq!(receipt.status, "partially-filled");
        assert_eq!(receipt.filled_quantity, 1.0);
        assert_eq!(receipt.broker_position_id.as_deref(), Some("position-1"));
        let snapshot = broker.reconcile().unwrap();
        assert_eq!(snapshot.positions.len(), 1);
        assert_eq!(snapshot.positions[0].quantity, 2.0);
        server.join().unwrap();
    }

    #[test]
    #[ignore = "requires configured cTrader FIX demo credentials"]
    fn live_ctrader_demo_sessions_log_on_quote_and_reconcile() {
        let project_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let config = CTraderFixConfig::load(project_root).unwrap();
        let symbol = config.symbol_map.keys().next().unwrap().clone();
        let broker = CTraderFixBroker::with_test_entry_validation_bypass(config.clone());
        assert!(broker.reference_price(&symbol).unwrap().unwrap() > 0.0);
        assert!(broker.reconcile().unwrap().connected);
        let feed = CTraderFixQuoteFeed::start(config).unwrap();
        let quote = feed.quote(&symbol).unwrap();
        assert!(quote.bid > 0.0 && quote.ask >= quote.bid && quote.mid > 0.0);
    }

    #[test]
    #[ignore = "submits and immediately closes a minimum-size order on cTrader Demo"]
    fn live_ctrader_demo_minimum_round_trip() {
        assert_eq!(
            std::env::var("ALLOW_CTRADER_DEMO_ROUND_TRIP").as_deref(),
            Ok("YES"),
            "explicit demo execution acknowledgement is required"
        );
        let project_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let config = CTraderFixConfig::load(project_root).unwrap();
        assert!(
            config.trade.host.to_ascii_lowercase().contains("demo"),
            "refusing to execute against a non-demo trade endpoint"
        );
        let instrument = if config.symbol_map.contains_key("BTCUSD") {
            "BTCUSD".to_owned()
        } else {
            panic!("BTCUSD is not configured in CTRADER_FIX_SYMBOL_MAP")
        };
        let broker = CTraderFixBroker::with_test_entry_validation_bypass(config);
        let before = broker.reconcile().unwrap();
        assert!(before.connected && before.complete);
        assert!(
            before
                .positions
                .iter()
                .all(|position| position.instrument != instrument),
            "refusing to run while an existing BTCUSD broker position is open"
        );

        let price = match broker.reference_price(&instrument) {
            Ok(Some(price)) => price,
            Ok(None) => {
                println!("DEMO_QUOTE_PREFLIGHT_UNAVAILABLE no reference price returned");
                1.0
            }
            Err(error) => {
                println!("DEMO_QUOTE_PREFLIGHT_UNAVAILABLE {error:#}");
                1.0
            }
        };
        let quantity = 0.01_f64;
        let now = Utc::now();
        let order = OrderRecord {
            id: Uuid::new_v4().to_string(),
            run_id: "demo-execution-test".into(),
            loop_id: "demo-execution-test".into(),
            decision_id: "operator-authorized-demo-test".into(),
            idempotency_key: Uuid::new_v4().to_string(),
            order_kind: "market_entry".into(),
            instrument: instrument.clone(),
            side: "BUY".into(),
            quantity,
            reference_price: price,
            notional: price * quantity,
            stop_loss_price: None,
            signal_at: now,
            status: "approved".into(),
            rejection_reasons: Vec::new(),
            created_by_event_id: Uuid::new_v4().to_string(),
            created_at: now,
        };
        println!("DEMO_ENTRY_REQUEST instrument={instrument} quantity={quantity} reference_price={price} approximate_usd_notional={}", price * quantity);
        let entry = broker
            .execute(&TradeRequest {
                run_id: order.run_id.clone(),
                loop_id: order.loop_id.clone(),
                decision_id: order.decision_id.clone(),
                execution_event_id: Uuid::new_v4().to_string(),
                action: Jev1Action::Long,
                confidence: 1.0,
                order: order.clone(),
            })
            .unwrap();
        println!("DEMO_ENTRY_RECEIPT {entry:?}");
        assert!(
            matches!(entry.status.as_str(), "filled" | "partially-filled")
                && entry.filled_quantity > 0.0,
            "demo entry was not filled: {entry:?}"
        );

        let position = PositionRecord {
            id: Uuid::new_v4().to_string(),
            run_id: order.run_id.clone(),
            loop_id: order.loop_id.clone(),
            opened_by_execution_id: entry.execution_id.clone(),
            broker_position_id: entry.broker_position_id.clone(),
            direction: "long".into(),
            state: "open".into(),
            opened_at: entry.executed_at,
            closed_by_execution_id: None,
            closed_at: None,
            last_event_id: entry.created_by_event_id.clone(),
        };
        let close_order = OrderRecord {
            id: Uuid::new_v4().to_string(),
            run_id: order.run_id.clone(),
            loop_id: order.loop_id.clone(),
            decision_id: "operator-authorized-demo-close".into(),
            idempotency_key: Uuid::new_v4().to_string(),
            order_kind: "market_close".into(),
            instrument: instrument.clone(),
            side: "SELL".into(),
            quantity: entry.filled_quantity,
            reference_price: entry.average_price.unwrap_or(price),
            notional: entry.filled_quantity * entry.average_price.unwrap_or(price),
            stop_loss_price: None,
            signal_at: Utc::now(),
            status: "approved".into(),
            rejection_reasons: Vec::new(),
            created_by_event_id: Uuid::new_v4().to_string(),
            created_at: Utc::now(),
        };
        let close = broker
            .close(
                &position,
                &close_order,
                &close_order.decision_id,
                &Uuid::new_v4().to_string(),
            )
            .unwrap();
        println!("DEMO_CLOSE_RECEIPT {close:?}");
        assert!(
            matches!(close.status.as_str(), "filled" | "partially-filled")
                && close.filled_quantity > 0.0,
            "demo close was not filled: {close:?}"
        );
        let after = broker.reconcile().unwrap();
        assert!(after.connected && after.complete);
        assert!(
            after
                .positions
                .iter()
                .all(|position| position.instrument != instrument),
            "BTCUSD remained open after the demo round trip: {:?}",
            after.positions
        );
        println!("DEMO_ROUND_TRIP_COMPLETE final_btcusd_position=flat");
    }

    fn fixture_endpoint(port: u16, sub_id: &str) -> FixEndpoint {
        FixEndpoint {
            host: "127.0.0.1".into(),
            port,
            ssl: false,
            username: "1".into(),
            password: "password".into(),
            sender_comp_id: "demo.broker.1".into(),
            sender_sub_id: sub_id.into(),
            target_comp_id: "CSERVER".into(),
            target_sub_id: sub_id.into(),
        }
    }

    fn fixture_order() -> OrderRecord {
        OrderRecord {
            id: "order-1".into(),
            run_id: "run-1".into(),
            loop_id: "loop-1".into(),
            decision_id: "decision-1".into(),
            idempotency_key: "key".into(),
            order_kind: "market_entry".into(),
            instrument: "BTCUSD".into(),
            side: "BUY".into(),
            quantity: 2.0,
            reference_price: 100.0,
            notional: 200.0,
            stop_loss_price: Some(98.0),
            signal_at: Utc::now(),
            status: "approved".into(),
            rejection_reasons: Vec::new(),
            created_by_event_id: "order-event".into(),
            created_at: Utc::now(),
        }
    }

    fn read_one(stream: &mut TcpStream) -> Vec<u8> {
        let mut buffer = Vec::new();
        loop {
            if let Some(end) = message_end(&buffer) {
                return buffer[..end].to_vec();
            }
            let mut chunk = [0u8; 1024];
            let read = stream.read(&mut chunk).unwrap();
            assert!(read > 0);
            buffer.extend_from_slice(&chunk[..read]);
        }
    }
}
