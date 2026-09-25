use crate::domain::*;
use crate::ports::ExecutionBroker;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use native_tls::{TlsConnector, TlsStream};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::env;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

const SOH: u8 = 1;

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
    heartbeat_seconds: u64,
    timeout_seconds: u64,
    reconnect_attempts: u32,
}

pub struct CTraderFixQuoteFeed {
    latest: Arc<Mutex<HashMap<String, QuoteSnapshot>>>,
    recent: Arc<Mutex<HashMap<String, VecDeque<QuoteSnapshot>>>>,
    errors: Arc<Mutex<HashMap<String, String>>>,
    timeout: Duration,
}

impl CTraderFixQuoteFeed {
    pub fn start(config: CTraderFixConfig) -> Result<Self> {
        let latest = Arc::new(Mutex::new(HashMap::new()));
        let recent = Arc::new(Mutex::new(HashMap::new()));
        let errors = Arc::new(Mutex::new(HashMap::new()));
        for (instrument, symbol) in config.symbol_map.clone() {
            let endpoint = config.price.clone();
            let latest_state = Arc::clone(&latest);
            let error_state = Arc::clone(&errors);
            let recent_state = Arc::clone(&recent);
            let heartbeat = config.heartbeat_seconds;
            let timeout = Duration::from_secs(config.timeout_seconds);
            let reconnect_attempts = config.reconnect_attempts.max(1);
            thread::Builder::new()
                .name(format!("ctrader-fix-price-{instrument}"))
                .spawn(move || loop {
                    match stream_quotes(
                        &instrument,
                        &symbol,
                        &endpoint,
                        heartbeat,
                        timeout,
                        &latest_state,
                        &recent_state,
                    ) {
                        Ok(()) => {}
                        Err(error) => {
                            error_state
                                .lock()
                                .insert(instrument.clone(), error.to_string());
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
            timeout: Duration::from_secs(config.timeout_seconds),
        })
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
) -> Result<()> {
    let mut session = FixSession::connect(endpoint, heartbeat, timeout)?;
    session.send("V", &market_data_request_fields(symbol))?;
    loop {
        let response = session.read_message()?;
        match value(&response, "35") {
            Some("W") | Some("X") => {
                let (message_bid, message_ask) = market_data_prices(&response);
                let previous = latest.lock().get(instrument).cloned();
                let bid = message_bid.or_else(|| previous.as_ref().map(|quote| quote.bid));
                let ask = message_ask.or_else(|| previous.as_ref().map(|quote| quote.ask));
                if let (Some(bid), Some(ask)) = (bid, ask) {
                    if ask < bid {
                        continue;
                    }
                    let now = Utc::now();
                    let source_timestamp = value(&response, "52")
                        .and_then(|value| {
                            chrono::NaiveDateTime::parse_from_str(value, "%Y%m%d-%H:%M:%S%.f").ok()
                        })
                        .map(|value| value.and_utc())
                        .unwrap_or(now);
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
            Some("0") => session.send("0", &[])?,
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
    pub fn load(project_root: &Path) -> Result<Self> {
        let env_path = project_root.join(".env");
        dotenvy::from_path_override(&env_path)
            .with_context(|| format!("parse cTrader settings from {}", env_path.display()))?;
        let trade = endpoint("TRADE")?;
        let price = endpoint("PRICE")?;
        let symbol_map = required("CTRADER_FIX_SYMBOL_MAP")?
            .split(',')
            .map(|entry| {
                let (name, id) = entry
                    .split_once(':')
                    .with_context(|| format!("invalid CTRADER_FIX_SYMBOL_MAP entry {entry}"))?;
                Ok((normalize_symbol(name), id.trim().to_owned()))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        if symbol_map.is_empty() {
            bail!("CTRADER_FIX_SYMBOL_MAP must contain at least one SYMBOL:FIX_ID mapping");
        }
        Ok(Self {
            trade,
            price,
            symbol_map,
            heartbeat_seconds: optional_u64("CTRADER_FIX_HEARTBEAT_SECONDS", 30)?,
            timeout_seconds: optional_u64("CTRADER_FIX_TIMEOUT_SECONDS", 15)?,
            reconnect_attempts: optional_u64("CTRADER_FIX_RECONNECT_ATTEMPTS", 3)? as u32,
        })
    }
}

fn endpoint(kind: &str) -> Result<FixEndpoint> {
    let prefix = format!("CTRADER_FIX_{kind}_");
    Ok(FixEndpoint {
        host: required(&format!("{prefix}HOST"))?,
        port: required(&format!("{prefix}PORT"))?
            .parse()
            .with_context(|| format!("{prefix}PORT must be a TCP port"))?,
        ssl: optional_bool(&format!("{prefix}SSL"), false)?,
        username: required(&format!("{prefix}USERNAME"))?,
        password: required(&format!("{prefix}PASSWORD"))?,
        sender_comp_id: required(&format!("{prefix}SENDER_COMP_ID"))?,
        sender_sub_id: required(&format!("{prefix}SENDER_SUB_ID"))?,
        target_comp_id: env::var(format!("{prefix}TARGET_COMP_ID"))
            .unwrap_or_else(|_| "CSERVER".into()),
        target_sub_id: required(&format!("{prefix}TARGET_SUB_ID"))?,
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
    read_buffer: Vec<u8>,
}

impl FixSession {
    fn connect(endpoint: &FixEndpoint, heartbeat: u64, timeout: Duration) -> Result<Self> {
        let address = (endpoint.host.as_str(), endpoint.port)
            .to_socket_addrs()?
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
                return parse(&bytes);
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
}

impl CTraderFixBroker {
    pub fn new(config: CTraderFixConfig) -> Self {
        Self {
            config,
            operation_lock: Mutex::new(()),
        }
    }

    fn symbol_id(&self, instrument: &str) -> Result<&str> {
        self.config
            .symbol_map
            .get(&normalize_symbol(instrument))
            .map(String::as_str)
            .with_context(|| format!("no cTrader FIX symbol ID configured for {instrument}"))
    }

    fn instrument_name(&self, symbol_id: &str) -> String {
        self.config
            .symbol_map
            .iter()
            .find(|(_, configured_id)| configured_id.as_str() == symbol_id)
            .map(|(instrument, _)| instrument.clone())
            .unwrap_or_else(|| symbol_id.to_owned())
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
        let client_id = order.id.clone();
        let mut fields = vec![
            ("11", client_id),
            ("55", symbol),
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
                    let status = value(&response, "39").unwrap_or("");
                    let exec_type = value(&response, "150").unwrap_or("");
                    if status == "8" || exec_type == "8" {
                        return Ok(receipt(order, &response, "rejected"));
                    }
                    if status == "2" {
                        return Ok(receipt(order, &response, "filled"));
                    }
                    if status == "1" || exec_type == "F" {
                        let cumulative_fill = value(&response, "14")
                            .or_else(|| value(&response, "32"))
                            .and_then(|quantity| quantity.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let fill_status = if cumulative_fill + f64::EPSILON >= order.quantity {
                            "filled"
                        } else {
                            "partially-filled"
                        };
                        return Ok(receipt(order, &response, fill_status));
                    }
                }
                Some("j") | Some("3") | Some("9") => {
                    return Ok(receipt(order, &response, "rejected"));
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

impl ExecutionBroker for CTraderFixBroker {
    fn execute(&self, request: &TradeRequest) -> Result<ExecutionReceipt> {
        validate_order(&request.order, "market_entry")?;
        let mut receipt = self.submit(&request.order, None)?;
        receipt.created_by_event_id = request.execution_event_id.clone();
        Ok(receipt)
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

    fn reconcile(&self) -> Result<BrokerSnapshot> {
        let _guard = self.operation_lock.lock();
        let mut session = self.session(&self.config.trade)?;
        session.send("AN", &[("710", Uuid::new_v4().to_string())])?;
        let mut positions = Vec::new();
        loop {
            let response = session.read_message()?;
            match value(&response, "35") {
                Some("AP") => {
                    if value(&response, "728") == Some("2") {
                        break;
                    }
                    if value(&response, "728") == Some("0") {
                        let long: f64 = value(&response, "704").unwrap_or("0").parse()?;
                        let short: f64 = value(&response, "705").unwrap_or("0").parse()?;
                        positions.push(BrokerPosition {
                            broker_position_id: value(&response, "721").unwrap_or("").into(),
                            instrument: self.instrument_name(value(&response, "55").unwrap_or("")),
                            side: if long > 0.0 { "BUY" } else { "SELL" }.into(),
                            quantity: long.max(short),
                            average_price: value(&response, "730")
                                .and_then(|price| price.parse().ok()),
                        });
                        let total: usize = value(&response, "727").unwrap_or("1").parse()?;
                        if positions.len() >= total {
                            break;
                        }
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
    Ok(())
}

fn receipt(order: &OrderRecord, message: &[(String, String)], status: &str) -> ExecutionReceipt {
    let filled_quantity = value(message, "14")
        .or_else(|| value(message, "32"))
        .and_then(|quantity| quantity.parse().ok())
        .unwrap_or(0.0);
    ExecutionReceipt {
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
        status: status.into(),
        broker_reference: value(message, "37").unwrap_or(&order.id).into(),
        broker_position_id: value(message, "721").map(str::to_owned),
        filled_quantity,
        average_price: value(message, "6").and_then(|price| price.parse().ok()),
        rejection_reason: (status == "rejected").then(|| {
            value(message, "58")
                .unwrap_or("broker rejected order")
                .to_owned()
        }),
        created_by_event_id: order.created_by_event_id.clone(),
        executed_at: Utc::now(),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

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
    fn adapter_runs_quote_partial_fill_and_reconciliation_over_a_fix_socket() {
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
                let request = read_one(&mut stream);
                assert_eq!(value(&parse(&request).unwrap(), "35"), Some(expected));
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
                        2,
                        "8",
                        &[
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
                            ("728", "0".into()),
                            ("727", "1".into()),
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
            heartbeat_seconds: 30,
            timeout_seconds: 2,
            reconnect_attempts: 1,
        };
        let broker = CTraderFixBroker::new(config);
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
        let broker = CTraderFixBroker::new(config.clone());
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
        let broker = CTraderFixBroker::new(config);
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
