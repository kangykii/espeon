//! Narrow, read-only cTrader MCP adapter for World Model evidence requests.
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const SYMBOL_DETAILS_CACHE_TTL_SECONDS: i64 = 86_400;
const MODERN_MCP_VERSION: &str = "2026-07-28";
const LEGACY_MCP_VERSION: &str = "2025-11-25";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum McpProtocol {
    Legacy,
    Modern,
}

pub struct McpBrokerContext {
    endpoint: String,
    account_id: String,
    environment: String,
    client: Client,
    cache: Mutex<HashMap<String, (DateTime<Utc>, Value)>>,
    last_success_at: Mutex<Option<DateTime<Utc>>>,
    protocol: Mutex<Option<McpProtocol>>,
    legacy_session: Mutex<Option<LegacySession>>,
}

#[derive(Clone)]
struct LegacySession {
    protocol_version: String,
    session_id: Option<String>,
}

impl McpBrokerContext {
    pub fn probe(root: &Path) -> Result<String> {
        let client = Self::load(root)?.context("cTrader MCP is disabled in connector settings")?;
        let balance = client.cached_call("get_balance", json!({}), 15)?;
        client
            .validate_account(&balance)
            .context("cTrader MCP responded, but account validation failed")?;
        Ok(format!(
            "Connected to cTrader {} account {}; read-only account response received at {}",
            client.environment,
            client.account_id,
            Utc::now()
        ))
    }
    pub fn load(root: &Path) -> Result<Option<Self>> {
        load_dotenv(root)?;
        let enabled = std::env::var("CTRADER_MCP_ENABLED")
            .unwrap_or_default()
            .eq_ignore_ascii_case("true");
        if !enabled {
            return Ok(None);
        }
        let required = |name: &str| -> Result<String> {
            let value = std::env::var(name).with_context(|| format!("missing {name}"))?;
            if value.trim().is_empty() {
                bail!("{name} is required when cTrader MCP is enabled");
            }
            Ok(value)
        };
        let endpoint = std::env::var("CTRADER_MCP_ENDPOINT").unwrap_or_default();
        let endpoint = if endpoint.trim().is_empty() {
            "http://127.0.0.1:9876/mcp/".to_owned()
        } else {
            endpoint.trim().to_owned()
        };
        if !endpoint.starts_with("http://127.0.0.1:") && !endpoint.starts_with("http://localhost:")
        {
            bail!("read-only cTrader MCP endpoint must be local");
        }
        let environment = required("CTRADER_MCP_ENVIRONMENT")?
            .trim()
            .to_ascii_lowercase();
        if !matches!(environment.as_str(), "demo" | "live") {
            bail!("CTRADER_MCP_ENVIRONMENT must be demo or live");
        }
        Ok(Some(Self {
            endpoint,
            account_id: required("CTRADER_MCP_ACCOUNT_ID")?.trim().to_owned(),
            environment,
            client: Client::builder().timeout(Duration::from_secs(8)).build()?,
            cache: Mutex::new(HashMap::new()),
            last_success_at: Mutex::new(None),
            protocol: Mutex::new(None),
            legacy_session: Mutex::new(None),
        }))
    }

    pub fn fetch_requests(&self, requests: &[String], symbol: Option<&str>) -> Result<Vec<Value>> {
        let mut result = Vec::new();
        for request in requests {
            let (tool, arguments, ttl) = match request.as_str() {
                "account" => ("get_balance", json!({}), 15),
                "positions" => ("get_positions", json!({}), 15),
                "symbol_details" => (
                    "get_symbol_details",
                    json!({"symbolName":symbol.context("symbol detail request has no instrument")?}),
                    SYMBOL_DETAILS_CACHE_TTL_SECONDS,
                ),
                _ => bail!("World Model requested a non-read-only or unsupported broker tool"),
            };
            let (balance_observed_at, balance) =
                self.cached_call_with_timestamp("get_balance", json!({}), 15)?;
            self.validate_account(&balance)?;
            let (data_observed_at, value) = if tool == "get_balance" {
                (balance_observed_at, balance)
            } else {
                self.cached_call_with_timestamp(tool, arguments, ttl)?
            };
            result.push(json!({"request":request,"source":"ctrader-local-mcp-read-only","accountId":self.account_id,
                "environment":self.environment,"retrievedAt":Utc::now(),"dataFetchedAt":data_observed_at,
                "lastSuccessAt":*self.last_success_at.lock(),"data":value}));
        }
        Ok(result)
    }

    fn validate_account(&self, balance: &Value) -> Result<()> {
        // The active Local-MCP account may be absent from get_accounts_list.
        // When listed, bind the balance identity to that exact record. When it
        // is not listed, require the configured ID to equal the active balance
        // identity rather than trusting display names or broker labels.
        let accounts = self.cached_call("get_accounts_list", json!({}), 30)?;
        let account = find_account_record(&accounts, &self.account_id)?;
        let balance_ids = collect_account_ids(balance);
        let balance_trader_ids = collect_trader_ids(balance);
        if balance_ids.is_empty() && balance_trader_ids.is_empty() {
            bail!("cTrader balance omitted account identity fields");
        }
        if let Some(account) = account {
            let account_ids = collect_account_ids(account);
            if account_ids.is_empty() {
                bail!("configured cTrader account record omitted account identity fields");
            }
            // `get_balance.traderId` identifies the trader, not the account.
            // The Local MCP's account-list record instead exposes `id` and
            // `login`. Compare like identity fields, and when balance exposes
            // only traderId require the configured account to be unambiguous.
            if !balance_ids.is_empty() && !balance_ids.is_subset(&account_ids) {
                bail!(
                    "cTrader balance account identity does not match the configured account record"
                );
            }
            let account_trader_ids = collect_trader_ids(account);
            if !balance_trader_ids.is_empty()
                && !account_trader_ids.is_empty()
                && !balance_trader_ids.is_subset(&account_trader_ids)
            {
                bail!(
                    "cTrader balance trader identity does not match the configured account record"
                );
            }
            if balance_ids.is_empty()
                && account_trader_ids.is_empty()
                && count_account_records(&accounts) != 1
            {
                bail!("cTrader balance exposes only trader identity and the account list is ambiguous");
            }
        } else if !balance_ids.contains(&self.account_id) {
            bail!("configured cTrader account is not the active account in get_balance");
        }
        // Account names are optional display labels, not identity fields.
        // cTrader currently returns null from get_balance and an empty string
        // in get_accounts_list for this field; comparing serialized JSON
        // values would incorrectly treat null and "" as different accounts.
        for (balance_key, account_key) in [
            ("brokerName", "brokerTitle"),
            ("accountType", "accountType"),
        ] {
            let balance_value = find_key(balance, balance_key)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let account_value = account
                .and_then(|record| find_key(record, account_key))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty());
            if let (Some(balance_value), Some(account_value)) = (balance_value, account_value) {
                if !balance_value.eq_ignore_ascii_case(account_value) {
                    bail!("cTrader balance {balance_key} does not match the configured account record");
                }
            }
        }
        let account_environment_result = match account {
            Some(record) => account_environment(record)?,
            None => None,
        };
        let balance_environment_result = account_environment(balance)?;
        let is_live = match (account_environment_result, balance_environment_result) {
            (Some(account), Some(balance)) if account != balance => {
                bail!("cTrader balance and account record disagree on demo/live environment")
            }
            (Some(account), _) => account,
            (_, Some(balance)) => balance,
            (None, None) => {
                bail!("MCP account response omitted verifiable demo/live environment")
            }
        };
        if is_live != (self.environment == "live") {
            bail!("cTrader MCP account environment differs from configured environment");
        }
        Ok(())
    }

    fn cached_call(&self, tool: &str, arguments: Value, ttl_seconds: i64) -> Result<Value> {
        self.cached_call_with_timestamp(tool, arguments, ttl_seconds)
            .map(|(_, value)| value)
    }

    fn cached_call_with_timestamp(
        &self,
        tool: &str,
        arguments: Value,
        ttl_seconds: i64,
    ) -> Result<(DateTime<Utc>, Value)> {
        let key = format!("{tool}:{arguments}");
        if let Some((at, value)) = self.cache.lock().get(&key) {
            if u64::try_from(ttl_seconds)
                .ok()
                .is_some_and(|ttl| crate::freshness::is_fresh(Utc::now(), *at, ttl))
            {
                return Ok((*at, value.clone()));
            }
        }
        let mut failure = None;
        for attempt in 0..3 {
            match self.call(tool, &arguments) {
                Ok(value) => {
                    let now = Utc::now();
                    self.cache.lock().insert(key.clone(), (now, value.clone()));
                    *self.last_success_at.lock() = Some(now);
                    return Ok((now, value));
                }
                Err(error) => {
                    let retry = retryable_mcp_error(&error) && attempt < 2;
                    failure = Some(error);
                    if !retry {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(200 * (1 << attempt)));
                }
            }
        }
        Err(failure.context("cTrader MCP request failed; transient retry policy ended")?)
    }

    fn call(&self, tool: &str, arguments: &Value) -> Result<Value> {
        // cTrader Local MCP currently implements the established 2025
        // initialize/session handshake. Prefer it directly; the 2026
        // server/discover handshake is an optional newer protocol and adds a
        // request that current cTrader builds reject.
        let preferred = *self.protocol.lock();
        match preferred {
            Some(McpProtocol::Legacy) => {
                return match self.call_legacy(tool, arguments) {
                    Ok(value) => Ok(value),
                    Err(error) if should_fall_back_to_modern(&error) => {
                        let value = self.call_modern(tool, arguments).with_context(|| {
                            format!(
                                "legacy MCP call failed ({error:#}); modern fallback also failed"
                            )
                        })?;
                        if self.protocol.lock().is_none() {
                            *self.protocol.lock() = Some(McpProtocol::Modern);
                        }
                        Ok(value)
                    }
                    Err(error) => Err(error),
                };
            }
            Some(McpProtocol::Modern) => {
                return match self.call_modern(tool, arguments) {
                    Ok(value) => Ok(value),
                    Err(error) if should_fall_back_to_legacy(&error) => {
                        let value = self.call_legacy(tool, arguments).with_context(|| {
                            format!(
                                "modern MCP call failed ({error:#}); legacy fallback also failed"
                            )
                        })?;
                        *self.protocol.lock() = Some(McpProtocol::Legacy);
                        Ok(value)
                    }
                    Err(error) => Err(error),
                };
            }
            None => {}
        }

        match self.call_legacy(tool, arguments) {
            Ok(value) => {
                *self.protocol.lock() = Some(McpProtocol::Legacy);
                Ok(value)
            }
            Err(legacy_error) if should_fall_back_to_modern(&legacy_error) => {
                let value = self.call_modern(tool, arguments).with_context(|| {
                    format!("legacy MCP handshake failed ({legacy_error:#}); modern fallback also failed")
                })?;
                if self.protocol.lock().is_none() {
                    *self.protocol.lock() = Some(McpProtocol::Modern);
                }
                Ok(value)
            }
            Err(error) => Err(error).context("cTrader MCP legacy handshake failed"),
        }
    }

    fn call_modern(&self, tool: &str, arguments: &Value) -> Result<Value> {
        let discovery = self.post(
            None,
            &json!({"jsonrpc":"2.0","id":"espeon-discover-1","method":"server/discover","params":{"_meta":{
                "io.modelcontextprotocol/protocolVersion":MODERN_MCP_VERSION,
                "io.modelcontextprotocol/clientCapabilities":{},
                "io.modelcontextprotocol/clientInfo":{"name":"Espeon","version":"0.1.2"}}}}),
            Some(MODERN_MCP_VERSION),
        );

        let (discovery, modern_method_missing) = match discovery {
            Ok((response, _)) => (response, false),
            Err(error) if should_fall_back_to_legacy(&error) => {
                return Err(error).context("cTrader MCP rejected modern discovery");
            }
            Err(error) if is_json_rpc_method_not_found(&error) => (Value::Null, true),
            Err(error) => return Err(error).context("cTrader MCP modern discovery failed"),
        };

        if let Some(error) = discovery.get("error") {
            if is_unsupported_protocol_error(error) && advertises_legacy_version(error) {
                bail!("cTrader MCP only supports a legacy protocol version");
            }
            if !modern_method_missing {
                bail!("cTrader MCP discovery failed: {error}");
            }
        }

        if !modern_method_missing {
            let supported = discovery
                .get("result")
                .and_then(|result| result.get("supportedVersions"))
                .and_then(Value::as_array)
                .context("cTrader MCP discovery omitted supportedVersions")?;
            if !supported
                .iter()
                .any(|version| version.as_str() == Some(MODERN_MCP_VERSION))
            {
                if supported
                    .iter()
                    .any(|version| version.as_str().is_some_and(is_legacy_mcp_version))
                {
                    bail!("cTrader MCP only advertises legacy protocol versions");
                }
                bail!("cTrader MCP does not advertise a supported protocol version");
            }
        }

        let response = self
            .post(
                None,
                &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                    "name":tool,"arguments":arguments,"_meta":{
                        "io.modelcontextprotocol/protocolVersion":MODERN_MCP_VERSION,
                        "io.modelcontextprotocol/clientCapabilities":{},
                        "io.modelcontextprotocol/clientInfo":{"name":"Espeon","version":"0.1.2"}}}}),
                Some(MODERN_MCP_VERSION),
            )?
            .0;
        self.read_tool_result(response, tool)
    }

    fn call_legacy(&self, tool: &str, arguments: &Value) -> Result<Value> {
        // The legacy protocol is session based. Keep one negotiated session
        // for this connector instead of creating a new server session for
        // every broker read.
        for attempt in 0..2 {
            let mut legacy_session = self.legacy_session.lock();
            let had_session = legacy_session.is_some();
            let session = if let Some(session) = legacy_session.as_ref() {
                session.clone()
            } else {
                let init = self.post(None, &json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                    "protocolVersion":LEGACY_MCP_VERSION,"capabilities":{},"clientInfo":{"name":"Espeon","version":"0.1.2"}}}), None)?;
                let protocol_version = init
                    .0
                    .get("result")
                    .and_then(|result| result.get("protocolVersion"))
                    .and_then(Value::as_str)
                    .context("cTrader MCP initialize response omitted negotiated protocol version")?
                    .to_owned();
                let session = LegacySession {
                    protocol_version,
                    session_id: init.1,
                };
                let _ = self.post(
                    session.session_id.as_deref(),
                    &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                    Some(&session.protocol_version),
                )?;
                *legacy_session = Some(session.clone());
                session
            };
            let response = self.post(
                session.session_id.as_deref(),
                &json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                    "name":tool,"arguments":arguments}}),
                Some(&session.protocol_version),
            );
            match response {
                Ok((response, _)) => return self.read_tool_result(response, tool),
                Err(error)
                    if attempt == 0
                        && had_session
                        && session.session_id.is_some()
                        && http_error(&error).is_some_and(|http| http.status == 404) =>
                {
                    // cTrader can expire a legacy MCP session while keeping
                    // the endpoint alive. Discard only the rejected session,
                    // negotiate a fresh one, and retry this read once.
                    *legacy_session = None;
                }
                Err(error) => return Err(error),
            }
        }
        bail!("cTrader MCP legacy session recovery exhausted")
    }

    fn read_tool_result(&self, response: Value, tool: &str) -> Result<Value> {
        if let Some(error) = response.get("error") {
            bail!("cTrader MCP tool {tool} failed: {error}");
        }
        let result = response
            .get("result")
            .context("MCP response omitted result")?;
        if result.get("isError").and_then(Value::as_bool) == Some(true) {
            let details = result
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("; ");
            if details.trim().is_empty() {
                bail!("cTrader MCP tool {tool} returned isError=true without diagnostic content");
            }
            bail!("cTrader MCP tool {tool} returned an error: {details}");
        }
        if let Some(structured) = result.get("structuredContent") {
            return Ok(structured.clone());
        }
        let content = result
            .get("content")
            .and_then(Value::as_array)
            .context("MCP tool omitted content")?;
        let text = content
            .iter()
            .find_map(|entry| entry.get("text").and_then(Value::as_str))
            .context("MCP tool omitted text result")?;
        Ok(serde_json::from_str(text).unwrap_or_else(|_| json!({"text":text})))
    }

    fn post(
        &self,
        session: Option<&str>,
        body: &Value,
        protocol_version: Option<&str>,
    ) -> Result<(Value, Option<String>)> {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let tool = body
            .get("params")
            .and_then(|params| params.get("name"))
            .and_then(Value::as_str);
        let request_label = tool
            .map(|name| format!("{method} ({name})"))
            .unwrap_or_else(|| method.to_owned());
        let mut request = self
            .client
            .post(&self.endpoint)
            .header("Accept", "application/json, text/event-stream")
            .json(body);
        if let Some(version) = protocol_version {
            request = request.header("MCP-Protocol-Version", version);
            if version == MODERN_MCP_VERSION {
                request = request.header("Mcp-Method", method);
                if let Some(tool) = tool {
                    request = request.header("Mcp-Name", tool);
                }
            }
        }
        if let Some(session) = session {
            request = request.header("Mcp-Session-Id", session);
        }
        let response = request
            .send()
            .with_context(|| format!("cTrader MCP {request_label} transport error"))?;
        let status = response.status();
        if !status.is_success() {
            let detail = response.text().unwrap_or_default();
            return Err(McpHttpError {
                status: status.as_u16(),
                body: detail.chars().take(400).collect(),
            })
            .with_context(|| format!("cTrader MCP {request_label} at {}", self.endpoint));
        }
        let session = response
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .or_else(|| session.map(str::to_owned));
        let text = response.text().context("read MCP response")?;
        if text.trim().is_empty() {
            return Ok((Value::Null, session));
        }
        let value = if text.trim_start().starts_with("data:") || text.contains("\ndata:") {
            text.lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
                .last()
                .context("MCP event stream omitted JSON-RPC data")?
        } else {
            serde_json::from_str(&text).context("MCP response is not JSON")?
        };
        Ok((value, session))
    }
}

fn load_dotenv(root: &Path) -> Result<()> {
    let path = root.join(".env");
    if path.exists() {
        dotenvy::from_path_override(&path)
            .with_context(|| format!("parse cTrader MCP settings from {}", path.display()))?;
    }
    Ok(())
}

#[derive(Debug)]
struct McpHttpError {
    status: u16,
    body: String,
}

impl std::fmt::Display for McpHttpError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "HTTP {}", self.status)?;
        if self.body.trim().is_empty() {
            if self.status == 404 {
                write!(
                    formatter,
                    " (empty response body; MCP endpoint route not found)"
                )
            } else {
                write!(formatter, " (empty response body)")
            }
        } else {
            write!(formatter, ": {}", self.body)
        }
    }
}

impl std::error::Error for McpHttpError {}

fn http_error(error: &anyhow::Error) -> Option<&McpHttpError> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<McpHttpError>())
}

fn response_error_body(error: &anyhow::Error) -> Option<Value> {
    http_error(error).and_then(|http| serde_json::from_str(&http.body).ok())
}

fn json_rpc_error(value: &Value) -> Option<&Value> {
    value.get("error")
}

fn error_code(value: &Value) -> Option<i64> {
    value.get("code").and_then(Value::as_i64)
}

fn is_json_rpc_method_not_found(error: &anyhow::Error) -> bool {
    response_error_body(error)
        .as_ref()
        .and_then(json_rpc_error)
        .is_some_and(|error| error_code(error) == Some(-32601))
}

fn is_unsupported_protocol_error(error: &Value) -> bool {
    error_code(error) == Some(-32022)
}

fn advertises_legacy_version(error: &Value) -> bool {
    error
        .get("data")
        .and_then(|data| data.get("supported"))
        .and_then(Value::as_array)
        .is_some_and(|versions| {
            versions
                .iter()
                .filter_map(Value::as_str)
                .any(is_legacy_mcp_version)
        })
}

fn is_legacy_mcp_version(version: &str) -> bool {
    matches!(
        version,
        "2025-11-25" | "2025-06-18" | "2025-03-26" | "2024-11-05"
    )
}

fn should_fall_back_to_legacy(error: &anyhow::Error) -> bool {
    if format!("{error:#}").contains("only supports a legacy protocol version")
        || format!("{error:#}").contains("only advertises legacy protocol versions")
    {
        return true;
    }
    let Some(http) = http_error(error) else {
        return false;
    };
    if !matches!(http.status, 400 | 404 | 405) {
        return false;
    }
    let Some(body) = response_error_body(error) else {
        // An empty/non-JSON HTTP error is a route or transport failure, not
        // evidence that the server rejected the legacy MCP protocol.
        return false;
    };
    let Some(protocol_error) = json_rpc_error(&body) else {
        return false;
    };
    error_code(protocol_error) == Some(-32022) && advertises_legacy_version(protocol_error)
}

fn should_fall_back_to_modern(error: &anyhow::Error) -> bool {
    let Some(http) = http_error(error) else {
        return false;
    };
    if !matches!(http.status, 400 | 404 | 405) {
        return false;
    }
    let Some(body) = response_error_body(error) else {
        return false;
    };
    let Some(protocol_error) = json_rpc_error(&body) else {
        return false;
    };
    error_code(protocol_error) == Some(-32601)
        || (error_code(protocol_error) == Some(-32022)
            && protocol_error
                .get("data")
                .and_then(|data| data.get("supported"))
                .and_then(Value::as_array)
                .is_some_and(|versions| {
                    versions
                        .iter()
                        .filter_map(Value::as_str)
                        .any(|version| version == MODERN_MCP_VERSION)
                }))
}

fn retryable_mcp_error(error: &anyhow::Error) -> bool {
    if http_error(error).is_some_and(|http| http.status == 429 || (500..600).contains(&http.status))
    {
        return true;
    }
    let message = format!("{error:#}");
    if message.contains(" transport error") {
        return true;
    }
    if let Some(index) = message.find(" returned HTTP ") {
        let status = message[index + " returned HTTP ".len()..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>();
        if let Ok(status) = status.parse::<u16>() {
            return status == 429 || (500..600).contains(&status);
        }
    }
    false
}

fn find_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => map
            .get(key)
            .or_else(|| map.values().find_map(|value| find_key(value, key))),
        Value::Array(items) => items.iter().find_map(|item| find_key(item, key)),
        _ => None,
    }
}

fn account_environment(value: &Value) -> Result<Option<bool>> {
    fn collect(value: &Value, environments: &mut Vec<bool>) {
        match value {
            Value::Object(map) => {
                if let Some(is_live) = map.get("isLive").and_then(Value::as_bool) {
                    environments.push(is_live);
                }
                if let Some(is_demo) = map.get("isDemo").and_then(Value::as_bool) {
                    environments.push(!is_demo);
                }
                if let Some(environment) = map.get("environment").and_then(Value::as_str) {
                    if environment.trim().eq_ignore_ascii_case("live") {
                        environments.push(true);
                    } else if environment.trim().eq_ignore_ascii_case("demo") {
                        environments.push(false);
                    }
                }
                for child in map.values() {
                    collect(child, environments);
                }
            }
            Value::Array(items) => {
                for child in items {
                    collect(child, environments);
                }
            }
            _ => {}
        }
    }
    let mut environments = Vec::new();
    collect(value, &mut environments);
    let Some(first) = environments.first().copied() else {
        return Ok(None);
    };
    if environments.iter().any(|value| *value != first) {
        bail!("cTrader account response contains conflicting demo/live indicators");
    }
    Ok(Some(first))
}

fn find_account_record<'a>(value: &'a Value, account_id: &str) -> Result<Option<&'a Value>> {
    fn collect<'a>(value: &'a Value, account_id: &str, matches: &mut Vec<(u8, &'a Value)>) {
        const IDENTIFIERS: [(&str, u8); 3] = [("accountId", 0), ("login", 1), ("id", 2)];
        match value {
            Value::Object(map) => {
                let has_account_record_fields = [
                    "accountType",
                    "brokerTitle",
                    "accountName",
                    "depositAssetId",
                    "balance",
                    "equity",
                    "isLive",
                    "isDemo",
                    "environment",
                ]
                .iter()
                .any(|key| map.contains_key(*key));
                if has_account_record_fields {
                    for (key, priority) in IDENTIFIERS {
                        if map.get(key).is_some_and(|id| {
                            id.as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| id.to_string())
                                .trim()
                                == account_id.trim()
                        }) {
                            matches.push((priority, value));
                        }
                    }
                }
                for child in map.values() {
                    collect(child, account_id, matches);
                }
            }
            Value::Array(items) => {
                for child in items {
                    collect(child, account_id, matches);
                }
            }
            _ => {}
        }
    }
    let mut matches = Vec::new();
    collect(value, account_id, &mut matches);
    if matches.is_empty() {
        return Ok(None);
    }
    let mut unique_records: Vec<&'a Value> = Vec::new();
    for (_, candidate) in matches {
        if !unique_records
            .iter()
            .any(|existing| std::ptr::eq(*existing, candidate))
        {
            unique_records.push(candidate);
        }
    }
    if unique_records.len() != 1 {
        bail!("configured cTrader account identifier matched multiple account records");
    }
    Ok(unique_records.into_iter().next())
}

fn collect_account_ids(value: &Value) -> std::collections::HashSet<String> {
    fn collect(value: &Value, output: &mut std::collections::HashSet<String>) {
        const ID_FIELDS: [&str; 3] = ["accountId", "login", "id"];
        match value {
            Value::Object(map) => {
                for field in ID_FIELDS {
                    if let Some(id) = map.get(field) {
                        let id = id
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| id.to_string());
                        if !id.trim().is_empty() {
                            output.insert(id.trim().to_owned());
                        }
                    }
                }
                for child in map.values() {
                    collect(child, output);
                }
            }
            Value::Array(items) => {
                for child in items {
                    collect(child, output);
                }
            }
            _ => {}
        }
    }
    let mut result = std::collections::HashSet::new();
    collect(value, &mut result);
    result
}

fn collect_trader_ids(value: &Value) -> std::collections::HashSet<String> {
    fn collect(value: &Value, output: &mut std::collections::HashSet<String>) {
        match value {
            Value::Object(map) => {
                if let Some(id) = map.get("traderId") {
                    let id = id
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| id.to_string());
                    if !id.trim().is_empty() {
                        output.insert(id.trim().to_owned());
                    }
                }
                for child in map.values() {
                    collect(child, output);
                }
            }
            Value::Array(items) => {
                for child in items {
                    collect(child, output);
                }
            }
            _ => {}
        }
    }
    let mut result = std::collections::HashSet::new();
    collect(value, &mut result);
    result
}

fn count_account_records(value: &Value) -> usize {
    fn collect<'a>(value: &'a Value, records: &mut Vec<&'a Value>) {
        match value {
            Value::Object(map) => {
                let has_record_fields = ["accountType", "brokerTitle", "accountName", "login"]
                    .iter()
                    .any(|key| map.contains_key(*key));
                if has_record_fields
                    && (map.contains_key("id") || map.contains_key("accountId"))
                    && !records.iter().any(|record| std::ptr::eq(*record, value))
                {
                    records.push(value);
                }
                for child in map.values() {
                    collect(child, records);
                }
            }
            Value::Array(items) => {
                for child in items {
                    collect(child, records);
                }
            }
            _ => {}
        }
    }
    let mut records = Vec::new();
    collect(value, &mut records);
    records.len()
}

#[derive(Clone)]
pub struct McpExecutionValidator {
    state: Arc<Mutex<HashMap<String, (DateTime<Utc>, DateTime<Utc>, f64, f64, Option<f64>)>>>,
    errors: Arc<Mutex<HashMap<String, String>>>,
}

impl McpExecutionValidator {
    pub fn start(root: &Path, symbols: Vec<String>) -> Result<Self> {
        let client = McpBrokerContext::load(root)?.context(
            "enable cTrader MCP to validate broker account and volume rules before new FIX entries",
        )?;
        let open_api = crate::market_data::CTraderOpenApiConfig::load_optional(root)?
            .context("configure cTrader Open API credentials to discover symbol lot size and volume limits automatically")?;
        let state = Arc::new(Mutex::new(HashMap::new()));
        let errors = Arc::new(Mutex::new(HashMap::new()));
        let validator = Self {
            state: Arc::clone(&state),
            errors: Arc::clone(&errors),
        };
        std::thread::Builder::new()
            .name("mcp-account-and-volume-validation".into())
            .spawn(move || {
                let mut cached_rules = None;
                let mut rules_fetched_at: Option<DateTime<Utc>> = None;
                loop {
                    let broker_account_at = match client.fetch_requests(&["account".into()], None) {
                        Ok(records) => records
                            .first()
                            .and_then(|record| record["dataFetchedAt"].as_str())
                            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                            .map(|value| value.with_timezone(&Utc)),
                        Err(error) => {
                            for symbol in &symbols {
                                errors.lock().insert(symbol.clone(), error.to_string());
                            }
                            None
                        }
                    };
                    let rules_expired = rules_fetched_at.map_or(true, |at| {
                        !crate::freshness::is_fresh(
                            Utc::now(),
                            at,
                            SYMBOL_DETAILS_CACHE_TTL_SECONDS as u64,
                        )
                    });
                    if rules_expired {
                        match open_api.volume_rules(&symbols) {
                            Ok(rules) => {
                                cached_rules = Some(rules);
                                rules_fetched_at = Some(Utc::now());
                            }
                            Err(error) => {
                                cached_rules = None;
                                for symbol in &symbols {
                                    errors.lock().insert(
                                        symbol.clone(),
                                        format!("Open API volume metadata unavailable: {error}"),
                                    );
                                }
                            }
                        }
                    }
                    match (broker_account_at, cached_rules.as_ref()) {
                        (Some(account_at), Some(rules)) => {
                            for symbol in &symbols {
                                let normalized = normalize_symbol(symbol);
                                if let Some(rule) = rules.get(&normalized) {
                                    state.lock().insert(
                                        normalized.clone(),
                                        (
                                            account_at,
                                            rules_fetched_at.unwrap_or_else(Utc::now),
                                            rule.minimum_lots,
                                            rule.step_lots,
                                            rule.maximum_lots,
                                        ),
                                    );
                                    errors.lock().remove(symbol);
                                } else {
                                    state.lock().remove(&normalized);
                                    errors.lock().insert(
                                        symbol.clone(),
                                        "cTrader Open API returned no volume rules for this symbol"
                                            .into(),
                                    );
                                }
                            }
                        }
                        (None, Some(_)) => {
                            for symbol in &symbols {
                                state.lock().remove(&normalize_symbol(symbol));
                            }
                        }
                        (_, None) => {
                            for symbol in &symbols {
                                state.lock().remove(&normalize_symbol(symbol));
                            }
                        }
                    }
                    std::thread::sleep(Duration::from_secs(15));
                }
            })?;
        Ok(validator)
    }

    pub fn validate_entry(&self, symbol: &str, quantity: f64) -> Result<()> {
        let normalized = normalize_symbol(symbol);
        let state = self.state.lock();
        let (account_at, metadata_at, minimum, step, maximum) =
            state.get(&normalized).copied().with_context(|| {
                self.errors
                    .lock()
                    .get(symbol)
                    .cloned()
                    .unwrap_or_else(|| "MCP account/symbol validation has not completed".into())
            })?;
        if !crate::freshness::is_fresh(Utc::now(), account_at, 30) {
            bail!("cTrader MCP account identity snapshot is stale");
        }
        if !crate::freshness::is_fresh(
            Utc::now(),
            metadata_at,
            SYMBOL_DETAILS_CACHE_TTL_SECONDS as u64,
        ) {
            bail!("cTrader broker volume metadata is stale");
        }
        if quantity + 1e-9 < minimum
            || maximum.is_some_and(|maximum| quantity > maximum + 1e-9)
            || ((quantity - minimum) / step - ((quantity - minimum) / step).round()).abs() > 1e-6
        {
            bail!("FIX order quantity does not match cTrader Open API minimum, increment, and maximum");
        }
        Ok(())
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.to_ascii_uppercase().replace(['/', '-', '_'], "")
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires a running cTrader local MCP server with configured account settings"]
    fn live_ctrader_mcp_read_only_account_probe() {
        let project_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let client = McpBrokerContext::load(project_root).unwrap().unwrap();
        let balance = client.cached_call("get_balance", json!({}), 0).unwrap();
        client.validate_account(&balance).unwrap();
        let details = client
            .fetch_requests(&["symbol_details".into()], Some("BTCUSD"))
            .unwrap();
        let detail = &details[0]["data"];
        println!(
            "BTCUSD MCP metadata: symbolName={:?}, minVolume={:?}, volumeStep={:?}, volumeMinimum={:?}",
            find_key(detail, "symbolName"),
            find_key(detail, "minVolume"),
            find_key(detail, "volumeStep"),
            find_key(detail, "volumeMinimum")
        );
        let response = format!(
            "connected to cTrader MCP configured account {} and symbol metadata",
            client.account_id
        );
        println!("{response}");
    }

    #[test]
    fn malformed_dotenv_is_reported_instead_of_using_inherited_connector_values() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join(".env"),
            "CTRADER_MCP_ENDPOINT=\"unterminated\n",
        )
        .unwrap();
        let error = load_dotenv(directory.path()).unwrap_err();
        assert!(format!("{error:#}").contains("parse cTrader MCP settings"));
    }

    #[test]
    fn nested_account_identifier_is_found() {
        assert_eq!(
            find_key(&json!({"data":{"traderId":123}}), "traderId"),
            Some(&json!(123))
        );
    }

    #[test]
    fn modern_discovery_uses_per_request_metadata_and_transport_headers() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        fn read_request(stream: &mut std::net::TcpStream) -> (String, String) {
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            let header_end = loop {
                let count = stream.read(&mut buffer).unwrap();
                assert!(count > 0, "client closed before sending a complete request");
                request.extend_from_slice(&buffer[..count]);
                if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    break index + 4;
                }
            };
            let headers = String::from_utf8_lossy(&request[..header_end]).into_owned();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            while request.len() < header_end + content_length {
                let count = stream.read(&mut buffer).unwrap();
                assert!(count > 0, "client closed before sending the JSON body");
                request.extend_from_slice(&buffer[..count]);
            }
            let body = String::from_utf8_lossy(&request[header_end..header_end + content_length])
                .into_owned();
            (headers, body)
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/mcp/", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut discovery_stream, _) = listener.accept().unwrap();
            let (headers, body) = read_request(&mut discovery_stream);
            assert!(headers.contains("POST /mcp/ HTTP/1.1"));
            assert!(headers
                .to_ascii_lowercase()
                .contains("mcp-protocol-version: 2026-07-28"));
            assert!(headers
                .to_ascii_lowercase()
                .contains("mcp-method: server/discover"));
            let body: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(body["method"], "server/discover");
            assert_eq!(
                body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
                MODERN_MCP_VERSION
            );
            let discovery = json!({"jsonrpc":"2.0","id":"espeon-discover-1","result":{
                "supportedVersions":[MODERN_MCP_VERSION],"capabilities":{"tools":{}}}});
            write!(
                discovery_stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                discovery.to_string().len(),
                discovery
            )
            .unwrap();

            let (mut tool_stream, _) = listener.accept().unwrap();
            let (headers, body) = read_request(&mut tool_stream);
            assert!(headers
                .to_ascii_lowercase()
                .contains("mcp-method: tools/call"));
            assert!(headers
                .to_ascii_lowercase()
                .contains("mcp-name: get_balance"));
            let body: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(body["method"], "tools/call");
            assert_eq!(body["params"]["name"], "get_balance");
            let tool_response =
                json!({"jsonrpc":"2.0","id":1,"result":{"structuredContent":{"balance":100}}});
            write!(
                tool_stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                tool_response.to_string().len(),
                tool_response
            )
            .unwrap();
        });

        let context = McpBrokerContext {
            endpoint,
            account_id: "demo-account".into(),
            environment: "demo".into(),
            client: Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap(),
            cache: Mutex::new(HashMap::new()),
            last_success_at: Mutex::new(None),
            protocol: Mutex::new(Some(McpProtocol::Modern)),
            legacy_session: Mutex::new(None),
        };

        assert_eq!(
            context.call("get_balance", &json!({})).unwrap(),
            json!({"balance":100})
        );
        server.join().unwrap();
    }

    #[test]
    fn unknown_server_prefers_the_legacy_initialize_handshake() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        fn read_request(stream: &mut std::net::TcpStream) -> (String, Value) {
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            let header_end = loop {
                let count = stream.read(&mut buffer).unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
                if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    break index + 4;
                }
            };
            let headers = String::from_utf8_lossy(&request[..header_end]).into_owned();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            while request.len() < header_end + content_length {
                let count = stream.read(&mut buffer).unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
            }
            let body =
                serde_json::from_slice(&request[header_end..header_end + content_length]).unwrap();
            (headers, body)
        }

        fn respond(stream: &mut std::net::TcpStream, status: &str, body: &Value) {
            let text = body.to_string();
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                text.len()
            )
            .unwrap();
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/mcp/", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut initialize, _) = listener.accept().unwrap();
            let (headers, body) = read_request(&mut initialize);
            assert!(headers.to_ascii_lowercase().contains("post /mcp/ http/1.1"));
            assert!(!headers
                .to_ascii_lowercase()
                .contains("mcp-protocol-version:"));
            assert_eq!(body["method"], "initialize");
            assert_eq!(body["params"]["protocolVersion"], LEGACY_MCP_VERSION);
            let initialize_response = json!({"jsonrpc":"2.0","id":1,"result":{"protocolVersion":LEGACY_MCP_VERSION,"capabilities":{"tools":{}},"serverInfo":{"name":"ctrader","version":"test"}}}).to_string();
            write!(initialize,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nMcp-Session-Id: session-1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                initialize_response.len(), initialize_response
            ).unwrap();

            let (mut initialized, _) = listener.accept().unwrap();
            let (headers, body) = read_request(&mut initialized);
            assert!(headers
                .to_ascii_lowercase()
                .contains(&format!("mcp-protocol-version: {LEGACY_MCP_VERSION}")));
            assert!(headers
                .to_ascii_lowercase()
                .contains("mcp-session-id: session-1"));
            assert_eq!(body["method"], "notifications/initialized");
            write!(
                initialized,
                "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();

            let (mut tool_call, _) = listener.accept().unwrap();
            let (headers, body) = read_request(&mut tool_call);
            assert!(headers
                .to_ascii_lowercase()
                .contains(&format!("mcp-protocol-version: {LEGACY_MCP_VERSION}")));
            assert!(headers
                .to_ascii_lowercase()
                .contains("mcp-session-id: session-1"));
            assert_eq!(body["method"], "tools/call");
            assert_eq!(body["params"]["name"], "get_balance");
            respond(
                &mut tool_call,
                "200 OK",
                &json!({"jsonrpc":"2.0","id":2,"result":{"structuredContent":{"balance":100}}}),
            );

            let (mut next_call, _) = listener.accept().unwrap();
            let (headers, body) = read_request(&mut next_call);
            assert!(headers
                .to_ascii_lowercase()
                .contains("mcp-session-id: session-1"));
            assert_eq!(body["method"], "tools/call");
            assert_eq!(body["params"]["name"], "get_positions");
            respond(
                &mut next_call,
                "200 OK",
                &json!({"jsonrpc":"2.0","id":2,"result":{"structuredContent":{"positions":[]}}}),
            );
        });

        let context = McpBrokerContext {
            endpoint,
            account_id: "demo-account".into(),
            environment: "demo".into(),
            client: Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap(),
            cache: Mutex::new(HashMap::new()),
            last_success_at: Mutex::new(None),
            protocol: Mutex::new(None),
            legacy_session: Mutex::new(None),
        };
        assert_eq!(
            context.call("get_balance", &json!({})).unwrap(),
            json!({"balance":100})
        );
        assert_eq!(
            context.call("get_positions", &json!({})).unwrap(),
            json!({"positions":[]})
        );
        assert_eq!(*context.protocol.lock(), Some(McpProtocol::Legacy));
        server.join().unwrap();
    }

    #[test]
    fn modern_http_errors_fall_back_only_when_the_response_is_not_a_modern_error() {
        let empty_not_found = anyhow::Error::new(McpHttpError {
            status: 404,
            body: String::new(),
        });
        assert!(!should_fall_back_to_legacy(&empty_not_found));
        assert!(!should_fall_back_to_modern(&empty_not_found));
        assert!(!retryable_mcp_error(&empty_not_found));
        assert!(empty_not_found
            .to_string()
            .contains("MCP endpoint route not found"));

        let method_missing = anyhow::Error::new(McpHttpError {
            status: 404,
            body: r#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"}}"#
                .into(),
        });
        assert!(!should_fall_back_to_legacy(&method_missing));
        assert!(is_json_rpc_method_not_found(&method_missing));

        let header_mismatch = anyhow::Error::new(McpHttpError {
            status: 400,
            body: r#"{"jsonrpc":"2.0","error":{"code":-32020,"message":"Header mismatch"}}"#.into(),
        });
        assert!(!should_fall_back_to_legacy(&header_mismatch));

        let supports_legacy = anyhow::Error::new(McpHttpError {
            status: 400,
            body:
                r#"{"jsonrpc":"2.0","error":{"code":-32022,"data":{"supported":["2025-11-25"]}}}"#
                    .into(),
        });
        assert!(should_fall_back_to_legacy(&supports_legacy));

        let transient_server_error = anyhow::Error::new(McpHttpError {
            status: 503,
            body: "service unavailable".into(),
        });
        assert!(retryable_mcp_error(&transient_server_error));
    }
}
