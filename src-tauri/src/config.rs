use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessConfig {
    pub runtime_directory: String,
    pub world_model_adapter: String,
    pub jev_adapter: String,
    pub broker_adapter: String,
    pub minimum_confidence: f64,
    pub risk_policy: crate::domain::RiskPolicyConfig,
    #[serde(default)]
    pub autonomous_review: crate::domain::AutonomousReviewPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorSettings {
    pub world_model_adapter: String,
    pub jev_adapter: String,
    pub broker_adapter: String,
    pub risk_readiness: String,
    pub sections: Vec<ConnectorSection>,
    pub restart_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorSection {
    pub id: String,
    pub title: String,
    pub description: String,
    pub fields: Vec<ConnectorField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorField {
    pub key: String,
    pub label: String,
    pub value: String,
    pub kind: String,
    pub configured: bool,
    pub required: bool,
    pub placeholder: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorSettingsUpdate {
    pub world_model_adapter: String,
    pub jev_adapter: String,
    pub broker_adapter: String,
    pub values: HashMap<String, String>,
}

const CONNECTOR_FIELDS: &[(&str, &str, &str, bool, bool, &str)] = &[
    (
        "updates",
        "ESPEON_GITHUB_TOKEN",
        "Optional GitHub release token",
        true,
        false,
        "Not required for public releases; optional for higher API rate limits",
    ),
    (
        "autonomous-review",
        "AUTONOMOUS_REVIEW_ENABLED",
        "Autonomous reviews enabled",
        false,
        false,
        "true",
    ),
    (
        "autonomous-review",
        "REVIEW_NO_TRADE_HORIZON_MODE",
        "No-trade horizon mode",
        false,
        false,
        "hypothesis_horizon",
    ),
    (
        "autonomous-review",
        "REVIEW_NO_TRADE_MIN_DECISIONS",
        "Minimum no-trade decisions",
        false,
        false,
        "6",
    ),
    (
        "autonomous-review",
        "REVIEW_NO_TRADE_MAX_DECISIONS",
        "Maximum no-trade decisions",
        false,
        false,
        "20",
    ),
    (
        "autonomous-review",
        "REVIEW_CONSECUTIVE_LOSSES",
        "Consecutive losses",
        false,
        false,
        "3",
    ),
    (
        "autonomous-review",
        "REVIEW_PERIODIC_TRADES",
        "Completed trades per review",
        false,
        false,
        "10",
    ),
    (
        "autonomous-review",
        "REVIEW_MAX_ACTIVE_LOOPS",
        "Maximum active loops",
        false,
        false,
        "3",
    ),
    (
        "autonomous-review",
        "REVIEW_SPLIT_CONFIDENCE",
        "Split confidence threshold",
        false,
        false,
        "0.80",
    ),
    (
        "autonomous-review",
        "WORLD_MODEL_REVIEW_CONFIDENCE_THRESHOLD",
        "Base confidence threshold",
        false,
        false,
        "0.70",
    ),
    (
        "autonomous-review",
        "WORLD_MODEL_CONTRADICTION_CONFIDENCE_THRESHOLD",
        "Contradiction escalation threshold",
        false,
        false,
        "0.80",
    ),
    (
        "autonomous-review",
        "REVIEW_RETRY_ATTEMPTS",
        "Review retry attempts",
        false,
        false,
        "3",
    ),
    (
        "autonomous-review",
        "REVIEW_RETRY_BASE_SECONDS",
        "Review retry base seconds",
        false,
        false,
        "5",
    ),
    (
        "world-model",
        "OPENROUTER_API_KEY",
        "OpenRouter API key",
        true,
        true,
        "sk-or-v1-…",
    ),
    (
        "world-model",
        "OPENROUTER_BASE_URL",
        "OpenRouter base URL",
        false,
        false,
        "https://openrouter.ai/api/v1",
    ),
    (
        "world-model",
        "WORLD_MODEL_BASE_MODEL",
        "Base model ID",
        false,
        true,
        "openai/gpt-6-luna-pro",
    ),
    (
        "world-model",
        "WORLD_MODEL_ESCALATION_MODEL_1",
        "Escalation model ID",
        false,
        true,
        "anthropic/claude-opus-5.5",
    ),
    (
        "world-model",
        "WORLD_MODEL_WEB_SEARCH_ENABLED",
        "Internet search enabled",
        false,
        false,
        "true",
    ),
    (
        "jev",
        "JEV_API_KEY",
        "TypeSafe Jev API key",
        true,
        true,
        "Enter API key",
    ),
    (
        "jev",
        "TYPESAFE_BASE_URL",
        "TypeSafe base URL",
        false,
        false,
        "https://api.typesafe.ai",
    ),
    (
        "jev",
        "TYPESAFE_MODEL",
        "TypeSafe model",
        false,
        false,
        "Auto-discover",
    ),
    (
        "ctrader-mcp",
        "CTRADER_MCP_ENABLED",
        "Enable local cTrader MCP",
        false,
        false,
        "false",
    ),
    (
        "ctrader-mcp",
        "CTRADER_MCP_ENDPOINT",
        "MCP server URL",
        false,
        false,
        "http://127.0.0.1:9876/mcp/",
    ),
    (
        "ctrader-mcp",
        "CTRADER_MCP_ACCOUNT_ID",
        "Account ID",
        false,
        true,
        "Account ID or login shown in cTrader",
    ),
    (
        "twelve-data",
        "TWELVE_DATA_API_KEY",
        "Twelve Data API key",
        true,
        false,
        "Optional for price-only FIX strategies",
    ),
    (
        "twelve-data",
        "TWELVE_DATA_SYMBOL_MAP",
        "Broker → provider symbols",
        false,
        false,
        "BTCUSD:BTC/USD,EURUSD:EUR/USD",
    ),
    (
        "twelve-data",
        "TWELVE_DATA_HISTORY_DEPTH",
        "1m history depth",
        false,
        false,
        "250",
    ),
    (
        "twelve-data",
        "TWELVE_DATA_REST_INTERVAL_SECONDS",
        "REST confirmation interval",
        false,
        false,
        "65",
    ),
    (
        "twelve-data",
        "MARKET_PRICE_DIVERGENCE_LIMITS",
        "Per-symbol price divergence limits",
        false,
        false,
        "US100:0.005",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_CLIENT_ID",
        "Open API client ID",
        false,
        false,
        "From your cTrader Open API app",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_CLIENT_SECRET",
        "Open API client secret",
        true,
        false,
        "From your cTrader Open API app",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_ACCESS_TOKEN",
        "Open API access token",
        true,
        false,
        "Authorize the selected cTrader account",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_REFRESH_TOKEN",
        "Open API refresh token",
        true,
        false,
        "Save the refresh token returned with your access token",
    ),
    (
        "ctrader-open-api",
        "CTRADER_MARKET_QUOTE_MAX_AGE_SECONDS",
        "Quote maximum age seconds",
        false,
        false,
        "15",
    ),
    (
        "ctrader-open-api",
        "CTRADER_MARKET_HISTORY_DEPTH",
        "History depth",
        false,
        false,
        "250",
    ),
    (
        "fix-common",
        "CTRADER_FIX_DEMO_FIXED_QUANTITY_MAP",
        "Fixed entry quantity for Demo symbols",
        false,
        false,
        "BTCUSD:0.01",
    ),
    (
        "fix-common",
        "CTRADER_FIX_SYMBOL_MAP",
        "Symbol map",
        false,
        true,
        "BTCUSD:FIX_ID",
    ),
    (
        "fix-common",
        "CTRADER_FIX_HEARTBEAT_SECONDS",
        "Heartbeat seconds",
        false,
        false,
        "30",
    ),
    (
        "fix-common",
        "CTRADER_FIX_TIMEOUT_SECONDS",
        "Timeout seconds",
        false,
        false,
        "15",
    ),
    (
        "fix-common",
        "CTRADER_FIX_RECONNECT_ATTEMPTS",
        "Reconnect attempts",
        false,
        false,
        "3",
    ),
    (
        "fix-price",
        "CTRADER_FIX_PRICE_HOST",
        "Price host",
        false,
        true,
        "Host name",
    ),
    (
        "fix-price",
        "CTRADER_FIX_PRICE_PORT",
        "Price port",
        false,
        true,
        "TCP port",
    ),
    (
        "fix-price",
        "CTRADER_FIX_PRICE_SSL",
        "Use TLS",
        false,
        false,
        "true",
    ),
    (
        "fix-price",
        "CTRADER_FIX_PRICE_USERNAME",
        "Price username",
        false,
        true,
        "Username",
    ),
    (
        "fix-price",
        "CTRADER_FIX_PRICE_PASSWORD",
        "Price password",
        true,
        true,
        "Enter password",
    ),
    (
        "fix-price",
        "CTRADER_FIX_PRICE_SENDER_COMP_ID",
        "Sender Comp ID",
        false,
        true,
        "Sender Comp ID",
    ),
    (
        "fix-price",
        "CTRADER_FIX_PRICE_SENDER_SUB_ID",
        "Sender Sub ID",
        false,
        true,
        "QUOTE",
    ),
    (
        "fix-price",
        "CTRADER_FIX_PRICE_TARGET_COMP_ID",
        "Target Comp ID",
        false,
        false,
        "CSERVER",
    ),
    (
        "fix-price",
        "CTRADER_FIX_PRICE_TARGET_SUB_ID",
        "Target Sub ID",
        false,
        true,
        "QUOTE",
    ),
    (
        "fix-trade",
        "CTRADER_FIX_TRADE_HOST",
        "Trade host",
        false,
        true,
        "Host name",
    ),
    (
        "fix-trade",
        "CTRADER_FIX_TRADE_PORT",
        "Trade port",
        false,
        true,
        "TCP port",
    ),
    (
        "fix-trade",
        "CTRADER_FIX_TRADE_SSL",
        "Use TLS",
        false,
        false,
        "true",
    ),
    (
        "fix-trade",
        "CTRADER_FIX_TRADE_USERNAME",
        "Trade username",
        false,
        true,
        "Username",
    ),
    (
        "fix-trade",
        "CTRADER_FIX_TRADE_PASSWORD",
        "Trade password",
        true,
        true,
        "Enter password",
    ),
    (
        "fix-trade",
        "CTRADER_FIX_TRADE_SENDER_COMP_ID",
        "Sender Comp ID",
        false,
        true,
        "Sender Comp ID",
    ),
    (
        "fix-trade",
        "CTRADER_FIX_TRADE_SENDER_SUB_ID",
        "Sender Sub ID",
        false,
        true,
        "TRADE",
    ),
    (
        "fix-trade",
        "CTRADER_FIX_TRADE_TARGET_COMP_ID",
        "Target Comp ID",
        false,
        false,
        "CSERVER",
    ),
    (
        "fix-trade",
        "CTRADER_FIX_TRADE_TARGET_SUB_ID",
        "Target Sub ID",
        false,
        true,
        "TRADE",
    ),
];

impl HarnessConfig {
    pub fn load(project_root: &Path) -> Result<Self> {
        let path = project_root.join("config").join("harness.json");
        let mut config: Self = serde_json::from_str(
            &fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?,
        )?;
        if !matches!(
            config.world_model_adapter.as_str(),
            "simulated" | "openrouter"
        ) || !matches!(config.jev_adapter.as_str(), "simulated" | "typesafe")
            || !matches!(config.broker_adapter.as_str(), "simulated" | "ctrader-fix")
        {
            bail!("unsupported adapter selection in harness.json");
        }
        // Paper currency is only used to size simulated orders. Requiring it
        // to be valid during a live FIX startup makes an unused paper setting
        // an unrelated app-start blocker.
        if config.broker_adapter == "simulated" {
            let paper_currency = config.risk_policy.paper_account_currency.trim();
            if !crate::risk::is_supported_currency_code(paper_currency) {
                bail!(
                    "riskPolicy.paperAccountCurrency must be a supported fiat or crypto currency code"
                );
            }
            config.risk_policy.paper_account_currency = paper_currency.to_ascii_uppercase();
        }
        let values = env_file_values(project_root)?;
        let parse = |key: &str| {
            values
                .get(key)
                .map(String::as_str)
                .filter(|value| !value.trim().is_empty())
        };
        if let Some(value) = parse("AUTONOMOUS_REVIEW_ENABLED") {
            config.autonomous_review.enabled = value.eq_ignore_ascii_case("true");
        }
        if config.autonomous_review.enabled {
            if let Some(value) = parse("REVIEW_NO_TRADE_HORIZON_MODE") {
                config.autonomous_review.no_trade_horizon_mode = value.to_owned();
            }
            macro_rules! parse_number {
                ($key:literal, $field:ident) => {
                    if let Some(value) = parse($key) {
                        config.autonomous_review.$field =
                            value.parse().with_context(|| format!("invalid {}", $key))?;
                    }
                };
            }
            parse_number!("REVIEW_NO_TRADE_MIN_DECISIONS", no_trade_min_decisions);
            parse_number!("REVIEW_NO_TRADE_MAX_DECISIONS", no_trade_max_decisions);
            parse_number!("REVIEW_CONSECUTIVE_LOSSES", consecutive_loss_threshold);
            parse_number!("REVIEW_PERIODIC_TRADES", periodic_trade_threshold);
            parse_number!("REVIEW_MAX_ACTIVE_LOOPS", max_active_loops);
            parse_number!("REVIEW_SPLIT_CONFIDENCE", split_confidence_threshold);
            parse_number!("REVIEW_RETRY_ATTEMPTS", retry_attempts);
            parse_number!("REVIEW_RETRY_BASE_SECONDS", retry_base_seconds);
            if config.autonomous_review.no_trade_min_decisions == 0
                || config.autonomous_review.no_trade_min_decisions
                    > config.autonomous_review.no_trade_max_decisions
                || config.autonomous_review.consecutive_loss_threshold == 0
                || config.autonomous_review.periodic_trade_threshold == 0
                || config.autonomous_review.max_active_loops == 0
                || !(0.0..=1.0).contains(&config.autonomous_review.split_confidence_threshold)
                || config.autonomous_review.retry_attempts == 0
            {
                bail!("invalid autonomous review policy");
            }
            if !matches!(
                config.autonomous_review.no_trade_horizon_mode.as_str(),
                "hypothesis_horizon" | "fixed_minimum"
            ) {
                bail!("unsupported REVIEW_NO_TRADE_HORIZON_MODE");
            }
        }
        Ok(config)
    }

    pub fn runtime_path(&self, project_root: &Path) -> Result<PathBuf> {
        let candidate = PathBuf::from(&self.runtime_directory);
        let absolute = if candidate.is_absolute() {
            candidate
        } else {
            project_root.join(candidate)
        };
        if absolute
            .to_string_lossy()
            .to_ascii_lowercase()
            .contains("onedrive")
        {
            bail!("runtime directory must remain local and cannot resolve inside OneDrive");
        }
        Ok(absolute)
    }
}

pub(crate) fn env_file_values(project_root: &Path) -> Result<HashMap<String, String>> {
    let path = project_root.join(".env");
    if !path.exists() {
        return Ok(HashMap::new());
    }
    Ok(dotenvy::from_path_iter(path)?.collect::<Result<HashMap<_, _>, _>>()?)
}

fn encode_dotenv_value(value: &str) -> Result<String> {
    if value.contains(['\r', '\n']) {
        bail!("connector settings cannot contain line breaks");
    }
    let mut encoded = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => encoded.push_str("\\\\"),
            '"' => encoded.push_str("\\\""),
            '$' => encoded.push_str("\\$"),
            other => encoded.push(other),
        }
    }
    Ok(format!("\"{encoded}\""))
}

fn usable(value: Option<&String>) -> bool {
    value
        .map(|value| {
            let value = value.trim();
            !value.is_empty()
                && !value.starts_with("REQUIRED_")
                && !value.starts_with("CHANGE_ME")
                && !value.starts_with("YOUR_")
        })
        .unwrap_or(false)
}

pub fn load_connector_settings(project_root: &Path) -> Result<ConnectorSettings> {
    let config = HarnessConfig::load(project_root)?;
    let values = env_file_values(project_root)?;
    let sections = [
        (
            "updates",
            "Signed updates",
            "Public signed Espeon releases install without a GitHub token. A token is optional for higher API rate limits.",
        ),
        (
            "world-model",
            "World model",
            "OpenRouter routing, model selection, and live web research.",
        ),
        (
            "autonomous-review",
            "Autonomous reviews",
            "Deterministic review triggers, escalation policy, and loop limits.",
        ),
        (
            "jev",
            "TypeSafe Jev",
            "Independent Jev1/Jev2 model connection.",
        ),
        (
            "ctrader-mcp",
            "cTrader MCP",
            "Set the local server and cTrader account ID. Environment is inferred from the FIX session and checked against the active MCP account.",
        ),
        (
            "ctrader-open-api",
            "cTrader Open API",
            "Historical broker candles and tick volume. Espeon discovers the authorized account and symbol IDs, and rotates saved tokens on startup.",
        ),
        (
            "twelve-data",
            "Twelve Data market history",
            "Optional REST-confirmed candles and WebSocket prices; FIX remains execution truth.",
        ),
        (
            "fix-common",
            "FIX execution",
            "Shared FIX settings. Set the intended trade quantity; cTrader Open API supplies symbol lot size and broker quantity limits.",
        ),
        (
            "fix-price",
            "FIX price session",
            "Read-only price connection used by the harness.",
        ),
        (
            "fix-trade",
            "FIX trade session",
            "Order and position connection used by the harness. Symbol lot size and min/step/max volumes are discovered from cTrader Open API.",
        ),
    ]
    .into_iter()
    .map(|(id, title, description)| ConnectorSection {
        id: id.to_owned(),
        title: title.to_owned(),
        description: description.to_owned(),
        fields: CONNECTOR_FIELDS
            .iter()
            .filter(|field| field.0 == id)
            .map(|(_, key, label, secret, required, placeholder)| {
                let configured = if *key == "CTRADER_MCP_ENABLED" {
                    values
                        .get(*key)
                        .is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "true" | "false"))
                } else if *key == "CTRADER_MCP_ENDPOINT" {
                    true
                } else {
                    usable(values.get(*key))
                };
                ConnectorField {
                    key: (*key).to_owned(),
                    label: (*label).to_owned(),
                    value: if *secret {
                        String::new()
                    } else if *key == "CTRADER_MCP_ENDPOINT" {
                        values
                            .get(*key)
                            .filter(|value| usable(Some(value)))
                            .cloned()
                            .unwrap_or_else(|| "http://127.0.0.1:9876/mcp/".to_owned())
                    } else if *key == "CTRADER_MCP_ENABLED" {
                        values
                            .get(*key)
                            .map(|value| value.to_ascii_lowercase())
                            .filter(|value| matches!(value.as_str(), "true" | "false"))
                            .unwrap_or_else(|| "false".into())
                    } else {
                        values
                            .get(*key)
                            .filter(|value| usable(Some(value)))
                            .cloned()
                            .unwrap_or_default()
                    },
                    kind: if *key == "CTRADER_MCP_ENABLED" {
                        "boolean"
                    } else if *secret {
                        "secret"
                    } else {
                        "text"
                    }
                    .to_owned(),
                    configured,
                    required: *required,
                    placeholder: if *secret && configured {
                        "Stored — leave blank to keep".to_owned()
                    } else {
                        (*placeholder).to_owned()
                    },
                }
            })
            .collect(),
    })
    .collect();
    let risk_readiness = if config.broker_adapter == "simulated" {
        "Simulated execution uses the configured paper risk policy. No cTrader account is used."
            .into()
    } else {
        "Automatic account risk snapshots are unavailable. Select an active run and use the human verification panel to enter current cTrader account values and symbol limits for one decision cycle; otherwise no live entry is submitted.".into()
    };
    Ok(ConnectorSettings {
        world_model_adapter: config.world_model_adapter,
        jev_adapter: config.jev_adapter,
        broker_adapter: config.broker_adapter,
        risk_readiness,
        sections,
        restart_required: false,
    })
}

pub fn save_connector_settings(
    project_root: &Path,
    update: ConnectorSettingsUpdate,
) -> Result<ConnectorSettings> {
    if !matches!(
        update.world_model_adapter.as_str(),
        "simulated" | "openrouter"
    ) || !matches!(update.jev_adapter.as_str(), "simulated" | "typesafe")
        || !matches!(update.broker_adapter.as_str(), "simulated" | "ctrader-fix")
    {
        bail!("unsupported adapter selection");
    }
    let allowed: std::collections::HashSet<&str> =
        CONNECTOR_FIELDS.iter().map(|field| field.1).collect();
    if let Some(key) = update
        .values
        .keys()
        .find(|key| !allowed.contains(key.as_str()))
    {
        bail!("unsupported connector setting {key}");
    }

    let env_path = project_root.join(".env");
    let existing = fs::read_to_string(&env_path).unwrap_or_default();
    let mut pending = update.values;
    let saved_values = env_file_values(project_root)?;
    let derive_environment = |key: &str| {
        pending
            .get(key)
            .or_else(|| saved_values.get(key))
            .and_then(|value| value.split('.').next())
            .map(str::trim)
            .filter(|environment| {
                environment.eq_ignore_ascii_case("demo") || environment.eq_ignore_ascii_case("live")
            })
            .map(str::to_ascii_lowercase)
    };
    let derived_environment = derive_environment("CTRADER_FIX_TRADE_SENDER_COMP_ID")
        .or_else(|| derive_environment("CTRADER_FIX_PRICE_SENDER_COMP_ID"));
    drop(derive_environment);
    if let Some(environment) = derived_environment {
        pending.insert("CTRADER_MCP_ENVIRONMENT".into(), environment);
    }
    let secret_keys: std::collections::HashSet<&str> = CONNECTOR_FIELDS
        .iter()
        .filter(|field| field.3)
        .map(|field| field.1)
        .collect();
    // Secret inputs are intentionally blank in the UI until a user replaces
    // them. Preserve the effective dotenv value and normalize duplicate keys
    // to one definition so the UI and runtime cannot select different values.
    let blank_secrets: Vec<String> = pending
        .iter()
        .filter(|(key, value)| secret_keys.contains(key.as_str()) && value.trim().is_empty())
        .map(|(key, _)| key.clone())
        .collect();
    for key in blank_secrets {
        if let Some(value) = saved_values.get(&key) {
            pending.insert(key, value.clone());
        } else {
            pending.remove(&key);
        }
    }
    let keys_to_write: std::collections::HashSet<String> = pending.keys().cloned().collect();
    let mut output = Vec::new();
    for line in existing.lines() {
        let Some((raw_key, _)) = line.split_once('=') else {
            output.push(line.to_owned());
            continue;
        };
        let key = raw_key.trim();
        if !keys_to_write.contains(key) {
            output.push(line.to_owned());
        }
    }
    let mut pending: Vec<_> = pending.into_iter().collect();
    pending.sort_by(|left, right| left.0.cmp(&right.0));
    for (key, value) in pending {
        if secret_keys.contains(key.as_str()) && value.trim().is_empty() {
            continue;
        }
        output.push(format!("{key}={}", encode_dotenv_value(value.trim())?));
    }
    fs::write(&env_path, format!("{}\n", output.join("\n")))?;

    let config_path = project_root.join("config").join("harness.json");
    let mut config_json: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&config_path)?)?;
    config_json["worldModelAdapter"] = update.world_model_adapter.into();
    config_json["jevAdapter"] = update.jev_adapter.into();
    config_json["brokerAdapter"] = update.broker_adapter.into();
    fs::write(
        &config_path,
        serde_json::to_string_pretty(&config_json)? + "\n",
    )?;

    let mut settings = load_connector_settings(project_root)?;
    settings.restart_required = true;
    Ok(settings)
}

/// Persist cTrader's rotated Open API token pair without rewriting unrelated
/// connector settings. cTrader invalidates the previous refresh token when it
/// issues a replacement, so both values must be saved together.
pub(crate) fn persist_open_api_tokens(
    project_root: &Path,
    access_token: &str,
    refresh_token: &str,
    expires_at: i64,
) -> Result<()> {
    let env_path = project_root.join(".env");
    let existing = fs::read_to_string(&env_path).unwrap_or_default();
    let pending = HashMap::from([
        ("CTRADER_OPEN_API_ACCESS_TOKEN", access_token.to_owned()),
        ("CTRADER_OPEN_API_REFRESH_TOKEN", refresh_token.to_owned()),
        (
            "CTRADER_OPEN_API_ACCESS_TOKEN_EXPIRES_AT",
            expires_at.to_string(),
        ),
    ]);
    let mut written = std::collections::HashSet::new();
    let mut output = Vec::new();
    for line in existing.lines() {
        let Some((raw_key, _)) = line.split_once('=') else {
            output.push(line.to_owned());
            continue;
        };
        let key = raw_key.trim();
        if let Some(value) = pending.get(key) {
            if written.insert(key.to_owned()) {
                output.push(format!("{key}={}", encode_dotenv_value(value)?));
            }
        } else {
            output.push(line.to_owned());
        }
    }
    for (key, value) in pending {
        if written.contains(key) {
            continue;
        }
        output.push(format!("{key}={}", encode_dotenv_value(&value)?));
    }
    fs::write(&env_path, format!("{}\n", output.join("\n")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotenv_serializer_round_trips_secret_characters() {
        let expected = r#"token with spaces # 'quotes' $HOME \path"#;
        let file = tempfile::NamedTempFile::new().unwrap();
        fs::write(
            file.path(),
            format!(
                "TEST_CONNECTOR_SECRET={}\n",
                encode_dotenv_value(expected).unwrap()
            ),
        )
        .unwrap();
        let parsed = dotenvy::from_path_iter(file.path())
            .unwrap()
            .collect::<Result<HashMap<_, _>, _>>()
            .unwrap();
        assert_eq!(parsed.get("TEST_CONNECTOR_SECRET").unwrap(), expected);
    }

    #[test]
    fn saving_settings_keeps_the_saved_secret_and_removes_duplicate_definitions() {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir_all(directory.path().join("config")).unwrap();
        fs::write(
            directory.path().join("config/harness.json"),
            include_str!("../../config/harness.json"),
        )
        .unwrap();
        let secret = r#"saved # secret $HOME \ path"#;
        fs::write(
            directory.path().join(".env"),
            format!(
                "CTRADER_MCP_ENDPOINT=http://old.invalid/mcp/\nCTRADER_MCP_ENDPOINT=http://current.invalid/mcp/\nCTRADER_OPEN_API_ACCESS_TOKEN={}\nKEEP_THIS=value\n",
                encode_dotenv_value(secret).unwrap()
            ),
        )
        .unwrap();

        save_connector_settings(
            directory.path(),
            ConnectorSettingsUpdate {
                world_model_adapter: "openrouter".into(),
                jev_adapter: "typesafe".into(),
                broker_adapter: "ctrader-fix".into(),
                values: HashMap::from([
                    (
                        "CTRADER_MCP_ENDPOINT".into(),
                        "http://updated.invalid/mcp/".into(),
                    ),
                    ("CTRADER_OPEN_API_ACCESS_TOKEN".into(), String::new()),
                ]),
            },
        )
        .unwrap();

        let parsed = env_file_values(directory.path()).unwrap();
        assert_eq!(
            parsed.get("CTRADER_MCP_ENDPOINT").unwrap(),
            "http://updated.invalid/mcp/"
        );
        assert_eq!(parsed.get("CTRADER_OPEN_API_ACCESS_TOKEN").unwrap(), secret);
        assert_eq!(parsed.get("KEEP_THIS").unwrap(), "value");
        let file = fs::read_to_string(directory.path().join(".env")).unwrap();
        assert_eq!(
            file.lines()
                .filter(|line| line.starts_with("CTRADER_MCP_ENDPOINT="))
                .count(),
            1
        );
    }

    #[test]
    fn invalid_review_overrides_do_not_block_startup_when_reviews_are_disabled() {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir_all(directory.path().join("config")).unwrap();
        fs::write(
            directory.path().join("config/harness.json"),
            include_str!("../../config/harness.json"),
        )
        .unwrap();
        fs::write(
            directory.path().join(".env"),
            "AUTONOMOUS_REVIEW_ENABLED=false\nREVIEW_MAX_ACTIVE_LOOPS=not-a-number\nREVIEW_NO_TRADE_HORIZON_MODE=invalid\n",
        )
        .unwrap();

        let config = HarnessConfig::load(directory.path()).unwrap();

        assert!(!config.autonomous_review.enabled);
    }
}
