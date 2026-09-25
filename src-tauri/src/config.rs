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
        "GitHub token (Contents: read)",
        true,
        false,
        "Fine-grained token for private Espeon releases",
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
        "Enable read-only MCP",
        false,
        false,
        "false",
    ),
    (
        "ctrader-mcp",
        "CTRADER_MCP_ENDPOINT",
        "MCP endpoint",
        false,
        false,
        "http://127.0.0.1:…",
    ),
    (
        "ctrader-mcp",
        "CTRADER_MCP_ACCOUNT_ID",
        "Account ID",
        false,
        false,
        "cTrader account ID",
    ),
    (
        "ctrader-mcp",
        "CTRADER_MCP_AUTH_TOKEN",
        "MCP auth token",
        true,
        false,
        "Enter auth token",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_ENVIRONMENT",
        "Environment",
        false,
        true,
        "demo",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_CLIENT_ID",
        "Open API client ID",
        false,
        true,
        "Application client ID",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_CLIENT_SECRET",
        "Open API client secret",
        true,
        true,
        "Application client secret",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_ACCESS_TOKEN",
        "Read-only access token",
        true,
        true,
        "OAuth access token",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_REFRESH_TOKEN",
        "Refresh token",
        true,
        false,
        "Optional OAuth refresh token",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_ACCOUNT_ID",
        "Account ID",
        false,
        true,
        "cTID trader account ID",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_SYMBOL_MAP",
        "Symbol map",
        false,
        true,
        "BTCUSD:OPEN_API_SYMBOL_ID",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_PORT",
        "TLS port",
        false,
        false,
        "5035",
    ),
    (
        "ctrader-open-api",
        "CTRADER_OPEN_API_TIMEOUT_SECONDS",
        "Request timeout seconds",
        false,
        false,
        "15",
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
        let values = env_file_values(project_root)?;
        let parse = |key: &str| values.get(key).map(String::as_str);
        if let Some(value) = parse("AUTONOMOUS_REVIEW_ENABLED") {
            config.autonomous_review.enabled = value.eq_ignore_ascii_case("true");
        }
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

fn env_file_values(project_root: &Path) -> Result<HashMap<String, String>> {
    let path = project_root.join(".env");
    let contents = fs::read_to_string(&path).unwrap_or_default();
    Ok(contents
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            line.split_once('=')
                .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()))
        })
        .collect())
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
            "Private updates",
            "Read-only GitHub access for signed Espeon releases. An authenticated GitHub CLI also works.",
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
            "Read-only cTrader data available to research and context.",
        ),
        (
            "ctrader-open-api",
            "cTrader Open API market history",
            "Read-only completed trendbars used to seed and repair deterministic live Jev context.",
        ),
        (
            "fix-common",
            "FIX execution",
            "Shared deterministic FIX execution settings.",
        ),
        (
            "fix-price",
            "FIX price session",
            "Read-only price connection used by the harness.",
        ),
        (
            "fix-trade",
            "FIX trade session",
            "Order and position connection used by the harness.",
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
                let configured = usable(values.get(*key));
                ConnectorField {
                    key: (*key).to_owned(),
                    label: (*label).to_owned(),
                    value: if *secret {
                        String::new()
                    } else {
                        values
                            .get(*key)
                            .filter(|value| usable(Some(value)))
                            .cloned()
                            .unwrap_or_default()
                    },
                    kind: if *secret { "secret" } else { "text" }.to_owned(),
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
    Ok(ConnectorSettings {
        world_model_adapter: config.world_model_adapter,
        jev_adapter: config.jev_adapter,
        broker_adapter: config.broker_adapter,
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
    let secret_keys: std::collections::HashSet<&str> = CONNECTOR_FIELDS
        .iter()
        .filter(|field| field.3)
        .map(|field| field.1)
        .collect();
    let mut output = Vec::new();
    for line in existing.lines() {
        let Some((raw_key, _)) = line.split_once('=') else {
            output.push(line.to_owned());
            continue;
        };
        let key = raw_key.trim();
        if let Some(value) = pending.remove(key) {
            if secret_keys.contains(key) && value.trim().is_empty() {
                output.push(line.to_owned());
            } else {
                output.push(format!("{key}={}", value.trim()));
            }
        } else {
            output.push(line.to_owned());
        }
    }
    for (key, value) in pending {
        if !(secret_keys.contains(key.as_str()) && value.trim().is_empty()) {
            output.push(format!("{key}={}", value.trim()));
        }
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
