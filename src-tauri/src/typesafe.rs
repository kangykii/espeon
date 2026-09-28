use crate::domain::*;
use crate::ports::JevEngine;
use anyhow::{bail, Context, Result};
use parking_lot::Mutex;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct TypeSafeConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: Option<String>,
    pub timeout_seconds: u64,
}

impl TypeSafeConfig {
    pub fn load(project_root: &std::path::Path) -> Result<Self> {
        let file_values = crate::config::env_file_values(project_root)?;
        let setting = |name: &str| {
            file_values
                .get(name)
                .cloned()
                .or_else(|| std::env::var(name).ok())
                .filter(|value| !value.trim().is_empty())
        };
        let api_key = setting("JEV_API_KEY")
            .context("JEV_API_KEY is required for the TypeSafe Jev adapter")?;
        if api_key.trim().is_empty() || api_key.starts_with("REQUIRED_") {
            bail!("a real TypeSafe API key is required");
        }
        Ok(Self {
            base_url: setting("TYPESAFE_BASE_URL")
                .unwrap_or_else(|| "https://api.typesafe.ai".into()),
            api_key,
            model: setting("TYPESAFE_MODEL"),
            timeout_seconds: 10,
        })
    }
}

pub struct TypeSafeJev {
    client: Client,
    config: TypeSafeConfig,
    model: Mutex<Option<String>>,
}

#[derive(Deserialize)]
struct ModelsResponse {
    models: Vec<ModelMetadata>,
}

#[derive(Deserialize)]
struct ModelMetadata {
    name: String,
}

#[derive(Deserialize)]
struct SystemOneResponse {
    model: String,
    answers: BTreeMap<String, Value>,
    usage: JevUsage,
}

impl TypeSafeJev {
    pub fn new(config: TypeSafeConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .build()?;
        Ok(Self {
            client,
            config,
            model: Mutex::new(None),
        })
    }

    fn discover_model(client: &Client, config: &TypeSafeConfig) -> Result<String> {
        let response = client
            .get(format!(
                "{}/v1/models",
                config.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&config.api_key)
            .send()
            .context("TypeSafe model discovery network failure")?;
        if !response.status().is_success() {
            bail!(
                "TypeSafe model discovery failed with HTTP {}",
                response.status()
            );
        }
        let available: ModelsResponse = response
            .json()
            .context("TypeSafe model discovery returned invalid JSON")?;
        if available.models.is_empty() {
            bail!("TypeSafe model endpoint returned no available models");
        }
        if let Some(requested) = &config.model {
            if available
                .models
                .iter()
                .any(|model| &model.name == requested)
            {
                return Ok(requested.clone());
            }
            bail!("configured TypeSafe model {requested} was not returned by /v1/models");
        }
        available
            .models
            .iter()
            .find(|model| model.name == "jev-latest")
            .or_else(|| available.models.first())
            .map(|model| model.name.clone())
            .context("TypeSafe model discovery returned no usable model")
    }

    fn choice(
        &self,
        state: &ResolvedJevState,
        question_id: &str,
        instructions: &str,
        options: &[(&str, &str)],
    ) -> Result<(String, f64, JevInferenceMetadata)> {
        let model = {
            let mut selected = self.model.lock();
            if selected.is_none() {
                *selected = Some(Self::discover_model(&self.client, &self.config)?);
            }
            selected
                .as_ref()
                .expect("model discovery completed")
                .clone()
        };
        let criteria: BTreeMap<&str, &str> = options.iter().copied().collect();
        let mut provider_state = state.clone();
        if let Some(snapshot) = provider_state.live_context_snapshot.as_mut() {
            let latest_candles: Vec<_> = snapshot
                .candles
                .iter()
                .rev()
                .fold(BTreeMap::new(), |mut latest, candle| {
                    latest
                        .entry(format!("{:?}", candle.period))
                        .or_insert_with(|| candle.clone());
                    latest
                })
                .into_values()
                .collect();
            provider_state.market_state["latestCompletedCandles"] = json!(latest_candles);
            snapshot.candles.clear();
            for field in &mut snapshot.fields {
                field.source_observation_ids.truncate(24);
                field.provenance.sort();
                field.provenance.dedup();
            }
        }
        let payload = json!({
            "model": model,
            "state": provider_state,
            "questions": {
                question_id: {
                    "type": "choice",
                    "instructions": instructions,
                    "criteria": criteria,
                }
            }
        });
        crate::evidence::enforce_model_request_budget(&payload, "TypeSafe Jev")?;
        let endpoint = format!(
            "{}/v1/systemone",
            self.config.base_url.trim_end_matches('/')
        );
        let mut attempt = 0_u8;
        let (body, request_id) = loop {
            attempt += 1;
            let response = self
                .client
                .post(&endpoint)
                .bearer_auth(&self.config.api_key)
                .header(reqwest::header::ACCEPT, "application/json")
                .json(&payload)
                .send()
                .context("TypeSafe inference network failure")?;
            let request_id = response
                .headers()
                .get("x-typesafe-request-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let status = response.status();
            let body = response
                .text()
                .context("TypeSafe inference response body could not be read")?;
            if status.is_success() {
                break (body, request_id);
            }

            // The provider has occasionally returned a transient 400 for a request that
            // succeeds unchanged moments later. Inference is side-effect free, so one
            // bounded retry is safe; a persistent rejection is surfaced with its body.
            let retryable = status.as_u16() == 400
                || status.as_u16() == 408
                || status.as_u16() == 429
                || status.is_server_error();
            if retryable && attempt == 1 {
                std::thread::sleep(Duration::from_millis(150));
                continue;
            }
            let detail = compact_provider_error(&body);
            let request_suffix = request_id
                .as_deref()
                .map(|id| format!(" (request {id})"))
                .unwrap_or_default();
            bail!("TypeSafe inference failed with HTTP {status}{request_suffix}: {detail}");
        };
        let response: SystemOneResponse =
            serde_json::from_str(&body).context("TypeSafe inference returned invalid JSON")?;
        if response.answers.len() != 1 {
            bail!("TypeSafe response did not contain exactly the expected answer");
        }
        let answer = response
            .answers
            .get(question_id)
            .context("TypeSafe response omitted the expected answer")?;
        if answer.get("type").and_then(Value::as_str) != Some("choice") {
            bail!("TypeSafe answer type did not match submitted Choice question");
        }
        let choice = answer
            .get("choice")
            .and_then(Value::as_str)
            .context("TypeSafe Choice answer omitted choice")?
            .to_owned();
        let allowed: BTreeSet<&str> = options.iter().map(|(name, _)| *name).collect();
        if !allowed.contains(choice.as_str()) {
            bail!("TypeSafe Choice answer was outside the declared option set");
        }
        let confidence = valid_probability(answer.get("confidence"), "confidence")?;
        let probabilities_value = answer
            .get("probabilities")
            .and_then(Value::as_object)
            .context("TypeSafe Choice answer omitted probabilities")?;
        let mut probabilities = BTreeMap::new();
        for option in &allowed {
            let probability = valid_probability(probabilities_value.get(*option), option)?;
            probabilities.insert((*option).to_owned(), probability);
        }
        if probabilities_value.len() != allowed.len() {
            bail!("TypeSafe probabilities did not match the declared option set");
        }
        let sum: f64 = probabilities.values().sum();
        if (sum - 1.0).abs() > 0.02 {
            bail!("TypeSafe Choice probabilities do not sum approximately to one");
        }
        Ok((
            choice,
            confidence,
            JevInferenceMetadata {
                provider: "typesafe".into(),
                requested_model: model,
                returned_model: response.model,
                question_id: question_id.into(),
                answer_type: "choice".into(),
                probabilities,
                usage: response.usage,
                request_id,
            },
        ))
    }
}

fn compact_provider_error(body: &str) -> String {
    const MAX_CHARS: usize = 1_200;
    let flattened = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if flattened.is_empty() {
        return "empty response body".into();
    }
    let mut chars = flattened.chars();
    let prefix: String = chars.by_ref().take(MAX_CHARS).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

impl JevEngine for TypeSafeJev {
    fn decide_entry(&self, state: &ResolvedJevState, question: &str) -> Result<Jev1Decision> {
        let (choice, confidence, inference) = self.choice(
            state,
            "entry_action",
            question,
            &[
                ("LONG", "Open a long position."),
                ("SHORT", "Open a short position."),
            ],
        )?;
        let action = match choice.as_str() {
            "LONG" => Jev1Action::Long,
            "SHORT" => Jev1Action::Short,
            _ => unreachable!(),
        };
        Ok(Jev1Decision {
            action,
            confidence,
            rationale: "Typed TypeSafe Choice result; probability distribution is stored in inference metadata.".into(),
            inference,
        })
    }

    fn manage_position(&self, state: &ResolvedJevState, question: &str) -> Result<Jev2Decision> {
        if state.position.is_none() {
            bail!("Jev2 requires minimum current-position state");
        }
        let (choice, confidence, inference) = self.choice(
            state,
            "position_action",
            question,
            &[
                ("HOLD", "Keep the current position unchanged."),
                ("SELL", "Close the current position."),
                (
                    "BUY MORE",
                    "Request a new Jev1 confirmation before adding exposure.",
                ),
            ],
        )?;
        let action = match choice.as_str() {
            "HOLD" => Jev2Action::Hold,
            "SELL" => Jev2Action::Sell,
            "BUY MORE" => Jev2Action::BuyMore,
            _ => unreachable!(),
        };
        Ok(Jev2Decision {
            action,
            confidence,
            rationale: "Typed TypeSafe Choice result; probability distribution is stored in inference metadata.".into(),
            inference,
        })
    }
}

fn valid_probability(value: Option<&Value>, label: &str) -> Result<f64> {
    let value = value
        .and_then(Value::as_f64)
        .with_context(|| format!("TypeSafe {label} was not numeric"))?;
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        bail!("TypeSafe {label} was outside 0..=1");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;

    fn mock_server(bodies: Vec<Value>) -> (String, Arc<Mutex<Vec<String>>>) {
        mock_server_responses(bodies.into_iter().map(|body| (200, body)).collect())
    }

    fn mock_server_responses(responses: Vec<(u16, Value)>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        thread::spawn(move || {
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let count = stream.read(&mut buffer).unwrap();
                    if count == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..count]);
                    let request = String::from_utf8_lossy(&bytes);
                    if let Some(header_end) = request.find("\r\n\r\n") {
                        let content_length = request[..header_end]
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= header_end + 4 + content_length {
                            break;
                        }
                    }
                }
                captured
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&bytes).into_owned());
                let body = serde_json::to_string(&body).unwrap();
                let reason = if status == 200 { "OK" } else { "Bad Request" };
                write!(
                    stream,
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nx-typesafe-request-id: req-test\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
            }
        });
        (format!("http://{address}"), requests)
    }

    fn state(with_position: bool) -> ResolvedJevState {
        ResolvedJevState {
            thesis: "BTC continuation".into(),
            thesis_version_id: "thesis-1".into(),
            context_version_id: "context-1".into(),
            instruments: vec!["BTCUSD".into()],
            timeframe: TimeframeDefinition {
                label: "60 minute".into(),
                horizon_minutes: 60,
                source: "world-model-selected".into(),
                rationale: "test".into(),
            },
            context: vec![ResolvedContextField {
                name: "price".into(),
                value: json!(100),
                source_id: "feed-price".into(),
                source_uri: "feed://btc".into(),
                observed_at: Utc::now(),
            }],
            market_state: json!({"regime":"trend"}),
            position: with_position
                .then(|| json!({"positionId":"position-1","direction":"Long","state":"open"})),
            live_context_snapshot: None,
            resolved_at: Utc::now(),
        }
    }

    #[test]
    fn typesafe_adapter_discovers_model_and_validates_jev1_and_jev2() {
        let (base_url, requests) = mock_server(vec![
            json!({"models":[{"name":"jev-live-test","description":"test","release_date":"2026-09-15"}]}),
            json!({
                "model":"jev-live-test",
                "answers":{"entry_action":{"type":"choice","choice":"LONG","confidence":0.8,"probabilities":{"LONG":0.9,"SHORT":0.1}}},
                "usage":{"input_tokens":100,"output_tokens":10}
            }),
            json!({
                "model":"jev-live-test",
                "answers":{"position_action":{"type":"choice","choice":"BUY MORE","confidence":0.7,"probabilities":{"HOLD":0.2,"SELL":0.1,"BUY MORE":0.7}}},
                "usage":{"input_tokens":120,"output_tokens":10}
            }),
        ]);
        let adapter = TypeSafeJev::new(TypeSafeConfig {
            base_url,
            api_key: "test-key".into(),
            model: None,
            timeout_seconds: 5,
        })
        .unwrap();
        let entry = adapter
            .decide_entry(&state(false), "Choose the entry action.")
            .unwrap();
        assert!(matches!(entry.action, Jev1Action::Long));
        assert_eq!(entry.inference.returned_model, "jev-live-test");
        assert_eq!(entry.inference.request_id.as_deref(), Some("req-test"));
        let management = adapter
            .manage_position(&state(true), "Choose the position action.")
            .unwrap();
        assert!(matches!(management.action, Jev2Action::BuyMore));

        let requests = requests.lock().unwrap();
        let entry_request = requests
            .iter()
            .find(|request| request.contains("entry_action"))
            .unwrap();
        assert!(entry_request.contains("\"LONG\""));
        assert!(entry_request.contains("\"SHORT\""));
        assert!(!entry_request.contains("NO TRADE"));
        assert!(!entry_request.to_ascii_lowercase().contains("capital"));
        let management_request = requests
            .iter()
            .find(|request| request.contains("position_action"))
            .unwrap();
        assert!(management_request.contains("\"BUY MORE\""));
    }

    #[test]
    fn typesafe_adapter_retries_one_transient_400() {
        let (base_url, requests) = mock_server_responses(vec![
            (
                200,
                json!({"models":[{"name":"jev-live-test","description":"test","release_date":"2026-09-15"}]}),
            ),
            (400, json!({"detail":"temporary provider rejection"})),
            (
                200,
                json!({
                    "model":"jev-live-test",
                    "answers":{"entry_action":{"type":"choice","choice":"LONG","confidence":0.59,"probabilities":{"LONG":0.6,"SHORT":0.4}}},
                    "usage":{"input_tokens":100,"output_tokens":10}
                }),
            ),
        ]);
        let adapter = TypeSafeJev::new(TypeSafeConfig {
            base_url,
            api_key: "test-key".into(),
            model: None,
            timeout_seconds: 5,
        })
        .unwrap();

        let decision = adapter
            .decide_entry(&state(false), "Choose the entry action.")
            .unwrap();
        assert!(matches!(decision.action, Jev1Action::Long));
        assert_eq!(decision.confidence, 0.59);
        assert_eq!(requests.lock().unwrap().len(), 3);
    }

    #[test]
    fn typesafe_adapter_surfaces_persistent_provider_error_body() {
        let (base_url, _) = mock_server_responses(vec![
            (
                200,
                json!({"models":[{"name":"jev-live-test","description":"test","release_date":"2026-09-15"}]}),
            ),
            (400, json!({"detail":"invalid request state"})),
            (400, json!({"detail":"invalid request state"})),
        ]);
        let adapter = TypeSafeJev::new(TypeSafeConfig {
            base_url,
            api_key: "test-key".into(),
            model: None,
            timeout_seconds: 5,
        })
        .unwrap();

        let error = adapter
            .decide_entry(&state(false), "Choose the entry action.")
            .unwrap_err()
            .to_string();
        assert!(error.contains("400 Bad Request"));
        assert!(error.contains("invalid request state"));
        assert!(error.contains("req-test"));
    }

    #[test]
    #[ignore = "requires the locally configured real TypeSafe/Jev API key"]
    fn live_typesafe_smoke_test() {
        let project_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        let adapter = TypeSafeJev::new(TypeSafeConfig::load(&project_root).unwrap()).unwrap();
        let entry = adapter
            .decide_entry(&state(false), "Choose LONG or SHORT for this test state.")
            .unwrap();
        assert_eq!(entry.inference.provider, "typesafe");
        assert!(!entry.inference.returned_model.is_empty());
        assert_eq!(entry.inference.probabilities.len(), 2);
        let management = adapter
            .manage_position(
                &state(true),
                "Choose HOLD, SELL, or BUY MORE for this test position.",
            )
            .unwrap();
        assert_eq!(management.inference.provider, "typesafe");
        assert!(!management.inference.returned_model.is_empty());
        assert_eq!(management.inference.probabilities.len(), 3);
    }

    #[test]
    #[ignore = "requires the locally configured real TypeSafe/Jev API key"]
    fn live_typesafe_compacted_large_state_smoke_test() {
        let project_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        let adapter = TypeSafeJev::new(TypeSafeConfig::load(&project_root).unwrap()).unwrap();
        let mut resolved = state(false);
        resolved.context = (0..20)
            .map(|index| ResolvedContextField {
                name: format!("retrieved evidence {index}"),
                value: json!(format!(
                    "market evidence {index}: {}",
                    "detail ".repeat(600)
                )),
                source_id: format!("canonical-record-{index}"),
                source_uri: "qdrant://historical-context".into(),
                observed_at: Utc::now(),
            })
            .collect();
        crate::context::compact_for_jev(&mut resolved);
        assert!(serde_json::to_vec(&resolved).unwrap().len() <= 7_500);

        let entry = adapter
            .decide_entry(
                &resolved,
                "Choose LONG or SHORT using only the compacted supplied state.",
            )
            .unwrap();
        assert_eq!(entry.inference.provider, "typesafe");
        assert_eq!(entry.inference.probabilities.len(), 2);
    }
}
