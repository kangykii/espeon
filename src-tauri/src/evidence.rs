use anyhow::{bail, Result};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Keep model request bodies comfortably below provider context limits. The
/// limit is measured on the complete serialized request, including schema.
pub const MAX_MODEL_REQUEST_BYTES: usize = 64 * 1024;

/// Bound each indexed text unit. Large documents are split before embedding.
pub const MAX_INDEX_CHUNK_CHARS: usize = 1_800;
const INDEX_CHUNK_OVERLAP_CHARS: usize = 160;
const MAX_EVENT_PROJECTION_CHARS: usize = 7_000;
const MAX_MODEL_EVIDENCE_HITS: usize = 8;
const MAX_MODEL_EVIDENCE_TEXT_CHARS: usize = 900;

pub fn enforce_model_request_budget(request: &Value, provider: &str) -> Result<()> {
    let bytes = serde_json::to_vec(request)?.len();
    if bytes > MAX_MODEL_REQUEST_BYTES {
        bail!(
            "{provider} request exceeds the shared model-input boundary ({bytes} bytes; maximum {})",
            MAX_MODEL_REQUEST_BYTES
        );
    }
    Ok(())
}

/// Convert retrieval traces to a compact, citation-safe model representation.
/// Canonical events retain the complete trace separately for audit and replay.
pub fn compact_model_input(value: Value) -> Value {
    if let Some(trace) = as_retrieval_trace(&value) {
        return compact_retrieval_trace(trace);
    }
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| (key, compact_model_input(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(compact_model_input).collect()),
        scalar => scalar,
    }
}

fn as_retrieval_trace(value: &Value) -> Option<&Value> {
    let object = value.as_object()?;
    let hits = object.get("hits")?.as_array()?;
    if hits.iter().any(|hit| {
        hit.get("canonical_event_id").is_some()
            || hit.get("canonicalEventId").is_some()
            || hit.get("provenance_uri").is_some()
            || hit.get("provenanceUri").is_some()
    }) {
        Some(value)
    } else {
        None
    }
}

fn compact_retrieval_trace(trace: &Value) -> Value {
    let hits = trace
        .get("hits")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(MAX_MODEL_EVIDENCE_HITS)
        .map(|hit| {
            let event_id = string_field(hit, "canonical_event_id", "canonicalEventId");
            let entity_id = string_field(hit, "canonical_entity_id", "canonicalEntityId");
            let citation = if event_id.is_empty() { &entity_id } else { &event_id };
            let mut metadata_budget = 1_200usize;
            let metadata = compact_value(
                hit.get("metadata").unwrap_or(&Value::Null),
                &mut metadata_budget,
                0,
            );
            json!({
                "evidenceId": citation,
                "title": truncate_chars(string_field(hit, "title", "title"), 240),
                "text": truncate_chars(string_field(hit, "text", "text"), MAX_MODEL_EVIDENCE_TEXT_CHARS),
                "sourceClass": string_field(hit, "source_class", "sourceClass"),
                "trustLevel": string_field(hit, "trust_level", "trustLevel"),
                "observedAt": string_field(hit, "observed_at", "observedAt"),
                "provenance": string_field(hit, "provenance_uri", "provenanceUri"),
                "score": hit.get("score").cloned().unwrap_or(Value::Null),
                "metadata": metadata
            })
        })
        .collect::<Vec<_>>();
    json!({
        "question": string_field(trace, "question", "question"),
        "sufficient": trace.get("sufficient").cloned().unwrap_or(Value::Bool(false)),
        "missingSourceClasses": trace
            .get("missing_source_classes")
            .or_else(|| trace.get("missingSourceClasses"))
            .cloned()
            .unwrap_or_else(|| json!([])),
        "hits": hits,
        "instruction": "Cite only the exact evidenceId values shown here."
    })
}

fn string_field(value: &Value, snake: &str, camel: &str) -> String {
    value
        .get(snake)
        .or_else(|| value.get(camel))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn truncate_chars(value: String, maximum: usize) -> String {
    let mut chars = value.chars();
    let prefix = chars.by_ref().take(maximum).collect::<String>();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

/// Compact arbitrary canonical JSON for the retrieval index only. The source
/// event is not modified. Nested retrieval traces are never copied into vectors.
pub fn compact_index_value(value: &Value) -> Value {
    let mut budget = MAX_EVENT_PROJECTION_CHARS;
    compact_value(value, &mut budget, 0)
}

fn compact_value(value: &Value, budget: &mut usize, depth: usize) -> Value {
    if *budget == 0 || depth >= 7 {
        return Value::Null;
    }
    match value {
        Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| field_priority(key));
            let mut output = Map::new();
            for (key, child) in entries {
                if is_excluded_index_field(key) || *budget <= key.len() {
                    continue;
                }
                *budget -= key.len();
                let compacted = compact_value(child, budget, depth + 1);
                if !compacted.is_null() {
                    output.insert(key.clone(), compacted);
                }
                if *budget == 0 {
                    break;
                }
            }
            Value::Object(output)
        }
        Value::Array(items) => {
            let mut output = Vec::new();
            for item in items.iter().take(16) {
                if *budget == 0 {
                    break;
                }
                output.push(compact_value(item, budget, depth + 1));
            }
            Value::Array(output)
        }
        Value::String(text) => {
            let result = truncate_chars(text.clone(), (*budget).min(1_200));
            *budget = budget.saturating_sub(result.len());
            Value::String(result)
        }
        Value::Number(_) | Value::Bool(_) => {
            *budget = budget.saturating_sub(16);
            value.clone()
        }
        Value::Null => Value::Null,
    }
}

fn field_priority(key: &str) -> u8 {
    match key.to_ascii_lowercase().as_str() {
        "summary" | "thesis" | "text" | "content" | "title" => 0,
        "action" | "outcome" | "status" | "state" | "instrument" | "instruments" => 1,
        "observedat" | "occurredat" | "source" | "sourceuri" | "provenanceuri" => 2,
        _ => 3,
    }
}

fn is_excluded_index_field(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "retrievaltrace"
            | "retrieval_trace"
            | "embedding"
            | "embeddings"
            | "vector"
            | "vectors"
            | "rawresponse"
            | "raw_response"
    )
}

/// Sanitize and split one source record before it is embedded. Point IDs are
/// stable so rebuilds and retries overwrite the same chunks instead of growing
/// duplicate vectors.
pub fn index_chunks(text: &str, base_id: &str) -> Vec<(String, String, usize, usize)> {
    let normalized = normalize_index_text(text);
    let chunks = split_text(&normalized);
    let total = chunks.len().max(1);
    chunks
        .into_iter()
        .enumerate()
        .map(|(index, chunk)| (stable_point_id(base_id, index), chunk, index + 1, total))
        .collect()
}

fn normalize_index_text(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
            return serde_json::to_string(&compact_index_value(&value))
                .unwrap_or_else(|_| truncate_chars(trimmed.to_owned(), MAX_INDEX_CHUNK_CHARS));
        }
    }
    trimmed.to_owned()
}

fn split_text(text: &str) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let chars = text.chars().collect::<Vec<_>>();
    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < chars.len() {
        let hard_end = (start + MAX_INDEX_CHUNK_CHARS).min(chars.len());
        let mut end = hard_end;
        if hard_end < chars.len() {
            if let Some(boundary) = chars[start..hard_end]
                .iter()
                .rposition(|character| character.is_whitespace())
            {
                if boundary > MAX_INDEX_CHUNK_CHARS / 2 {
                    end = start + boundary;
                }
            }
        }
        let chunk = chars[start..end]
            .iter()
            .collect::<String>()
            .trim()
            .to_owned();
        if !chunk.is_empty() {
            chunks.push(chunk);
        }
        if end >= chars.len() {
            break;
        }
        start = end.saturating_sub(INDEX_CHUNK_OVERLAP_CHARS);
    }
    chunks
}

fn stable_point_id(base_id: &str, chunk_index: usize) -> String {
    let digest = Sha256::digest(format!("evidence-index-v2:{base_id}:{chunk_index}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes).to_string()
}
