use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tauri::AppHandle;
use tauri_plugin_updater::UpdaterExt;

const RELEASE_URL: &str = "https://api.github.com/repos/kangykii/espeon/releases/latest";
static CHECKING: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateStatus {
    pub state: String,
    pub message: String,
    pub version: Option<String>,
}

impl UpdateStatus {
    fn new(state: &str, message: impl Into<String>, version: Option<String>) -> Self {
        Self { state: state.into(), message: message.into(), version }
    }
}

#[derive(Deserialize)]
struct Release {
    assets: Vec<ReleaseAsset>,
}

#[derive(Deserialize)]
struct ReleaseAsset {
    name: String,
    url: String,
}

struct CheckGuard;

impl Drop for CheckGuard {
    fn drop(&mut self) { CHECKING.store(false, Ordering::Release); }
}

fn github_token(project_root: &Path) -> Option<String> {
    let from_env = std::env::var("ESPEON_GITHUB_TOKEN").ok();
    let from_file = std::fs::read_to_string(project_root.join(".env"))
        .ok()
        .and_then(|contents| contents.lines().filter_map(|line| line.split_once('='))
            .find(|(name, _)| name.trim() == "ESPEON_GITHUB_TOKEN")
            .map(|(_, value)| value.trim().to_owned()));
    if let Some(token) = from_env.or(from_file).filter(|value| !value.trim().is_empty()) {
        return Some(token);
    }

    let mut command = Command::new("gh");
    command.args(["auth", "token"]);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000);
    }
    command.output().ok().and_then(|output| {
        if output.status.success() { String::from_utf8(output.stdout).ok() } else { None }
    }).map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
}

pub async fn check(app: AppHandle, project_root: &Path, active_runs: bool) -> UpdateStatus {
    if CHECKING.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
        return UpdateStatus::new("checking", "An update check is already running.", None);
    }
    let _guard = CheckGuard;
    let Some(token) = github_token(project_root) else {
        return UpdateStatus::new("authRequired", "Connect GitHub in settings to receive private releases.", None);
    };
    match check_authenticated(app, &token, active_runs).await {
        Ok(status) => status,
        Err(error) => UpdateStatus::new("error", error, None),
    }
}

async fn check_authenticated(app: AppHandle, token: &str, active_runs: bool) -> Result<UpdateStatus, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| format!("Could not prepare update check: {error}"))?;
    let response = client.get(RELEASE_URL)
        .header("User-Agent", "Espeon-Updater")
        .header("Accept", "application/vnd.github+json")
        .bearer_auth(token)
        .send().await
        .map_err(|error| format!("Could not reach GitHub releases: {error}"))?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(UpdateStatus::new("current", "No published Espeon release is available yet.", None));
    }
    if response.status() == reqwest::StatusCode::UNAUTHORIZED || response.status() == reqwest::StatusCode::FORBIDDEN {
        return Ok(UpdateStatus::new("authRequired", "GitHub access expired. Update the read-only token in settings.", None));
    }
    let response = response.error_for_status().map_err(|error| format!("Release check failed: {error}"))?;
    let release: Release = response.json().await.map_err(|error| format!("Invalid release metadata: {error}"))?;
    let manifest = release.assets.iter().find(|asset| asset.name == "latest.json")
        .ok_or_else(|| "The latest release has no signed update manifest.".to_owned())?;
    let endpoint = manifest.url.parse().map_err(|error| format!("Invalid update URL: {error}"))?;
    let update = app.updater_builder()
        .header("Authorization", format!("Bearer {token}"))
        .map_err(|error| format!("Could not set updater authentication: {error}"))?
        .header("Accept", "application/octet-stream")
        .map_err(|error| format!("Could not set updater asset format: {error}"))?
        .endpoints(vec![endpoint]).map_err(|error| format!("Invalid updater endpoint: {error}"))?
        .build().map_err(|error| format!("Could not prepare updater: {error}"))?
        .check().await.map_err(|error| format!("Signed update check failed: {error}"))?;
    let Some(update) = update else {
        return Ok(UpdateStatus::new("current", "Espeon is up to date.", None));
    };
    let version = update.version.clone();
    if active_runs {
        return Ok(UpdateStatus::new("waiting", "Update ready; installation waits until active experiments stop.", Some(version)));
    }
    update.download_and_install(|_, _| {}, || {})
        .await.map_err(|error| format!("Update installation failed: {error}"))?;
    app.restart();
}
