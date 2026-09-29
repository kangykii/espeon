use crate::harness::HarnessController;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
        Self {
            state: state.into(),
            message: message.into(),
            version,
        }
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
    fn drop(&mut self) {
        CHECKING.store(false, Ordering::Release);
    }
}

fn github_token(project_root: &Path) -> Option<String> {
    let from_env = std::env::var("ESPEON_GITHUB_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let from_file = crate::config::env_file_values(project_root)
        .ok()
        .and_then(|values| values.get("ESPEON_GITHUB_TOKEN").cloned())
        .filter(|value| !value.is_empty());
    if let Some(token) = from_env.or(from_file) {
        return Some(token);
    }

    let mut command = Command::new("gh");
    command.args(["auth", "token"]);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000);
    }
    command
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

pub async fn check(
    app: AppHandle,
    project_root: &Path,
    controller: Arc<Mutex<HarnessController>>,
    installing: Arc<AtomicBool>,
) -> UpdateStatus {
    if CHECKING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return UpdateStatus::new("checking", "An update check is already running.", None);
    }
    let _guard = CheckGuard;
    let token = github_token(project_root);
    match check_authenticated(app, token.as_deref(), controller, installing).await {
        Ok(status) => status,
        Err(error) => UpdateStatus::new("error", error, None),
    }
}

async fn check_authenticated(
    app: AppHandle,
    token: Option<&str>,
    controller: Arc<Mutex<HarnessController>>,
    installing: Arc<AtomicBool>,
) -> Result<UpdateStatus, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| format!("Could not prepare update check: {error}"))?;
    let mut release_request = client
        .get(RELEASE_URL)
        .header("User-Agent", "Espeon-Updater")
        .header("Accept", "application/vnd.github+json");
    if let Some(token) = token {
        release_request = release_request.bearer_auth(token);
    }
    let response = release_request
        .send()
        .await
        .map_err(|error| format!("Could not reach GitHub releases: {error}"))?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(UpdateStatus::new(
            "authRequired",
            "No public Espeon release is available yet.",
            None,
        ));
    }
    if response.status() == reqwest::StatusCode::UNAUTHORIZED
        || response.status() == reqwest::StatusCode::FORBIDDEN
    {
        return Ok(UpdateStatus::new(
            "authRequired",
            "GitHub rejected the optional release token. Remove or replace it in settings.",
            None,
        ));
    }
    let response = response
        .error_for_status()
        .map_err(|error| format!("Release check failed: {error}"))?;
    let release: Release = response
        .json()
        .await
        .map_err(|error| format!("Invalid release metadata: {error}"))?;
    let manifest = release
        .assets
        .iter()
        .find(|asset| asset.name == "latest.json")
        .ok_or_else(|| "The latest release has no signed update manifest.".to_owned())?;
    let endpoint = manifest
        .url
        .parse()
        .map_err(|error| format!("Invalid update URL: {error}"))?;
    let mut updater_builder = app.updater_builder();
    if let Some(token) = token {
        updater_builder = updater_builder
            .header("Authorization", format!("Bearer {token}"))
            .map_err(|error| format!("Could not set updater authentication: {error}"))?;
    }
    let update = updater_builder
        .header("Accept", "application/octet-stream")
        .map_err(|error| format!("Could not set updater asset format: {error}"))?
        .endpoints(vec![endpoint])
        .map_err(|error| format!("Invalid updater endpoint: {error}"))?
        .build()
        .map_err(|error| format!("Could not prepare updater: {error}"))?
        .check()
        .await
        .map_err(|error| format!("Signed update check failed: {error}"))?;
    let Some(update) = update else {
        return Ok(UpdateStatus::new("current", "Espeon is up to date.", None));
    };
    let version = update.version.clone();
    if controller.lock().has_active_runs() {
        return Ok(UpdateStatus::new(
            "waiting",
            "Update ready; installation waits until active experiments stop.",
            Some(version),
        ));
    }
    let package = update
        .download(|_, _| {}, || {})
        .await
        .map_err(|error| format!("Update download or signature verification failed: {error}"))?;
    installing.store(true, Ordering::SeqCst);
    if controller.lock().has_active_runs() {
        installing.store(false, Ordering::SeqCst);
        return Ok(UpdateStatus::new(
            "waiting",
            "Update ready; installation waits until active experiments stop.",
            Some(version),
        ));
    }
    if let Err(error) = update.install(package) {
        installing.store(false, Ordering::SeqCst);
        return Err(format!("Update installation failed: {error}"));
    }
    app.restart();
}
