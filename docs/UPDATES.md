# Signed updates

Espeon checks the public `kangykii/espeon` GitHub repository at startup and every six hours. A newer signed installer is downloaded and installed only when no experiment is active. The **Check now** action is available in settings.

Public release checks do not require GitHub authentication. An optional read-only GitHub token can raise the API rate limit; it remains in the local ignored `.env` file and is not bundled in the app. The updater verifies each installer against the public signing key embedded in Espeon.

To publish a release, update the versions in `package.json`, `src-tauri/tauri.conf.json`, and `src-tauri/Cargo.toml`, commit the changes, and push a matching version tag such as `v0.1.4`. The Windows workflow builds a signed NSIS installer, creates the updater manifest after uploading the installer and signature, and then publishes the release. Keep the updater signing private key only in secure local storage and GitHub Actions secrets. Losing that key prevents existing installations from verifying future updates.

The standalone `target/release/espeon.exe` is a build artifact. Install a release installer to place Espeon in the app location the updater can replace. A fresh installation creates local configuration; the Qdrant retrieval bridge requires a local Python environment and its dependencies.
