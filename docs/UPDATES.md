# Private Espeon releases

Espeon checks the private `kangykii/espeon` GitHub repository when it starts and every six hours. A new signed release installs automatically when no experiment is active. While an experiment is active, Espeon waits and checks again after it stops. The app also has a **Check now** button in Connectors and settings.

Private releases require GitHub authentication on each computer. Sign in with GitHub CLI (`gh auth login`), or enter a fine-grained token with **Contents: read** access to `espeon` in the **Private updates** settings section. The token remains in the local ignored `.env` file and is never bundled into the app or sent to the interface. Tauri verifies each installer against the public signing key embedded in the app.

To publish a new release, update the version in `package.json` and `src-tauri/tauri.conf.json` and `src-tauri/Cargo.toml`, commit, then push a matching tag such as `v0.1.2`. The Windows workflow builds the signed NSIS installer, uploads the installer and signature, creates an authenticated `latest.json`, and publishes the release after all assets are ready. Keep the local updater signing key and GitHub Actions `TAURI_SIGNING_PRIVATE_KEY` secret safe. Losing it means existing installations cannot verify future updates.

The standalone `target/release/espeon.exe` is a build artifact. Install a release installer once to place Espeon in the installed app location that the updater can replace. A fresh installation creates its own local configuration; the full Qdrant retrieval bridge still requires local Python dependencies.
