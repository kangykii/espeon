# Espeon

Espeon is a Windows desktop workbench for supervised Jev trading experiments. Its Tauri frontend displays the Rust harness's canonical runs, loops, positions, evidence, and replay.

## Development

Install Node.js, Rust, and the Windows Tauri build prerequisites. Run `npm ci` and `npm run tauri:dev` from the repository root. The dev command starts Vite through `src-tauri/tauri.dev.conf.json`. A normal release build embeds the frontend and does not need a localhost server.

Local connector secrets live in the ignored `.env` file; adapter defaults live in `config/harness.json`. Runtime data in `runtime-harness/` is ignored. A fresh installed app creates a simulated local configuration under `%LOCALAPPDATA%\Espeon` when the source tree is absent. The Qdrant retrieval bridge requires a local Python environment and its dependencies.

## Releases and updates

Install Espeon from the signed NSIS installer in the private GitHub release. Espeon checks for newer signed releases at launch and every six hours. Private release access uses an authenticated GitHub CLI or a read-only GitHub token entered in settings. Updates wait until active experiments stop. See [private updates](docs/UPDATES.md) for publication and signing details.

The repository also contains earlier cTrader bot and research materials. Their design documents are [DESIGN.md](DESIGN.md), [SCHEMAS.md](SCHEMAS.md), and [VALIDATION.md](VALIDATION.md).
