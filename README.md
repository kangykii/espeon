# Espeon Trader

Espeon is an experimental Windows desktop workbench for AI-assisted trading research and supervised cTrader experiments. Its Tauri UI is backed by a Rust harness that records decisions, evidence, broker context, orders, and run state in a canonical event store.

> **Trading risk:** Espeon can submit real orders when configured for a live cTrader account. AI output can be wrong, broker and market data can be delayed, and trading can lose money. Use a demo account while evaluating the software. This project is experimental and does not provide financial advice.

## What is here

- Tauri 2 desktop app with a TypeScript/Vite frontend and Rust orchestration layer.
- World-model and Jev integrations, tool/skill routing, evidence retrieval, and canonical activity replay.
- Hybrid cTrader connectivity: Local MCP for account context, Open API for account/symbol metadata and historical bars, and FIX for quotes and order execution.
- Twelve Data candle/stream support and local Qdrant retrieval bridge.
- Demo and simulated configurations for development; live-account setup is intentionally separate.

The app is under active development. See [architecture](ARCHITECTURE.md), [cTrader FIX notes](docs/ctrader-fix.md), and [current startup and mismatch investigations](STARTUP_BLOCKERS.md) for implementation context.

## Run locally

Requirements: Windows 10/11, Node.js 22, Rust stable, and the [Tauri Windows prerequisites](https://v2.tauri.app/start/prerequisites/).

```powershell
npm ci
npm run tauri:dev
```

The dev server runs on `127.0.0.1:1420`. A local `.env` is optional for simulated development and required for external providers. Start from `.env.example`; never commit `.env`, broker credentials, API tokens, or runtime data. Runtime state is stored outside the source tree in the Espeon app-data directory.

Useful checks:

```powershell
npm audit
npm run build
cargo check --manifest-path src-tauri/Cargo.toml --release
cargo test --manifest-path src-tauri/Cargo.toml
```

## Community and contributions

Bug reports and integration feedback are welcome. Please use GitHub Issues for reproducible defects and Discussions for setup/design questions. Code pull requests will be accepted after an open-source license is selected and added. Never include API keys, account numbers, access tokens, private broker logs, or personal trading records in public reports. See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

## Releases and updates

Windows installers are built and signed by GitHub Actions when a version tag is pushed. The app verifies update signatures before installation and waits while an experiment is active. Public signed releases do not need a GitHub token. See [release and signing details](docs/UPDATES.md); the updater signing private key belongs only in local secure storage and GitHub Actions secrets.

## License status

This repository does not yet contain an open-source license. It is public for review and community feedback, but a license must be selected before others can safely reuse, modify, or redistribute the code. See [GitHub's explanation of public repositories without a license](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/licensing-a-repository).
