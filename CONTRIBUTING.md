# Contributing to Espeon

Thanks for helping improve Espeon. Until an open-source license is selected and added, please use Issues and Discussions for feedback; do not submit code pull requests. Once the license is in place, start with an Issue or Discussion for larger changes so the expected behavior can be agreed before implementation.

## Local development

1. Install Windows, Node.js 22, Rust stable, and the Tauri prerequisites listed in the README.
2. Run `npm ci` and `npm run tauri:dev`.
3. Keep credentials in an ignored `.env` file. Use simulated or cTrader Demo accounts for development.
4. Before opening a pull request, run `npm run build`, `cargo check --manifest-path src-tauri/Cargo.toml --release`, and the relevant `cargo test --manifest-path src-tauri/Cargo.toml` checks.

## Pull requests (after a license is added)

- Explain the user-visible behavior and any broker/API assumptions.
- Include a regression check for bug fixes and screenshots for visible UI changes.
- Keep secrets, account identifiers, personal trade records, runtime databases, build outputs, and generated local configuration out of commits.
- Do not change live-order behavior or risk sizing without describing the impact and adding relevant coverage.
- Keep changes focused; disclose any known limitation instead of hiding it behind a fallback.

## License

The repository's open-source license has not been selected yet. Until a `LICENSE` file is added, code reuse, redistribution, and external code contributions are not permitted. The maintainers will update this guide when the license is in place.
