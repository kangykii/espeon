# Security policy

Espeon connects to broker accounts and can submit live orders. Please do not report suspected vulnerabilities in public Issues or Discussions.

Use GitHub's **Report a vulnerability** action on the repository's Security tab to send a private report. Include affected versions, a concise impact description, and reproduction steps. Remove account identifiers, access tokens, API secrets, private logs, and personal trading data from any evidence before submission.

There is no guaranteed response time while the project is maintained by a small team. Please allow the maintainer an opportunity to investigate and coordinate a fix before public disclosure.

## Current automated-scan findings

- The Cargo lockfile includes `glib 0.18.5` through Tauri's Linux GTK dependencies. The current supported and released platform is Windows, and the CI and release workflows target Windows. This advisory remains relevant if Linux builds become supported; revisit it when Tauri's Linux dependency stack can use the fixed `glib 0.20` line.
- CodeQL reports filesystem path flows for the configured project/runtime directories, local Qdrant storage and model-cache directories, and the journal paths explicitly supplied to the offline research tool. These paths are selected by the local operator. They are not taken from broker, model, or other network responses. Reassess this boundary if any path becomes remotely controllable.
- The cTrader MCP URL is parsed and restricted to loopback HTTP endpoints with an explicit port, and the HTTP client does not follow redirects. The scanner may not infer this validation across the helper boundary; the URL handling is covered by a regression test.
