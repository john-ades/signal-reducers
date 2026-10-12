# Security policy

This alpha is intended for a developer running synthetic inputs on a local machine. There is no production support or security-response SLA. Only the current alpha receives fixes; other platforms and deployment arrangements remain unqualified.

## Trust and data boundaries

Redis must remain bound to loopback. Signal source names are not authentication, and the CLI does not provide multi-user authorization. Signals and captured observations sent to reducers, DREX, and System 2 leave the machine through OpenRouter. Use synthetic data and review provider terms before sending other data.

Only validated System 1 decisions can change acknowledgement/resolution or reserve work. Model output and generated summaries remain untrusted: valid JSON and evidence IDs do not establish factual accuracy. Transport is best effort and unsuitable for guaranteed processing or durable recovery.

Keep credentials in the environment or ignored `.env` files. Generated reports can contain captured facts and text; do not upload them without review. Dependencies and model weights are installed separately and keep their own licenses and terms. See [third-party notices](THIRD_PARTY_NOTICES.md).

## Reporting

Once a public repository is available, use its private vulnerability-reporting feature if enabled. If no private channel is available, open a public issue containing only a high-level request for a private contact; do not attach credentials, sensitive data, or exploit details. No dedicated security address is configured for this initial release preparation.

Include the version, relevant dependency versions, a synthetic reproduction, expected behavior, and the trust boundary affected. If a credential is exposed, revoke it through the provider and remove it from shared artifacts.
