# Helios runbook

Do not deploy without explicit operator approval. Before deployment, record the current
container/image, checkout tag, monitoring target, token-file metadata (never contents),
and rollback command. Verify that the digest-only keys file is mode `0600` and reuse the
existing token directory without moving or rewriting its document.

Render first with `docker compose --profile live config --quiet`. Deploy the immutable
release tag, then validate container health, `/health`, `/ready`, authenticated synthetic
smoke requests, `/metrics`, bounded/redacted logs, network membership, restart policy,
and crash recovery. Never print Compose environment or inspect secret-bearing values.
