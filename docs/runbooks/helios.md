# Helios runbook

Do not deploy without explicit operator approval. Before deployment, record the current
container/image, checkout tag, monitoring target, token-file metadata (never contents),
and rollback command. Verify that the digest-only keys file is mode `0600` and reuse the
existing token directory without moving or rewriting its document.

Render first with `docker compose --profile live config --quiet`. Deploy the immutable
release tag, then validate container health, `/health`, `/ready`, authenticated synthetic
smoke requests, `/metrics`, bounded/redacted logs, network membership, restart policy,
and crash recovery. Never print Compose environment or inspect secret-bearing values.

## Prometheus configuration updates

The live Prometheus configuration is a single-file Docker bind mount from
`/opt/monitoring/prometheus.yml` to `/etc/prometheus/prometheus.yml`. Never update the host
file with `sed -i`, `mv`, or another rename-based replacement: Docker remains attached to
the old inode and a successful hot reload can silently reload stale configuration.

Render the intended configuration into a temporary file, verify the exact target change,
then copy its bytes into both the persistent host path and the container's mounted path.
Both copies must be made in place before validation and reload:

```bash
phase6_prometheus_next=$(mktemp)
trap 'rm -f "$phase6_prometheus_next"' EXIT
sed 's/butterfly_schwab_gateway_live:8011/schwab-gateway:8011/' \
  /opt/monitoring/prometheus.yml > "$phase6_prometheus_next"
test "$(rg -c 'schwab-gateway:8011' "$phase6_prometheus_next")" -eq 1
if rg -q 'butterfly_schwab_gateway_live:8011' "$phase6_prometheus_next"; then exit 1; fi
cp "$phase6_prometheus_next" /opt/monitoring/prometheus.yml
docker exec --user 1001:1001 -i butterfly_prometheus sh -c \
  'cat > /etc/prometheus/prometheus.yml' < "$phase6_prometheus_next"
docker exec butterfly_prometheus promtool check config /etc/prometheus/prometheus.yml
curl --fail --request POST http://127.0.0.1:9090/-/reload
```

Prometheus runs as `65534:65534`, while the host config is owned by `1001:1001`; the
one-shot writer therefore uses the file owner's identity without changing the running
service user or file permissions. Arm the documented automatic rollback before copying
either file. Before reloading, confirm the host and container views both name the intended
target. After reloading, require the Prometheus targets API to report that exact target as
`up`. The configuration contains no gateway keys or Schwab credentials, but it still must
not be printed as resolved Compose output.
