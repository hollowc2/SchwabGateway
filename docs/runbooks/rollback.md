# Rollback runbook

Keep the legacy gateway container and image intact through the stability window. A failed
standalone cutover is restored by stopping the standalone container, starting the
preserved legacy container with `docker start butterfly_schwab_gateway_live`, restoring
Prometheus's prior target, validating its configuration, and rechecking health/readiness.

Record exact image IDs and monitoring state before either action. Do not rebuild the
legacy image during rollback and do not remove either container or image without explicit
approval.

The Prometheus configuration is a single-file bind mount. Restore it in place in both the
host and running-container views; do not use `sed -i` or `mv`. The one-shot container write
runs as the config file owner `1001:1001`; Prometheus itself remains `65534:65534`:

```bash
cp /opt/monitoring/prometheus.yml.phase6-precutover /opt/monitoring/prometheus.yml
docker exec --user 1001:1001 -i butterfly_prometheus sh -c \
  'cat > /etc/prometheus/prometheus.yml' \
  < /opt/monitoring/prometheus.yml.phase6-precutover
docker exec butterfly_prometheus promtool check config /etc/prometheus/prometheus.yml
curl --fail --request POST http://127.0.0.1:9090/-/reload
```

Require the targets API to report `butterfly_schwab_gateway_live:8011` as `up` before
declaring rollback complete.
