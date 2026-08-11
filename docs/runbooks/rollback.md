# Rollback runbook

Keep the legacy gateway container and image intact through the stability window. A failed
standalone cutover is restored by stopping the standalone container, starting the
preserved legacy container with `docker start butterfly_schwab_gateway_live`, restoring
Prometheus's prior target, validating its configuration, and rechecking health/readiness.

Record exact image IDs and monitoring state before either action. Do not rebuild the
legacy image during rollback and do not remove either container or image without explicit
approval.
