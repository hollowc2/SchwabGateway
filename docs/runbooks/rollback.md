# Helios production rollback runbook

Use this runbook when an approved standalone SchwabGateway activation fails or behaves
incorrectly. The preflight record from [the deployment runbook](helios.md) is the source
of truth. Do not infer a baseline from a tag, rebuild an old release, delete a container,
or remove an image or volume during rollback.

Rollback is part of the approved deployment only when the approval named the exact
production service and these restore paths. Stop and get approval if the target or
rollback scope differs. Unless Corey directs otherwise, restore the recorded baseline
before investigating a failed live release.

## Required baseline

Do not start deployment without a private, sanitized rollback record containing:

- host, repository, Git SHA/ref, Compose project/files/service, and timestamp;
- previous production configured image and exact resolved image ID/digest;
- candidate container, exact image ID/digest, state, port, and alias;
- legacy container, configured image, exact image ID/digest, and state;
- production/candidate/legacy container identities and health states;
- key-file and token-file metadata only, never contents or hashes of secrets/tokens;
- Prometheus config path, owner/group/mode/inode/SHA-256, current target and health;
- a unique Prometheus backup path and SHA-256 if monitoring will change; and
- exact primary rollback and emergency legacy-fallback commands.

Keep the previous standalone image and the legacy container/image intact through the
stability window. Confirm each recorded image still exists before activation.

## When to roll back

Roll back immediately if the production container cannot become healthy, the running
image differs from the approved ID, readiness or authenticated contracts fail,
credentials appear in logs, the token becomes corrupt/unavailable, Prometheus cannot
scrape the exact production target, network/port/security assertions fail, or the service
shows incorrect behavior. Do not keep recreating or debugging the bad release in place.

## Path A: restore the previous standalone image

This is the primary rollback when the prior `schwab_gateway_live` deployment was healthy.
Use the exact previous image ID/digest from the record, not its mutable tag:

```bash
ssh -F /dev/null -o BatchMode=yes billy@helios
cd /opt/schwab-gateway
sg_previous_image='sha256:<recorded-previous-image-id>'

printf 'CHECK rollback-image-exists\n'
docker image inspect "$sg_previous_image" --format '{{.Id}}'
printf 'PASS rollback-image-exists\n'

printf 'CHECK rollback-compose-render\n'
SCHWAB_GATEWAY_PRODUCTION_IMAGE="$sg_previous_image" \
  docker compose --project-name schwab_gateway \
  -f compose.yml -f compose.production.yml --profile live config --quiet
printf 'PASS rollback-compose-render\n'

printf 'CHECK restore-previous-production\n'
SCHWAB_GATEWAY_PRODUCTION_IMAGE="$sg_previous_image" \
  docker compose --project-name schwab_gateway \
  -f compose.yml -f compose.production.yml --profile live \
  up --detach --no-build --no-deps --force-recreate live
printf 'PASS restore-previous-production\n'
```

Do not rebuild or pull during rollback. Restore the exact prior Git checkout only if the
recorded Compose files are incompatible with the previous image; use the recorded commit
and an approved, non-destructive checkout procedure. Never use `git reset --hard` or
discard untracked server files.

If monitoring was not changed and still targets `schwab-gateway:8011`, leave it alone.
Validate Path A in the order below. If Path A cannot restore service, use Path B.

## Path B: restore the preserved legacy gateway

This is the emergency fallback to `butterfly_schwab_gateway_live`. Confirm its exact
recorded image ID is still present and its state matches the baseline. Then stop the
standalone production container before starting legacy so two production identities are
not active during target restoration:

```bash
printf 'CHECK legacy-image-exists\n'
test "$(docker inspect butterfly_schwab_gateway_live --format '{{.Image}}')" = \
  'sha256:<recorded-legacy-image-id>'
printf 'PASS legacy-image-exists\n'

printf 'CHECK stop-failed-standalone\n'
docker stop schwab_gateway_live
printf 'PASS stop-failed-standalone\n'

printf 'CHECK start-preserved-legacy\n'
docker start butterfly_schwab_gateway_live
printf 'PASS start-preserved-legacy\n'
```

Do not rebuild, recreate, rename, or remove the legacy container. Do not alter the
candidate unless its baseline/approved rollback procedure specifically requires it.

Restore Prometheus to the exact legacy target saved in the rollback record. The historic
target was `butterfly_schwab_gateway_live:8011`, but use the record rather than assuming
that value.

## Restore Prometheus in place

Skip this section when monitoring never changed. The config is a single-file bind mount;
restore both the host and running-container views in place. Do not use `sed -i`, `mv`, or
another rename-based replacement, because Docker would remain attached to the old inode.

Set `sg_prometheus_backup` to the unique path and verify its recorded SHA-256 before any
copy:

```bash
sg_prometheus_backup='/opt/monitoring/prometheus.yml.pre-<recorded-change-id>'

printf 'CHECK prometheus-backup\n'
test "$(sha256sum "$sg_prometheus_backup" | awk '{print $1}')" = \
  '<recorded-backup-sha256>'
printf 'PASS prometheus-backup\n'

printf 'CHECK restore-prometheus-host-view\n'
cp "$sg_prometheus_backup" /opt/monitoring/prometheus.yml
printf 'PASS restore-prometheus-host-view\n'

printf 'CHECK restore-prometheus-container-view\n'
docker exec --user 1001:1001 -i butterfly_prometheus sh -c \
  'cat > /etc/prometheus/prometheus.yml' < "$sg_prometheus_backup"
printf 'PASS restore-prometheus-container-view\n'

printf 'CHECK prometheus-config\n'
docker exec butterfly_prometheus promtool check config \
  /etc/prometheus/prometheus.yml
printf 'PASS prometheus-config\n'

printf 'CHECK prometheus-view-parity\n'
sg_prometheus_host_sha=$(sha256sum /opt/monitoring/prometheus.yml | awk '{print $1}')
sg_prometheus_mount_sha=$(docker exec butterfly_prometheus sha256sum \
  /etc/prometheus/prometheus.yml | awk '{print $1}')
test "$sg_prometheus_host_sha" = "$sg_prometheus_mount_sha"
printf 'PASS prometheus-view-parity\n'

printf 'CHECK prometheus-reload\n'
curl --fail --request POST http://127.0.0.1:9090/-/reload
printf 'PASS prometheus-reload\n'
```

The one-shot writer uses the config owner's observed UID/GID (currently `1001:1001`),
while Prometheus itself currently runs as `65534:65534`. Use values recorded by preflight
if either differs. Before reload, require host and container views to have matching
SHA-256 hashes and the recorded target. After reload, require the targets API to report
that exact target as `up`.

## Rollback validation order

For either restore path, validate in this order and stop at the first failure:

1. Restored container state and Docker health.
2. Exact running image ID/digest equality with the rollback record.
3. Bounded startup logs with no credential exposure or startup errors.
4. Expected port/network target, UID/GID, read-only root, capabilities, security options,
   restart policy, and order writes disabled.
5. `/health` returns `200`, `/ready` returns `200` with token state `ready`, and `/metrics`
   returns `200` from the restored gateway.
6. The bounded unauthenticated `401` contract and approved authenticated synthetic
   market-data contract succeed without logging keys or response bodies.
7. Prometheus reports the exact restored target `up`.
8. Exactly one production identity is active; candidate and legacy/standalone states
   match the selected rollback path and recorded plan.

Run each assertion as its own `CHECK` / `PASS` pair. The last `CHECK` without `PASS` is
the rollback failure point. Do not deliberately crash the restored service unless a new
approval explicitly authorizes that test.

## Completion record

Record the trigger, failed validation gate, rollback path, restored Git/image identity,
container and Prometheus results, token readiness, candidate/legacy states, start/end UTC
times, and any remaining risk. Preserve the failed and restored images, all containers,
the monitoring backup, and the sanitized preflight record until Corey closes the
stability window. Any cleanup requires separate approval.
