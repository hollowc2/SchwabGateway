# Helios production deployment runbook

This procedure deploys the standalone SchwabGateway on `billy@helios`. A rebuild,
recreate, restart, monitoring change, or cutover is a production change. Complete the
read-only preflight and the rollback record, then stop for Corey's explicit approval of
the named release and actions. Approval to prepare or inspect is not approval to deploy.

Never print `.env`, resolved Compose environment, token contents, application secrets,
raw internal API keys, or secret-bearing `docker inspect` output. The internal keys file
contains digests only, must remain mode `0600`, and is reused in place. Reuse the existing
token directory and do not move, replace, copy, or display `tokens.json`.

## Deployment topology

The required topology is:

| Role | Compose project and files | Container | Endpoint / alias | Expected state |
|---|---|---|---|---|
| Production | `schwab_gateway`; `compose.yml` + `compose.production.yml` | `schwab_gateway_live` | `127.0.0.1:8011`; `schwab-gateway` | healthy and scraped by Prometheus |
| Emergency legacy | `butterfly_gateway_foundation` | `butterfly_schwab_gateway_live` | legacy internal target | stopped but preserved |

There is exactly one live gateway. `schwab_gateway_candidate` must be absent, port 8012
must remain unused, and `compose.candidate.yml` must not be invoked. Validate the release
offline, build one immutable image, and recreate only the production `live` service after
approval. Keep the stopped legacy rollback container and both production image baselines
intact through the stability window. Never copy or reset the token or add another writer.

The 2026-08-22 audit also found the Helios checkout detached at a release tag with the
production overlays untracked; the server's production overlay also still
selects a mutable version tag. Treat a dirty checkout, a mutable production image, or a
checkout not at the intended exact release as a failed gate even when the files happen to
render correctly. A detached checkout at the intended exact tag is acceptable. Do not
delete or overwrite the server files. First compare them with the committed release,
then obtain approval for the exact fetch/checkout/staging action and rerun preflight.
Staging code must not recreate or restart the gateway.

## Immutable release contract

Production always uses both Compose files in this order:

```text
compose.yml
compose.production.yml
```

`compose.production.yml` removes the build definition, sets `pull_policy: never`, and
requires `SCHWAB_GATEWAY_PRODUCTION_IMAGE`. Set that variable to one of:

- an exact local Docker image ID, `sha256:<64 hexadecimal characters>`; or
- an immutable registry reference, `repository@sha256:<64 hexadecimal characters>`.

A release tag alone, including a version tag, is not an immutable deployment identity.
Record the human-readable release tag and Git SHA for traceability, but deploy and verify
the image ID/digest. Never run `docker compose build`, use `--build`, pull a reusable tag,
or create a candidate during the cutover.

## Read-only preflight and rollback record

Run from the intended local release checkout. Replace the example image value with the
exact locally built and reviewed ID/digest; it is not a secret:

```bash
sg_record="/tmp/schwab-gateway-preflight-$(date -u +%Y%m%dT%H%M%SZ).txt"
SCHWAB_GATEWAY_PRODUCTION_IMAGE='sha256:<exact-image-id>' \
  scripts/helios_preflight.sh --phase predeploy --record "$sg_record"
```

The script defaults to `billy@helios`, `/opt/schwab-gateway`, `compose.yml` plus
`compose.production.yml`, the production container, and the production health,
readiness, network, secret-metadata, disk, and Prometheus expectations. In `predeploy`
phase it records the current live baseline, requires the intended immutable image to
resolve locally, and requires the retired candidate identity to be absent. In
`postdeploy` phase it instead requires production to run the intended image. It prints only
step-labelled `CHECK`, `PASS`, or `FAIL` results, followed by a sanitized rollback
record. Exit `0` is required. Exit `1` means a failed/transport gate; exit `2` means bad
arguments. The record is local and mode `0600`; attach it to the private change record,
not Git.

Use `--host`, `--repo`, repeated `--compose-file`, or the other documented flags only
when the approved target intentionally differs from the production defaults. A green
preflight is evidence, not deployment approval. Resolve every failure and rerun from the
beginning.

In addition to the generated record, ensure the private change record has all of these
exact baseline fields:

```text
observed_at_utc=
operator=
host=billy@helios
repo=/opt/schwab-gateway
repo_git_sha=
repo_git_ref=
repo_worktree_state=
compose_project=schwab_gateway
compose_files=compose.yml,compose.production.yml
compose_service=live
production_container=schwab_gateway_live
production_configured_image=
production_image_id_or_digest=
production_state_and_health=
production_port_and_network_alias=
retired_candidate_container=schwab_gateway_candidate
retired_candidate_absent=true
legacy_container=butterfly_schwab_gateway_live
legacy_configured_image=
legacy_image_id_or_digest=
legacy_state_and_health=
keys_path_owner_group_mode=
token_path_owner_group_mode_inode_mtime=
prometheus_config_path_owner_group_mode_inode_sha256=
prometheus_current_target_and_health=
prometheus_backup_path_and_sha256=
intended_git_sha_and_release_tag=
intended_image_reference=
intended_resolved_image_id=
exact_deploy_command=
exact_primary_rollback_command=
exact_legacy_fallback_command=
```

Record metadata only for key and token files. Do not hash or display the token or any
secret. Before approval, verify the intended image exists locally on Helios and record
its exact resolved image ID.

## Mandatory approval stop

**STOP. Do not continue below until Corey explicitly approves the named host, production
service, intended Git release, exact image ID/digest, production recreate, any Prometheus
change, validation plan, and both rollback paths.**

The approval request must summarize:

- preflight result and record location;
- current production and legacy states, immutable image identities, and proof that the
  retired candidate identity is absent;
- intended Git SHA/release and exact image ID/digest;
- whether Schwab authorization or token readiness needs operator action;
- expected impact and confirmation that order writes remain disabled;
- validation sequence, Prometheus action (normally none), and rollback commands.

If the intended image, Compose files, target, or scope changes after approval, stop and
obtain new approval.

## Approved production activation

After approval, connect to Helios and set the approved immutable reference exactly as
recorded. Run each assertion as a separate command so the failing gate is unambiguous:

```bash
ssh -F /dev/null -o BatchMode=yes billy@helios
cd /opt/schwab-gateway
sg_production_image='sha256:<approved-exact-image-id>'

printf 'CHECK immutable-image-exists\n'
docker image inspect "$sg_production_image" --format '{{.Id}}'
printf 'PASS immutable-image-exists\n'

printf 'CHECK compose-render\n'
SCHWAB_GATEWAY_PRODUCTION_IMAGE="$sg_production_image" \
  docker compose --project-name schwab_gateway \
  -f compose.yml -f compose.production.yml --profile live config --quiet
printf 'PASS compose-render\n'

printf 'CHECK activate-production\n'
SCHWAB_GATEWAY_PRODUCTION_IMAGE="$sg_production_image" \
  docker compose --project-name schwab_gateway \
  -f compose.yml -f compose.production.yml --profile live \
  up --detach --no-build --no-deps live
printf 'PASS activate-production\n'
```

Do not use `down`, remove containers/images/volumes, or create a candidate. If the
activation command fails or any required validation fails, follow
[the rollback runbook](rollback.md) before debugging further unless Corey directs
otherwise.

## Validation order

Validate in this order. Prefix every assertion with `CHECK <name>` and emit
`PASS <name>` only after it succeeds. Do not combine shell assignments: a missing newline
between the port and alias assignments caused the 2026-08-15 retry validator—not the
gateway—to trigger rollback.

1. Confirm the production container is running and Docker reports `healthy`.
2. Confirm its resolved image ID equals the approved image ID; a matching tag is not
   sufficient.
3. Confirm UID/GID, read-only root, all capabilities dropped,
   `no-new-privileges`, `unless-stopped`, order writes disabled, and only
   `127.0.0.1:8011` published.
4. Confirm membership in `monitoring_net` with alias `schwab-gateway`; confirm no
   `schwab_gateway_candidate` exists and nothing is bound to port 8012.
5. Review bounded startup logs for errors without printing environment or credentials.
6. Require `GET http://127.0.0.1:8011/health` to return `200`, then `/ready` to return
   `200` with token state `ready`, then `/metrics` to return `200`.
7. Require an unauthenticated bounded market-data request to return `401`. Run the
   approved authenticated synthetic quote/spot checks through the existing consumer
   secret injection, recording status/schema assertions only—never the key or full
   response body.
8. Require the Prometheus targets API to report job `schwab_gateway`, scrape URL
   `http://schwab-gateway:8011/metrics`, and health `up`.
9. Confirm there is exactly one production gateway process and exactly one SPX, NDX, and
   XSP ButterflyGuy process; the stopped legacy container/image remains preserved.
10. Only if the approval explicitly includes an intentional crash-recovery test, perform
    it and repeat steps 1–9. A deliberate stop/restart is another live change.

From the intended local release checkout, run the post-deployment phase with the same
approved image as an acceptance gate; keep its sanitized record with the change evidence:

```bash
sg_post_record="/tmp/schwab-gateway-postdeploy-$(date -u +%Y%m%dT%H%M%SZ).txt"
SCHWAB_GATEWAY_PRODUCTION_IMAGE='sha256:<approved-exact-image-id>' \
  scripts/helios_preflight.sh --phase postdeploy --record "$sg_post_record"
```

Example port and alias gates:

```bash
phase6_ports=$(docker inspect schwab_gateway_live \
  --format '{{json .NetworkSettings.Ports}}')
phase6_aliases=$(docker inspect schwab_gateway_live \
  --format '{{json .NetworkSettings.Networks}}' | \
  jq -r '.monitoring_net.Aliases | join(",")')

printf 'CHECK port-publication\n'
test "$phase6_ports" = \
  '{"8011/tcp":[{"HostIp":"127.0.0.1","HostPort":"8011"}]}'
printf 'PASS port-publication\n'

printf 'CHECK network-alias\n'
printf '%s\n' "$phase6_aliases" | tr ',' '\n' | rg -q '^schwab-gateway$'
printf 'PASS network-alias\n'
```

If rollback runs, the last `CHECK` without a corresponding `PASS` identifies the failed
gate. Never print response bodies, credentials, resolved Compose environments, or
secret-bearing inspection output.

## Prometheus configuration changes

The normal production activation needs no Prometheus edit when the existing target is
already `schwab-gateway:8011`. Verify it and leave the file untouched.

If the approved change requires a target update, remember that
`/opt/monitoring/prometheus.yml` is a single-file bind mount at
`/etc/prometheus/prometheus.yml`. Never update the host file with `sed -i`, `mv`, or any
rename-based replacement: Docker remains attached to the old inode and a successful hot
reload can silently reload stale configuration.

Before changing it, create and record a uniquely named backup, including its SHA-256,
mode, owner, and inode. Render the intended content into a temporary file; validate that
only the approved target changes. Then copy bytes into the existing host and container
paths in place:

```bash
sg_change_id='<approved-change-id>'
sg_prometheus_old_target='<recorded-current-target>'
sg_prometheus_new_target='<approved-new-target>'
sg_prometheus_backup="/opt/monitoring/prometheus.yml.pre-${sg_change_id}"
sg_prometheus_next=$(mktemp)
trap 'rm -f "$sg_prometheus_next"' EXIT

cp --preserve=mode,ownership,timestamps \
  /opt/monitoring/prometheus.yml "$sg_prometheus_backup"
sha256sum "$sg_prometheus_backup"
stat -c '%u:%g %a %i %n' "$sg_prometheus_backup"

test "$(rg -F -c -- "$sg_prometheus_old_target" \
  /opt/monitoring/prometheus.yml)" -eq 1
if rg -F -q -- "$sg_prometheus_new_target" \
  /opt/monitoring/prometheus.yml; then exit 1; fi
sed "s|$sg_prometheus_old_target|$sg_prometheus_new_target|" \
  /opt/monitoring/prometheus.yml > "$sg_prometheus_next"
test "$(rg -F -c -- "$sg_prometheus_new_target" \
  "$sg_prometheus_next")" -eq 1
if rg -F -q -- "$sg_prometheus_old_target" "$sg_prometheus_next"; then exit 1; fi
docker exec -i butterfly_prometheus promtool check config /dev/stdin \
  < "$sg_prometheus_next"

cp "$sg_prometheus_next" /opt/monitoring/prometheus.yml
docker exec --user 1001:1001 -i butterfly_prometheus sh -c \
  'cat > /etc/prometheus/prometheus.yml' < "$sg_prometheus_next"
docker exec butterfly_prometheus promtool check config \
  /etc/prometheus/prometheus.yml
sg_prometheus_host_sha=$(sha256sum /opt/monitoring/prometheus.yml | awk '{print $1}')
sg_prometheus_mount_sha=$(docker exec butterfly_prometheus sha256sum \
  /etc/prometheus/prometheus.yml | awk '{print $1}')
test "$sg_prometheus_host_sha" = "$sg_prometheus_mount_sha"
curl --fail --request POST http://127.0.0.1:9090/-/reload
```

The host config is currently owned by `1001:1001`; Prometheus runs as `65534:65534`.
Verify those facts during preflight rather than assuming they are permanent. Stop and
adjust the approved procedure if the owner has changed. The one-shot writer uses the
observed file owner without changing the running service user or file permissions.
Before reload, require the host and mounted-container views to have the intended target
and matching SHA-256 hashes. After reload, require the exact target to be `up`. If any
check fails, restore the recorded backup in place using the rollback runbook.

## Stability window

After successful validation, record the deployed Git SHA, configured immutable image
reference, resolved image ID, validation results, and any Prometheus backup. Keep the
previous production image, legacy image/container, and
monitoring backup until Corey closes the stability window. Removal or cleanup requires a
separate explicit approval.
