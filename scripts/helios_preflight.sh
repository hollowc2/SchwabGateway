#!/usr/bin/env bash
# Read-only, secret-safe deployment preflight and rollback baseline for SchwabGateway.

set -uo pipefail
umask 077

usage() {
    cat <<'EOF'
Usage: scripts/helios_preflight.sh [options]

Runs a read-only audit on Helios through SSH. Use --local to run the same audit on
the current machine (primarily for tests). Failed gates exit 1; invalid usage exits 2.

Options:
  --host HOST                 SSH destination (default: billy@helios)
  --repo PATH                 Remote repository (default: /opt/schwab-gateway)
  --compose-file PATH         Compose file relative to --repo; repeatable
  --production-image IMAGE    Exact sha256 image ID or repository digest; defaults to
                              SCHWAB_GATEWAY_PRODUCTION_IMAGE
  --phase PHASE               predeploy or postdeploy (default: predeploy)
  --container NAME            Live container (default: schwab_gateway_live)
  --candidate-container NAME  Retired candidate identity that must be absent
                              (default: schwab_gateway_candidate)
  --legacy-container NAME     Preserved rollback container
                              (default: butterfly_schwab_gateway_live)
  --service NAME              Compose service (default: live)
  --expected-host NAME        Expected short hostname (default: helios)
  --network NAME              Expected Docker network (default: monitoring_net)
  --network-alias NAME        Required alias (default: schwab-gateway)
  --candidate-network-alias NAME
                              Retired compatibility option; no candidate is contacted
  --port PORT                 Required loopback host/container port (default: 8011)
  --candidate-port PORT       Retired compatibility option; port must remain unused
  --health-url URL            Health URL (default: http://127.0.0.1:8011/health)
  --ready-url URL             Readiness URL (default: http://127.0.0.1:8011/ready)
  --candidate-ready-url URL   Retired compatibility option; URL is never contacted
  --prometheus-url URL        Prometheus targets API
  --prometheus-target TARGET  Required active/up target (default: schwab-gateway:8011)
  --keys-path PATH            Digest-key file; otherwise derived from its container mount
  --token-path PATH           Token file; otherwise derived from its writable mount
  --expected-user UID:GID     Required container user (default: 1001:1001)
  --restart-policy POLICY     Required restart policy (default: unless-stopped)
  --min-disk-mb MB            Minimum repository-filesystem free space (default: 2048)
  --record PATH               Save sanitized output locally with mode 0600
  --local                     Do not use SSH
  -h, --help                  Show this help

When no --compose-file is supplied, compose.yml is used and
compose.production.yml is added if it exists.
EOF
}

die_usage() {
    printf 'ERROR %s\n' "$*" >&2
    usage >&2
    exit 2
}

host="${HELIOS_PREFLIGHT_HOST:-billy@helios}"
repo="${HELIOS_PREFLIGHT_REPO:-/opt/schwab-gateway}"
container="${HELIOS_PREFLIGHT_CONTAINER:-schwab_gateway_live}"
candidate_container="${HELIOS_PREFLIGHT_CANDIDATE_CONTAINER:-schwab_gateway_candidate}"
legacy_container="${HELIOS_PREFLIGHT_LEGACY_CONTAINER:-butterfly_schwab_gateway_live}"
service="${HELIOS_PREFLIGHT_SERVICE:-live}"
expected_host="${HELIOS_PREFLIGHT_EXPECTED_HOST:-helios}"
network="${HELIOS_PREFLIGHT_NETWORK:-monitoring_net}"
network_alias="${HELIOS_PREFLIGHT_NETWORK_ALIAS:-schwab-gateway}"
candidate_network_alias="${HELIOS_PREFLIGHT_CANDIDATE_NETWORK_ALIAS:-schwab-gateway-candidate}"
port="${HELIOS_PREFLIGHT_PORT:-8011}"
candidate_port="${HELIOS_PREFLIGHT_CANDIDATE_PORT:-8012}"
health_url="${HELIOS_PREFLIGHT_HEALTH_URL:-http://127.0.0.1:8011/health}"
ready_url="${HELIOS_PREFLIGHT_READY_URL:-http://127.0.0.1:8011/ready}"
candidate_ready_url="${HELIOS_PREFLIGHT_CANDIDATE_READY_URL:-http://127.0.0.1:8012/ready}"
prometheus_url="${HELIOS_PREFLIGHT_PROMETHEUS_URL:-http://127.0.0.1:9090/api/v1/targets?state=active}"
prometheus_target="${HELIOS_PREFLIGHT_PROMETHEUS_TARGET:-schwab-gateway:8011}"
production_image="${SCHWAB_GATEWAY_PRODUCTION_IMAGE:-}"
phase="${HELIOS_PREFLIGHT_PHASE:-predeploy}"
keys_path="${HELIOS_PREFLIGHT_KEYS_PATH:-}"
token_path="${HELIOS_PREFLIGHT_TOKEN_PATH:-}"
expected_user="${HELIOS_PREFLIGHT_EXPECTED_USER:-1001:1001}"
restart_policy="${HELIOS_PREFLIGHT_RESTART_POLICY:-unless-stopped}"
min_disk_mb="${HELIOS_PREFLIGHT_MIN_DISK_MB:-2048}"
record_path=""
local_mode=false
internal_audit=false
requested_host=""
compose_files=()

while (($#)); do
    case "$1" in
        --host) [[ $# -ge 2 ]] || die_usage "--host requires a value"; host=$2; shift 2 ;;
        --repo) [[ $# -ge 2 ]] || die_usage "--repo requires a value"; repo=$2; shift 2 ;;
        --compose-file) [[ $# -ge 2 ]] || die_usage "--compose-file requires a value"; compose_files+=("$2"); shift 2 ;;
        --production-image) [[ $# -ge 2 ]] || die_usage "--production-image requires a value"; production_image=$2; shift 2 ;;
        --phase) [[ $# -ge 2 ]] || die_usage "--phase requires a value"; phase=$2; shift 2 ;;
        --container) [[ $# -ge 2 ]] || die_usage "--container requires a value"; container=$2; shift 2 ;;
        --candidate-container) [[ $# -ge 2 ]] || die_usage "--candidate-container requires a value"; candidate_container=$2; shift 2 ;;
        --legacy-container) [[ $# -ge 2 ]] || die_usage "--legacy-container requires a value"; legacy_container=$2; shift 2 ;;
        --service) [[ $# -ge 2 ]] || die_usage "--service requires a value"; service=$2; shift 2 ;;
        --expected-host) [[ $# -ge 2 ]] || die_usage "--expected-host requires a value"; expected_host=$2; shift 2 ;;
        --network) [[ $# -ge 2 ]] || die_usage "--network requires a value"; network=$2; shift 2 ;;
        --network-alias) [[ $# -ge 2 ]] || die_usage "--network-alias requires a value"; network_alias=$2; shift 2 ;;
        --candidate-network-alias) [[ $# -ge 2 ]] || die_usage "--candidate-network-alias requires a value"; candidate_network_alias=$2; shift 2 ;;
        --port) [[ $# -ge 2 ]] || die_usage "--port requires a value"; port=$2; shift 2 ;;
        --candidate-port) [[ $# -ge 2 ]] || die_usage "--candidate-port requires a value"; candidate_port=$2; shift 2 ;;
        --health-url) [[ $# -ge 2 ]] || die_usage "--health-url requires a value"; health_url=$2; shift 2 ;;
        --ready-url) [[ $# -ge 2 ]] || die_usage "--ready-url requires a value"; ready_url=$2; shift 2 ;;
        --candidate-ready-url) [[ $# -ge 2 ]] || die_usage "--candidate-ready-url requires a value"; candidate_ready_url=$2; shift 2 ;;
        --prometheus-url) [[ $# -ge 2 ]] || die_usage "--prometheus-url requires a value"; prometheus_url=$2; shift 2 ;;
        --prometheus-target) [[ $# -ge 2 ]] || die_usage "--prometheus-target requires a value"; prometheus_target=$2; shift 2 ;;
        --keys-path) [[ $# -ge 2 ]] || die_usage "--keys-path requires a value"; keys_path=$2; shift 2 ;;
        --token-path) [[ $# -ge 2 ]] || die_usage "--token-path requires a value"; token_path=$2; shift 2 ;;
        --expected-user) [[ $# -ge 2 ]] || die_usage "--expected-user requires a value"; expected_user=$2; shift 2 ;;
        --restart-policy) [[ $# -ge 2 ]] || die_usage "--restart-policy requires a value"; restart_policy=$2; shift 2 ;;
        --min-disk-mb) [[ $# -ge 2 ]] || die_usage "--min-disk-mb requires a value"; min_disk_mb=$2; shift 2 ;;
        --record) [[ $# -ge 2 ]] || die_usage "--record requires a value"; record_path=$2; shift 2 ;;
        --requested-host) [[ $# -ge 2 ]] || die_usage "--requested-host requires a value"; requested_host=$2; shift 2 ;;
        --local) local_mode=true; shift ;;
        --run-audit) internal_audit=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die_usage "unknown option: $1" ;;
    esac
done

[[ "$port" =~ ^[0-9]+$ ]] || die_usage "--port must be numeric"
[[ "$candidate_port" =~ ^[0-9]+$ ]] || die_usage "--candidate-port must be numeric"
[[ "$min_disk_mb" =~ ^[0-9]+$ ]] || die_usage "--min-disk-mb must be numeric"
[[ "$phase" == predeploy || "$phase" == postdeploy ]] || die_usage "--phase must be predeploy or postdeploy"

failures=0

check() {
    printf 'CHECK %s\n' "$1"
}

pass() {
    local label=$1
    shift
    printf 'PASS %s' "$label"
    (($# == 0)) || printf ' %s' "$*"
    printf '\n'
}

fail() {
    local label=$1
    shift
    printf 'FAIL %s' "$label"
    (($# == 0)) || printf ' %s' "$*"
    printf '\n'
    failures=$((failures + 1))
}

safe_stat() {
    # Metadata only. Never open, hash, parse, or copy the secret-bearing file.
    stat -c '%F|%a|%u:%g|%s|%Y' -- "$1" 2>/dev/null
}

inspect_field() {
    local format=$1
    local target=$2
    docker inspect --type container --format "$format" "$target" 2>/dev/null
}

compose_command() {
    SCHWAB_GATEWAY_PRODUCTION_IMAGE="$production_image" docker compose "$@"
}

audit_file_metadata() {
    local label=$1
    local path=$2
    local metadata file_type mode owner size modified

    check "$label-metadata"
    if [[ -z "$path" ]]; then
        fail "$label-metadata" "path-unavailable"
        return
    fi
    if ! metadata=$(safe_stat "$path"); then
        fail "$label-metadata" "path=$path missing-or-inaccessible"
        return
    fi
    IFS='|' read -r file_type mode owner size modified <<<"$metadata"
    if [[ "$file_type" != "regular file" || "$mode" != "600" ]]; then
        fail "$label-metadata" "path=$path type=$file_type mode=$mode owner=$owner size=$size modified_epoch=$modified"
        return
    fi
    pass "$label-metadata" "path=$path type=regular-file mode=$mode owner=$owner size=$size modified_epoch=$modified"
}

run_audit() {
    local actual_host top_level repo_sha repo_tag repo_branch repo_status
    local -a compose_args=()
    local compose_file compose_services compose_images expected_image
    local live_image live_image_id tagged_image_id live_state live_health live_restart live_user
    local live_read_only live_ports live_aliases legacy_image legacy_image_id legacy_state
    local candidate_image candidate_image_id candidate_state candidate_health candidate_restart
    local candidate_user candidate_read_only candidate_ports candidate_aliases candidate_exists
    local derived_token_dir disk_values disk_available disk_capacity
    local prom_state generated_at compose_display

    actual_host=$(hostname -s 2>/dev/null || true)
    requested_host=${requested_host:-$actual_host}

    check identity
    if [[ "$actual_host" == "$expected_host" ]]; then
        pass identity "requested=$requested_host actual=$actual_host"
    else
        fail identity "requested=$requested_host expected=$expected_host actual=${actual_host:-unknown}"
    fi

    check repository-path
    if ! cd -- "$repo" 2>/dev/null; then
        fail repository-path "path=$repo inaccessible"
        printf 'SUMMARY FAIL failures=%d\n' "$failures"
        return 1
    fi
    top_level=$(git rev-parse --show-toplevel 2>/dev/null || true)
    if [[ -n "$top_level" && "$(pwd -P)" == "$(cd -- "$top_level" 2>/dev/null && pwd -P)" ]]; then
        pass repository-path "path=$(pwd -P)"
    else
        fail repository-path "path=$repo not-git-top-level"
    fi

    repo_sha=$(git rev-parse HEAD 2>/dev/null || true)
    check repository-sha
    if [[ "$repo_sha" =~ ^[0-9a-fA-F]{40}$ ]]; then
        pass repository-sha "sha=$repo_sha"
    else
        fail repository-sha "sha=unavailable"
    fi

    repo_tag=$(git describe --tags --exact-match HEAD 2>/dev/null || true)
    check repository-tag
    if [[ -n "$repo_tag" ]]; then
        pass repository-tag "tag=$repo_tag"
    else
        fail repository-tag "tag=none exact-release-tag-required"
        repo_tag=none
    fi

    repo_branch=$(git symbolic-ref --short -q HEAD 2>/dev/null || true)
    repo_branch=${repo_branch:-detached}
    repo_status=$(git status --porcelain --untracked-files=all 2>/dev/null || true)
    check repository-status
    if [[ -z "$repo_status" ]]; then
        pass repository-status "branch=$repo_branch clean=true"
    else
        fail repository-status "branch=$repo_branch clean=false"
    fi

    if ((${#compose_files[@]} == 0)); then
        compose_files=(compose.yml)
        [[ ! -f compose.production.yml ]] || compose_files+=(compose.production.yml)
    fi
    compose_display=""
    for compose_file in "${compose_files[@]}"; do
        compose_args+=(-f "$compose_file")
        compose_display+="${compose_display:+,}$compose_file"
    done

    check compose-files
    local compose_missing=false
    for compose_file in "${compose_files[@]}"; do
        if [[ ! -f "$compose_file" ]]; then
            compose_missing=true
        fi
    done
    if [[ "$compose_missing" == false ]]; then
        pass compose-files "files=$compose_display"
    else
        fail compose-files "files=$compose_display missing=true"
    fi

    check immutable-production-image
    if [[ "$production_image" =~ ^sha256:[0-9a-f]{64}$ || "$production_image" =~ ^[^@[:space:]]+@sha256:[0-9a-f]{64}$ ]]; then
        pass immutable-production-image "reference=$production_image"
    else
        fail immutable-production-image "reference=${production_image:-unset} expected=sha256-id-or-repository-digest"
    fi

    check preflight-phase
    pass preflight-phase "phase=$phase"

    check compose-validity
    if compose_command "${compose_args[@]}" --profile live config --quiet >/dev/null 2>&1; then
        pass compose-validity
    else
        fail compose-validity "resolved-output-suppressed"
    fi

    compose_services=$(compose_command "${compose_args[@]}" --profile live config --services 2>/dev/null || true)
    check compose-service
    if printf '%s\n' "$compose_services" | grep -Fxq -- "$service"; then
        pass compose-service "service=$service"
    else
        fail compose-service "service=$service absent"
    fi

    compose_images=$(compose_command "${compose_args[@]}" --profile live config --images 2>/dev/null || true)
    expected_image=$production_image
    check compose-image
    if [[ -n "$expected_image" ]] && printf '%s\n' "$compose_images" | grep -Fxq -- "$expected_image"; then
        pass compose-image "image=$expected_image"
    else
        fail compose-image "expected=${expected_image:-unset} compose-image-mismatch"
    fi

    tagged_image_id=""
    [[ -z "$expected_image" ]] || tagged_image_id=$(docker image inspect --format '{{.Id}}' "$expected_image" 2>/dev/null || true)
    check intended-image-resolution
    if [[ -n "$tagged_image_id" && "$tagged_image_id" =~ ^sha256:[0-9a-f]{64}$ ]]; then
        pass intended-image-resolution "reference=$expected_image id=$tagged_image_id"
    else
        fail intended-image-resolution "reference=${expected_image:-unset} id=${tagged_image_id:-unavailable}"
    fi

    check live-container
    if docker inspect --type container "$container" >/dev/null 2>&1; then
        pass live-container "name=$container"
    else
        fail live-container "name=$container absent"
    fi

    live_image=$(inspect_field '{{.Config.Image}}' "$container" || true)
    live_image_id=$(inspect_field '{{.Image}}' "$container" || true)
    check live-image
    if [[ "$phase" == predeploy && -n "$live_image" && -n "$live_image_id" ]]; then
        pass live-image "baseline-reference=$live_image baseline-id=$live_image_id intended-id=${tagged_image_id:-unavailable}"
    elif [[ "$phase" == postdeploy && -n "$live_image" && "$live_image" == "$expected_image" && -n "$live_image_id" && "$live_image_id" == "$tagged_image_id" ]]; then
        pass live-image "reference=$live_image id=$live_image_id"
    else
        fail live-image "phase=$phase running-reference=${live_image:-unavailable} intended-reference=${expected_image:-unavailable} running-id=${live_image_id:-unavailable} intended-id=${tagged_image_id:-unavailable}"
    fi

    live_state=$(inspect_field '{{.State.Status}}' "$container" || true)
    live_health=$(inspect_field '{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}' "$container" || true)
    check live-health
    if [[ "$live_state" == running && "$live_health" == healthy ]]; then
        pass live-health "state=$live_state health=$live_health"
    else
        fail live-health "state=${live_state:-unavailable} health=${live_health:-unavailable}"
    fi

    live_restart=$(inspect_field '{{.HostConfig.RestartPolicy.Name}}' "$container" || true)
    check restart-policy
    if [[ "$live_restart" == "$restart_policy" ]]; then
        pass restart-policy "actual=$live_restart"
    else
        fail restart-policy "expected=$restart_policy actual=${live_restart:-unavailable}"
    fi

    live_user=$(inspect_field '{{.Config.User}}' "$container" || true)
    check container-user
    if [[ "$live_user" == "$expected_user" ]]; then
        pass container-user "actual=$live_user"
    else
        fail container-user "expected=$expected_user actual=${live_user:-unset}"
    fi

    live_read_only=$(inspect_field '{{.HostConfig.ReadonlyRootfs}}' "$container" || true)
    check read-only-root
    if [[ "$live_read_only" == true ]]; then
        pass read-only-root
    else
        fail read-only-root "actual=${live_read_only:-unavailable}"
    fi

    live_ports=$(inspect_field "{{json (index .NetworkSettings.Ports \"$port/tcp\")}}" "$container" || true)
    check loopback-port
    if [[ "$live_ports" == "[{\"HostIp\":\"127.0.0.1\",\"HostPort\":\"$port\"}]" ]]; then
        pass loopback-port "binding=127.0.0.1:$port:$port"
    else
        fail loopback-port "expected=127.0.0.1:$port:$port actual=${live_ports:-unavailable}"
    fi

    live_aliases=$(inspect_field "{{range \$name, \$settings := .NetworkSettings.Networks}}{{if eq \$name \"$network\"}}{{range \$settings.Aliases}}{{println .}}{{end}}{{end}}{{end}}" "$container" || true)
    check network-alias
    if printf '%s\n' "$live_aliases" | grep -Fxq -- "$network_alias"; then
        pass network-alias "network=$network alias=$network_alias"
    else
        fail network-alias "network=$network alias=$network_alias absent"
    fi

    candidate_exists=false
    if docker inspect --type container "$candidate_container" >/dev/null 2>&1; then
        candidate_exists=true
    fi
    candidate_image=$(inspect_field '{{.Config.Image}}' "$candidate_container" || true)
    candidate_image_id=$(inspect_field '{{.Image}}' "$candidate_container" || true)
    candidate_state=$(inspect_field '{{.State.Status}}' "$candidate_container" || true)
    candidate_health=$(inspect_field '{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}' "$candidate_container" || true)

    check retired-candidate-absent
    if [[ "$candidate_exists" == false ]]; then
        pass retired-candidate-absent "name=$candidate_container port=$candidate_port"
    else
        fail retired-candidate-absent "name=$candidate_container state=${candidate_state:-unavailable} port=$candidate_port must-not-exist"
    fi

    check rollback-container
    if docker inspect --type container "$legacy_container" >/dev/null 2>&1; then
        pass rollback-container "name=$legacy_container"
    else
        fail rollback-container "name=$legacy_container absent"
    fi
    legacy_image=$(inspect_field '{{.Config.Image}}' "$legacy_container" || true)
    legacy_image_id=$(inspect_field '{{.Image}}' "$legacy_container" || true)
    legacy_state=$(inspect_field '{{.State.Status}}' "$legacy_container" || true)
    check rollback-image
    if [[ -n "$legacy_image" && -n "$legacy_image_id" ]]; then
        pass rollback-image "reference=$legacy_image id=$legacy_image_id state=${legacy_state:-unknown}"
    else
        fail rollback-image "reference=${legacy_image:-unavailable} id=${legacy_image_id:-unavailable}"
    fi

    check disk-space
    disk_values=$(df -Pk -- "$repo" 2>/dev/null | awk 'NR==2 {print $4 " " $5}' || true)
    read -r disk_available disk_capacity <<<"$disk_values"
    if [[ "$disk_available" =~ ^[0-9]+$ ]] && ((disk_available >= min_disk_mb * 1024)); then
        pass disk-space "available_mb=$((disk_available / 1024)) capacity_used=${disk_capacity:-unknown} minimum_mb=$min_disk_mb"
    else
        fail disk-space "available_kb=${disk_available:-unavailable} capacity_used=${disk_capacity:-unknown} minimum_mb=$min_disk_mb"
    fi

    check endpoint-health
    if curl --fail --silent --show-error --max-time 5 --output /dev/null -- "$health_url" 2>/dev/null; then
        pass endpoint-health "url=$health_url"
    else
        fail endpoint-health "url=$health_url"
    fi

    check endpoint-readiness
    if curl --fail --silent --show-error --max-time 5 --output /dev/null -- "$ready_url" 2>/dev/null; then
        pass endpoint-readiness "url=$ready_url"
    else
        fail endpoint-readiness "url=$ready_url"
    fi

    check prometheus-target
    prom_state=$(curl --fail --silent --show-error --max-time 5 -- "$prometheus_url" 2>/dev/null | python3 -c '
import json
import sys
from urllib.parse import urlsplit

expected = sys.argv[1]
try:
    payload = json.load(sys.stdin)
    targets = payload.get("data", {}).get("activeTargets", [])
    matches = []
    for target in targets:
        identities = {
            target.get("labels", {}).get("instance", ""),
            target.get("discoveredLabels", {}).get("__address__", ""),
            urlsplit(target.get("scrapeUrl", "")).netloc,
        }
        if expected in identities:
            matches.append(target)
    print("up" if any(target.get("health") == "up" for target in matches) else ("down" if matches else "missing"))
except Exception:
    print("invalid")
    raise SystemExit(1)
' "$prometheus_target" 2>/dev/null || true)
    if [[ "$prom_state" == up ]]; then
        pass prometheus-target "target=$prometheus_target state=up"
    else
        fail prometheus-target "target=$prometheus_target state=${prom_state:-unavailable}"
    fi

    if [[ -z "$keys_path" ]]; then
        keys_path=$(inspect_field '{{range .Mounts}}{{if eq .Destination "/run/secrets/schwab-gateway-keys.json"}}{{.Source}}{{end}}{{end}}' "$container" || true)
    fi
    if [[ -z "$token_path" ]]; then
        derived_token_dir=$(inspect_field '{{range .Mounts}}{{if and .RW (ne .Destination "/run/secrets/schwab-gateway-keys.json")}}{{.Source}}{{end}}{{end}}' "$container" || true)
        [[ -z "$derived_token_dir" ]] || token_path="${derived_token_dir%/}/tokens.json"
    fi
    audit_file_metadata digest-keys "$keys_path"
    audit_file_metadata token "$token_path"

    generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    printf 'BEGIN ROLLBACK RECORD\n'
    printf 'generated_at=%s\n' "$generated_at"
    printf 'host_requested=%s\n' "$requested_host"
    printf 'host_actual=%s\n' "${actual_host:-unknown}"
    printf 'repository=%s\n' "$(pwd -P)"
    printf 'repository_sha=%s\n' "${repo_sha:-unavailable}"
    printf 'repository_tag=%s\n' "${repo_tag:-none}"
    printf 'repository_branch=%s\n' "$repo_branch"
    printf 'repository_clean=%s\n' "$([[ -z "$repo_status" ]] && printf true || printf false)"
    printf 'compose_files=%s\n' "$compose_display"
    printf 'phase=%s\n' "$phase"
    printf 'production_image=%s\n' "${production_image:-unavailable}"
    printf 'production_image_resolved_id=%s\n' "${tagged_image_id:-unavailable}"
    printf 'live_container=%s\n' "$container"
    printf 'live_image_reference=%s\n' "${live_image:-unavailable}"
    printf 'live_image_id=%s\n' "${live_image_id:-unavailable}"
    printf 'live_state=%s\n' "${live_state:-unavailable}"
    printf 'candidate_container=%s\n' "$candidate_container"
    printf 'candidate_image_reference=%s\n' "${candidate_image:-unavailable}"
    printf 'candidate_image_id=%s\n' "${candidate_image_id:-unavailable}"
    printf 'candidate_state=%s\n' "${candidate_state:-unavailable}"
    printf 'candidate_health=%s\n' "${candidate_health:-unavailable}"
    printf 'rollback_container=%s\n' "$legacy_container"
    printf 'rollback_image_reference=%s\n' "${legacy_image:-unavailable}"
    printf 'rollback_image_id=%s\n' "${legacy_image_id:-unavailable}"
    printf 'rollback_state=%s\n' "${legacy_state:-unavailable}"
    printf 'prometheus_target=%s\n' "$prometheus_target"
    printf 'rollback_primary=SCHWAB_GATEWAY_PRODUCTION_IMAGE=%q docker compose --project-name schwab_gateway' "$live_image_id"
    for compose_file in "${compose_files[@]}"; do
        printf ' -f %q' "$compose_file"
    done
    printf ' --profile live up --detach --no-build --no-deps --force-recreate %q\n' "$service"
    printf 'rollback_legacy_step_1=docker stop %q\n' "$container"
    printf 'rollback_legacy_step_2=docker start %q\n' "$legacy_container"
    printf 'prometheus_rollback=not-required-no-change\n'
    printf 'END ROLLBACK RECORD\n'

    if ((failures == 0)); then
        printf 'SUMMARY PASS failures=0\n'
        return 0
    fi
    printf 'SUMMARY FAIL failures=%d\n' "$failures"
    return 1
}

if [[ "$internal_audit" == true ]]; then
    run_audit
    exit $?
fi

if [[ "$local_mode" == true ]]; then
    if [[ -n "$record_path" ]]; then
        mkdir -p -- "$(dirname -- "$record_path")" || exit 1
        : >"$record_path" || exit 1
        chmod 600 -- "$record_path" || exit 1
        set +e
        run_audit | tee "$record_path"
        audit_status=${PIPESTATUS[0]}
        set -e
        exit "$audit_status"
    fi
    run_audit
    exit $?
fi

audit_args=(
    --run-audit
    --repo "$repo"
    --production-image "$production_image"
    --phase "$phase"
    --container "$container"
    --candidate-container "$candidate_container"
    --legacy-container "$legacy_container"
    --service "$service"
    --expected-host "$expected_host"
    --network "$network"
    --network-alias "$network_alias"
    --candidate-network-alias "$candidate_network_alias"
    --port "$port"
    --candidate-port "$candidate_port"
    --health-url "$health_url"
    --ready-url "$ready_url"
    --candidate-ready-url "$candidate_ready_url"
    --prometheus-url "$prometheus_url"
    --prometheus-target "$prometheus_target"
    --expected-user "$expected_user"
    --restart-policy "$restart_policy"
    --min-disk-mb "$min_disk_mb"
    --requested-host "$host"
)
[[ -z "$keys_path" ]] || audit_args+=(--keys-path "$keys_path")
[[ -z "$token_path" ]] || audit_args+=(--token-path "$token_path")
for compose_file in "${compose_files[@]}"; do
    audit_args+=(--compose-file "$compose_file")
done

printf -v remote_command '%q ' bash -s -- "${audit_args[@]}"
if [[ -n "$record_path" ]]; then
    mkdir -p -- "$(dirname -- "$record_path")" || exit 1
    : >"$record_path" || exit 1
    chmod 600 -- "$record_path" || exit 1
    set +e
    ssh -F /dev/null -o BatchMode=yes -- "$host" "$remote_command" <"$0" | tee "$record_path"
    remote_status=${PIPESTATUS[0]}
    set -e
    exit "$remote_status"
fi

ssh -F /dev/null -o BatchMode=yes -- "$host" "$remote_command" <"$0"
