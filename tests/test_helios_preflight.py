import os
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "scripts" / "helios_preflight.sh"
IMAGE_ID = "sha256:" + "b" * 64
RUNNING_IMAGE_ID = IMAGE_ID
LEGACY_IMAGE_ID = "sha256:" + "c" * 64
REPO_SHA = "a" * 40


def write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)


def fake_tools(tmp_path: Path, repo: Path, keys: Path, token: Path) -> Path:
    tools = tmp_path / "bin"
    tools.mkdir()

    write_executable(
        tools / "git",
        f"""#!/usr/bin/env bash
case "$*" in
  "rev-parse --show-toplevel") printf '%s\\n' "$FAKE_REPO" ;;
  "rev-parse HEAD") printf '%s\\n' '{REPO_SHA}' ;;
  "describe --tags --exact-match HEAD") printf '%s\\n' 'v0.1.0' ;;
  "symbolic-ref --short -q HEAD") printf '%s\\n' 'main' ;;
  "status --porcelain --untracked-files=all")
    [[ "${{FAKE_DIRTY:-false}}" != true ]] || printf '%s\\n' ' M compose.yml'
    ;;
  *) exit 2 ;;
esac
""",
    )

    write_executable(
        tools / "docker",
        f"""#!/usr/bin/env bash
if [[ "$1" == compose ]]; then
  case " $* " in
    *" config --quiet "*) exit 0 ;;
    *" config --services "*) printf '%s\\n' live; exit 0 ;;
    *" config --images "*) printf '%s\\n' "$SCHWAB_GATEWAY_PRODUCTION_IMAGE"; exit 0 ;;
  esac
fi

if [[ "$1" == image && "$2" == inspect ]]; then
  printf '%s\\n' '{RUNNING_IMAGE_ID}'
  exit 0
fi

if [[ "$1" != inspect ]]; then
  exit 2
fi

target="${{@: -1}}"
if [[ "$target" == butterfly_schwab_gateway_live && "${{FAKE_NO_LEGACY:-false}}" == true ]]; then
  exit 1
fi
case "$target" in
  schwab_gateway_live|schwab_gateway_candidate|butterfly_schwab_gateway_live) ;;
  *) exit 1 ;;
esac
if [[ "$*" != *"--format"* ]]; then
  exit 0
fi

format=""
for ((i=1; i<=$#; i++)); do
  if [[ "${{!i}}" == --format ]]; then
    j=$((i + 1))
    format="${{!j}}"
    break
  fi
done

if [[ "$target" == butterfly_schwab_gateway_live ]]; then
  case "$format" in
    *Config.Image*) printf '%s\\n' 'legacy-gateway:v0.1.0' ;;
    *'.Image'*) printf '%s\\n' '{LEGACY_IMAGE_ID}' ;;
    *State.Status*) printf '%s\\n' exited ;;
    *) exit 1 ;;
  esac
  exit 0
fi

if [[ "$target" == schwab_gateway_candidate ]]; then
  case "$format" in
    *Config.Image*) printf '%s\\n' 'schwab-gateway-candidate:local' ;;
    *'.Image'*)
      if [[ "${{FAKE_BAD_CANDIDATE_IMAGE:-false}}" == true ]]; then
        printf '%s\\n' '{LEGACY_IMAGE_ID}'
      else
        printf '%s\\n' '{RUNNING_IMAGE_ID}'
      fi
      ;;
    *State.Status*) printf '%s\\n' running ;;
    *State.Health*) printf '%s\\n' healthy ;;
    *RestartPolicy.Name*) printf '%s\\n' unless-stopped ;;
    *Config.User*) printf '%s\\n' '1001:1001' ;;
    *ReadonlyRootfs*) printf '%s\\n' true ;;
    *NetworkSettings.Ports*) printf '%s\\n' '[{{"HostIp":"127.0.0.1","HostPort":"8012"}}]' ;;
    *NetworkSettings.Networks*) printf '%s\\n' schwab-gateway-candidate ;;
    *) exit 1 ;;
  esac
  exit 0
fi

case "$format" in
  *Config.Image*)
    if [[ "${{FAKE_BAD_IMAGE:-false}}" == true ]]; then
      printf '%s\\n' 'schwab-gateway:mutable'
    else
      printf '%s\\n' "$SCHWAB_GATEWAY_PRODUCTION_IMAGE"
    fi
    ;;
  *'.Image'*) printf '%s\\n' '{RUNNING_IMAGE_ID}' ;;
  *State.Status*) printf '%s\\n' running ;;
  *State.Health*)
    if [[ "${{FAKE_UNHEALTHY:-false}}" == true ]]; then
      printf '%s\\n' unhealthy
    else
      printf '%s\\n' healthy
    fi
    ;;
  *RestartPolicy.Name*) printf '%s\\n' unless-stopped ;;
  *Config.User*) printf '%s\\n' '1001:1001' ;;
  *ReadonlyRootfs*) printf '%s\\n' true ;;
  *NetworkSettings.Ports*) printf '%s\\n' '[{{"HostIp":"127.0.0.1","HostPort":"8011"}}]' ;;
  *NetworkSettings.Networks*) printf '%s\\n' schwab-gateway ;;
  *'.RW'*) printf '%s\\n' "$FAKE_TOKEN_DIR" ;;
  *'/run/secrets/schwab-gateway-keys.json'*) printf '%s\\n' "$FAKE_KEYS_PATH" ;;
  *) exit 1 ;;
esac
""",
    )

    write_executable(
        tools / "curl",
        """#!/usr/bin/env bash
if [[ "$*" == *"api/v1/targets"* ]]; then
  if [[ "${FAKE_PROM_DOWN:-false}" == true ]]; then
    health=down
  else
    health=up
  fi
  printf '%s' '{"data":{"activeTargets":[{"labels":{"instance":"schwab-gateway:8011"},"health":"'
  printf '%s' "$health"
  printf '%s\\n' '","scrapeUrl":"http://schwab-gateway:8011/metrics"}]}}'
fi
""",
    )

    return tools


def run_preflight(
    tmp_path: Path,
    *,
    extra_env: dict[str, str] | None = None,
    image: str = IMAGE_ID,
    phase: str = "predeploy",
) -> tuple[subprocess.CompletedProcess[str], Path, str, str]:
    repo = tmp_path / "remote repo"
    repo.mkdir()
    (repo / "compose.yml").write_text("services: {}\n", encoding="utf-8")
    (repo / "compose.production.yml").write_text("services: {}\n", encoding="utf-8")

    keys_secret = "DIGEST_KEYS_SECRET_SENTINEL"
    token_secret = "OAUTH_TOKEN_SECRET_SENTINEL"
    keys = tmp_path / "keys.json"
    token_dir = tmp_path / "token-dir"
    token_dir.mkdir()
    token = token_dir / "tokens.json"
    keys.write_text(keys_secret, encoding="utf-8")
    token.write_text(token_secret, encoding="utf-8")
    keys.chmod(0o600)
    token.chmod(0o600)

    tools = fake_tools(tmp_path, repo, keys, token)
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{tools}:{environment['PATH']}",
            "FAKE_REPO": str(repo),
            "FAKE_KEYS_PATH": str(keys),
            "FAKE_TOKEN_DIR": str(token_dir),
            "SCHWAB_GATEWAY_PRODUCTION_IMAGE": image,
        }
    )
    if extra_env:
        environment.update(extra_env)

    actual_host = subprocess.run(
        ["hostname", "-s"], check=True, capture_output=True, text=True
    ).stdout.strip()
    record = tmp_path / "evidence" / "preflight.txt"
    result = subprocess.run(
        [
            str(SCRIPT),
            "--local",
            "--repo",
            str(repo),
            "--phase",
            phase,
            "--expected-host",
            actual_host,
            "--min-disk-mb",
            "1",
            "--record",
            str(record),
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    return result, record, keys_secret, token_secret


def test_preflight_passes_and_writes_sanitized_rollback_record(tmp_path: Path) -> None:
    result, record, keys_secret, token_secret = run_preflight(tmp_path)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS immutable-production-image" in result.stdout
    assert (
        f"PASS live-image baseline-reference={IMAGE_ID} baseline-id={RUNNING_IMAGE_ID}"
        in result.stdout
    )
    assert (
        "PASS candidate-image reference=schwab-gateway-candidate:local "
        f"id={RUNNING_IMAGE_ID}" in result.stdout
    )
    assert "PASS prometheus-target target=schwab-gateway:8011 state=up" in result.stdout
    assert "PASS digest-keys-metadata" in result.stdout
    assert "PASS token-metadata" in result.stdout
    assert "type=regular-file mode=600 owner=" in result.stdout
    assert " size=27 modified_epoch=" in result.stdout
    assert "BEGIN ROLLBACK RECORD" in result.stdout
    assert f"repository_sha={REPO_SHA}" in result.stdout
    assert f"production_image={IMAGE_ID}" in result.stdout
    assert f"rollback_image_id={LEGACY_IMAGE_ID}" in result.stdout
    assert (
        f"rollback_primary=SCHWAB_GATEWAY_PRODUCTION_IMAGE={RUNNING_IMAGE_ID} "
        "docker compose --project-name schwab_gateway "
        "-f compose.yml -f compose.production.yml --profile live "
        "up --detach --no-build --no-deps --force-recreate live"
        in result.stdout
    )
    assert "prometheus_rollback=not-required-no-change" in result.stdout
    assert "SUMMARY PASS failures=0" in result.stdout
    assert keys_secret not in result.stdout
    assert token_secret not in result.stdout
    assert record.read_text(encoding="utf-8") == result.stdout
    assert stat.S_IMODE(record.stat().st_mode) == 0o600


def test_preflight_fails_closed_without_leaking_secret_values(tmp_path: Path) -> None:
    result, record, keys_secret, token_secret = run_preflight(
        tmp_path,
        extra_env={
            "FAKE_DIRTY": "true",
            "FAKE_UNHEALTHY": "true",
            "FAKE_BAD_IMAGE": "true",
            "FAKE_PROM_DOWN": "true",
        },
        image="schwab-gateway:mutable",
    )

    assert result.returncode == 1
    assert "FAIL immutable-production-image" in result.stdout
    assert "FAIL repository-status" in result.stdout
    assert "FAIL live-health" in result.stdout
    assert "FAIL prometheus-target" in result.stdout
    assert "SUMMARY FAIL failures=" in result.stdout
    assert keys_secret not in result.stdout
    assert token_secret not in result.stdout
    assert keys_secret not in record.read_text(encoding="utf-8")
    assert token_secret not in record.read_text(encoding="utf-8")


def test_invalid_immutable_image_argument_is_a_failed_gate(tmp_path: Path) -> None:
    result, _, _, _ = run_preflight(tmp_path, image="registry.example/gateway:latest")

    assert result.returncode == 1
    assert "FAIL immutable-production-image" in result.stdout


def test_postdeploy_requires_live_to_match_intended_image(tmp_path: Path) -> None:
    result, _, _, _ = run_preflight(
        tmp_path,
        extra_env={"FAKE_BAD_IMAGE": "true"},
        phase="postdeploy",
    )

    assert result.returncode == 1
    assert "FAIL live-image phase=postdeploy" in result.stdout
    assert "PASS candidate-report" in result.stdout


def test_predeploy_records_different_live_baseline_without_blocking(tmp_path: Path) -> None:
    result, _, _, _ = run_preflight(
        tmp_path,
        extra_env={"FAKE_BAD_IMAGE": "true"},
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS live-image baseline-reference=schwab-gateway:mutable" in result.stdout
    assert "SUMMARY PASS failures=0" in result.stdout


def test_predeploy_requires_candidate_to_resolve_to_intended_image(tmp_path: Path) -> None:
    result, _, _, _ = run_preflight(
        tmp_path,
        extra_env={"FAKE_BAD_CANDIDATE_IMAGE": "true"},
    )

    assert result.returncode == 1
    assert "FAIL candidate-image" in result.stdout


def test_help_documents_read_only_local_and_record_modes() -> None:
    result = subprocess.run(
        [str(SCRIPT), "--help"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "read-only audit" in result.stdout
    assert "--production-image IMAGE" in result.stdout
    assert "--record PATH" in result.stdout
    assert "--local" in result.stdout
