import json
import os
import re
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
SYNTHETIC_LIVE_ENV = {
    "SCHWAB_API_KEY": "synthetic-api-key",
    "SCHWAB_SECRET_KEY": "synthetic-secret-key",
    "SCHWAB_GATEWAY_TOKEN_DIR": "/tmp/synthetic-token-dir",
    "SCHWAB_GATEWAY_KEYS_PATH": "/tmp/synthetic-keys.json",
}
IMAGE_ID = "sha256:" + "a" * 64
REPOSITORY_DIGEST = "registry.example/schwab-gateway@sha256:" + "b" * 64


def compose() -> dict:
    return yaml.safe_load((ROOT / "compose.yml").read_text())


def rendered_compose(
    *overrides: str,
    production_image: str | None = None,
) -> dict:
    environment = os.environ.copy()
    environment.update(SYNTHETIC_LIVE_ENV)
    environment.pop("SCHWAB_GATEWAY_PRODUCTION_IMAGE", None)
    if production_image is not None:
        environment["SCHWAB_GATEWAY_PRODUCTION_IMAGE"] = production_image

    command = ["docker", "compose", "-f", str(ROOT / "compose.yml")]
    for override in overrides:
        command.extend(["-f", str(ROOT / override)])
    command.extend(["--profile", "*", "config", "--format", "json"])

    result = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def published_port(service: dict) -> tuple[str, int, str]:
    [port] = service["ports"]
    return port["host_ip"], port["target"], port["published"]


def is_immutable_image_reference(reference: str) -> bool:
    image_id = re.fullmatch(r"sha256:[0-9a-f]{64}", reference)
    repository_digest = re.fullmatch(r"[^@\s]+@sha256:[0-9a-f]{64}", reference)
    return image_id is not None or repository_digest is not None


def test_demo_and_live_are_explicit_profiles() -> None:
    services = compose()["services"]
    assert services["demo"]["profiles"] == ["demo"]
    assert services["live"]["profiles"] == ["live"]
    assert services["live"]["container_name"] == "schwab_gateway_live"


def test_live_container_is_unprivileged_read_only_and_internal() -> None:
    live = compose()["services"]["live"]
    assert live["read_only"] is True
    assert live["user"]
    assert live["cap_drop"] == ["ALL"]
    assert live["security_opt"] == ["no-new-privileges:true"]
    assert live["ports"] == ["127.0.0.1:8011:8011"]
    assert live["networks"]["monitoring_net"]["aliases"] == ["schwab-gateway"]
    assert live["environment"]["SCHWAB_GATEWAY_ORDER_WRITES_ENABLED"] == "false"


def test_base_compose_does_not_require_the_production_image_override() -> None:
    rendered = rendered_compose()
    assert rendered["services"]["demo"]["profiles"] == ["demo"]


def test_candidate_layer_retargets_only_candidate_identity_and_ingress() -> None:
    live = rendered_compose("compose.candidate.yml")["services"]["live"]

    assert live["container_name"] == "schwab_gateway_candidate"
    assert live["build"]["context"] == str(ROOT)
    assert live["environment"]["SCHWAB_GATEWAY_PORT"] == "8012"
    assert live["environment"]["SCHWAB_GATEWAY_ORDER_WRITES_ENABLED"] == "false"
    assert published_port(live) == ("127.0.0.1", 8012, "8012")
    assert live["networks"]["monitoring_net"]["aliases"] == [
        "schwab-gateway-candidate"
    ]
    assert "http://127.0.0.1:8012/ready" in live["healthcheck"]["test"][-1]


def test_production_layer_requires_an_explicit_image() -> None:
    environment = os.environ.copy()
    environment.update(SYNTHETIC_LIVE_ENV)
    environment.pop("SCHWAB_GATEWAY_PRODUCTION_IMAGE", None)

    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT / "compose.yml"),
            "-f",
            str(ROOT / "compose.production.yml"),
            "--profile",
            "live",
            "config",
            "--quiet",
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "SCHWAB_GATEWAY_PRODUCTION_IMAGE" in result.stderr


def test_production_layer_disables_builds_and_registry_fallback() -> None:
    live = rendered_compose(
        "compose.production.yml", production_image=IMAGE_ID
    )["services"]["live"]

    assert "build" not in live
    assert live["image"] == IMAGE_ID
    assert live["pull_policy"] == "never"
    assert live["container_name"] == "schwab_gateway_live"
    assert live["environment"]["SCHWAB_GATEWAY_ORDER_WRITES_ENABLED"] == "false"
    assert published_port(live) == ("127.0.0.1", 8011, "8011")
    assert live["networks"]["monitoring_net"]["aliases"] == ["schwab-gateway"]


def test_production_layer_preserves_supported_immutable_references() -> None:
    for reference in (IMAGE_ID, REPOSITORY_DIGEST):
        assert is_immutable_image_reference(reference)
        live = rendered_compose(
            "compose.production.yml", production_image=reference
        )["services"]["live"]
        assert live["image"] == reference

    assert not is_immutable_image_reference("schwab_gateway_live:v0.1.0")


def test_only_minimal_secret_inputs_are_admitted() -> None:
    environment = compose()["services"]["live"]["environment"]
    assert set(environment) == {
        "SCHWAB_API_KEY",
        "SCHWAB_SECRET_KEY",
        "SCHWAB_TOKEN_PATH",
        "SCHWAB_GATEWAY_BIND_HOST",
        "SCHWAB_GATEWAY_PORT",
        "SCHWAB_GATEWAY_INTERNAL_KEYS_PATH",
        "SCHWAB_GATEWAY_ORDER_WRITES_ENABLED",
        "SCHWAB_GATEWAY_OPTION_CHAIN_CACHE_TTL_SECONDS",
        "SCHWAB_GATEWAY_OPTION_CHAIN_CACHE_MAX_ENTRIES",
        "SCHWAB_GATEWAY_OPTION_CHAIN_MAX_INFLIGHT",
        "SCHWAB_GATEWAY_ORDER_BOOK_STREAM_ENABLED",
        "SCHWAB_GATEWAY_ORDER_BOOK_STREAM_VENUE",
        "SCHWAB_GATEWAY_ORDER_BOOK_STREAM_SYMBOLS",
        "SCHWAB_GATEWAY_ORDER_BOOK_HISTORY_LIMIT",
        "SCHWAB_GATEWAY_ORDER_BOOK_SUBSCRIBER_QUEUE_LIMIT",
        "SCHWAB_GATEWAY_ORDER_BOOK_STREAM_PROTECTED_CAPACITY",
        "SCHWAB_GATEWAY_ORDER_BOOK_STREAM_BACKGROUND_CAPACITY",
        "SCHWAB_GATEWAY_ORDER_BOOK_MAX_SNAPSHOT_AGE_SECONDS",
    }
    assert not any(name.startswith(("DATABASE", "SCHWAB_ACCOUNT")) for name in environment)


def test_alert_rules_keep_gateway_metric_names() -> None:
    alerts = Path("infra/alerts.yml").read_text()
    assert "schwab_gateway_token_state" in alerts
    assert 'job="schwab_gateway"' in alerts


def test_runbooks_update_prometheus_single_file_bind_mount_safely() -> None:
    helios = Path("docs/runbooks/helios.md").read_text()
    rollback = Path("docs/runbooks/rollback.md").read_text()

    assert "sed -i" in helios
    assert "Never update" in helios
    assert "docker exec --user 1001:1001 -i" in helios
    assert "docker exec --user 1001:1001 -i" in rollback
    assert "cat > /etc/prometheus/prometheus.yml" in helios
    assert "cat > /etc/prometheus/prometheus.yml" in rollback
    assert "butterfly_schwab_gateway_live:8011" in rollback
    assert "phase6_ports=$(docker inspect" in helios
    assert "phase6_aliases=$(docker inspect" in helios
    assert ")phase6_aliases=" not in helios
    assert "CHECK port-publication" in helios
    assert "PASS network-alias" in helios
