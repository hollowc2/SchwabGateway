from pathlib import Path

import yaml


def compose() -> dict:
    return yaml.safe_load(Path("compose.yml").read_text())


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
