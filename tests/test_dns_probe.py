"""The DNS diagnostic must remain bounded and keep each environment observable."""

import importlib.util
import json
import signal
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location(
    "probe_dns", Path(__file__).parents[1] / "tools" / "probe_dns.py",
)
dns = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dns)


def test_resolver_hang_has_a_deadline(monkeypatch):
    def hang(command, **kwargs):
        assert kwargs["timeout"] == 0.25
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(dns.subprocess, "run", hang)
    result = dns.probe("api.schwabapi.com", "schwab_gateway_live", 0.25)
    assert result["success"] is False
    assert result["error_type"] == "lookup_deadline_exceeded"
    assert result["environment"] == "container:schwab_gateway_live"


def test_dns_failure_is_distinct_from_docker_failure(monkeypatch):
    monkeypatch.setattr(dns.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        returncode=0,
        stdout='{"success":false,"error_type":"gaierror","error_number":-3}',
    ))
    result = dns.probe("api.schwabapi.com", None, 3)
    assert result["error_type"] == "gaierror"
    assert result["error_number"] == -3

    monkeypatch.setattr(dns.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        returncode=1, stderr="untrusted diagnostic text",
    ))
    result = dns.probe("api.schwabapi.com", "missing-container", 3)
    assert result["error_type"] == "probe_execution_failed"
    assert "untrusted" not in json.dumps(result)


@pytest.mark.parametrize("returncode", [-signal.SIGALRM, 128 + signal.SIGALRM])
def test_child_deadline_is_identified_from_process_status(monkeypatch, returncode):
    monkeypatch.setattr(dns.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        returncode=returncode,
    ))
    assert dns.probe("api.schwabapi.com", "gateway", 3)["error_type"] == (
        "lookup_deadline_exceeded"
    )


def test_failed_host_probe_does_not_skip_containers(monkeypatch, capsys):
    calls = []

    def fake_probe(hostname, container, timeout):
        calls.append((hostname, container))
        return {"success": container is not None}

    monkeypatch.setattr(dns, "probe", fake_probe)
    assert dns.main(["--container", "gateway", "--hostnames", "schwab", "binance"]) == 1
    assert calls == [("schwab", None), ("binance", None),
                     ("schwab", "gateway"), ("binance", "gateway")]
    assert len(capsys.readouterr().out.splitlines()) == 4


@pytest.mark.parametrize("timeout", ["0", "-1", "nan", "inf"])
def test_invalid_deadline_rejected_before_any_probe(timeout):
    with pytest.raises(SystemExit) as error:
        dns.main(["--timeout-seconds", timeout])
    assert error.value.code == 2


def test_resolver_child_uses_standard_library_only():
    result = subprocess.run(
        [dns.sys.executable, "-c", dns.RESOLVE, "localhost", "2"],
        capture_output=True, text=True, timeout=3, check=True,
    )
    assert json.loads(result.stdout)["success"] is True


def test_container_child_also_exits_when_its_resolver_hangs():
    # The Docker client can time out independently of its remote exec process.
    # Simulate a blocked libc lookup and verify the child's own kernel deadline.
    blocked = dns.RESOLVE.replace(
        "addresses = socket.getaddrinfo",
        "import time; time.sleep(30); addresses = socket.getaddrinfo",
    )
    result = subprocess.run(
        [dns.sys.executable, "-c", blocked, "localhost", "0.05"],
        capture_output=True, text=True, timeout=3, check=False,
    )
    assert result.returncode == -signal.SIGALRM
