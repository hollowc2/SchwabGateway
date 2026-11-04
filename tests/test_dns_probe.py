"""The DNS diagnostic must remain bounded and keep each environment observable."""

import importlib.util
import json
import signal
import subprocess
import threading
import time
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


def test_resolver_state_records_selection_and_continued_address_list(monkeypatch):
    output = """Link 2 (eth0)
Current DNS Server: 153.92.2.6
       DNS Servers: 153.92.2.6 1.1.1.1
                    2001:4860:4860::8888
        DNS Domain: private.example
"""
    monkeypatch.setattr(dns.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        returncode=0, stdout=output,
    ))
    result = dns.resolver_state("eth0")
    assert result == {"interface": "eth0", "selected_server": "153.92.2.6",
                      "servers": ["153.92.2.6", "1.1.1.1", "2001:4860:4860::8888"]}
    assert "private.example" not in json.dumps(result)


def test_resolver_state_failure_keeps_raw_error_text_private(monkeypatch):
    def fail(*args, **kwargs):
        assert kwargs["timeout"] == 1
        raise subprocess.TimeoutExpired(args[0], 1, stderr="untrusted diagnostic text")

    monkeypatch.setattr(dns.subprocess, "run", fail)
    result = dns.resolver_state("eth0")
    assert result["error_type"] == "resolver_state_unavailable"
    assert result["servers"] == []
    assert "untrusted" not in json.dumps(result)


@pytest.mark.parametrize("status", ["NOERROR", "SERVFAIL", "NXDOMAIN", "REFUSED"])
def test_upstream_query_preserves_response_status_and_one_attempt(monkeypatch, status):
    def run(command, **kwargs):
        assert command == ["dig", "@1.1.1.1", "api.schwabapi.com", "AAAA", "+time=1",
                           "+tries=1", "+noall", "+comments", "+stats"]
        assert kwargs["timeout"] == 1.5
        return SimpleNamespace(returncode=0, stdout=f"status: {status},\nQuery time: 12 msec")

    monkeypatch.setattr(dns.subprocess, "run", run)
    result = dns.upstream_query("1.1.1.1", "api.schwabapi.com", "AAAA")
    assert result["response_status"] == status
    assert result["query_ms"] == 12


@pytest.mark.parametrize("exception,error_type", [
    (FileNotFoundError(), "dns_query_tool_unavailable"),
    (subprocess.TimeoutExpired("dig", 1.5), "dns_query_deadline_exceeded"),
])
def test_upstream_query_failure_is_bounded_and_structured(monkeypatch, exception, error_type):
    def fail(*args, **kwargs):
        raise exception

    monkeypatch.setattr(dns.subprocess, "run", fail)
    result = dns.upstream_query("1.1.1.1", "api.schwabapi.com", "A")
    assert result["error_type"] == error_type


def test_upstream_comparison_bounds_query_count_and_parallelism(monkeypatch):
    calls = []
    active = 0
    peak = 0
    lock = threading.Lock()

    def query(server, host, kind):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            calls.append((server, host, kind))
        time.sleep(0.01)
        with lock:
            active -= 1
        return {"server": server, "hostname": host, "record_type": kind}

    monkeypatch.setattr(dns, "upstream_query", query)
    results = dns.compare_upstreams(["1.1.1.1"] * 5, ["a", "a", "b", "c"])
    assert len(results) == len(calls) == 12
    assert {h for _, h, _ in calls} == {"a", "b"}
    assert {kind for _, _, kind in calls} == {"A", "AAAA"}
    assert 1 < peak <= 4


@pytest.mark.parametrize("failed", [False, True])
def test_monitor_captures_upstream_switch_and_compares_only_after_failure(
    monkeypatch, capsys, failed,
):
    before = {"interface": "eth0", "servers": ["153.92.2.6", "1.1.1.1"],
              "selected_server": "153.92.2.6"}
    after = {**before, "selected_server": "1.1.1.1"}
    states = iter([before, *([after] if failed else []), after])
    monkeypatch.setattr(dns, "resolver_state", lambda interface: next(states))
    monkeypatch.setattr(dns, "probe", lambda host, container, timeout: {"success": not failed})
    comparisons = []

    def compare(servers, hosts):
        comparisons.append((servers, hosts))
        return [{"server": "153.92.2.6", "error_type": "dns_query_failed"}]

    monkeypatch.setattr(dns, "compare_upstreams", compare)
    assert dns.main(["--resolver-interface", "eth0", "--compare-upstreams-on-failure"]) == (
        int(failed)
    )
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert rows[0]["state"]["selected_server"] == "153.92.2.6"
    assert rows[-1]["state"]["selected_server"] == "1.1.1.1"
    assert len(comparisons) == int(failed)
    if failed:
        assert rows[2]["event"] == "dns_upstream_comparison"
        assert rows[2]["resolver_state_at_failure"]["selected_server"] == "1.1.1.1"
        assert comparisons[0][0] == ["153.92.2.6", "1.1.1.1"]


def test_comparison_flag_requires_resolver_interface():
    with pytest.raises(SystemExit) as error:
        dns.main(["--compare-upstreams-on-failure"])
    assert error.value.code == 2
