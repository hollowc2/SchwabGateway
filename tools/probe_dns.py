"""One bounded DNS snapshot from the host and selected running containers.

Run on Helios with: python3 tools/probe_dns.py --container schwab_gateway_live
Prints JSON lines and exits nonzero if any lookup fails. No HTTP requests, token
access, retries, configuration changes or recurring jobs are performed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import ipaddress
import json
import math
import re
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

RESOLVE = """
import json, signal, socket, sys
# Docker client cancellation alone can leave an exec process behind. A default
# SIGALRM also bounds the Linux child even while getaddrinfo is blocked in libc.
signal.signal(signal.SIGALRM, signal.SIG_DFL)
signal.setitimer(signal.ITIMER_REAL, float(sys.argv[2]))
try:
    addresses = socket.getaddrinfo(sys.argv[1], 443, type=socket.SOCK_STREAM)
except OSError as error:
    print(json.dumps(dict(success=False, error_type=type(error).__name__,
                         error_number=error.errno)))
else:
    print(json.dumps(dict(success=True, address_count=len({a[4][0] for a in addresses}))))
"""


def probe(hostname: str, container: str | None, timeout_seconds: float) -> dict:
    """Bound the entire child process, including a resolver or Docker hang."""
    command = (
        ["docker", "exec", container, "python"] if container else [sys.executable]
    ) + ["-c", RESOLVE, hostname, str(timeout_seconds)]
    started = time.monotonic()
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout_seconds, check=False,
        )
        if result.returncode in {-signal.SIGALRM, 128 + signal.SIGALRM}:
            outcome = {"success": False, "error_type": "lookup_deadline_exceeded"}
        elif result.returncode:
            outcome = {"success": False, "error_type": "probe_execution_failed"}
        else:
            outcome = json.loads(result.stdout)
    except subprocess.TimeoutExpired:
        outcome = {"success": False, "error_type": "lookup_deadline_exceeded"}
    except OSError as error:
        outcome = {"success": False, "error_type": type(error).__name__}
    except ValueError:
        outcome = {"success": False, "error_type": "invalid_probe_output"}
    return {
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "environment": f"container:{container}" if container else "host",
        "hostname": hostname,
        "duration_ms": round((time.monotonic() - started) * 1000, 2),
        **outcome,
    }


def resolver_state(interface: str) -> dict:
    """Read only the numeric upstream addresses; never emit raw command output."""
    state = {"interface": interface, "servers": [], "selected_server": None}
    try:
        result = subprocess.run(
            ["resolvectl", "status", interface], capture_output=True, text=True,
            timeout=1, check=False,
        )
        if result.returncode:
            raise ValueError("resolver state unavailable")
        collecting = False
        for line in result.stdout.splitlines():
            if "Current DNS Server:" in line:
                value = line.split("Current DNS Server:", 1)[1].strip()
                state["selected_server"] = str(ipaddress.ip_address(value))
            elif "DNS Servers:" in line:
                line = line.split("DNS Servers:", 1)[1]
                collecting = True
            elif not collecting:
                continue
            else:
                # Only bare addresses on continuation lines belong to this list.
                line = line.strip()
            if collecting:
                for value in line.split():
                    try:
                        address = str(ipaddress.ip_address(value))
                    except ValueError:
                        collecting = False
                        break
                    if address not in state["servers"]:
                        state["servers"].append(address)
        if not state["servers"]:
            state["error_type"] = "upstream_addresses_unavailable"
    except (OSError, ValueError, subprocess.TimeoutExpired):
        state["error_type"] = "resolver_state_unavailable"
    return state


def upstream_query(server: str, hostname: str, record_type: str) -> dict:
    """One bounded direct DNS query, with no retries and no application requests."""
    started = time.monotonic()
    outcome = {"server": server, "hostname": hostname, "record_type": record_type}
    try:
        result = subprocess.run(
            ["dig", "@" + server, hostname, record_type, "+time=1", "+tries=1",
             "+noall", "+comments", "+stats"],
            capture_output=True, text=True, timeout=1.5, check=False,
        )
        status = re.search(r"status: ([A-Z]+),", result.stdout)
        elapsed = re.search(r"Query time: (\d+) msec", result.stdout)
        outcome["response_status"] = status.group(1) if status else None
        outcome["query_ms"] = int(elapsed.group(1)) if elapsed else None
        if result.returncode or status is None:
            outcome["error_type"] = "dns_query_failed"
    except subprocess.TimeoutExpired:
        outcome["error_type"] = "dns_query_deadline_exceeded"
    except OSError:
        outcome["error_type"] = "dns_query_tool_unavailable"
    outcome["duration_ms"] = round((time.monotonic() - started) * 1000, 2)
    return outcome


def compare_upstreams(servers: list[str], hostnames: list[str]) -> list[dict]:
    # At most 12 queries, in three groups of four: under 4.5s of waiting. Limit
    # diagnostic traffic even when a caller selects many names or DNS servers.
    queries = [(server, host, kind) for server in servers[:3]
               for host in list(dict.fromkeys(hostnames))[:2] for kind in ("A", "AAAA")]
    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(lambda args: upstream_query(*args), queries))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", action="append", default=[])
    parser.add_argument("--hostnames", nargs="+", default=["api.schwabapi.com", "api.binance.com"])
    parser.add_argument("--timeout-seconds", type=float, default=3.0)
    parser.add_argument("--resolver-interface", help="Record systemd-resolved upstream selection")
    parser.add_argument("--compare-upstreams-on-failure", action="store_true")
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be finite and positive")
    if args.compare_upstreams_on_failure and not args.resolver_interface:
        parser.error("--compare-upstreams-on-failure requires --resolver-interface")
    before = resolver_state(args.resolver_interface) if args.resolver_interface else None
    if before is not None:
        print(json.dumps({"event": "dns_resolver_state_before", "state": before,
                          "timestamp": dt.datetime.now(dt.timezone.utc).isoformat()}), flush=True)
    failed = False
    compared = False
    for container in [None, *args.container]:
        for hostname in args.hostnames:
            result = probe(hostname, container, args.timeout_seconds)
            print(json.dumps(result), flush=True)
            failed |= not result["success"]
            if (not result["success"] and args.compare_upstreams_on_failure
                    and not compared):
                # Capture immediately: later probes may outlast a transient stall.
                at_failure = resolver_state(args.resolver_interface)
                servers = list(dict.fromkeys([*before["servers"], *at_failure["servers"]]))
                print(json.dumps({
                    "event": "dns_upstream_comparison",
                    "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "resolver_state_at_failure": at_failure,
                    "queried_servers": servers[:3], "omitted_servers": servers[3:],
                    "queried_hostnames": list(dict.fromkeys(args.hostnames))[:2],
                    "results": compare_upstreams(servers, args.hostnames),
                }), flush=True)
                compared = True
    if before is not None:
        after = resolver_state(args.resolver_interface)
        print(json.dumps({"event": "dns_resolver_state_after", "state": after,
                          "timestamp": dt.datetime.now(dt.timezone.utc).isoformat()}), flush=True)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
