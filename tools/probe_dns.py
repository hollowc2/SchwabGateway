"""One bounded DNS snapshot from the host and selected running containers.

Run on Helios with: python3 tools/probe_dns.py --container schwab_gateway_live
Prints JSON lines and exits nonzero if any lookup fails. No HTTP requests, token
access, retries, configuration changes or recurring jobs are performed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import signal
import subprocess
import sys
import time

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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", action="append", default=[])
    parser.add_argument("--hostnames", nargs="+", default=["api.schwabapi.com", "api.binance.com"])
    parser.add_argument("--timeout-seconds", type=float, default=3.0)
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be finite and positive")
    failed = False
    for container in [None, *args.container]:
        for hostname in args.hostnames:
            result = probe(hostname, container, args.timeout_seconds)
            print(json.dumps(result), flush=True)
            failed |= not result["success"]
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
