"""Developer helper to run and verify the app.

Usage (from the repo root, with the virtualenv active or via make targets):

    python -m scripts.dev start     # start infra + API + dashboard, then smoke-test
    python -m scripts.dev stop      # stop API + dashboard
    python -m scripts.dev status    # show what is running
    python -m scripts.dev smoke     # verify the API, dashboard, and database

`make.ps1 serve|stop|status|smoke` and `make serve|stop|status|smoke` wrap these.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import psutil
import requests

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
LOGS = ROOT / "logs"

API_PORT = 8000
DASHBOARD_PORT = 8501

API_CMD = [PY, "-m", "uvicorn", "api.app:app", "--host", "127.0.0.1", "--port", str(API_PORT)]
DASH_CMD = [
    PY,
    "-m",
    "streamlit",
    "run",
    "dashboard/app.py",
    "--server.headless=true",
    "--server.port",
    str(DASHBOARD_PORT),
]


def _port_listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _pids_on_port(port: int) -> set[int]:
    pids: set[int] = set()
    try:
        for conn in psutil.net_connections(kind="tcp"):
            if (
                conn.status == psutil.CONN_LISTEN
                and conn.laddr
                and conn.laddr.port == port
                and conn.pid
            ):
                pids.add(conn.pid)
    except (psutil.AccessDenied, PermissionError):
        print(f"  warning: cannot enumerate processes on :{port} (permission denied)")
    return pids


def _start_process(cmd: list[str], name: str) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    log = (LOGS / f"{name}.log").open("ab")
    kwargs = {"stdout": log, "stderr": log, "stdin": subprocess.DEVNULL, "cwd": str(ROOT)}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    subprocess.Popen(cmd, **kwargs)
    print(f"  started {name} (log: logs/{name}.log)")


def _wait_for(url: str, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=2).status_code < 500:
                return True
        except requests.RequestException:
            pass
        time.sleep(1)
    return False


def cmd_start(args: argparse.Namespace) -> int:
    if not args.no_infra:
        print("Ensuring infrastructure is up (docker compose up -d)...")
        result = subprocess.run(["docker", "compose", "up", "-d"], cwd=str(ROOT))
        if result.returncode != 0:
            print("  warning: could not start docker services (is Docker Desktop running?)")

    if _port_listening(API_PORT):
        print(f"  API already running on :{API_PORT}")
    else:
        _start_process(API_CMD, "api")

    if _port_listening(DASHBOARD_PORT):
        print(f"  dashboard already running on :{DASHBOARD_PORT}")
    else:
        _start_process(DASH_CMD, "dashboard")

    print("Waiting for services...")
    api_ok = _wait_for(f"http://127.0.0.1:{API_PORT}/healthz")
    dash_ok = _wait_for(f"http://127.0.0.1:{DASHBOARD_PORT}")

    print()
    print(f"  API docs : http://localhost:{API_PORT}/docs   [{'up' if api_ok else 'DOWN'}]")
    print(
        f"  Dashboard: http://localhost:{DASHBOARD_PORT}          [{'up' if dash_ok else 'DOWN'}]"
    )
    print()

    if not args.no_smoke:
        return cmd_smoke(args)
    return 0 if (api_ok and dash_ok) else 1


def cmd_stop(_: argparse.Namespace) -> int:
    stopped = 0
    for port, name in ((API_PORT, "api"), (DASHBOARD_PORT, "dashboard")):
        for pid in _pids_on_port(port):
            try:
                psutil.Process(pid).terminate()
                print(f"  stopped {name} (pid {pid})")
                stopped += 1
            except psutil.Error:
                pass
    if stopped == 0:
        print("  nothing to stop")
    return 0


def cmd_status(_: argparse.Namespace) -> int:
    for port, name in ((API_PORT, "api"), (DASHBOARD_PORT, "dashboard")):
        state = "up" if _port_listening(port) else "down"
        print(f"  {name:<10} :{port}  {state}")
    return 0


def cmd_smoke(args: argparse.Namespace) -> int:
    base = f"http://127.0.0.1:{API_PORT}"
    checks: list[tuple[str, bool]] = []

    def check(name: str, fn) -> None:
        try:
            checks.append((name, bool(fn())))
        except Exception as exc:  # noqa: BLE001
            print(f"  FAIL {name}: {exc}")
            checks.append((name, False))

    check(
        "api healthz",
        lambda: requests.get(f"{base}/healthz", timeout=10).json() == {"status": "ok"},
    )
    check(
        "api revenue rows",
        lambda: (
            len(
                requests.get(
                    f"{base}/metrics/revenue?start_date=2025-01-01&end_date=2025-04-30", timeout=15
                ).json()
            )
            > 0
        ),
    )
    check(
        "api demand (no filters)",
        lambda: requests.get(f"{base}/metrics/demand", timeout=15).status_code == 200,
    )
    check(
        "api weather-impact (no condition)",
        lambda: requests.get(f"{base}/metrics/weather-impact", timeout=15).status_code == 200,
    )
    check(
        "dashboard reachable",
        lambda: requests.get(f"http://127.0.0.1:{DASHBOARD_PORT}", timeout=10).status_code == 200,
    )
    check("database marts have rows", _db_has_rows)

    ok = all(passed for _, passed in checks)
    for name, passed in checks:
        print(f"  {'PASS' if passed else 'FAIL'} {name}")
    print()
    print("SMOKE OK" if ok else "SMOKE FAILED")
    return 0 if ok else 1


def _db_has_rows() -> bool:
    from pipeline.config import postgres_dsn
    from sqlalchemy import create_engine, text

    engine = create_engine(postgres_dsn().replace("postgresql://", "postgresql+psycopg://", 1))
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM marts.daily_demand")).scalar()
    return bool(count and count > 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run and verify the taxi analytics app.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_start = sub.add_parser("start", help="start infra, API, and dashboard")
    p_start.add_argument("--no-infra", action="store_true", help="do not run docker compose up")
    p_start.add_argument("--no-smoke", action="store_true", help="skip smoke checks")
    p_start.set_defaults(func=cmd_start)

    sub.add_parser("stop", help="stop API and dashboard").set_defaults(func=cmd_stop)
    sub.add_parser("status", help="show running services").set_defaults(func=cmd_status)
    sub.add_parser("smoke", help="verify API, dashboard, and database").set_defaults(func=cmd_smoke)

    args = parser.parse_args()
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    main()
