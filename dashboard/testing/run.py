#!/usr/bin/env python3
"""Start, verify, inspect, or stop the isolated production-portal test stack."""
import argparse
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
RUNTIME = HERE / ".runtime"


def command(args, capture=False, env=None):
    result = subprocess.run(args, cwd=ROOT, env=env, text=True,
                            stdout=subprocess.PIPE if capture else None,
                            stderr=subprocess.STDOUT if capture else None)
    if result.returncode:
        if capture:
            if RUNTIME.exists():
                (RUNTIME / "last-failure.log").write_text(result.stdout, encoding="utf-8")
            print(result.stdout)
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(args)}")
    return result.stdout if capture else ""


def prepare(port):
    RUNTIME.mkdir(parents=True, exist_ok=True)
    env_file = RUNTIME / "test.env"
    if not env_file.exists():
        values = {
            "TEST_ADMIN_PASSWORD": secrets.token_urlsafe(24),
            "TEST_NODE_TOKEN": secrets.token_urlsafe(32),
            "TEST_OTLP_TOKEN": secrets.token_urlsafe(32),
            "TEST_HMAC_KEY": secrets.token_hex(32),
            "TEST_PORTAL_PORT": str(port),
        }
        env_file.write_text("".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8")
        env_file.chmod(0o600)
        credentials = RUNTIME / "credentials.json"
        credentials.write_text(json.dumps({"url": f"https://localhost:{port}/admin.html",
                                          "username": "test-operator",
                                          "password": values["TEST_ADMIN_PASSWORD"]}, indent=2), encoding="utf-8")
        credentials.chmod(0o600)
    if not (RUNTIME / "tls.crt").exists() or not (RUNTIME / "tls.key").exists():
        openssl = shutil.which("openssl")
        if not openssl and os.name == "nt":
            candidate = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Git/usr/bin/openssl.exe"
            if candidate.exists():
                openssl = str(candidate)
        if not openssl:
            raise RuntimeError("OpenSSL is required to generate test certificates (Git for Windows includes it).")
        command([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                 "-keyout", str(RUNTIME / "tls.key"), "-out", str(RUNTIME / "tls.crt"),
                 "-days", "30", "-subj", "/CN=localhost",
                 "-addext", "subjectAltName=DNS:localhost,DNS:portal,DNS:node-1,DNS:node-2,DNS:node-3,IP:127.0.0.1"], capture=True)
        # Test-only certificate/key must be readable by UID 10001 in Linux containers.
        (RUNTIME / "tls.crt").chmod(0o644)
        (RUNTIME / "tls.key").chmod(0o644)


def compose(*args, capture=False):
    return command(["docker", "compose", "--env-file", str(RUNTIME / "test.env"),
                    "-f", str(HERE / "compose.yml"), *args], capture=capture)


def unit_checks():
    env = dict(os.environ, WT4J_ENV="development")
    for name in ("test_dashboard_unit.py", "test_production_security.py"):
        output = command([sys.executable, str(HERE.parent / name)], capture=True, env=env)
        (RUNTIME / f"{name}.log").write_text(output, encoding="utf-8")
        print(f"PASS {name}", flush=True)
    node = shutil.which("node")
    if not node:
        bundled = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node.exe"
        if bundled.exists():
            node = str(bundled)
    if not node:
        raise RuntimeError("Node.js is required for frontend syntax checks.")
    for name in ("app.js", "admin.js"):
        command([node, "--check", str(HERE.parent / name)])
        print(f"PASS JavaScript syntax: {name}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("up", "test", "status", "logs", "down", "reset"))
    parser.add_argument("--port", type=int, default=18443, help="Host HTTPS port when first creating the test environment")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    prepare(args.port)
    if args.action in ("up", "test"):
        unit_checks()
    if args.action == "up":
        compose("config", "--quiet")
        print("Building Java test nodes and production portal (first run downloads dependencies)...", flush=True)
        build_log = compose("build", "node-1", "portal", capture=True)
        (RUNTIME / "build.log").write_text(build_log, encoding="utf-8")
        print("Starting the isolated stack and waiting for health checks...", flush=True)
        startup_log = compose("up", "-d", "--no-build", "--force-recreate", "--wait", "--wait-timeout", "180", capture=True)
        (RUNTIME / "startup.log").write_text(startup_log, encoding="utf-8")
    if args.action in ("up", "test"):
        print("Running live HTTPS, QUIC, management, and analytics checks...", flush=True)
        output = compose("--profile", "verify", "run", "--rm", "verify", capture=True)
        (RUNTIME / "integration-test.log").write_text(output, encoding="utf-8")
        print(output, flush=True)
        print(f"Test credentials: {RUNTIME / 'credentials.json'}")
        print(f"Reports and test TLS certificate: {RUNTIME}")
    elif args.action == "status":
        compose("ps")
    elif args.action == "logs":
        compose("logs", "--tail", "100")
    elif args.action in ("down", "reset"):
        compose("down", "--remove-orphans", *( ["--volumes"] if args.action == "reset" else []))
        if args.action == "reset":
            # Delete only the known runtime directory under this script's directory.
            if RUNTIME.resolve().parent != HERE.resolve():
                raise RuntimeError("Refusing to remove an unexpected runtime path")
            shutil.rmtree(RUNTIME)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError) as error:
        print(f"FAILED: {error}", file=sys.stderr)
        sys.exit(1)
