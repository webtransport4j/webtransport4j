#!/usr/bin/env python3
"""Run bounded TLC safety/liveness checks and known-broken negative controls."""
import argparse
import hashlib
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
CHECKSUM = "c2fe4e56e43bde19f213b4a7e441d037297fda733e503579623e859b79348239"


def run(jar, output, label, model, config, expected=None, diagnostic=False):
    work = output / label
    work.mkdir(parents=True, exist_ok=True)
    cfg = work / (model + ".cfg")
    cfg.write_text(config, encoding="utf-8")
    result = subprocess.run(
        ["java", "-cp", str(jar), "tlc2.TLC", "-workers", "1", "-noGenerateSpecTE",
         "-metadir", str(work / "states"), "-config", str(cfg), model + ".tla"],
        cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, timeout=120,
    )
    (work / "tlc.log").write_text(result.stdout, encoding="utf-8")
    if expected:
        passed = result.returncode != 0 and f"Invariant {expected} is violated" in result.stdout
    else:
        passed = result.returncode == 0 and "Model checking completed. No error has been found." in result.stdout
    if not passed:
        print(result.stdout, file=sys.stderr)
        raise RuntimeError(f"Unexpected TLC result for {label}; see {work / 'tlc.log'}")
    counts = re.findall(r"\d+ states generated, \d+ distinct states found", result.stdout)
    detail = "detected " + expected if expected else counts[-1] if counts else "complete"
    verdict = "EXPECTED COUNTEREXAMPLE" if diagnostic else "PASS"
    print(f"{verdict} {label}: {detail}", flush=True)


def additional_models(jar, output):
    cases = [
        ("Mailbox", "mailbox-datagrams", "SingleRelease = TRUE", "SingleRelease = FALSE", "NoDoubleRelease"),
        ("ReactivePublisher", "publisher-contract", "SingleTerminal = TRUE", "SingleTerminal = FALSE", "SingleTerminalSignal"),
        ("TlsReload", "tls-guarded", "CheckRunning = TRUE", "CheckRunning = FALSE", "StoppedHasNoContext"),
        ("FlowControl", "flow-atomic", "AtomicReservation = TRUE", "AtomicReservation = FALSE", "CreditBound"),
        ("AsyncMetrics", "metrics", "EnforceCapacity = TRUE", "EnforceCapacity = FALSE", "CapacityBound"),
        ("ConfigReload", "config-guarded", "AtomicPublish = TRUE", "AtomicPublish = FALSE", "PublishedSnapshotCoherent"),
    ]
    for model, label, old, new, invariant in cases:
        config = (HERE / (model + ".cfg")).read_text(encoding="utf-8")
        run(jar, output, label, model, config)
        negative_label = {"TlsReload": "tls-shutdown-race",
                          "FlowControl": "flow-check-increment-race"}.get(model, label + "-negative")
        run(jar, output, negative_label, model, config.replace(old, new), invariant,
            diagnostic=model in {"TlsReload", "FlowControl"})
    mailbox = (HERE / "Mailbox.cfg").read_text(encoding="utf-8")
    run(jar, output, "mailbox-streams", "Mailbox", mailbox.replace("Bounded = TRUE", "Bounded = FALSE"))
    run(jar, output, "mailbox-lost-wakeup", "Mailbox", mailbox.replace("Recheck = TRUE", "Recheck = FALSE"), "NoStrandedWork")
    publisher = (HERE / "ReactivePublisher.cfg").read_text(encoding="utf-8")
    run(jar, output, "publisher-terminal-race", "ReactivePublisher",
        publisher.replace("RecheckEmit = TRUE", "RecheckEmit = FALSE"), "NoPostTerminalQueue", diagnostic=True)
    run(jar, output, "publisher-demand-negative", "ReactivePublisher",
        publisher.replace("RespectDemand = TRUE", "RespectDemand = FALSE"), "DemandRespected")
    tls = (HERE / "TlsReload.cfg").read_text(encoding="utf-8")
    run(jar, output, "tls-stale-reload", "TlsReload",
        tls.replace("CheckGeneration = TRUE", "CheckGeneration = FALSE"), "MonotonicPublication", diagnostic=True)
    config = (HERE / "ConfigReload.cfg").read_text(encoding="utf-8")
    run(jar, output, "config-stale-reload", "ConfigReload",
        config.replace("CheckGeneration = TRUE", "CheckGeneration = FALSE"), "NoStalePublication", diagnostic=True)
    run(jar, output, "config-mixed-reader", "ConfigReload",
        config.replace("SnapshotRead = TRUE", "SnapshotRead = FALSE"), "ReaderSnapshotCoherent", diagnostic=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", required=True, type=Path, help="Official tla2tools.jar v1.8.0")
    parser.add_argument("--output", type=Path, help="Directory to retain logs and counterexamples")
    args = parser.parse_args()
    jar = args.jar.resolve()
    if not shutil.which("java"):
        parser.error("Java is required")
    if not jar.is_file() or hashlib.sha256(jar.read_bytes()).hexdigest() != CHECKSUM:
        parser.error("Provide the official v1.8.0 tla2tools.jar matching the documented SHA-256")
    with tempfile.TemporaryDirectory(prefix="wt4j-tlc-") as temporary:
        output = args.output.resolve() if args.output else Path(temporary)
        additional_models(jar, output)
        lifecycle = (HERE / "ServerLifecycle.cfg").read_text(encoding="utf-8")
        admission = (HERE / "SessionAdmission.cfg").read_text(encoding="utf-8")
        run(jar, output, "lifecycle", "ServerLifecycle", lifecycle)
        run(jar, output, "admission", "SessionAdmission", admission)
        run(jar, output, "global-capacity", "SessionAdmission",
            admission.replace("GlobalLimit = 2", "GlobalLimit = 1")
                     .replace("ConnectionLimit = 1", "ConnectionLimit = 2"))
        run(jar, output, "broken-epoch", "ServerLifecycle",
            lifecycle.replace("CheckEpoch = TRUE", "CheckEpoch = FALSE"), "NoStalePublication")
        run(jar, output, "broken-completion", "SessionAdmission",
            admission.replace("SingleWinner = TRUE", "SingleWinner = FALSE"), "Capacity")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"FAILED: {error}", file=sys.stderr)
        sys.exit(1)
