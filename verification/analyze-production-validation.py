"""Summarize server-only CSV/JFR evidence; allocation samples are estimates, not zero-GC proof."""
import collections
import csv
import datetime
import json
import pathlib
import re
import subprocess
import sys


def analyze(directory):
    root = pathlib.Path(directory)
    duration = int(root.name.split("-")[1])
    with (root / "server.csv").open() as source:
        rows = [{key: float(value) for key, value in row.items()} for row in csv.DictReader(source)]
    measured = [row for row in rows if 10 <= row["seconds"] <= duration]
    if len(measured) < 2:
        raise ValueError("Need at least two steady-state samples after ten-second warmup")
    first, last = measured[0], measured[-1]
    payload_units = (last["bytes"] - first["bytes"]) / 256
    allocations = last["allocated_sampled_cumulative"] - first["allocated_sampled_cumulative"]
    raw = subprocess.check_output([
        "jfr", "print", "--json", "--events", "jdk.ObjectAllocationSample",
        "--stack-depth", "12", str(root / "server.jfr")], text=True)
    events = json.loads(raw)["recording"]["events"]
    types = collections.Counter()
    stacks = collections.Counter()
    sample_count = 0
    for event in events:
        values = event["values"]
        time_ms = datetime.datetime.fromisoformat(values["startTime"].replace("Z", "+00:00")).timestamp() * 1000
        if not first["epoch_ms"] <= time_ms <= last["epoch_ms"]:
            continue
        weight = values["weight"]
        types[values["objectClass"]["name"]] += weight
        sample_count += 1
        frames = (values.get("stackTrace") or {}).get("frames", [])
        application = "[JDK/Netty or truncated stack]"
        for frame in frames:
            method = frame["method"]
            name = method["type"]["name"].replace("/", ".")
            if name.startswith("io.github.webtransport4j"):
                application = name + "." + method["name"]
                break
        stacks[application] += weight
    logs = {name: (root / (name + ".log")).read_text() for name in ("soak", "attack", "control")}
    health = [dict(zip(("successes", "failures", "p99_us"), map(int, match)))
              for text in logs.values()
              for match in re.findall(r"HEALTH successes=(\d+) failures=(\d+) p99_us=(\d+)", text)]
    result = {
        "directory": str(root), "soak_seconds": duration,
        "measurement_seconds": last["seconds"] - first["seconds"],
        "payload_256_byte_equivalents": payload_units,
        "sampled_thread_allocation_bytes": allocations,
        "sampled_thread_bytes_per_payload_equivalent": allocations / payload_units,
        "jfr_allocation_samples": sample_count,
        "jfr_weighted_allocation_bytes": sum(types.values()),
        "top_allocated_classes": types.most_common(10),
        "top_application_allocation_sites": stacks.most_common(10),
        "heap_max_bytes": max(row["heap_used"] for row in measured),
        "heap_first_half_min_bytes": min(row["heap_used"] for row in measured[:len(measured)//2]),
        "heap_second_half_min_bytes": min(row["heap_used"] for row in measured[len(measured)//2:]),
        "idle_final_heap_bytes": rows[-1]["heap_used"],
        "gc_count_during_measurement": last["gc_count"] - first["gc_count"],
        "gc_ms_during_measurement": last["gc_ms"] - first["gc_ms"],
        "server_payload_bytes": rows[-1]["bytes"],
        "server_datagrams": rows[-1]["datagrams"],
        "server_payload_errors": rows[-1]["errors"],
        "final_sessions": rows[-1]["sessions"],
        "final_active_children": rows[-1]["active_children"],
        "transport_uni_created": rows[-1]["transport_uni_created"],
        "transport_uni_closed": rows[-1]["transport_uni_closed"],
        "final_retained_quic_uni": rows[-1]["retained_quic_uni"],
        "peak_retained_quic_uni": max(row["retained_quic_uni"] for row in rows),
        "netty_direct_max_bytes": max(row["netty_direct"] for row in rows),
        "health": health,
        "soak_summary": re.findall(r"SOAK_COMPLETE.*", logs["soak"]),
        "attack_summary": re.findall(r"ATTACK repeat=.*", logs["attack"]),
        "control_summary": re.findall(r"ATTACK repeat=.*", logs["control"]),
    }
    if result["server_payload_errors"] or result["final_sessions"] or result["final_retained_quic_uni"]:
        raise AssertionError("Server did not recover cleanly")
    if result["transport_uni_created"] != result["transport_uni_closed"]:
        raise AssertionError("Transport child close accounting mismatch")
    if len(health) != 3 or any(item["failures"] for item in health):
        raise AssertionError("Missing or failed health checks")
    (root / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    for argument in sys.argv[1:]:
        print(json.dumps(analyze(argument), indent=2))
