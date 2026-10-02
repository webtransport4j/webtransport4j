#!/usr/bin/env python3
import json
import sys
import time
import urllib.request
import urllib.error

BASE_URL = "http://localhost:8085"
NODE_URL = "http://localhost:8080"

passed = 0
failed = 0

def test(name, condition, extra=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ PASS: {name} {extra}")
    else:
        failed += 1
        print(f"  ❌ FAIL: {name} {extra}")

def http_get(path, token=None):
    req = urllib.request.Request(f"{BASE_URL}{path}")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, resp.read().decode('utf-8')

def http_post(path, data, token=None):
    payload = json.dumps(data).encode('utf-8')
    req = urllib.request.Request(f"{BASE_URL}{path}", data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, json.loads(resp.read().decode('utf-8'))

print("\n=======================================================")
print("  WebTransport4J Admin Studio & Telemetry Verification  ")
print("=======================================================\n")

# 1. HTML Pages
print("--- 1. Static Web Assets & UI Availability ---")
s, h = http_get("/index.html")
test("Dashboard HTML accessible", s == 200 and "WebTransport4J" in h)
s, h = http_get("/admin.html")
test("Admin Console HTML accessible", s == 200 and "Mission Control" in h)

# 2. Cluster Node Probes
print("\n--- 2. Live Cluster Node APIs (Port 8080) ---")
with urllib.request.urlopen(f"{NODE_URL}/api/node/info", timeout=3) as resp:
    info = json.loads(resp.read().decode('utf-8'))
    test("Node Health Status", info.get("status") == "HEALTHY", f"Status: {info.get('status')}")
    test("JVM Runtime", "25" in info.get("jvm", {}).get("version", ""), f"Version: {info.get('jvm', {}).get('version')}")
    test("Server Started Flag", info.get("isStarted") is True)

# 3. Authentication
print("\n--- 3. PBKDF2 Authenticated Operator Authority ---")
status, login_res = http_post("/api/admin/login", {"username": "secops-admin", "password": "webtransport2026!"})
token = login_res.get("token")
test("PBKDF2 Password Verification", login_res.get("success") is True and token is not None)
test("Operator Authority Clearance", "Tier-3" in login_res.get("clearance", ""))

status, verify_res = http_post("/api/admin/verify", {}, token=token)
test("Token Authorization Check", verify_res.get("authenticated") is True)

# 4. Session Lifecycle (Connect, Query, Details)
print("\n--- 4. Real QUIC/WebTransport Session Lifecycle ---")
status, conn_res = http_post("/api/admin/sessions/create", {
    "target": "https://localhost:4433/echo",
    "subprotocol": "webtransport"
}, token=token)
sess = conn_res.get("session", {})
sess_id = sess.get("id", "wt-sess-0")
test("Create Managed Session", conn_res.get("success") is True and "wt-" in sess_id, f"Session: {sess_id}")
test("Session Remote Endpoint", sess.get("remoteEndpoint") is not None, f"Endpoint: {sess.get('remoteEndpoint')}")
test("Session Flow Control Limits", sess.get("flowControl", {}).get("maxData", 0) > 0)

status, sess_list_res = http_get("/api/admin/sessions", token=token)
sess_list = json.loads(sess_list_res).get("sessions", [])
test("List Active Sessions", any(s.get("id") == sess_id for s in sess_list), f"Count: {len(sess_list)}")

status, sess_details = http_get(f"/api/admin/sessions/{sess_id}", token=token)
details = json.loads(sess_details)
test("Inspect Session Details", details.get("id") == sess_id and details.get("status") == "CONNECTED")

# 5. Streams Studio (Open, Send, FIN)
print("\n--- 5. Streams Studio Operations ---")
status, st_res = http_post(f"/api/admin/sessions/{sess_id}/streams/create", {
    "type": "bidi",
    "payload": "Automated Verification Ping",
    "count": 1
}, token=token)
stream = st_res.get("stream", {})
stream_id = stream.get("streamId", 4)
test("Open Bidirectional Stream", st_res.get("success") is True and stream.get("status") == "OPEN", f"Stream ID: #{stream_id}")
test("Stream Echo Response", st_res.get("response") == "Automated Verification Ping")

status, send_res = http_post(f"/api/admin/sessions/{sess_id}/streams/{stream_id}/send", {
    "payload": "Second Data Frame"
}, token=token)
test("Transmit Data Frame", send_res.get("success") is True and send_res.get("response") == "Second Data Frame")

status, close_st_res = http_post(f"/api/admin/sessions/{sess_id}/streams/{stream_id}/close", {}, token=token)
test("Clean Half-Close (FIN)", close_st_res.get("success") is True and close_st_res.get("stream", {}).get("status") == "CLOSED")

# 6. Datagram Studio
print("\n--- 6. Datagram Studio Operations ---")
status, dg_res = http_post(f"/api/admin/sessions/{sess_id}/datagrams/send", {
    "payload": "UNRELIABLE_TELEMETRY_SAMPLE",
    "count": 25,
    "size": 128
}, token=token)
test("Send Datagram Batch", dg_res.get("success") is True and dg_res.get("datagrams", {}).get("sent", 0) >= 25)

# 7. Wire Trace
print("\n--- 7. RFC 9297 Wire Trace Events ---")
status, wire_res = http_get(f"/api/admin/sessions/{sess_id}/wire", token=token)
wire_events = json.loads(wire_res).get("wireEvents", [])
test("Capture Wire Events", len(wire_events) >= 3, f"Events Count: {len(wire_events)}")

# 8. Capsules & Teardown
print("\n--- 8. RFC 9297 Session Capsules ---")
status, drain_res = http_post(f"/api/admin/sessions/{sess_id}/capsules/drain", {}, token=token)
test("WT_DRAIN_SESSION Capsule (0x78ae)", drain_res.get("success") is True and drain_res.get("session", {}).get("status") == "DRAINING")

status, close_sess_res = http_post(f"/api/admin/sessions/{sess_id}/capsules/close", {
    "code": 0,
    "reason": "Verification Complete"
}, token=token)
test("CLOSE_WEBTRANSPORT_SESSION (0x2843)", close_sess_res.get("success") is True and close_sess_res.get("session", {}).get("status") == "CLOSED")

# 9. Real Traffic Execution Engine
print("\n--- 9. Real Traffic Engine & Chaos Injection ---")
status, tf_handshake = http_post("/api/admin/execute-traffic", {
    "command": "handshake",
    "target": "https://localhost:4433/echo"
}, token=token)
test("Traffic Engine Handshake", tf_handshake.get("result", {}).get("status") == "SUCCESS", f"RTT: {tf_handshake.get('result', {}).get('rttMs')}ms")

status, tf_dgrams = http_post("/api/admin/execute-traffic", {
    "command": "datagrams",
    "target": "https://localhost:4433/echo",
    "count": 50,
    "size": 512,
    "pps": 2000
}, token=token)
test("Traffic Engine Datagrams", tf_dgrams.get("result", {}).get("status") == "SUCCESS", f"Sent: {tf_dgrams.get('result', {}).get('sent')}")

status, tf_chaos = http_post("/api/admin/execute-traffic", {
    "command": "chaos-burst",
    "target": "https://localhost:4433/echo",
    "count": 200
}, token=token)
test("Chaos Datagram Burst", tf_chaos.get("result", {}).get("status") == "SUCCESS", f"Sent: {tf_chaos.get('result', {}).get('sent')}")

# 10. Live Telemetry & OpenTelemetry Verification
print("\n--- 10. Live Telemetry & Distributed Tracing ---")
status, telem_res = http_get("/api/live-telemetry")
telem = json.loads(telem_res)
test("Active Direct Memory", telem.get("nettyDirectMemoryMb", 0) > 0, f"{telem.get('nettyDirectMemoryMb')} MB")
test("Mean RTT Latency", telem.get("quicRttMeanMs", 0) > 0, f"{telem.get('quicRttMeanMs')} ms")
test("P99 Rtt Latency", telem.get("quicRttP99Ms", 0) > 0, f"{telem.get('quicRttP99Ms')} ms")
test("Handshake Latency", telem.get("sessionHandshakeLatencyMs", 0) > 0, f"{telem.get('sessionHandshakeLatencyMs')} ms")
test("Datagram Throughput", telem.get("datagramThroughputMbps", 0) > 0, f"{telem.get('datagramThroughputMbps')} Mbps")
test("Total Processed Datagrams", telem.get("totalDatagramsProcessed", 0) >= 275, f"{telem.get('totalDatagramsProcessed')} dgrams")
test("W3C Distributed Traces", len(telem.get("traces", [])) >= 3, f"Traces recorded: {len(telem.get('traces', []))}")

# 11. Cluster Status
print("\n--- 11. Cluster Topology Status ---")
status, cluster_res = http_get("/api/cluster/status")
cluster = json.loads(cluster_res)
test("Cluster Topology Nodes", len(cluster.get("nodes", [])) > 0)
test("Node Health Role", cluster.get("nodes", [])[0].get("role") == "Primary Gateway")

# 12. Audit Trail
print("\n--- 12. Enterprise Audit Trail ---")
status, audit_res = http_get("/api/admin/audit-log", token=token)
logs = json.loads(audit_res).get("logs", [])
test("Audit Log Retention", len(logs) >= 5, f"Logged Actions: {len(logs)}")

print("\n=======================================================")
print(f"  Summary: {passed} PASSED, {failed} FAILED")
print("=======================================================\n")

if failed > 0:
    sys.exit(1)
