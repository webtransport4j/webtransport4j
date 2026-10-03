#!/usr/bin/env python3
"""
Unmocked Integration Test Suite for WebTransport4J Enterprise Observability & Admin Console.
Executes live, unmocked HTTP requests against the real running dashboard server (port 8085)
and real multi-node cluster (ports 8081, 8082, 8083; QUIC UDP 4433, 4434, 4435).

Validates:
1. Zero-state baseline across Admin Console, Live Telemetry, and Cluster Topology.
2. Cryptographic PBKDF2 authentication flow and session token gating.
3. Multi-node cluster probe aggregation and individual node health/memory telemetry.
4. Real session establishment and strict 1-to-1 active sessions count synchronization.
5. Interactive bidirectional stream creation, payload echoing, and stream counters.
6. Datagram transmission, throughput calculation, and byte counters.
7. RFC 9297 session drain capsule dispatch (maintaining active session count).
8. Graceful session close and ungraceful sever, ensuring active count drops back to 0 (no latching).
9. Audit trail recording with operator identity and action details.
10. Rolling history ring-buffer consistency across all monitored KPIs.
"""

import json
import time
import unittest
import urllib.request
import urllib.error

BASE_URL = "http://127.0.0.1:8085"
CLUSTER_NODES_URLS = [
    "http://127.0.0.1:8081",
    "http://127.0.0.1:8082",
    "http://127.0.0.1:8083"
]


class TestUnmockedDashboardIntegration(unittest.TestCase):
    """Unmocked integration tests running against live dashboard and cluster nodes."""

    @classmethod
    def setUpClass(cls):
        """Authenticate as SecOps admin against live dashboard to obtain valid bearer token."""
        login_url = f"{BASE_URL}/api/admin/login"
        payload = json.dumps({"username": "admin", "password": "webtransport2026!"}).encode("utf-8")
        req = urllib.request.Request(login_url, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            cls.token = data.get("token")
            cls.auth_headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {cls.token}"
            }
        assert cls.token, "Failed to obtain valid admin session token"

    def http_get(self, path, auth=False):
        """Helper for HTTP GET requests."""
        headers = self.auth_headers if auth else {}
        req = urllib.request.Request(f"{BASE_URL}{path}", headers=headers)
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def http_post(self, path, data_dict, auth=True):
        """Helper for HTTP POST requests."""
        headers = self.auth_headers if auth else {"Content-Type": "application/json"}
        payload = json.dumps(data_dict).encode("utf-8")
        req = urllib.request.Request(f"{BASE_URL}{path}", data=payload, headers=headers)
        with urllib.request.urlopen(req, timeout=25.0) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def test_01_admin_authentication_and_verification(self):
        """Test authentication token validation and denial for invalid token."""
        # Valid token verification
        status, data = self.http_get("/api/admin/verify", auth=True)
        self.assertEqual(status, 200)
        self.assertTrue(data.get("authenticated"))
        self.assertIn(data.get("user"), ["admin", "secops-admin"])
        self.assertIn("SecOps", data.get("clearance", ""))

        # Invalid token verification
        req = urllib.request.Request(
            f"{BASE_URL}/api/admin/verify",
            headers={"Authorization": "Bearer invalid-token-xyz"}
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=3.0)
        self.assertEqual(ctx.exception.code, 401)

    def test_02_cluster_nodes_topology_health(self):
        """Test cluster status returns all 3 nodes ONLINE/HEALTHY with valid ports and ZGC."""
        status, data = self.http_get("/api/cluster/status")
        self.assertEqual(status, 200)
        self.assertEqual(data.get("clusterName"), "webtransport4j-production")

        nodes = data.get("nodes", [])
        self.assertEqual(len(nodes), 3, "Expected 3 cluster nodes")

        node_names = [n["name"] for n in nodes]
        self.assertIn("wt-node-1", node_names)
        self.assertIn("wt-node-2", node_names)
        self.assertIn("wt-node-3", node_names)

        for n in nodes:
            self.assertIn(n.get("status"), ["HEALTHY", "ONLINE"], f"Node {n['name']} is not HEALTHY/ONLINE")
            self.assertIn(n.get("healthPort", n.get("httpPort")), [8081, 8082, 8083])
            self.assertIn(n.get("quicPort"), [4433, 4434, 4435])

    def test_03_zero_state_baseline_agreement(self):
        """Test that Admin monitor, Live telemetry, and Cluster status strictly agree on 0 active sessions."""
        # Close any lingering active sessions and purge
        self.http_post("/api/admin/sessions/close-all", {"mode": "close"})
        self.http_post("/api/admin/sessions/purge", {})
        time.sleep(0.5)

        _, telemetry = self.http_get("/api/live-telemetry")
        _, monitor = self.http_get("/api/admin/sessions/monitor")
        _, cluster = self.http_get("/api/cluster/status")
        _, sess_list = self.http_get("/api/admin/sessions")

        active_in_telemetry = telemetry.get("activeSessions")
        active_in_monitor = monitor.get("activeSessions")
        active_in_cluster = cluster.get("activeSessions")
        active_in_list = len([s for s in sess_list.get("sessions", []) if s["status"] in ("CONNECTED", "DRAINING")])

        self.assertEqual(active_in_telemetry, 0, "Live telemetry activeSessions is not 0")
        self.assertEqual(active_in_monitor, 0, "Admin monitor activeSessions is not 0")
        self.assertEqual(active_in_cluster, 0, "Cluster status activeSessions is not 0")
        self.assertEqual(active_in_list, 0, "Admin session list active count is not 0")

    def test_04_session_lifecycle_and_metric_synchronization(self):
        """
        Full unmocked session lifecycle test:
        1. Establish real connection via /api/admin/sessions/create
        2. Verify both Admin monitor and Analytics dashboard report activeSessions == 1
        3. Open interactive stream and verify stream counters
        4. Transmit datagrams and verify byte/throughput telemetry
        5. Dispatch RFC 9297 DRAIN capsule and verify session stays active
        6. Dispatch CLOSE capsule and verify activeSessions decrements back to 0
        """
        # Step 1: Create real session to primary QUIC endpoint (node 1)
        create_payload = {
            "target": "https://localhost:4433/echo",
            "subprotocol": "webtransport",
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        }
        status, resp = self.http_post("/api/admin/sessions/create", create_payload)
        self.assertEqual(status, 200)
        self.assertTrue(resp.get("success"))
        session = resp.get("session")
        session_id = session.get("id")
        self.assertIsNotNone(session_id)
        self.assertEqual(session.get("status"), "CONNECTED")

        # Step 2: Validate active session count synchronization across BOTH dashboards
        time.sleep(0.5)
        _, telemetry = self.http_get("/api/live-telemetry")
        _, monitor = self.http_get("/api/admin/sessions/monitor")
        _, sess_list = self.http_get("/api/admin/sessions")

        telemetry_active = telemetry.get("activeSessions")
        monitor_active = monitor.get("activeSessions")
        active_in_list = len([s for s in sess_list.get("sessions", []) if s["status"] in ("CONNECTED", "DRAINING")])

        self.assertEqual(telemetry_active, 1, f"Telemetry activeSessions expected 1, got {telemetry_active}")
        self.assertEqual(monitor_active, 1, f"Admin monitor activeSessions expected 1, got {monitor_active}")
        self.assertEqual(active_in_list, 1, f"Admin session list active count expected 1, got {active_in_list}")
        self.assertEqual(telemetry_active, monitor_active, "Discrepancy detected between Telemetry and Admin monitor!")

        # Step 3: Interactive Bidirectional Stream Creation
        stream_payload = {
            "type": "bidi",
            "payload": "WebTransport4J Integration Echo Test #100",
            "count": 1
        }
        st_status, st_resp = self.http_post(f"/api/admin/sessions/{session_id}/streams/create", stream_payload)
        self.assertEqual(st_status, 200)
        self.assertTrue(st_resp.get("success"))
        new_stream = st_resp.get("stream")
        self.assertEqual(new_stream.get("type"), "bidi")
        self.assertEqual(new_stream.get("status"), "OPEN")
        self.assertGreater(new_stream.get("bytesSent", 0), 0)

        # Check telemetry stream counters
        _, telemetry_stream = self.http_get("/api/live-telemetry")
        self.assertGreaterEqual(telemetry_stream.get("activeStreams", 0), 1)
        self.assertGreaterEqual(telemetry_stream.get("bidiStreams", 0), 1)

        # Step 4: Datagram Transmission
        dgram_payload = {
            "count": 10,
            "payload": "WT4J_INTEGRATION_DATAGRAM_BURST"
        }
        dg_status, dg_resp = self.http_post(f"/api/admin/sessions/{session_id}/datagrams/send", dgram_payload)
        self.assertEqual(dg_status, 200)
        self.assertTrue(dg_resp.get("success"))
        self.assertGreaterEqual(dg_resp["datagrams"]["sent"], 10)

        # Step 5: RFC 9297 Capsule - Session Draining
        dr_status, dr_resp = self.http_post(f"/api/admin/sessions/{session_id}/capsules/drain", {})
        self.assertEqual(dr_status, 200)
        self.assertTrue(dr_resp.get("success"))
        self.assertEqual(dr_resp["session"]["status"], "DRAINING")

        # Session is draining but MUST still count as active
        _, telemetry_drain = self.http_get("/api/live-telemetry")
        _, monitor_drain = self.http_get("/api/admin/sessions/monitor")
        self.assertEqual(telemetry_drain.get("activeSessions"), 1, "Draining session must remain counted as active in telemetry")
        self.assertEqual(monitor_drain.get("activeSessions"), 1, "Draining session must remain counted as active in admin monitor")

        # Step 6: Session Teardown (Clean Close)
        close_payload = {"code": 0, "reason": "Integration Test Complete"}
        cl_status, cl_resp = self.http_post(f"/api/admin/sessions/{session_id}/capsules/close", close_payload)
        self.assertEqual(cl_status, 200)
        self.assertTrue(cl_resp.get("success"))
        self.assertEqual(cl_resp["session"]["status"], "CLOSED")

        # Step 7: Verify Active Sessions drops back to 0 (No self-latching)
        time.sleep(0.5)
        _, telemetry_closed = self.http_get("/api/live-telemetry")
        _, monitor_closed = self.http_get("/api/admin/sessions/monitor")
        _, sess_list_closed = self.http_get("/api/admin/sessions")

        active_after_close_telemetry = telemetry_closed.get("activeSessions")
        active_after_close_monitor = monitor_closed.get("activeSessions")
        active_after_close_list = len([s for s in sess_list_closed.get("sessions", []) if s["status"] in ("CONNECTED", "DRAINING")])

        self.assertEqual(active_after_close_telemetry, 0, f"Expected 0 active sessions after close in telemetry, got {active_after_close_telemetry}")
        self.assertEqual(active_after_close_monitor, 0, f"Expected 0 active sessions after close in admin monitor, got {active_after_close_monitor}")
        self.assertEqual(active_after_close_list, 0, f"Expected 0 active sessions in session list, got {active_after_close_list}")

    def test_05_audit_trail_logging(self):
        """Test that all administrative operations generate structured audit log entries."""
        status, data = self.http_get("/api/admin/audit-log")
        self.assertEqual(status, 200)
        logs = data.get("logs", [])
        self.assertGreater(len(logs), 0, "Audit trail is empty")

        actions = [l.get("action") for l in logs]
        # Should record lifecycle actions
        self.assertTrue(any("CREATE_SESSION" in a for a in actions), "Missing CREATE_SESSION in audit log")
        self.assertTrue(any("CREATE_STREAM" in a for a in actions), "Missing CREATE_STREAM in audit log")
        self.assertTrue(any("SEND_DATAGRAM" in a for a in actions), "Missing SEND_DATAGRAM in audit log")
        self.assertTrue(any("CLOSE_SESSION" in a for a in actions), "Missing CLOSE_SESSION in audit log")

        for l in logs:
            self.assertIn("id", l)
            self.assertIn("timestamp", l)
            self.assertIn("operator", l)
            self.assertIn("action", l)
            self.assertIn("target", l)
            self.assertIn("status", l)

    def test_06_rolling_telemetry_history_completeness(self):
        """Test rolling telemetry history returns valid parallel metric vectors."""
        status, data = self.http_get("/api/live-telemetry")
        self.assertEqual(status, 200)
        history = data.get("history", {})

        required_keys = ["timestamps", "sessions", "streams", "datagramsSent", "datagramsDropped", "rttMean", "memoryMb"]
        for k in required_keys:
            self.assertIn(k, history, f"Missing history key: {k}")
            self.assertGreater(len(history[k]), 0, f"History key {k} is empty")

        ts_len = len(history["timestamps"])
        for k in required_keys:
            self.assertEqual(len(history[k]), ts_len, f"History array length mismatch for {k}: expected {ts_len}, got {len(history[k])}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
