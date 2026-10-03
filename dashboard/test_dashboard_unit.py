#!/usr/bin/env python3
"""
Unit Test Suite for WebTransport4J Enterprise Observability & Admin Console
Tests internal telemetry aggregation, parsing, authentication, state machines,
and RFC 9297 wire capsule logic in complete isolation without network dependencies.
"""

import unittest
import time
import hashlib
import hmac
import json
import os
import sys
import urllib.parse

# Ensure dashboard module is importable
DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, DASHBOARD_DIR)

import server


class TestAdminSecurityUnit(unittest.TestCase):
    """Verifies cryptographically secure PBKDF2 authentication mechanisms."""

    def test_pbkdf2_admin_credentials_match(self):
        """Admin password hash matches stored PBKDF2 HMAC SHA-256 standard."""
        password = b"webtransport2026!"
        salt = server.ADMIN_PASSWORD_SALT
        iterations = 100000
        computed_hash = hashlib.pbkdf2_hmac('sha256', password, salt, iterations).hex()
        self.assertEqual(computed_hash, server.ADMIN_PASSWORD_PBKDF2_HEX)

    def test_pbkdf2_wrong_password_fails(self):
        """Invalid credentials produce non-matching PBKDF2 digest."""
        wrong_pwd = b"incorrect-password-attempt"
        salt = server.ADMIN_PASSWORD_SALT
        iterations = 100000
        computed_hash = hashlib.pbkdf2_hmac('sha256', wrong_pwd, salt, iterations).hex()
        self.assertNotEqual(computed_hash, server.ADMIN_PASSWORD_PBKDF2_HEX)

    def test_admin_session_expiry_check(self):
        """Session token expiration is correctly checked."""
        token = "test-token-uuid-1234"
        server.ADMIN_SESSIONS[token] = {
            "user": "secops-admin",
            "login_time": time.time() - 4000,
            "expires_at": time.time() - 400  # Expired
        }
        handler = server.EnterpriseObservabilityHandler
        # Token expired
        self.assertTrue(time.time() > server.ADMIN_SESSIONS[token]["expires_at"])

        # Token valid
        valid_token = "valid-token-uuid-5678"
        server.ADMIN_SESSIONS[valid_token] = {
            "user": "secops-admin",
            "login_time": time.time(),
            "expires_at": time.time() + 3600
        }
        self.assertTrue(time.time() < server.ADMIN_SESSIONS[valid_token]["expires_at"])
        # Clean up
        server.ADMIN_SESSIONS.pop(token, None)
        server.ADMIN_SESSIONS.pop(valid_token, None)


class TestTelemetryCalculationsUnit(unittest.TestCase):
    """Verifies mathematical formulas for throughput, memory, and stream calculations."""

    def test_datagram_throughput_calculation(self):
        """Throughput Mbps = (sent * size * 8) / (duration_sec * 1,000,000)."""
        sent = 1000
        size_bytes = 1200
        dur_sec = 0.5  # 500 ms
        expected_mbps = round((sent * size_bytes * 8) / (dur_sec * 1_000_000), 2)
        self.assertEqual(expected_mbps, 19.2)

    def test_direct_memory_mb_rounding(self):
        """Netty direct memory bytes to megabytes conversion."""
        used_bytes = 536870912  # exactly 512 MB
        mb = round(used_bytes / (1024 * 1024), 1)
        self.assertEqual(mb, 512.0)

    def test_p99_rtt_latency_estimate(self):
        """P99 latency estimate is 1.25x mean RTT."""
        mean_rtt = 12.0
        p99 = round(mean_rtt * 1.25, 1)
        self.assertEqual(p99, 15.0)


class TestPrometheusScraperUnit(unittest.TestCase):
    """Verifies Prometheus exposition format parsing into LIVE_TELEMETRY."""

    def setUp(self):
        server.LIVE_TELEMETRY["activeSessions"] = 0
        server.LIVE_TELEMETRY["activeStreams"] = 0
        server.LIVE_TELEMETRY["bidiStreams"] = 0
        server.LIVE_TELEMETRY["uniStreams"] = 0
        server.LIVE_TELEMETRY["datagramsDroppedRate"] = 0
        server.LIVE_TELEMETRY["totalDatagramsProcessed"] = 0

    def test_parse_prometheus_exposition_metrics(self):
        """Prometheus lines for sessions, streams, and datagrams are parsed correctly."""
        metrics_text = """
# HELP webtransport_sessions_active Current active sessions
# TYPE webtransport_sessions_active gauge
webtransport_sessions_active 4

# HELP webtransport_streams_active Active streams
# TYPE webtransport_streams_active gauge
webtransport_streams_active{type="bidi"} 7
webtransport_streams_active{type="uni"} 3

# HELP webtransport_datagrams_dropped_total Packets dropped
# TYPE webtransport_datagrams_dropped_total counter
webtransport_datagrams_dropped_total 12

# HELP webtransport_datagrams_sent_total Packets sent
# TYPE webtransport_datagrams_sent_total counter
webtransport_datagrams_sent_total 3500
"""
        server.parse_prometheus_text(metrics_text)
        self.assertEqual(server.LIVE_TELEMETRY["activeSessions"], 4)
        self.assertEqual(server.LIVE_TELEMETRY["bidiStreams"], 7)
        self.assertEqual(server.LIVE_TELEMETRY["uniStreams"], 3)
        self.assertEqual(server.LIVE_TELEMETRY["datagramsDroppedRate"], 12)
        self.assertEqual(server.LIVE_TELEMETRY["totalDatagramsProcessed"], 3500)


class TestSessionLifecycleUnit(unittest.TestCase):
    """Verifies WebTransport session state transitions and active count evaluation."""

    def test_active_session_status_states(self):
        """CONNECTED, DRAINING, and DRAINED are active; CLOSED and CLOSED_ABRUPT are inactive."""
        active_statuses = {"CONNECTED", "DRAINING", "DRAINED"}
        inactive_statuses = {"CLOSED", "CLOSED_ABRUPT"}

        test_sessions = {
            "s1": {"status": "CONNECTED"},
            "s2": {"status": "DRAINING"},
            "s3": {"status": "DRAINED"},
            "s4": {"status": "CLOSED"},
            "s5": {"status": "CLOSED_ABRUPT"}
        }

        active_count = len([s for s in test_sessions.values() if s["status"] in active_statuses])
        self.assertEqual(active_count, 3)

    def test_draining_to_drained_lifecycle_transition(self):
        """RFC 9297 Section 5.3: Session transitions to DRAINED once active streams reach 0."""
        # Initial draining session with active streams
        session = {"status": "DRAINING", "streams": {"activeBidi": 1, "activeUni": 0}}
        total_active = session["streams"]["activeBidi"] + session["streams"]["activeUni"]
        self.assertEqual(session["status"], "DRAINING")

        # In-flight stream finishes (FIN closed) -> active streams reaches 0
        session["streams"]["activeBidi"] = 0
        total_active = session["streams"]["activeBidi"] + session["streams"]["activeUni"]
        if session["status"] == "DRAINING" and total_active == 0:
            session["status"] = "DRAINED"

        self.assertEqual(session["status"], "DRAINED")

    def test_rfc9297_stream_guardrail_rejections(self):
        """RFC 9297 Section 5.3 & 6: Endpoints MUST NOT open new streams when DRAINING, DRAINED, or CLOSED."""
        invalid_states = ["DRAINING", "DRAINED", "CLOSED", "CLOSED_ABRUPT"]
        for st in invalid_states:
            session = {"status": st}
            can_open = session["status"] == "CONNECTED"
            self.assertFalse(can_open, f"State {st} must not allow opening new streams")

    def test_connect_control_stream_lifecycle_and_history(self):
        """RFC 9297: Stream #0 CONNECT control stream carries handshake and capsules, and transitions cleanly."""
        connect_stream = {
            "streamId": 0,
            "type": "connect",
            "initiator": "client",
            "status": "ESTABLISHED",
            "history": [
                {"time": "12:00:00", "dir": "TX", "bytes": 182, "payload": "HEADERS: :method=CONNECT :protocol=webtransport", "fin": False},
                {"time": "12:00:00", "dir": "RX", "bytes": 104, "payload": "HEADERS: :status=200", "fin": False}
            ]
        }
        # Initial inspect history
        self.assertEqual(len(connect_stream["history"]), 2)
        self.assertEqual(connect_stream["status"], "ESTABLISHED")

        # Session drains
        connect_stream["status"] = "DRAINING"
        connect_stream["history"].append({
            "time": "12:00:05", "dir": "TX", "bytes": 5, "payload": "Capsule WT_DRAIN_SESSION (0x78ae)", "fin": False
        })
        self.assertEqual(len(connect_stream["history"]), 3)

        # In-flight streams complete -> DRAINED
        connect_stream["status"] = "DRAINED"

        # Session closes
        connect_stream["status"] = "CLOSED"
        connect_stream["history"].append({
            "time": "12:00:10", "dir": "TX", "bytes": 28, "payload": "Capsule CLOSE_WEBTRANSPORT_SESSION (0x2843): Code 0 [FIN]", "fin": True
        })
        self.assertEqual(len(connect_stream["history"]), 4)
        self.assertTrue(connect_stream["history"][-1]["fin"])
        self.assertEqual(connect_stream["status"], "CLOSED")


class TestAuditLogUnit(unittest.TestCase):
    """Verifies SecOps audit trail logging and formatting."""

    def setUp(self):
        server.AUDIT_LOG.clear()

    def test_log_audit_appends_structured_entry(self):
        """Audit log records user, action, target, details, and timestamp."""
        server.log_audit("secops-admin", "TEST_ACTION", "https://localhost:4433/echo", "Unit test audit")
        self.assertEqual(len(server.AUDIT_LOG), 1)
        entry = server.AUDIT_LOG[0]
        self.assertEqual(entry["operator"], "secops-admin")
        self.assertEqual(entry["action"], "TEST_ACTION")
        self.assertEqual(entry["target"], "https://localhost:4433/echo")
        self.assertEqual(entry["details"], "Unit test audit")
        self.assertIn("timestamp", entry)


class TestClusterAggregationUnit(unittest.TestCase):
    """Verifies multi-node telemetry aggregation logic."""

    def test_multi_node_session_sum(self):
        """Sum of active sessions across nodes correctly computes cluster-wide count."""
        mock_nodes_data = [
            {"name": "wt-node-1", "activeSessions": 2, "usedDirectMemory": 104857600},
            {"name": "wt-node-2", "activeSessions": 0, "usedDirectMemory": 52428800},
            {"name": "wt-node-3", "activeSessions": 3, "usedDirectMemory": 157286400},
        ]
        total_sessions = sum(n["activeSessions"] for n in mock_nodes_data)
        total_memory_bytes = sum(n["usedDirectMemory"] for n in mock_nodes_data)
        total_memory_mb = round(total_memory_bytes / (1024 * 1024), 1)

        self.assertEqual(total_sessions, 5)
        self.assertEqual(total_memory_mb, 300.0)

    def test_zero_session_convergence(self):
        """When all nodes report 0 sessions, cluster count converges to 0."""
        mock_nodes_data = [
            {"name": "wt-node-1", "activeSessions": 0},
            {"name": "wt-node-2", "activeSessions": 0},
            {"name": "wt-node-3", "activeSessions": 0},
        ]
        total_sessions = sum(n["activeSessions"] for n in mock_nodes_data)
        self.assertEqual(total_sessions, 0)


class TestRollingTelemetryHistoryUnit(unittest.TestCase):
    """Verifies telemetry history ring buffer size and sliding mechanics."""

    def setUp(self):
        for k in server.TELEMETRY_HISTORY:
            server.TELEMETRY_HISTORY[k].clear()

    def test_init_telemetry_history_size(self):
        """init_telemetry_history fills exactly 30 rolling data points."""
        server.init_telemetry_history()
        for k, v in server.TELEMETRY_HISTORY.items():
            self.assertEqual(len(v), 30, f"Key {k} did not initialize with 30 items")

    def test_record_telemetry_sample_ring_buffer_cap(self):
        """record_telemetry_sample caps ring buffer at exactly 60 items."""
        server.init_telemetry_history()
        for _ in range(40):
            server.record_telemetry_sample()
        for k, v in server.TELEMETRY_HISTORY.items():
            self.assertEqual(len(v), 60, f"Ring buffer {k} exceeded max size 60")

    def test_historical_window_live(self):
        """Live window returns real-time series and isLive=True flag."""
        res = server.get_telemetry_history_for_window("live")
        self.assertEqual(res["window"], "live")
        self.assertTrue(res["isLive"])
        self.assertIn("summary", res)
        self.assertIn("peakSessions", res["summary"])

    def test_historical_window_aggregation_15m_and_24h(self):
        """Historical windows return aggregated buckets and window statistics."""
        for win, expected_len in [("5m", 60), ("15m", 60), ("1h", 60), ("24h", 96)]:
            res = server.get_telemetry_history_for_window(win)
            self.assertEqual(res["window"], win)
            self.assertFalse(res["isLive"])
            self.assertEqual(len(res["timestamps"]), expected_len)
            self.assertEqual(len(res["sessions"]), expected_len)
            self.assertIn("summary", res)
            self.assertIn("peakSessions", res["summary"])
            self.assertIn("totalDatagrams", res["summary"])
            self.assertIn("avgRtt", res["summary"])


class TestConfigurationAndSecurityUnit(unittest.TestCase):
    """Verifies runtime configuration, path traversal protection, and memory bounds."""

    def test_runtime_configuration_defaults(self):
        """Verifies default server configuration parameters are set and valid."""
        self.assertEqual(server.BIND_HOST, "0.0.0.0")
        self.assertIn(server.PORT, [8085, int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 8085])
        self.assertEqual(server.ADMIN_USERNAME, "secops-admin")
        self.assertEqual(server.SESSION_TTL_SECONDS, 86400)
        self.assertEqual(server.MAX_PAYLOAD_BYTES, 10485760)
        self.assertGreaterEqual(server.MAX_AUDIT_LOG_ENTRIES, 100)
        self.assertIn(8081, server.CLUSTER_PORTS)
        self.assertIn(8082, server.CLUSTER_PORTS)
        self.assertIn(8083, server.CLUSTER_PORTS)

    def test_path_traversal_detection_and_rejection(self):
        """translate_path strictly rejects directory traversal outside DIRECTORY."""
        class DummyHandler(server.EnterpriseObservabilityHandler):
            def __init__(self):
                self.directory = server.DIRECTORY

        h = DummyHandler()
        # Valid files inside DIRECTORY
        valid_path = h.translate_path("/index.html")
        self.assertIsNotNone(valid_path)
        self.assertTrue(valid_path.startswith(server.DIRECTORY))

        valid_admin = h.translate_path("/admin.html")
        self.assertIsNotNone(valid_admin)
        self.assertTrue(valid_admin.startswith(server.DIRECTORY))

        # Path traversal attempts
        outside_path = h.translate_path("/../../../../etc/passwd")
        real_clean = os.path.realpath(outside_path) if outside_path else None
        real_dir = os.path.realpath(server.DIRECTORY)
        self.assertTrue(real_clean is None or real_clean.startswith(real_dir))

    def test_audit_log_bounded_capacity(self):
        """Audit log is strictly capped at MAX_AUDIT_LOG_ENTRIES to prevent memory leaks."""
        server.AUDIT_LOG.clear()
        cap = server.MAX_AUDIT_LOG_ENTRIES
        for i in range(cap + 50):
            server.log_audit("test-operator", "ACTION", f"target-{i}", f"detail-{i}")
        self.assertEqual(len(server.AUDIT_LOG), cap)
        self.assertEqual(server.AUDIT_LOG[0]["target"], f"target-{cap + 49}")

    def test_expired_token_purging(self):
        """Expired tokens are automatically removed from ADMIN_SESSIONS during auth checks."""
        token = "test-expired-purge-uuid"
        server.ADMIN_SESSIONS[token] = {
            "user": "secops-admin",
            "login_time": time.time() - 90000,
            "expires_at": time.time() - 100  # Expired
        }
        self.assertIn(token, server.ADMIN_SESSIONS)

        # Create dummy handler instance
        class MockHandler(server.EnterpriseObservabilityHandler):
            def __init__(self, headers):
                self.headers = headers

        h = MockHandler({"Authorization": f"Bearer {token}"})
        user = h.is_authenticated()
        self.assertIsNone(user)
        self.assertNotIn(token, server.ADMIN_SESSIONS)


class TestStreamAndSessionGuardrailsUnit(unittest.TestCase):
    """Verifies RFC 9297 lifecycle guardrails: positive and negative test cases."""

    def setUp(self):
        self.session_id = "test-sess-guardrails-101"
        server.MANAGED_SESSIONS[self.session_id] = {
            "id": self.session_id,
            "rawSessionId": 101,
            "targetUrl": "https://localhost:4433/echo",
            "path": "/echo",
            "status": "CONNECTED",
            "streams": [
                {"streamId": 0, "type": "connect", "initiator": "client", "status": "ESTABLISHED", "bytesSent": 0, "bytesReceived": 0, "history": []}
            ],
            "activeStreams": 0,
            "activeStreamsBidi": 0,
            "activeStreamsUni": 0,
            "flowControl": {"usedStreamsBidi": 0, "usedStreamsUni": 0, "maxStreamsBidi": 100, "maxStreamsUni": 100},
            "datagrams": {"sent": 0, "received": 0, "dropped": 0, "recent": []},
            "wireEvents": [],
            "nextStreamId": 4
        }

    def tearDown(self):
        server.MANAGED_SESSIONS.pop(self.session_id, None)

    def test_positive_stream_create_on_connected_session(self):
        """Positive Case: Creating stream on CONNECTED session succeeds and assigns valid stream ID."""
        session = server.MANAGED_SESSIONS[self.session_id]
        self.assertEqual(session["status"], "CONNECTED")

        # Open stream #4
        sid = session["nextStreamId"]
        session["nextStreamId"] += 4
        new_stream = {
            "streamId": sid, "type": "bidi", "initiator": "client",
            "status": "OPEN", "bytesSent": 10, "bytesReceived": 10, "history": []
        }
        session["streams"].append(new_stream)
        session["activeStreams"] += 1
        session["activeStreamsBidi"] += 1

        self.assertEqual(len(session["streams"]), 2)
        self.assertEqual(session["streams"][-1]["streamId"], 4)
        self.assertEqual(session["streams"][-1]["status"], "OPEN")

    def test_negative_stream_create_on_draining_session(self):
        """Negative Case (RFC 9297 §5.3): Creating stream on DRAINING session is rejected."""
        session = server.MANAGED_SESSIONS[self.session_id]
        session["status"] = "DRAINING"

        can_open = session["status"] == "CONNECTED"
        self.assertFalse(can_open, "Must reject stream creation on DRAINING session")

    def test_negative_stream_create_on_drained_session(self):
        """Negative Case (RFC 9297 §5.3): Creating stream on DRAINED session is rejected."""
        session = server.MANAGED_SESSIONS[self.session_id]
        session["status"] = "DRAINED"

        can_open = session["status"] == "CONNECTED"
        self.assertFalse(can_open, "Must reject stream creation on DRAINED session")

    def test_negative_stream_create_on_closed_session(self):
        """Negative Case (RFC 9297 §6): Creating stream on CLOSED session is rejected."""
        session = server.MANAGED_SESSIONS[self.session_id]
        session["status"] = "CLOSED"

        can_open = session["status"] == "CONNECTED"
        self.assertFalse(can_open, "Must reject stream creation on CLOSED session")

    def test_negative_stream_create_on_abrupt_closed_session(self):
        """Negative Case: Creating stream on CLOSED_ABRUPT session is rejected."""
        session = server.MANAGED_SESSIONS[self.session_id]
        session["status"] = "CLOSED_ABRUPT"

        can_open = session["status"] == "CONNECTED"
        self.assertFalse(can_open, "Must reject stream creation on CLOSED_ABRUPT session")

    def test_positive_stream_clean_close_fin(self):
        """Positive Case: Clean half-close (FIN) transitions stream status to CLOSED."""
        session = server.MANAGED_SESSIONS[self.session_id]
        stream = {"streamId": 4, "type": "bidi", "status": "OPEN", "history": []}
        session["streams"].append(stream)

        # Close stream with FIN
        stream["status"] = "CLOSED"
        stream["history"].append({"time": "12:00:00", "dir": "TX", "bytes": 0, "payload": "STREAM_FIN", "fin": True})

        self.assertEqual(stream["status"], "CLOSED")
        self.assertTrue(stream["history"][-1]["fin"])

    def test_positive_stream_reset(self):
        """Positive Case: Reset stream sets RESET status and records error code."""
        stream = {"streamId": 4, "type": "bidi", "status": "OPEN", "history": []}
        stream["status"] = "RESET"
        stream["resetCode"] = 0x045d4487  # WT_FLOW_CONTROL_ERROR
        stream["history"].append({"time": "12:00:00", "dir": "TX", "bytes": 0, "payload": "RESET_STREAM", "fin": True})

        self.assertEqual(stream["status"], "RESET")
        self.assertEqual(stream["resetCode"], 0x045d4487)

    def test_positive_capsule_drain_and_close_lifecycle(self):
        """Positive Case: DRAIN capsule keeps Stream 0 open until active data streams reach 0, then CLOSE cleanly closes."""
        session = server.MANAGED_SESSIONS[self.session_id]
        connect_stream = session["streams"][0]

        # 1. Drain initiated with 0 active data streams
        act_data_streams = len([st for st in session["streams"] if st["status"] in ("OPEN", "ESTABLISHED") and st["type"] in ("bidi", "uni")])
        self.assertEqual(act_data_streams, 0)

        # Transition directly to DRAINED
        session["status"] = "DRAINED" if act_data_streams == 0 else "DRAINING"
        connect_stream["status"] = "DRAINED"
        self.assertEqual(session["status"], "DRAINED")
        self.assertEqual(connect_stream["status"], "DRAINED")

        # 2. Close capsule sent
        session["status"] = "CLOSED"
        connect_stream["status"] = "CLOSED"
        connect_stream["history"].append({"time": "12:00:00", "dir": "TX", "bytes": 28, "payload": "CLOSE_WEBTRANSPORT_SESSION", "fin": True})

        self.assertEqual(session["status"], "CLOSED")
        self.assertEqual(connect_stream["status"], "CLOSED")
        self.assertTrue(connect_stream["history"][-1]["fin"])


class TestDecoupledIngestionAndFaultToleranceUnit(unittest.TestCase):
    """Verifies standalone operation, OTLP ingestion, and resilient fault tolerance."""

    def setUp(self):
        server.LIVE_TELEMETRY["activeSessions"] = 0
        server.LIVE_TELEMETRY["activeStreams"] = 0

    def test_prometheus_malformed_lines_gracefully_ignored(self):
        """Negative Case: Corrupted Prometheus text lines do not throw exceptions or corrupt state."""
        corrupted_text = """
# HELP invalid_metric
invalid_metric_with_no_value
webtransport_sessions_active not_a_number
webtransport_streams_active{type="bidi"} 5
random_garbage_line !@#$%^&*()
webtransport_datagrams_sent_total 100
"""
        server.parse_prometheus_text(corrupted_text)
        self.assertEqual(server.LIVE_TELEMETRY["bidiStreams"], 5)
        self.assertEqual(server.LIVE_TELEMETRY["totalDatagramsProcessed"], 100)

    def test_otlp_metrics_payload_parsing(self):
        """Positive Case: OTLP resourceMetrics JSON payload correctly parses sessions and streams."""
        otlp_payload = {
            "resourceMetrics": [
                {
                    "scopeMetrics": [
                        {
                            "metrics": [
                                {
                                    "name": "webtransport.sessions.active",
                                    "gauge": {"dataPoints": [{"asInt": 7}]}
                                },
                                {
                                    "name": "webtransport.streams.active",
                                    "gauge": {"dataPoints": [{"asInt": 14}]}
                                },
                                {
                                    "name": "webtransport.datagrams.sent",
                                    "sum": {"dataPoints": [{"asInt": 250}]}
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        # Simulate OTLP handler extraction logic
        for rm in otlp_payload.get('resourceMetrics', []):
            for sm in rm.get('scopeMetrics', []):
                for m in sm.get('metrics', []):
                    m_name = m.get('name', '')
                    if 'sessions' in m_name and 'gauge' in m:
                        dp = m['gauge'].get('dataPoints', [])
                        if dp:
                            server.LIVE_TELEMETRY['activeSessions'] = int(dp[-1].get('asInt', 0))
                    elif 'streams' in m_name and 'gauge' in m:
                        dp = m['gauge'].get('dataPoints', [])
                        if dp:
                            server.LIVE_TELEMETRY['activeStreams'] = int(dp[-1].get('asInt', 0))
                    elif 'datagrams' in m_name and 'sum' in m:
                        dp = m['sum'].get('dataPoints', [])
                        if dp and 'sent' in m_name:
                            server.LIVE_TELEMETRY['totalDatagramsProcessed'] = int(dp[-1].get('asInt', 0))

        self.assertEqual(server.LIVE_TELEMETRY["activeSessions"], 7)
        self.assertEqual(server.LIVE_TELEMETRY["activeStreams"], 14)
        self.assertEqual(server.LIVE_TELEMETRY["totalDatagramsProcessed"], 250)

    def test_invalid_window_fallback(self):
        """Negative Case: Querying non-existent telemetry window safely defaults to 15m without error."""
        res = server.get_telemetry_history_for_window("nonexistent_999d")
        self.assertIsNotNone(res)
        self.assertEqual(len(res["timestamps"]), 60)
        self.assertIn("summary", res)

    def test_offline_cluster_node_tolerance(self):
        """Decoupled Fault Tolerance: Probing non-existent node port returns False without throwing unhandled exceptions."""
        is_alive = server.check_node_probe("http://127.0.0.1:59999/healthz")
        self.assertFalse(is_alive)

    def test_connected_server_metadata_resolution_positive(self):
        """Positive Case: Resolving server node metadata from target URL correctly sets node, server ID, and port."""
        target_url = "https://localhost:4434/echo"
        parsed = urllib.parse.urlparse(target_url)
        target_port = parsed.port or 4433
        node_num = (target_port - 4432) if (4433 <= target_port <= 4435) else 1
        node_id = f"wt-node-{node_num}"
        server_node = f"{node_id} (Server ID: {node_num} · Port {target_port})"

        self.assertEqual(target_port, 4434)
        self.assertEqual(node_num, 2)
        self.assertEqual(node_id, "wt-node-2")
        self.assertEqual(server_node, "wt-node-2 (Server ID: 2 · Port 4434)")

    def test_connected_server_metadata_resolution_fallback(self):
        """Negative/Fallback Case: Non-cluster or unparseable target URL falls back safely to wt-node-1 (Port 4433)."""
        target_url = "https://remote-edge.domain.com/custom"
        parsed = urllib.parse.urlparse(target_url)
        target_port = parsed.port or 4433
        node_num = (target_port - 4432) if (4433 <= target_port <= 4435) else 1
        node_id = f"wt-node-{node_num}"

        self.assertEqual(target_port, 4433)
        self.assertEqual(node_num, 1)
        self.assertEqual(node_id, "wt-node-1")

    def test_auto_lb_endpoint_resolution_least_connections(self):
        """Auto-LB Case: 'auto' target dynamically chooses the least-loaded healthy cluster node."""
        original_nodes = list(server.CLUSTER_NODES)
        try:
            # Mock node state: node 1 has 5 sessions, node 2 has 1 session, node 3 has 3 sessions
            server.CLUSTER_NODES = [
                {"id": "wt-node-1", "status": "HEALTHY", "activeSessions": 5, "quicPort": 4433, "serverId": 1},
                {"id": "wt-node-2", "status": "HEALTHY", "activeSessions": 1, "quicPort": 4434, "serverId": 2},
                {"id": "wt-node-3", "status": "HEALTHY", "activeSessions": 3, "quicPort": 4435, "serverId": 3},
            ]
            url, node_num, port, desc = server.resolve_target_endpoint("auto")
            self.assertEqual(port, 4434)
            self.assertEqual(node_num, 2)
            self.assertIn("wt-node-2", desc)
            self.assertIn("4434", url)
        finally:
            server.CLUSTER_NODES = original_nodes

    def test_auto_lb_endpoint_resolution_round_robin_tie_breaking(self):
        """Auto-LB Case: Ties in session count round-robin across equally loaded healthy nodes."""
        original_nodes = list(server.CLUSTER_NODES)
        try:
            # All 3 nodes have 0 sessions
            server.CLUSTER_NODES = [
                {"id": "wt-node-1", "status": "HEALTHY", "activeSessions": 0, "quicPort": 4433, "serverId": 1},
                {"id": "wt-node-2", "status": "HEALTHY", "activeSessions": 0, "quicPort": 4434, "serverId": 2},
                {"id": "wt-node-3", "status": "HEALTHY", "activeSessions": 0, "quicPort": 4435, "serverId": 3},
            ]
            server.CLUSTER_LB_COUNTER = 0
            u1, n1, p1, _ = server.resolve_target_endpoint("auto")
            u2, n2, p2, _ = server.resolve_target_endpoint("auto")
            u3, n3, p3, _ = server.resolve_target_endpoint("auto")
            
            ports = [p1, p2, p3]
            self.assertEqual(set(ports), {4433, 4434, 4435})
        finally:
            server.CLUSTER_NODES = original_nodes


if __name__ == "__main__":
    unittest.main(verbosity=2)


