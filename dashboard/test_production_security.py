"""Exercise production boundaries through real HTTP requests, without live nodes."""
import http.client
import json
import os
import threading
import unittest
from unittest.mock import patch

os.environ.setdefault("WT4J_ADMIN_PASSWORD", "production-regression-password")
import server


class ProductionSecurityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = server.BoundedThreadingServer(("127.0.0.1", 0), server.EnterpriseObservabilityHandler)
        cls.port = cls.httpd.server_address[1]
        cls.worker = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.worker.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.worker.join()

    def setUp(self):
        server.LOGIN_ATTEMPTS.clear()
        self.token = "production-test-session"
        server.ADMIN_SESSIONS[self.token] = {"user": "operations", "expires_at": server.time.time() + 60}

    def tearDown(self):
        server.ADMIN_SESSIONS.pop(self.token, None)

    def request(self, path, method="GET", body=None, authenticated=False, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        req_headers = dict(headers or {})
        if authenticated:
            req_headers["Authorization"] = "Bearer " + self.token
        if isinstance(body, dict):
            body = json.dumps(body)
            req_headers["Content-Type"] = "application/json"
        connection.request(method, path, body, req_headers)
        response = connection.getresponse()
        result = response.status, dict(response.getheaders()), response.read()
        connection.close()
        return result

    def test_private_routes_require_authentication(self):
        for path in ("/api/live-telemetry", "/api/telemetry/history?window=1h", "/api/admin/audit-log", "/api/cluster/status", "/jolokia/exec/java.lang:type=Memory/gc", "/api/node/jolokia/overview", "/metrics", "/api/reset"):
            with self.subTest(path=path):
                self.assertEqual(401, self.request(path)[0])
        self.assertEqual(401, self.request('/api/node/jolokia/exec/test', 'POST', {})[0])

    def test_static_files_allowlisted_for_get_and_head(self):
        for method in ('GET', 'HEAD'):
            for path in ('/server.py', '/Dockerfile', '/.env', '/test_dashboard_unit.py', '/README.md'):
                self.assertEqual(404, self.request(path, method)[0])
        # Test the allowlist with HEAD; the TLS integration test fetches the full UI.
        self.assertEqual(200, self.request('/admin.html', 'HEAD')[0])

    def test_health_and_headers(self):
        status, headers, _ = self.request('/healthz')
        self.assertEqual(200, status)
        self.assertEqual('no-store', headers['Cache-Control'])
        self.assertEqual('nosniff', headers['X-Content-Type-Options'])
        self.assertNotIn('Access-Control-Allow-Origin', self.request('/healthz', 'OPTIONS')[1])

    def test_otlp_requires_separate_token(self):
        with patch.object(server, 'OTLP_TOKEN', 'ingestion-test-token'):
            self.assertEqual(401, self.request('/v1/metrics', 'POST', {}, True)[0])
            self.assertEqual(200, self.request('/v1/metrics', 'POST', {}, headers={'Authorization': 'Bearer ingestion-test-token'})[0])

    def test_production_disables_development_injection(self):
        with patch.object(server, 'PRODUCTION', True):
            for path in ('/api/admin/execute-traffic', '/api/admin/sessions/create', '/api/admin/sessions/test/streams/create', '/api/admin/sessions/test/datagrams/send'):
                self.assertEqual(403, self.request(path, 'POST', {}, True)[0])

    def test_reset_is_post_and_preserves_sessions_and_audit(self):
        self.assertEqual(405, self.request('/api/reset', authenticated=True)[0])
        server.MANAGED_SESSIONS['preserve-test'] = {'status': 'CONNECTED'}
        before = server.log_audit('test', 'PRESERVE', 'test', 'test')
        with patch.object(server, 'close_all_server_sessions') as close, patch.object(server, 'sync_live_server_info'), patch.object(server, 'sync_live_server_sessions'):
            self.assertEqual(200, self.request('/api/reset', 'POST', {}, True)[0])
            close.assert_not_called()
        self.assertIn('preserve-test', server.MANAGED_SESSIONS)
        self.assertIn(before, server.AUDIT_LOG)
        server.MANAGED_SESSIONS.pop('preserve-test')

    def test_invalid_body_and_content_length(self):
        for body in ('[1]', '{invalid'):
            self.assertEqual(400, self.request('/api/admin/login', 'POST', body)[0])
        self.assertEqual(400, self.request('/api/admin/login', 'POST', headers={'Content-Length': '-1'})[0])
        self.assertEqual(413, self.request('/api/node/jolokia/exec/test', 'POST', authenticated=True, headers={'Content-Length': str(server.MAX_PAYLOAD_BYTES + 1)})[0])

    def test_login_is_rate_limited_and_username_is_exact(self):
        with patch.object(server, 'ADMIN_USERNAME', 'named-operator'):
            for _ in range(10):
                self.assertEqual(401, self.request('/api/admin/login', 'POST', {'username': 'admin', 'password': os.environ['WT4J_ADMIN_PASSWORD']})[0])
            self.assertEqual(429, self.request('/api/admin/login', 'POST', {'username': 'named-operator', 'password': 'wrong'})[0])

    def test_remote_targets_preserve_https_and_ipv6(self):
        with patch.dict(os.environ, {'WT4J_CLUSTER_NODES': 'https://node.example.com,https://[2001:db8::1]:8443'}):
            targets = server.parse_cluster_targets()
        self.assertEqual('https://node.example.com', targets[0]['baseUrl'])
        self.assertEqual(443, targets[0]['healthPort'])
        self.assertEqual('https://[2001:db8::1]:8443', targets[1]['baseUrl'])
        with patch.dict(os.environ, {'WT4J_CLUSTER_NODES': 'https://user:secret@node.example.com'}):
            with self.assertRaises(ValueError):
                server.parse_cluster_targets()

    def test_same_port_nodes_are_scraped_once_each_and_drained_individually(self):
        import io
        origins = ('https://node-one.example.com:8080', 'https://node-two.example.com:8080')
        nodes = [{"id": f"node-{index}", "baseUrl": origin, "healthPort": 8080, "status": "HEALTHY"} for index, origin in enumerate(origins)]
        seen = []
        def upstream(request, **kwargs):
            seen.append(request.full_url)
            response = io.BytesIO(json.dumps({"sessions": []}).encode())
            response.status = 200
            return response
        with patch.dict(os.environ, {"WT4J_CLUSTER_NODES": ','.join(origins)}), patch.object(server, 'CLUSTER_NODES', nodes), patch.object(server.urllib.request, 'urlopen', side_effect=upstream):
            server.sync_live_server_sessions()
            self.assertEqual([origin + '/api/node/sessions' for origin in origins], seen)
            seen.clear()
            with patch.object(server, 'PRODUCTION', True), patch.dict(server.MANAGED_SESSIONS, {'owned-session': {"managementOrigin": origins[0]}}):
                server.drain_server_session(7, 'owned-session')
                self.assertEqual([origins[0] + '/api/node/sessions/drain'], seen)

    def test_production_startup_fails_without_tls(self):
        with patch.object(server, 'PRODUCTION', True), patch.dict(os.environ, {'WT4J_TLS_CERT_FILE': '', 'WT4J_TLS_KEY_FILE': ''}):
            with self.assertRaisesRegex(RuntimeError, 'TLS'):
                server.run()

    def test_cluster_metrics_sum_nodes_and_preserve_live_gauges(self):
        samples = '''
webtransport_sessions_active{k8s_pod_name="node-1"} 2
webtransport_sessions_active{k8s_pod_name="node-2"} 3
webtransport_datagrams_sent_total{k8s_pod_name="node-1"} 20
webtransport_datagrams_sent_total{k8s_pod_name="node-2"} 7
webtransport_datagrams_sent_total{k8s_pod_name="bad-node"} NaN
'''
        with patch.dict(server.LIVE_TELEMETRY, {'activeSessions': 9, 'totalDatagramsProcessed': 0}):
            server.parse_prometheus_text(samples, update_gauges=False)
            self.assertEqual(9, server.LIVE_TELEMETRY['activeSessions'])
            self.assertEqual(27, server.LIVE_TELEMETRY['totalDatagramsProcessed'])
            server.parse_prometheus_text(samples)
            self.assertEqual(5, server.LIVE_TELEMETRY['activeSessions'])
            server.parse_prometheus_text('webtransport_sessions_active{k8s_pod_name="node-1"} 0')
            self.assertEqual(0, server.LIVE_TELEMETRY['activeSessions'])


if __name__ == '__main__':
    unittest.main()
