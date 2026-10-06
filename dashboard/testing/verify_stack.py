"""Live test of production portal, real Java nodes, QUIC traffic, and telemetry.

Runs only in the isolated Compose test network. Never target production nodes.
"""
import json
import os
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

TLS = ssl.create_default_context(cafile="/run/tls/tls.crt")
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=TLS))
PORTAL = "https://portal:8085"
NODE_TOKEN = os.environ["TEST_NODE_TOKEN"]
CHECKS = []
CLIENT = ["java", "-cp", "/app/classes:/app/lib/*", "io.github.webtransport4j.example.RealTrafficGenerator"]


def check(name, condition):
    if not condition:
        raise AssertionError(name)
    CHECKS.append(name)
    print(f"PASS {name}", flush=True)


def request(path, data=None, token=None, expected=200, base=PORTAL, method=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(base + path, data=body, headers=headers, method=method)
    try:
        response = HTTP.open(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        if response.status != expected:
            raise AssertionError(f"{path}: expected HTTP {expected}, got {response.status}: {response.read()[:500]!r}")
        payload = response.read()
        content = json.loads(payload) if payload and 'application/json' in response.headers.get('Content-Type', '') else payload
        return content


def eventually(name, predicate, seconds=35):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            check(name, True)
            return
        time.sleep(1)
    raise AssertionError(f"Timed out: {name}")


def traffic(command, node, *args):
    result = subprocess.run(CLIENT + [command, f"https://{node}:4433/echo", *map(str, args)],
                            capture_output=True, text=True, timeout=40)
    records = []
    for line in result.stdout.splitlines():
        try:
            records.append(json.loads(line))
        except ValueError:
            pass
    if result.returncode or not records:
        raise AssertionError(f"Real {command} traffic failed: {result.stdout[-2000:]} {result.stderr[-2000:]}")
    check(f"real QUIC {command} on {node}", records[-1].get("status") == "SUCCESS")
    return records[-1]


def node_sessions(node):
    return request('/api/node/sessions', token=NODE_TOKEN, base=f'http://{node}:8080').get('sessions', [])


def main():
    processes = []
    try:
        for node in ('node-1', 'node-2', 'node-3'):
            request('/api/node/sessions/close', {'all': True}, NODE_TOKEN, base=f'http://{node}:8080')
        request('/healthz')
        request('/')
        request('/admin.html')
        check('HTTPS and both UIs with certificate verification', True)
        for path in ('/api/live-telemetry', '/api/cluster/status', '/api/admin/audit-log', '/api/node/jolokia/version', '/metrics'):
            request(path, expected=401)
        check('anonymous analytics and management requests rejected', True)
        for path in ('/server.py', '/Dockerfile', '/.env'):
            request(path, expected=404)
            request(path, method='HEAD', expected=404)
        check('backend source and secrets are not served', True)
        request('/api/admin/login', {'username': 'admin', 'password': os.environ['TEST_ADMIN_PASSWORD']}, expected=401)
        login = request('/api/admin/login', {'username': 'test-operator', 'password': os.environ['TEST_ADMIN_PASSWORD']})
        token = login['token']
        check('configured operator login and verification', request('/api/admin/verify', token=token)['authenticated'])
        config = request('/api/config', token=token)
        check('production uses three remote management targets on the same port', config['clusterPorts'] == [8080, 8080, 8080])
        cluster = request('/api/cluster/status', token=token)
        check('all three real nodes are healthy', len(cluster['nodes']) == 3 and all(node['status'] == 'HEALTHY' for node in cluster['nodes']))
        check('nodes retain distinct remote origins', len({node['baseUrl'] for node in cluster['nodes']}) == 3)
        for node in cluster['nodes']:
            jmx = request('/api/node/jolokia/read/java.lang:type=Memory?node=' + urllib.parse.quote(node['id']), token=token)
            check(f"live JMX memory from {node['id']}", jmx.get('status') == 200 and jmx['value']['HeapMemoryUsage']['used'] > 0)
        for path in ('/api/admin/execute-traffic', '/api/admin/sessions/create', '/api/admin/sessions/unknown/streams/create', '/api/admin/sessions/unknown/datagrams/send'):
            request(path, {}, token, expected=403)
        check('development traffic injection blocked in production', True)
        request('/v1/metrics', {}, token, expected=401)
        request('/v1/metrics', {'resourceMetrics': []}, os.environ['TEST_OTLP_TOKEN'])
        trace_id = 'a' * 32
        request('/v1/traces', {'resourceSpans': [{'scopeSpans': [{'spans': [{'traceId': trace_id, 'spanId': 'b' * 16, 'name': '/integration-test-ingestion'}]}]}]}, os.environ['TEST_OTLP_TOKEN'])
        telemetry = request('/api/live-telemetry', token=token)
        check('authenticated OTLP ingestion contract and trace display', any(trace['traceId'] == trace_id for trace in telemetry['traces']))
        traffic('handshake', 'node-1')
        streams = traffic('streams', 'node-2', 'bidi', 3, 'portal-production-integration')
        check('three real bidirectional streams receive echoes', streams['count'] == 3)
        traffic('streams', 'node-3', 'uni', 3, 'portal-production-integration')
        datagrams = traffic('datagrams', 'node-1', 20, 256, 100)
        check('real datagrams sent', datagrams['sent'] == 20)
        eventually('real node OTLP metrics reach collector', lambda: 'webtransport' in request('/metrics', base='http://collector:8889').decode())
        eventually('portal analytics receives real datagram counters', lambda: request('/api/live-telemetry', token=token)['totalDatagramsProcessed'] >= 20)
        eventually('Prometheus scrapes collector successfully', lambda: any(result['value'][1] == '1' for result in request('/api/v1/query?query=up', base='http://prometheus:9090')['data']['result']))
        for node in ('node-1', 'node-2'):
            log = tempfile.TemporaryFile(mode='w+')
            proc = subprocess.Popen(CLIENT + ['session', f'https://{node}:4433/echo', '0', 'bidi', '120'], stdout=log, stderr=subprocess.STDOUT, text=True)
            processes.append((proc, log))
        eventually('persistent real sessions on two nodes', lambda: len(node_sessions('node-1')) == 1 and len(node_sessions('node-2')) == 1)
        sessions = request('/api/admin/sessions', token=token)['sessions']
        check('portal observes both live sessions', len(sessions) == 2)
        first = next(session for session in sessions if session['nodeName'] == 'node-1')
        second = next(session for session in sessions if session['nodeName'] == 'node-2')
        check('observations do not invent RTT or heartbeat acknowledgements', first.get('rttMs') is None and first.get('heartbeat', {}).get('status') == 'UNKNOWN')
        request('/api/reset', token=token, expected=405)
        request('/api/reset', {}, token)
        check('telemetry reset preserves real production sessions', len(node_sessions('node-1')) == 1 and len(node_sessions('node-2')) == 1)
        request(f"/api/admin/sessions/{first['id']}/capsules/drain", {}, token)
        eventually('individual drain reaches owning node', lambda: any(session['status'] == 'DRAINING' for session in node_sessions('node-1')))
        check('individual drain preserves other node session', any(session['id'] == second['id'] and session['status'] == 'CONNECTED' for session in node_sessions('node-2')))
        request(f"/api/admin/sessions/{first['id']}/capsules/close", {}, token)
        eventually('individual close reaches owning node', lambda: not node_sessions('node-1'))
        check('individual close preserves other node session', len(node_sessions('node-2')) == 1)
        audit = request('/api/admin/audit-log', token=token)['logs']
        check('audit retains login, reset, drain, and close actions', all(any(event['action'] == action for event in audit) for action in ('OPERATOR_AUTHENTICATION', 'RESET_TELEMETRY', 'DRAIN_SESSION', 'CLOSE_SESSION_CAPSULE')))
        request('/api/admin/sessions/close-all', {'mode': 'drain'}, token)
        eventually('bulk drain reaches remaining real node session',
                   lambda: any(session['status'] == 'DRAINING' for session in node_sessions('node-2')))
        request('/api/admin/sessions/close-all', {'mode': 'close'}, token)
        eventually('bulk close cleans up remaining sessions', lambda: not node_sessions('node-2'))
        history = request('/api/telemetry/history?window=live', token=token)
        check('analytics history endpoint returns series', 'timestamps' in history)
        request('/metrics', token=token)
        request('/api/admin/logout', {}, token)
        request('/api/config', token=token, expected=401)
        check('logout revokes token', True)
        if '--server-drain' in sys.argv:
            node = 'http://node-3:8080'
            check('node readiness before drain', request('/readyz', base=node)['ready'])
            request('/prestop', base=node, expected=401)
            check('unauthenticated remote preStop rejected', True)
            request('/prestop', token=NODE_TOKEN, base=node)
            check('node readiness fails after server drain',
                  not request('/readyz', base=node, expected=503)['ready'])
            rejected = subprocess.run(CLIENT + ['handshake', 'https://node-3:4433/echo'],
                                      capture_output=True, text=True, timeout=25)
            check('draining node refuses a new real QUIC session', rejected.returncode != 0)
        print(json.dumps({'passed': len(CHECKS), 'checks': CHECKS}), flush=True)
    finally:
        for proc, log in processes:
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            log.close()
        for node in ('node-1', 'node-2', 'node-3'):
            try:
                request('/api/node/sessions/close', {'all': True}, NODE_TOKEN, base=f'http://{node}:8080')
            except Exception as error:
                print(f'Cleanup warning for {node}: {error}', flush=True)


if __name__ == '__main__':
    main()
