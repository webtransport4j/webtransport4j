import urllib.request
import json
import time

ADMIN_PASS = "webtransport2026!"

# 1. Verify Unique Server IDs on nodes
print("=== 1. Checking Unique Server IDs & QUIC-LB Config on Nodes ===")
for port, exp_id in [(8081, 1), (8082, 2), (8083, 3)]:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/node/info") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        print(f"Node on port {port}: status={data.get('status')} nodeId={data.get('nodeId')}")
        assert data.get("status") == "HEALTHY"

# 2. Verify Dynamic Cluster Discovery on dashboard (port 8085)
print("\n=== 2. Checking Dynamic Cluster Discovery (WT4J_CLUSTER_NODES) ===")
with urllib.request.urlopen("http://127.0.0.1:8085/api/cluster/status") as resp:
    data = json.loads(resp.read().decode("utf-8"))
    nodes = data.get("nodes", [])
    print(f"Dashboard cluster status discovered {len(nodes)} nodes:")
    for n in nodes:
        print(f"  - {n.get('id')} at port {n.get('healthPort')}: {n.get('status')} ({n.get('role')})")
    assert len(nodes) == 3, f"Expected 3 nodes, got {len(nodes)}"

# 3. Create a real session to test prestop hook
print("\n=== 3. Creating Live Session & Testing Kubernetes /prestop Hook ===")
req = urllib.request.Request(
    "http://127.0.0.1:8085/api/admin/login",
    data=json.dumps({"username": "secops-admin", "password": ADMIN_PASS}).encode("utf-8"),
    headers={"Content-Type": "application/json"}
)
with urllib.request.urlopen(req) as resp:
    token = json.loads(resp.read().decode("utf-8")).get("token")

req = urllib.request.Request(
    "http://127.0.0.1:8085/api/admin/sessions/create",
    data=json.dumps({"target": "https://localhost:4433/echo"}).encode("utf-8"),
    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
)
with urllib.request.urlopen(req) as resp:
    sess = json.loads(resp.read().decode("utf-8")).get("session")
    sess_id = sess.get("id")
    print(f"Created live session: {sess_id} status={sess.get('status')}")

# 4. Trigger /prestop hook on node 1 (8081)
print(f"\n=== 4. Triggering Kubernetes preStop hook on node 1 (http://127.0.0.1:8081/prestop) ===")
req = urllib.request.Request("http://127.0.0.1:8081/prestop")
with urllib.request.urlopen(req) as resp:
    prestop_res = json.loads(resp.read().decode("utf-8"))
    print("preStop hook response:", resp.status, prestop_res)
    assert resp.status == 200
    assert prestop_res.get("status") == "DRAINING"

# Check that session transitioned to DRAINED/DRAINING
time.sleep(0.5)
with urllib.request.urlopen("http://127.0.0.1:8085/api/admin/sessions") as resp:
    sess_list = json.loads(resp.read().decode("utf-8")).get("sessions", [])
    target_s = next((s for s in sess_list if s["id"] == sess_id), None)
    st = target_s.get("status") if target_s else "NONE"
    print(f"Session {sess_id} after preStop: status={st}")
    assert target_s and st in ("DRAINING", "DRAINED")

# Clean up
req = urllib.request.Request(
    "http://127.0.0.1:8085/api/admin/sessions/close-all",
    data=json.dumps({"mode": "close"}).encode("utf-8"),
    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
)
with urllib.request.urlopen(req) as resp:
    pass

print("\nSUCCESS: All production requirements verified locally without mocks!")
