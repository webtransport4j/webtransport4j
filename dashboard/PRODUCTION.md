# Production admin portal and analytics

This deployment is for the admin/analytics changes on `feat/enterprise-observability`.
It connects to existing remote WebTransport nodes; it does not deploy or reconfigure
those nodes. The container runs the Python API and both UIs on the same HTTPS origin.

## Deploy on a remote host

1. Provision DNS (for example `operations.example.com`) and a trusted certificate for
   that name. Place the full certificate chain in `tls.crt` and the private key in
   `tls.key` in a protected directory. The container runs as UID/GID 10001; grant
   that identity read access to these files without making the key world-readable.
2. Copy `dashboard/.env.example` to `dashboard/.env`. Set a unique admin username,
   a long random password, a separate OTLP token, and the existing nodes' management
   bearer token. Set `WT4J_CLUSTER_NODES` to their reachable HTTP(S) management
   origins. HTTPS origins retain their scheme and validate the server certificate.
   Keep any HTTP management connections on a trusted private network.
3. Set `WT4J_TLS_DIR` to the certificate directory and `WT4J_PROMETHEUS_TARGETS`
   to reachable Prometheus exposition URLs (`/metrics`), rather than the Prometheus
   query API. The bundled collector target works when producers send OTLP metrics
   to the collector on the private Compose network. Collector and Prometheus ports
   are not published on the host. For an existing remote collector, set its private
   metrics URL instead.
4. From `dashboard`, run:

   ```sh
   docker compose --env-file .env config --quiet
   docker compose --env-file .env build --pull
   docker compose --env-file .env up -d
   docker compose ps
   ```

5. Allow the portal's HTTPS port only from your operator network/VPN. Open
   `https://operations.example.com:8443/admin.html`, sign in, and navigate to `/`
   for analytics. Authentication tokens stay in browser session storage. Both
   pages attach them to API requests and request a new login on expiration.
   The certificate must cover the domain in the URL. Do not disable verification.

The default exposed port is 8443; set `WT4J_PORTAL_PORT=443` if appropriate for
this host. Keep a single portal instance. Sessions, live history, datasource
changes, and the audit viewer are process-local and reset on restart. This is
not an HA deployment. Prometheus stores its time series in a persistent volume;
the portal's rolling history is not a durable Prometheus query frontend.

## Kubernetes

Build `dashboard/Dockerfile` with `dashboard` as its context, push to your registry,
and replace the dashboard manifest image with the tested digest. Create the
namespace if it does not already exist. Create `webtransport4j-dashboard-config`
from a protected env file containing `WT4J_ADMIN_USERNAME`, `WT4J_ADMIN_PASSWORD`,
`WT4J_CLUSTER_NODES`, `WT4J_PROMETHEUS_TARGETS`, `WT4J_NODE_MANAGEMENT_TOKEN`, and
`WT4J_OTLP_TOKEN`. Create `webtransport4j-dashboard-tls` using your TLS certificate
and key. For example:

```sh
kubectl create secret generic webtransport4j-dashboard-config -n webtransport-prod --from-env-file=dashboard.env
kubectl create secret tls webtransport4j-dashboard-tls -n webtransport-prod --cert=tls.crt --key=tls.key
kubectl apply -f k8s/dashboard-deployment.yaml
kubectl rollout status deployment/webtransport4j-dashboard -n webtransport-prod
```

The Service stays private. Route your operator-facing gateway to its HTTPS port
with backend certificate verification configured, or use TLS passthrough. Restrict
network access to operators, your ingestion producers, and required upstreams.
Replicas are deliberately one with Recreate updates because tokens and history
are not shared. A restart requires operators to sign in again. Mount a trusted CA
bundle and set `SSL_CERT_FILE` when remote nodes use a private PKI.

## Operational behavior and checks

- `/healthz` is an unauthenticated process liveness check. It does not claim that
  remote nodes or telemetry sources are healthy; confirm those in cluster status.
- Analytics, metrics, audit logs, and JMX proxies require an operator bearer token.
  `/v1/metrics` and `/v1/traces` instead require `Authorization: Bearer <WT4J_OTLP_TOKEN>`.
  Producers may send JSON OTLP traces directly to this authenticated HTTPS endpoint.
  The bundled collector logs traces; it does not forward them to the portal.
- Resets require POST and clear telemetry without closing production sessions or
  deleting the audit viewer. Node drain/close actions remain explicit operations.
- Audit events are emitted as JSON on stdout in production. Ship container logs
  to durable storage with access control and retention; the in-memory viewer is
  not an immutable archive.
- Traffic generation and stream/datagram injection are disabled in production.
  The branch's Java traffic generator trusts all certificates and some studio
  actions synthesize stream state. Use an independently secured load-test client;
  these development tools are not packaged in the production portal image.
- Non-local startup requires TLS and a password. Production also requires explicit
  node and metrics targets. Login attempts and HTTP worker concurrency are bounded.
- Rotate passwords/tokens and certificates by updating secrets and restarting the
  portal. Pin the built image digest for deployment and retain the prior digest
  for rollback. Back up the Prometheus volume before host migration.

Run `python -m unittest discover -s dashboard -p test_production_security.py` and
`python dashboard/test_dashboard_unit.py` before deploying. Check that unauthenticated
API calls return 401, source-file URLs return 404, TLS verification succeeds, login
and logout work, and remote nodes and real traffic metrics appear. Run a staging
smoke test against your actual nodes before granting production operator access.

For a repeatable isolated environment with real nodes, collector, Prometheus,
and live production-mode portal checks, follow [testing/README.md](testing/README.md)
and run `python dashboard/testing/run.py up` from the repository root.
