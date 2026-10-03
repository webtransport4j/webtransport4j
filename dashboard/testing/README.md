# Test the production admin portal and analytics

Requirements: Docker with Linux containers and Compose, Python 3, Node.js, and
OpenSSL (Git for Windows includes OpenSSL). No host Java installation is required.

From the repository root:

```sh
python dashboard/testing/run.py up
```

This command runs the unit/security checks and JavaScript syntax checks, builds
Java 25 test nodes and the production portal image, starts an isolated Compose
project, and runs live integration checks. The first build downloads Maven
dependencies and container images. Later builds reuse caches.
The test node image compiles this branch's Java sample applications for Java 25;
it does not validate the library's separate Java 8 release packaging.

Open `https://localhost:18443/admin.html`. The generated test-only username and
password are in `dashboard/testing/.runtime/credentials.json`. The analytics UI
is at `https://localhost:18443/` after signing in. The certificate is generated
for this environment and is not automatically trusted by your browser; review
`dashboard/testing/.runtime/tls.crt` before trusting it for manual testing.
The automated HTTPS client verifies this certificate without disabling TLS
verification. Only the portal port is published, on loopback. The three Java
nodes, collector, and Prometheus communicate across their private Docker network.

```sh
python dashboard/testing/run.py test    # Rerun unit checks and live integration
python dashboard/testing/run.py status  # View running services and health
python dashboard/testing/run.py logs    # Recent logs from all services
python dashboard/testing/run.py down    # Stop this test stack; preserve test data
python dashboard/testing/run.py reset   # Stop and delete this stack's volumes and generated files
```

Use `up --port 19443` on the first run to choose a different host port. The saved
test configuration preserves the selected port until reset. Generated credentials,
certificates, and test logs are git-ignored under `.runtime`; do not reuse these
credentials or keys for production. Test keys are readable by the unprivileged
container identity; they are intentionally disposable.

## Coverage

- Existing dashboard unit tests and production security regression tests.
- Both pages and actual Python APIs in the production Docker image over HTTPS.
- Anonymous request rejection, exact operator identity, login/logout, source
  blocking, production injection guards, and separate OTLP ingestion tokens.
- Three real Java nodes with different hostnames and the same management port.
- Real JMX memory, QUIC handshake, bidirectional echoes, unidirectional streams,
  datagrams, persistent sessions, and node metrics exported through the collector.
- Collector scraping by Prometheus, authenticated JSON OTLP ingestion (including
  an explicitly labeled test trace), analytics history and metrics endpoints.
- Individual session drain/close routing, preservation of another node's session,
  safe telemetry reset, audit retention, and explicit bulk session close.

The traffic generator runs only as a test client inside this isolated network.
It uses the branch's test client that accepts the disposable node certificates;
traffic generation remains disabled in the production portal. The setup does
not claim browser automation coverage, full QUIC standards interoperability,
production load capacity, HA, or validation of your real cloud network/DNS/PKI.
The repository's separate Java and interoperability suites cover transport
behavior beyond the admin/analytics scope.

If verification fails, the stack is left running for diagnosis. Check `logs` and
fix the reported issue, then run `test` again. All assertions exercise live
services; a missing service is a failure rather than a skipped test.
Each test run clears sessions on these disposable test nodes before and after
verification so reruns do not inherit stale traffic from an earlier run.
