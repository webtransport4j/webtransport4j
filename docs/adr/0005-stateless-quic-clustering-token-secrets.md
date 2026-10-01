# ADR 0005: Stateless QUIC Token Secret Coordination & Cluster Broadcast Bridge

## Status
Accepted

## Context
Deploying WebTransport in modern Kubernetes clusters or multi-region Anycast topologies places multiple server pods behind L4/UDP load balancers. A client sending an initial packet or migrating network paths (e.g., from WiFi to Cellular) may land on a different pod in the cluster.
If address validation tokens (RFC 9000 section 8.1) are signed with node-local random keys, token validation fails on peer nodes, forcing reconnects and breaking seamless connection migration.

## Decision
We implemented a cluster secret coordination and messaging architecture:
1. **Stateless Token Secret Provider SPI (`StatelessTokenSecretProvider`)**:
   - Provides synchronized HMAC keys across all pods in a cluster (via Kubernetes Secrets, HashiCorp Vault, or environment configuration).
   - `RotatingTokenSecretProvider` supports zero-downtime key rotation by preserving historical verification keys during rotation windows.
2. **Cluster Broadcast Bridge SPI (`ClusterBroadcastBridge`)**:
   - Strategy interface for cross-node topic publishing and subscriptions.
   - Decouples application broadcasting from specific pub/sub brokers (Redis, Kafka, NATS, or in-memory).

## Consequences

### Positive
- **Seamless Anycast / Multi-Pod Migration**: Clients can roam across cluster nodes without address validation rejections.
- **Zero-Downtime Secret Rotation**: Cryptographic keys can be rotated regularly according to security compliance without dropping connections.
- **Pluggable Multi-Node Scaling**: Decoupled pub/sub bridge allows scaling across tens or hundreds of cluster nodes.
