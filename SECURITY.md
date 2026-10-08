# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

The WebTransport4J team takes the security of our library seriously.

If you discover a security vulnerability within `webtransport4j`, please do **not** report it via a public GitHub issue.

Instead, please report security vulnerabilities privately using the repository's **[Report a vulnerability](https://github.com/webtransport4j/webtransport4j/security/advisories/new)** button under **Security → Advisories**. This route ensures your report is delivered securely to repository maintainers without requiring elevated repository permissions.

### What to include in your report:
* A description of the vulnerability and its potential impact.
* Steps to reproduce the issue, proof-of-concept code, or packet traces.
* Any potential mitigations or suggested fixes you have identified.

### Response Time:
We will acknowledge receipt of your vulnerability report within 48 hours and provide regular updates on the status of any fix and release.

---

## Production Security Hardening Guidelines

When deploying WebTransport4J in production environments, adhere to the following security recommendations:

1. **Disable Development Mode**:
   - Ensure `webtransport4j.dev_mode=false` in production.
   - Supply trusted, valid TLS certificates and private keys via `webtransport4j.ssl.cert.path` and `webtransport4j.ssl.key.path`. WebTransport requires TLS 1.3 as mandated by RFC 9001.

2. **Stateless QUIC Token Secret**:
   - In distributed or multi-instance server deployments, always configure a shared high-entropy HMAC key via `webtransport4j.quic.token.handler.hmac.key` to ensure safe address validation and prevent UDP amplification attacks.

3. **Origin & Authority Validation**:
   - Always validate the `:origin` and `:authority` pseudo-headers in your endpoint handlers to prevent Cross-Site WebTransport Session Hijacking (CSWSH).

4. **IP Rate Limiting & DoS Protection**:
   - Configure the built-in CIDR prefix rate limiter and Bloom filter (`IpRateLimitingHandler`, `IpBloomFilter`) to mitigate abusive clients and flood attacks.
   - Keep capsule size limits enforced (`webtransport4j.capsule.max_length=65536`, with close reason strings capped at 1024 bytes per RFC draft-16) to prevent memory allocation exhaustion.
