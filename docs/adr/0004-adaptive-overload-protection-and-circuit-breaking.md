# ADR 0004: Adaptive Overload Protection, Circuit Breaking & QoS Datagram Eviction

## Status
Accepted

## Context
Under traffic spikes, DDOS surges, or slow downstream backends, network servers can enter positive feedback loops of resource exhaustion (cascade failure, JVM heap exhaustion, GC thrashing, and packet loss).

## Decision
We introduced multi-layered overload protection and admission control:
1. **Admission Control SPI (`OverloadProtectionPolicy`)**:
   - Evaluates incoming WebTransport `CONNECT` sessions before channel handshake completion.
   - Rejection returns HTTP 503 `Service Unavailable`; the handler includes `Retry-After` only when the policy supplies a positive retry delay.
2. **Adaptive Heap & Concurrency Monitoring (`AdaptiveOverloadProtectionPolicy`)**:
   - Tracks JVM heap usage (`(total - free) / max`) against a configurable ceiling (default 85%).
   - Rejects new sessions under severe heap pressure to allow ongoing sessions to drain safely.
3. **Adaptive Circuit Breaker (`AdaptiveCircuitBreaker`)**:
   - Atomic state transitions (`CLOSED` -> `OPEN` -> `HALF_OPEN`).
   - Automatically trips open when consecutive failure thresholds are breached, shedding requests immediately.
4. **QoS Priority Datagram Scheduling (`DatagramMailbox`)**:
   - Dual-queue priority scheduling with eviction of older normal-priority frames when high-priority frames arrive on a saturated queue.

## Consequences

### Positive
- **Graceful Degradation**: Protects the server process from crashing; rejects new sessions with HTTP 503, includes a retry delay when supplied by the policy, and closes the CONNECT stream.
- **Fair QoS Delivery**: Urgent telemetry and control datagrams preempt bulk background datagrams under congestion.
- **Standard Protocol Compliance**: Employs standard HTTP/3 status codes and headers.
