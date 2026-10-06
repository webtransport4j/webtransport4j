# Checked results

Rechecked on 2026-10-06 with the official TLC v1.8.0 release asset
(SHA-256 `7beec0f04818732a62fa193731711a99aa4f11279499b2360a7d156c519ea78d`),
Java 25, and the committed finite configurations. All 24 runner checks produced
their expected outcomes: 10 successful safety/liveness explorations, 8 deliberately
broken negative controls, and 6 retained historical abstraction counterexamples.
The 10 successful explorations visited 6,632 distinct states in total (separate
state spaces; this is not one combined model).

| New area | Successful exploration | Retained historical counterexamples |
|---|---:|---|
| Datagram / stream mailboxes | 545 / 564 distinct states | None in the modeled abstraction |
| Reactive publisher contract | 2,118 distinct states | An emitter can offer after terminal drain |
| Guarded TLS reload contract | 144 distinct states | Publication after stop; older concurrent reload overwrites newer context |
| Atomic stream-credit contract | 594 distinct states | Separate check/increment can overspend with concurrent callers |
| Async metrics listener | 775 distinct states | None in the modeled policy; forced shutdown may abandon queued events |
| Guarded config snapshot contract | 185 distinct states | Stale publication; paired reads span two generations |

The guarded contract results do not prove Java correctness. The repaired implementation is separately exercised by [Java regression and JCStress checks](../concurrency/RESULTS.md).
See [the model guide](README.md) for mapping, atomicity, scheduling assumptions,
and exclusions. Counterexamples are modeling evidence to turn into Java race
regressions, not an independently demonstrated execution of the implementation.

Reproduce and retain full logs, including counterexample state sequences:

```sh
python3 verification/tla/check.py --jar /tmp/tla2tools.jar --output /tmp/webtransport4j-tlc
```

An invalid artifact was also tested and rejected before any model ran, with
an error reporting both expected and observed hashes. Strict artifact verification
remains enabled. The successful exploration state counts are unchanged.

Exact runner output:

```text
PASS mailbox-datagrams: 1347 states generated, 545 distinct states found
PASS mailbox-datagrams-negative: detected NoDoubleRelease
PASS publisher-contract: 5255 states generated, 2118 distinct states found
PASS publisher-contract-negative: detected SingleTerminalSignal
PASS tls-guarded: 213 states generated, 144 distinct states found
EXPECTED COUNTEREXAMPLE tls-shutdown-race: detected StoppedHasNoContext
PASS flow-atomic: 1540 states generated, 594 distinct states found
EXPECTED COUNTEREXAMPLE flow-check-increment-race: detected CreditBound
PASS metrics: 1614 states generated, 775 distinct states found
PASS metrics-negative: detected CapacityBound
PASS config-guarded: 397 states generated, 185 distinct states found
PASS config-guarded-negative: detected PublishedSnapshotCoherent
PASS mailbox-streams: 1457 states generated, 564 distinct states found
PASS mailbox-lost-wakeup: detected NoStrandedWork
EXPECTED COUNTEREXAMPLE publisher-terminal-race: detected NoPostTerminalQueue
PASS publisher-demand-negative: detected DemandRespected
EXPECTED COUNTEREXAMPLE tls-stale-reload: detected MonotonicPublication
EXPECTED COUNTEREXAMPLE config-stale-reload: detected NoStalePublication
EXPECTED COUNTEREXAMPLE config-mixed-reader: detected ReaderSnapshotCoherent
PASS lifecycle: 1695 states generated, 543 distinct states found
PASS admission: 1874 states generated, 587 distinct states found
PASS global-capacity: 1848 states generated, 577 distinct states found
PASS broken-epoch: detected NoStalePublication
PASS broken-completion: detected Capacity
```
