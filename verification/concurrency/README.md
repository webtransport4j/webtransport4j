# Java concurrency verification

These tests exercise the Java implementation alongside the [TLA+ specifications](../tla/README.md). They cover the additional areas: reactive publishers, TLS reload, flow control, async metrics, and configuration reload. Real QUIC tests also exercise established sessions during server drain.

| Area | Deterministic checks | Model/history checks | JCStress | Real QUIC |
| --- | --- | --- | --- | --- |
| Reactive publisher | Cancel during delivery; competing terminals; emit/complete ownership | All 1,024 five-command emit/request/complete/cancel histories | Item ownership and terminal count | — |
| TLS reload | Late callback after close; older callback after newer reload | — | Install/close publication | New handshakes use rotated certificate; existing connection remains active |
| Flow control | Two creators compete for one peer stream credit | 100 seeded, 20-command create/grant cumulative-credit histories | Concurrent stream reservation | Stream transport exercised; credit race uses controlled transport |
| Async metrics | Queue saturation; shutdown between admission check and execute | — | Submission/shutdown callback/drop accounting | — |
| Config reload | Reject stale reload; captured reads stay on one generation | 20 real file-replacement generations, complete snapshots and prior snapshot preservation | Single-property programmatic visibility | — |

The strategy varies by boundary; this is not four independent proofs of every area. Java model-based tests compare actual operations with small independent reference models. Histories are bounded and primarily sequential. JCStress samples concurrent outcomes and does not prove absence of races. Transport mocks isolate the reservation boundary in flow-control tests. TLS regressions invoke the actual private installation boundary using reflection; the ordering regression additionally builds real contexts through concurrent `checkAndReload()` calls. The normal watcher uses one scheduled worker, so its concurrent-reload regression applies when the public method is called concurrently.

Event-loop lock safety is checked separately by `verification/locking`: an ASM control-flow audit covers application-owned monitor and known blocking-call sites, while a Java agent checks those sites on actual Netty event-loop threads. CI includes a deliberately unsafe callback plus bypassed and caught-guard static controls. This cannot certify locks inside Netty, the JDK, providers, or arbitrary injected executor implementations.

## Passing regression checks

From the repository root, run:

```sh
mvn -Pbench -Dexec.skip=true test -Dtest=PublisherConcurrencyTest,PublisherModelBasedTest,MetricsConcurrencyTest,FlowControlModelBasedTest,ConfigReloadConcurrencyTest,QuicConcurrencyIntegrationTest,ConcurrencyRaceRegressionTest
```

These 21 tests are also included by ordinary Surefire discovery. QUIC tests require working local UDP sockets and the supported native QUIC library. They use real handshakes and trust the exact test certificate, with `localhost` as the peer name. Synchronization uses bounded latches, barriers and futures rather than sleeps. Configuration tests save and restore the root dynamic-properties file; run them serially, without an external process editing that file.

## Formerly failing race regressions

```sh
mvn -Pbench -Dexec.skip=true test -Dtest=ConcurrencyRaceRegressionTest
```

All six former diagnostics are now included in default Surefire discovery and must pass. The stream test pauses the first credit read while a second creator starts; the TLS ordering test overlaps two public reload calls. Each waits until the contender has completed or reached a monitor held by the paused owner before releasing that owner. Their old barriers assumed that both callers could enter a non-atomic boundary and would deadlock against the repaired serialization. The invariants remain strict: exactly one credit reserved and the newest TLS context installed.

The configuration pair test uses the additive `WebTransportConfig.snapshot()` API. Independent existing `get()` calls retain their behavior and may cross reloads. A captured snapshot holds one dynamic generation across both file reloads and programmatic set/remove operations. System-property and environment overrides still have precedence; the snapshot contract applies to dynamic settings, not concurrent changes to JVM system properties.

## Production fixes

Publisher terminal paths relinquish drain ownership and dispose late offers; completion/error publish only the winning terminal state. Stream admission and cumulative reservation are confined to the owning Netty event loop, without changing transport/session interfaces or refund semantics. Watchers serialize certificate build/installation on a separate control-plane lock so stop does not wait on a callback; installation is fenced by the server lifecycle lock and the captured startup epoch. Dynamic reloads reject superseded revisions, while programmatic updates use copy-on-write to preserve captured generations. Rate-limit rule construction, reloader settings, and session flow-control fallback settings read one captured generation. Metrics callbacks use a bounded lock-free mailbox and a dedicated worker.

## JCStress

The isolated module keeps the harness and Mockito out of production dependencies. Its shaded jar preserves the multi-release manifest so the library selects its Java-version-specific implementations. Install the current root artifact before building it; rebuild/reinstall after production changes so the harness uses the code under review.

```sh
mvn -Pbench -Dexec.skip=true -DskipTests install
jar --validate --file target-maven/webtransport4j-0.1.0-SNAPSHOT.jar
mvn -f verification/jcstress/pom.xml package
cd verification/jcstress/target
java -jar jcstress.jar -t 'io.github.webtransport4j.concurrency.*' \
  -m quick -f 2 -fsm 1 -iters 3 -time 300 -c 2 -af NONE -sc false \
  -strideSize 1 -strideCount 1 \
  -jvmArgs '-Xms64m -Xmx128m' -r reports
```

This is the exact bounded post-fix configuration verified locally with JCStress 0.16 (two forks, three iterations, 300 ms per iteration, normal optimizing JIT). It returns nonzero on forbidden outcomes. Read `target/reports/index.html` for outcomes and observations. For broader exploration, use the harness's `-h` options and increase forks, iterations, duration and JVM/compiler configurations; a short passing run is not exhaustive verification.

See [recorded results](RESULTS.md) for before/after findings and the scope of validation.
