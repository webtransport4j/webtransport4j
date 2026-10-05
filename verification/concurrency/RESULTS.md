# Java concurrency fixes and verification

Verified on 2026-10-04, macOS, Corretto JDK 25, working tree based on `6bf9bed`.

| Verification | Result |
| --- | --- |
| Complete isolated Maven test suite and install | 385 tests; 0 failures/errors; 2 external-server benchmarks skipped |
| Focused concurrency suite (final fixtures) | 21 passed; 0 failures/errors/skips |
| Strict race regressions included above | All 6 passed |
| JCStress 0.16 post-fix run | All 7 classes passed; no forbidden outcomes or error tests |
| TLC bounded checks | All 24 expected results: 10 positive explorations, 14 retained negative/control counterexamples |
| Checkstyle and explicit multi-release jar validation | Passed |
| Original stream/TLS mutation controls | Both detected the original assertion failures |
| Event-loop lock bytecode audit | 1,486 methods scanned; 19 application monitor sites, all control-plane guarded; no synchronized methods or unguarded explicit locks |
| Event-loop runtime agent and negative controls | 22 focused tests passed with 0 violations; unsafe monitor rejected before acquisition; guard bypass and caught-guard static controls rejected |

## Before/after failure table

| Original failure | Targeted production fix | Regression |
| --- | --- | --- |
| Publisher emitter passed admission, offered after terminal, and stranded its item (close count 0) | Release drain ownership on terminal paths; a late offer triggers cleanup | `emitThatPassedGateMustNotLeakAfterCompletion` |
| TLS callback published a context into a closed server | Check lifecycle under the installation lock; capture startup epoch in watcher callbacks | `delayedTlsInstallationMustNotRepublishAfterClose` |
| Two creators reserved 2 streams with peer credit 1 | Serialize admission check and cumulative reservation on the existing session | `concurrentStreamCreationMustNotOverspendPeerCredit` |
| Older config reload replaced published generation 2 with generation 1 | Reject reload publication superseded by a newer revision/write | `olderConfigReloadMustNotOverwriteNewerPublication` |
| Older public TLS reload callback replaced a newer context | Serialize context build and consumer callback per watcher on a separate reload lock | `olderTlsReloadMustNotOverwriteNewerPublication` |
| Related configuration getters observed generations 1 and 2 | Add explicit dynamic-generation snapshot; copy-on-write set/remove preserve captured views | `pairedConfigurationReadsMustUseOneGeneration` |

All six assertions are now default-discovered `ConcurrencyRaceRegressionTest` tests. Existing independent getters retain their behavior; callers requiring related dynamic settings from one generation use `WebTransportConfig.snapshot()`. Rate-limit rule construction, reloader settings, and session fallback settings use the snapshot. No transport/session abstraction is replaced, and stream bypass/failed-create cumulative-credit semantics are preserved.

Two additional regressions check snapshot preservation after programmatic set/remove and ensure a losing publisher error cannot overwrite buffered completion. A production-consumer regression also reloads a setting during rate-limit rule construction and checks that the rule keeps its captured generation. The focused suite has 21 tests in total, including two real QUIC integration tests.

## Mutation checks

The stream and TLS ordering tests were separately run against the original `WebTransportUtils` and `TlsCertificateWatcher` classes compiled into a temporary directory. They detected the original overspending (`2` with credit `1`) and stale-context publication assertions. This checks that their revised synchronization handles serialization without losing sensitivity to the bugs.

## Verification commands

The complete Maven test suite is run in the repository's isolated build output to prevent external editor compilation from replacing class files during the test JVM's lifetime:

```sh
mvn -Pbench -Dexec.skip=true clean install
jar --validate --file target-maven/webtransport4j-0.1.0-SNAPSHOT.jar
mvn checkstyle:check
python3 verification/tla/check.py --jar /path/to/tla2tools.jar
```

The existing `bench` profile selects `target-maven`; `exec.skip` skips benchmark execution and the exec-plugin packaging check, which is therefore run explicitly with `jar --validate`. It does not skip Surefire tests. An earlier run in shared `target` was interrupted after external compilation mixed synthetic Java access methods and caused `NoSuchMethodError` failures; no test exclusions or automatic retry masking were added.

JCStress is rebuilt against the newly installed fixed artifact. Its outcomes remain strict (forbidden results fail the process), and passing sampled runs do not prove absence of all JVM races. See [the guide](README.md) for harness commands and scope.

The event-loop gate is intentionally scoped to application-owned bytecode and known blocking calls. It does not claim that Netty, the JDK, a TLS provider, or an arbitrary user-supplied executor has no internal locks; those implementations are outside this repository's control. The static checker is fail-closed for monitor acquisition and requires a successful control-plane guard on every normal path. CI also runs the detector against a deliberately unsafe callback and requires that control to fail.

The JCStress run used two forks, three iterations, 300 ms per iteration, two CPUs, stride size/count one, normal optimizing JIT, and no affinity/compiler splitting. Its shaded jar was checked byte-for-byte against all seven changed production classes in the installed artifact, and the multi-release manifest was preserved. The prior publisher ownership and TLS shutdown forbidden outcomes were absent in this bounded run. The HTML report is at `verification/jcstress/target/reports/index.html`.
