# Concurrency model checks

These TLA+ specifications check bounded abstractions of server lifecycle and
session admission. They are executable design checks, not a proof of the Java
implementation, Netty, or the JVM memory model. No production code is generated
from them. TLC exhaustively explores the configured finite state spaces and checks
both invariants and temporal properties.

## Run

Requires Python 3 and Java 17 or newer. Download the official, pinned TLC release:

```sh
curl -fL https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar -o /tmp/tla2tools.jar
python3 verification/tla/check.py --jar /tmp/tla2tools.jar --output /tmp/webtransport4j-tlc
```

The runner verifies SHA-256
`7beec0f04818732a62fa193731711a99aa4f11279499b2360a7d156c519ea78d`
before executing the downloaded JAR. It runs the lifecycle/admission checks, six additional concurrency models,
negative controls, and retained counterexamples for unguarded historical abstractions. A negative control passes only when TLC
reports its expected invariant violation; parser errors and unrelated failures
fail the runner. Logs and counterexample traces are retained with `--output`.
Without that option, temporary output is removed when the run ends. Each check
has a two-minute timeout and uses one worker for reproducibility.

This pin matches the [official v1.8.0 release asset](https://github.com/tlaplus/tlaplus/releases/tag/v1.8.0)
published on 2026-10-06, with SHA-256 confirmed against GitHub's release metadata
and a fresh download. The verified GitHub release asset ID is `614143604`. The previous artifact at
the same URL had SHA-256
`411ab54221cf0c9fa7ae18f07a3e0ebbdf9e5ba6254b79017e7007f1feb44e89`.
Replacing a release asset does not automatically authorize a new checksum:
verify its provenance and rerun all models before updating the pin.

To open a model in the TLA+ Toolbox, use the module's accompanying `.cfg` values,
invariants, and properties. Background: [TLC configuration documentation](https://docs.tlapl.us/using:tlc:config_file)
and [Specifying Systems](https://lamport.azurewebsites.net/tla/book-02-02-27.pdf).

## Model-to-code mapping

| Model action/state | Java implementation | Checked properties |
|---|---|---|
| Lifecycle `Start`, `current`, `Publish` | `WebTransportServer.start`, `startEpoch`, `publishIfStillStarting` | Single published owner; stale startups cannot publish into a newer start |
| Lifecycle `Build`, `Discard`, `Fail` | `doStart`, unpublished resource cleanup, startup failure cleanup | Resources allocated after stop are eventually released; release occurs once |
| Lifecycle `Stop`, `Close`, `Release`, `FinishStop` | `stop`, sticky `permanentlyClosed`, resource teardown | Stopped/closed servers own no published resources; close is terminal; shutdown completes |
| Admission `Gate`, `ReserveLocal`, `ReserveGlobal` | `WebTransportHeadersHandler` admission check and global-slot CAS; `WebTransportSessionManager.reserveSession` | Local/global limits and reservation accounting |
| Admission `Commit`, `Cancel` | Header-write completion, close-future callback, `pending.compareAndSet`, register/unregister | One release owner; no leaked pending reservations under fair callback delivery |
| Admission `Drain`, `drained`, `FinishStop` | Server drain, registration during drain, session close and stop deadline | Active sessions receive drain notification; shutdown eventually becomes empty |

Lifecycle uses three distinct startup attempts. Old attempts can allocate local
resources after cancellation and after a new startup begins. A finite attempt ID
abstracts the monotonically increasing epoch without wrapping or reusing it.
An attempt's resource bundle represents the channel, group, watcher, generated
certificate and shutdown hook; individual resource allocation/cleanup failures
are not represented. Failed startup cleanup is collapsed into one transition.
Stop/close requests coalesce while teardown is underway.

Admission uses three requests across two connections. One connection owns one
request and the other owns two, selected deterministically by `CHOOSE`; request
identity includes the connection, so stream IDs need not be globally unique.
The baseline has global capacity two and local capacity one. The additional
configuration has global capacity one and local capacity two, exercising shared
global contention independently of the local limit. Requests are single-use.
Local and global reservation are separate steps, so cancellation can occur between
them. CAS loops abstract to atomic successful increments; a full limit leaves the
request waiting until cancellation, representing rejection/rollback.

The admission gate precedes drain and reservation. A request already checked
before drain may reserve and finish afterward, matching the current Java race.
The model does **not** assert that every post-drain handshake completion is
rejected. Commit during drain marks that session as drained.

## Atomicity and progress assumptions

Lifecycle publication and stop state transitions abstract `lifecycleLock` critical
sections. Admission completion/cancellation are modeled as atomic ownership
transfers and counter updates. Real Java counter decrements, map changes, channel
closure, and callback delivery are separate operations: this model does not check
all of their intermediate states. Drain notification is abstracted to one atomic
set update; it does not check iteration races, event-loop rejection, GOAWAY
encoding, or delivery failures. Flow-control negotiation and dynamic capacity
changes are outside the model.

Liveness assumes weak fairness for resource construction/disposal, completion
callbacks, and shutdown finalization. The admission model assumes every request
eventually closes or is cancelled (including forced closure at the shutdown
deadline). It does not promise that every client is admitted, or verify a real-time
shutdown bound. No symmetry reduction is used because temporal properties are
checked. Deadlock checks are disabled to allow legitimate terminal states;
explicit temporal properties still detect stalled shutdown/cleanup.

## Negative controls and maintenance

`CheckEpoch = FALSE` removes the startup epoch fence. TLC must find
`NoStalePublication` violated when an old startup publishes during a newer start.
`SingleWinner = FALSE` allows a duplicate release callback. TLC must find `Capacity`
violated when an already released slot is decremented again. These controls check
that important races are reachable and the invariants detect them.

Update these models whenever lifecycle ownership, admission sequencing, or drain
semantics change. Keep the mapping and atomicity assumptions accurate. Turn useful
counterexamples into Java regression tests, and run the ordinary Java integration
suite as well. Passing these bounded models does not certify production readiness.

## Additional concurrency areas

| Area / module | Implementation mapping | Properties and configurations |
|---|---|---|
| `Mailbox` | `StreamMailbox.enqueue/run/drainAndRelease`; `DatagramMailbox.enqueue/run/drainAndRelease` | Retained, queued, in-flight and released ownership; at-most-once release; worker empty-observe/clear/recheck wakeup race; eventual disposal under fair workers |
| `ReactivePublisher` | `WebTransportFlowPublisher.emitNext`, `request`, `cancel`, `drain`, `terminated` | Delivery within requested demand, at-most-one terminal signal, cancellation cleanup, completion progress when the queue is empty |
| `TlsReload` | `TlsCertificateWatcher.checkAndReload`; `WebTransportServer.installReloadedSslContext/newQuicSslEngine/stop` | Stable engine snapshots, guarded publication after shutdown, monotonically newer context publication, completion of reload attempts |
| `FlowControl` | `WebTransportUtils.wrapFutureStreamChannel`; session stream counters; peer limit capsule updates | Cumulative stream-credit accounting, no overspend with atomic reservation, monotonic grants, eventual request completion/cancellation |
| `AsyncMetrics` | `AsyncWebTransportMetricsListener.dispatch/close`; single-worker `ThreadPoolExecutor` | Queue bound, one in-flight callback, rejection accounting, completion or abandonment of accepted tasks |
| `ConfigReload` | `WebTransportConfig.reload/getVal`; pointer to newly built `Properties` | Whole-snapshot publication, ordered publication with a generation fence, coherent paired reads when capturing a snapshot, reload/read progress |

### Fidelity and retained bug counterexamples

The runner prints `EXPECTED COUNTEREXAMPLE` for six deliberately unguarded historical variants. They retain the bugs reproduced before the Java fixes as negative controls; they are no longer descriptions of the repaired implementation. A successful runner exit means both guarded models and the retained controls behaved as specified. It does not prove Java or JVM correctness. See [Java verification](../concurrency/README.md) and [before/after results](../concurrency/RESULTS.md).

* **TLS shutdown:** an old reload callback publishes after shutdown. Java installation now uses the lifecycle lock, checks terminal/stop state, and the watcher callback checks its startup epoch. The model assumes an atomic check/install boundary.
* **TLS generation:** concurrent public reload calls install out of order. Java serializes build and callback per watcher; scheduled polling and public calls use the same lock. This supplies ordering without changing the consumer abstraction.
* **Stream credit:** two callers separately check then increment. Java now makes check/reservation one critical section on the existing session; the passing atomic-reservation model abstracts that section. It does not require CAS specifically or model transport stream creation.
* **Publisher terminal race:** an emitter offers after terminal drain. Java now releases drain ownership on terminal paths so the late offer triggers cleanup, rather than leaving work-in-progress permanently set. The model's recheck represents the ownership contract; it does not encode the Java drain loop instruction by instruction or assert that the actual queue is never transiently nonempty after terminal.
* **Config stale publication:** an older build overwrites a newer publication. Java uses revision checks and atomic pointer publication.
* **Config mixed read:** separate ordinary getters cross a reload. Those getters intentionally remain independent; Java adds an explicit snapshot for related dynamic-setting reads and copy-on-write programmatic updates. The model's snapshot-reader contract applies to callers using this API.

### Bounds and assumptions for the additional models

* **Mailboxes:** three unique frames, capacity one, one worker. The datagram
  configuration bounds outstanding queue reservations, excluding the in-flight
  callback. The stream configuration is deliberately unbounded within the finite
  frame universe: stream watermarks are auto-read backpressure, not a hard queue
  capacity. Frame sets abstract away FIFO and batch order. `Admit` combines slot
  reservation and retain; queue publication is separate and can occur after close.
  Worker clearing and queue rechecking are separate transitions. Executor rejection
  shares the closed-and-draining abstraction. Caller-owned original references,
  temporarily incremented size values, retain failures, metrics failures, batch
  yielding and backpressure hysteresis are not verified. Fairness assumes callbacks
  return and cleanup actions run.
* **Publisher:** three unique items and three unit demand requests. Single subscriber,
  serialized drain callback effects and finite demand; Java long overflow/saturation,
  simultaneous subscriber registration, invalid demand, callback exceptions and
  FIFO are outside this model. No assertion promises completion while demand is
  exhausted and queued items remain. Delivery is atomic, so cancellation between
  poll and `onNext` and an already in-flight signal are not checked. The contract
  configuration rechecks the emit gate; the diagnostic disables that recheck.
* **TLS:** initial context version 1, reload versions 1 and 2, two handshakes, one
  shutdown. Engine context snapshots never change after creation. Publishing an
  entire validated context is atomic; key/certificate filesystem pair consistency,
  ticket key mutation, native context disposal, and restart epochs are not modeled.
  Reload progress assumes callback delivery or discard eventually executes.
* **Flow control:** three one-unit stream creation operations, initial cumulative
  credit one, maximum credit two. Closing a stream does not refund cumulative
  credit. Failed creation consumes its reserved index, matching current Java;
  therefore this model does not assert rollback of failed creation. Bidi/uni share
  the same algorithm and are represented by one counter. QUIC byte budgets,
  `WT_MAX_DATA` enforcement, integer overflow, bypass mode and decreasing peer
  capsule protocol errors are not checked. Progress assumes pending callbacks or
  cancellation eventually run; it does not guarantee a grant or successful open.
* **Metrics:** three single-use events, queue capacity one, one callback worker.
  Check-before-shutdown and actual executor submission are separate actions.
  Calls observing shutdown are ignored without incrementing the drop counter.
  Calls rejected by submission are counted, while tasks discarded by `shutdownNow`
  are abandoned without entering the rejection counter, matching the current
  policy. `delivered` means callback execution returned, including a contained
  runtime exception; it does not guarantee successful backend export. Fairness
  assumes delegates return. An uninterruptible delegate may outlive `close()`.
* **Config:** two complete snapshot generations and two readers reading two keys.
  `AtomicPublish = TRUE` reflects the volatile pointer swap. The stronger passing
  contract additionally assumes generation fencing and reader snapshot capture;
  Java now provides those boundaries for explicit dynamic snapshots. Programmatic
  copy-on-write `setProperty/removeProperty`, file parse failures, system/environment overrides,
  and downstream rate-limit rule construction are outside the model. The
  torn-publication negative control deliberately replaces the atomic pointer swap
  with two writes; it is not a claim about Java's actual pointer publication.

Additional negative controls remove mailbox single-release ownership and the
worker recheck, publisher terminal ownership and demand checks, exporter queue
capacity, and atomic config publication. They must violate the specific configured
invariant, rather than merely fail parsing. CI preserves TLC logs containing the
counterexample state sequences. The checker suppresses generation of extra trace
exploration source files in the repository.
