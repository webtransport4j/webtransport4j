---------------- MODULE TlsReload ----------------
EXTENDS Integers, FiniteSets
CONSTANTS Versions, Handshakes, CheckRunning, CheckGeneration
VARIABLES running, active, newestPublished, phase, engines
vars == <<running, active, newestPublished, phase, engines>>
Init == /\ running = TRUE /\ active = 1 /\ newestPublished = 1
        /\ phase = [v \in Versions |-> "new"] /\ engines = [h \in Handshakes |-> -1]
Build(v) == /\ running /\ phase[v] = "new"
            /\ phase' = [phase EXCEPT ![v] = "built"]
            /\ UNCHANGED <<running, active, newestPublished, engines>>
Publish(v) == /\ phase[v] = "built"
              /\ (~CheckRunning \/ running) /\ (~CheckGeneration \/ v >= newestPublished)
              /\ active' = v /\ newestPublished' = IF v > newestPublished THEN v ELSE newestPublished
              /\ phase' = [phase EXCEPT ![v] = "published"]
              /\ UNCHANGED <<running, engines>>
Discard(v) == /\ phase[v] = "built"
              /\ ((CheckRunning /\ ~running) \/ (CheckGeneration /\ v < newestPublished))
              /\ phase' = [phase EXCEPT ![v] = "discarded"]
              /\ UNCHANGED <<running, active, newestPublished, engines>>
Handshake(h) == /\ running /\ engines[h] = -1
                /\ engines' = [engines EXCEPT ![h] = active]
                /\ UNCHANGED <<running, active, newestPublished, phase>>
Stop == /\ running /\ running' = FALSE /\ active' = 0
        /\ UNCHANGED <<newestPublished, phase, engines>>
Next == Stop \/ (\E v \in Versions : Build(v) \/ Publish(v) \/ Discard(v))
        \/ (\E h \in Handshakes : Handshake(h))
Spec == Init /\ [][Next]_vars
        /\ (\A v \in Versions : WF_vars(Publish(v) \/ Discard(v)))
TypeOK == /\ running \in BOOLEAN /\ active \in Versions \cup {0}
          /\ newestPublished \in Versions \cup {0}
          /\ phase \in [Versions -> {"new", "built", "published", "discarded"}]
          /\ engines \in [Handshakes -> Versions \cup {0, -1}]
ValidEngines == \A h \in Handshakes : engines[h] \in Versions \cup {-1}
StoppedHasNoContext == ~running => active = 0
MonotonicPublication == running => active = newestPublished
EngineSnapshotsStable == \A h \in Handshakes : engines[h] # -1 => engines'[h] = engines[h]
BuiltContextsResolve == \A v \in Versions : phase[v] = "built" ~> phase[v] \in {"published", "discarded"}
EngineImmutability == [][EngineSnapshotsStable]_vars
=============================================================================
