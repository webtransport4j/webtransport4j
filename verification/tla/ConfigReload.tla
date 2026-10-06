---------------- MODULE ConfigReload ----------------
EXTENDS Naturals, FiniteSets
CONSTANTS Versions, Readers, AtomicPublish, CheckGeneration, SnapshotRead
VARIABLES phase, active, latest, readPhase, first, second, captured
vars == <<phase, active, latest, readPhase, first, second, captured>>
Init == /\ phase = [v \in Versions |-> "new"] /\ active = <<0, 0>> /\ latest = 0
        /\ readPhase = [r \in Readers |-> "new"] /\ first = [r \in Readers |-> 0]
        /\ second = [r \in Readers |-> 0] /\ captured = [r \in Readers |-> 0]
Build(v) == /\ phase[v] = "new" /\ phase' = [phase EXCEPT ![v] = "built"]
            /\ UNCHANGED <<active, latest, readPhase, first, second, captured>>
Publish(v) == /\ phase[v] = "built" /\ (~CheckGeneration \/ v >= latest)
              /\ active' = IF AtomicPublish THEN <<v, v>> ELSE <<v, active[2]>>
              /\ latest' = IF v > latest THEN v ELSE latest
              /\ phase' = [phase EXCEPT ![v] = IF AtomicPublish THEN "done" ELSE "half"]
              /\ UNCHANGED <<readPhase, first, second, captured>>
FinishPublish(v) == /\ phase[v] = "half" /\ active' = <<active[1], v>>
                    /\ phase' = [phase EXCEPT ![v] = "done"]
                    /\ UNCHANGED <<latest, readPhase, first, second, captured>>
Discard(v) == /\ phase[v] = "built" /\ CheckGeneration /\ v < latest
              /\ phase' = [phase EXCEPT ![v] = "done"]
              /\ UNCHANGED <<active, latest, readPhase, first, second, captured>>
ReadFirst(r) == /\ readPhase[r] = "new" /\ first' = [first EXCEPT ![r] = active[1]]
                /\ captured' = [captured EXCEPT ![r] = active[2]]
                /\ readPhase' = [readPhase EXCEPT ![r] = "first"]
                /\ UNCHANGED <<phase, active, latest, second>>
ReadSecond(r) == /\ readPhase[r] = "first"
                 /\ second' = [second EXCEPT ![r] = IF SnapshotRead THEN captured[r] ELSE active[2]]
                 /\ readPhase' = [readPhase EXCEPT ![r] = "done"]
                 /\ UNCHANGED <<phase, active, latest, first, captured>>
Next == (\E v \in Versions : Build(v) \/ Publish(v) \/ FinishPublish(v) \/ Discard(v))
        \/ (\E r \in Readers : ReadFirst(r) \/ ReadSecond(r))
Spec == Init /\ [][Next]_vars
        /\ (\A v \in Versions : WF_vars(Publish(v) \/ Discard(v)) /\ WF_vars(FinishPublish(v)))
        /\ (\A r \in Readers : WF_vars(ReadSecond(r)))
TypeOK == /\ phase \in [Versions -> {"new", "built", "half", "done"}]
          /\ active \in [1..2 -> Versions \cup {0}] /\ latest \in Versions \cup {0}
          /\ readPhase \in [Readers -> {"new", "first", "done"}]
          /\ first \in [Readers -> Versions \cup {0}] /\ second \in [Readers -> Versions \cup {0}]
          /\ captured \in [Readers -> Versions \cup {0}]
PublishedSnapshotCoherent == active[1] = active[2]
NoStalePublication == active[1] = latest
ReaderSnapshotCoherent == \A r \in Readers : readPhase[r] = "done" => first[r] = second[r]
ReloadsResolve == \A v \in Versions : phase[v] \in {"built", "half"} ~> phase[v] = "done"
ReadsResolve == \A r \in Readers : readPhase[r] = "first" ~> readPhase[r] = "done"
=============================================================================
