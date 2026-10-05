-------------------------- MODULE ServerLifecycle --------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Attempts, NoAttempt, CheckEpoch
VARIABLES state, closed, current, phase, releases
vars == <<state, closed, current, phase, releases>>
Published == {a \in Attempts : phase[a] = "published"}
Init == /\ state = "stopped" /\ closed = FALSE /\ current = NoAttempt
        /\ phase = [a \in Attempts |-> "unused"]
        /\ releases = [a \in Attempts |-> 0]
Start(a) == /\ state = "stopped" /\ ~closed /\ phase[a] = "unused"
            /\ state' = "starting" /\ current' = a
            /\ phase' = [phase EXCEPT ![a] = "preparing"]
            /\ UNCHANGED <<closed, releases>>
\* Resources may finish allocating after stop has already returned.
Build(a) == /\ phase[a] = "preparing"
            /\ phase' = [phase EXCEPT ![a] = "local"]
            /\ UNCHANGED <<state, closed, current, releases>>
Valid(a) == state = "starting" /\ ~closed /\ a = current
Publish(a) == /\ phase[a] = "local" /\ state = "starting" /\ ~closed
              /\ (~CheckEpoch \/ a = current)
              /\ phase' = [phase EXCEPT ![a] = "published"]
              /\ state' = "started"
              /\ UNCHANGED <<closed, current, releases>>
Discard(a) == /\ phase[a] = "local" /\ ~Valid(a)
              /\ phase' = [phase EXCEPT ![a] = "done"]
              /\ releases' = [releases EXCEPT ![a] = @ + 1]
              /\ UNCHANGED <<state, closed, current>>
Fail(a) == /\ phase[a] = "local" /\ Valid(a)
           /\ phase' = [phase EXCEPT ![a] = "done"]
           /\ releases' = [releases EXCEPT ![a] = @ + 1]
           /\ state' = "stopped" /\ current' = NoAttempt
           /\ UNCHANGED closed
Drain == /\ state = "started" /\ state' = "draining"
         /\ UNCHANGED <<closed, current, phase, releases>>
Stop == /\ state \in {"starting", "started", "draining"}
        /\ state' = "stopping" /\ current' = NoAttempt
        /\ UNCHANGED <<closed, phase, releases>>
Close == /\ ~closed /\ closed' = TRUE /\ state' = "stopping"
         /\ current' = NoAttempt /\ UNCHANGED <<phase, releases>>
Release(a) == /\ state = "stopping" /\ phase[a] = "published"
              /\ phase' = [phase EXCEPT ![a] = "done"]
              /\ releases' = [releases EXCEPT ![a] = @ + 1]
              /\ UNCHANGED <<state, closed, current>>
FinishStop == /\ state = "stopping" /\ Published = {}
              /\ state' = IF closed THEN "closed" ELSE "stopped"
              /\ UNCHANGED <<closed, current, phase, releases>>
Next == Drain \/ Stop \/ Close \/ FinishStop
        \/ (\E a \in Attempts : Start(a) \/ Build(a) \/ Publish(a)
            \/ Discard(a) \/ Fail(a) \/ Release(a))
Spec == Init /\ [][Next]_vars /\ WF_vars(FinishStop)
        /\ (\A a \in Attempts : WF_vars(Build(a)) /\ WF_vars(Discard(a))
             /\ WF_vars(Release(a)) /\ WF_vars(Publish(a) \/ Fail(a)))
TypeOK == /\ state \in {"stopped", "starting", "started", "draining", "stopping", "closed"}
          /\ closed \in BOOLEAN /\ current \in Attempts \cup {NoAttempt}
          /\ phase \in [Attempts -> {"unused", "preparing", "local", "published", "done"}]
          /\ releases \in [Attempts -> Nat]
SingleOwner == Cardinality(Published) <= 1
NoStalePublication == state \in {"started", "draining"} => Published = {current}
NoDoubleRelease == \A a \in Attempts : releases[a] <= 1
ResourceAccounting == \A a \in Attempts : (phase[a] = "done") <=> (releases[a] = 1)
TerminalClose == closed => state \in {"stopping", "closed"}
StoppedOwnsNothing == state \in {"stopped", "closed"} => Published = {}
ShutdownCompletes == state = "stopping" ~> state \in {"stopped", "closed"}
CloseCompletes == closed ~> state = "closed"
CancelledResourcesReleased == \A a \in Attempts :
    (phase[a] \in {"preparing", "local"} /\ ~Valid(a)) ~> phase[a] = "done"
=============================================================================
