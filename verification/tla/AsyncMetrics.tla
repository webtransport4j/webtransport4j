---------------- MODULE AsyncMetrics ----------------
EXTENDS Naturals, FiniteSets
CONSTANTS Events, NoEvent, Capacity, EnforceCapacity
VARIABLES phase, queue, flight, mode, checked, rejected
vars == <<phase, queue, flight, mode, checked, rejected>>
Init == /\ phase = [e \in Events |-> "new"] /\ queue = {} /\ flight = NoEvent
        /\ mode = "open" /\ checked = {} /\ rejected = {}
Check(e) == /\ phase[e] = "new"
            /\ phase' = [phase EXCEPT ![e] = IF mode = "open" THEN "checked" ELSE "ignored"]
            /\ checked' = IF mode = "open" THEN checked \cup {e} ELSE checked
            /\ UNCHANGED <<queue, flight, mode, rejected>>
Submit(e) == /\ phase[e] = "checked"
             /\ phase' = [phase EXCEPT ![e] =
                 IF mode = "open" /\ (~EnforceCapacity \/ Cardinality(queue) < Capacity)
                 THEN "queued" ELSE "rejected"]
             /\ queue' = IF phase'[e] = "queued" THEN queue \cup {e} ELSE queue
             /\ rejected' = IF phase'[e] = "rejected" THEN rejected \cup {e} ELSE rejected
             /\ UNCHANGED <<flight, mode, checked>>
Take(e) == /\ e \in queue /\ flight = NoEvent /\ mode # "forced"
           /\ queue' = queue \ {e} /\ flight' = e
           /\ phase' = [phase EXCEPT ![e] = "running"]
           /\ UNCHANGED <<mode, checked, rejected>>
Return == /\ flight # NoEvent
          /\ phase' = [phase EXCEPT ![flight] = "delivered"] /\ flight' = NoEvent
          /\ UNCHANGED <<queue, mode, checked, rejected>>
Shutdown == /\ mode = "open" /\ mode' = "shutdown"
            /\ UNCHANGED <<phase, queue, flight, checked, rejected>>
\* shutdownNow discards queued work; those tasks are not counted by rejection handler.
Force == /\ mode = "shutdown" /\ mode' = "forced" /\ queue' = {}
         /\ phase' = [e \in Events |-> IF e \in queue THEN "abandoned" ELSE phase[e]]
         /\ UNCHANGED <<flight, checked, rejected>>
Next == Return \/ Shutdown \/ Force \/ (\E e \in Events : Check(e) \/ Submit(e) \/ Take(e))
Spec == Init /\ [][Next]_vars /\ WF_vars(Return)
        /\ (\A e \in Events : WF_vars(Submit(e)) /\ WF_vars(Take(e)))
TypeOK == /\ phase \in [Events -> {"new", "checked", "ignored", "queued", "rejected", "running", "delivered", "abandoned"}]
          /\ queue \subseteq Events /\ flight \in Events \cup {NoEvent}
          /\ mode \in {"open", "shutdown", "forced"} /\ checked \subseteq Events /\ rejected \subseteq Events
CapacityBound == Cardinality(queue) <= Capacity
QueueAccounting == queue = {e \in Events : phase[e] = "queued"}
FlightAccounting == {e \in Events : phase[e] = "running"} =
                    IF flight = NoEvent THEN {} ELSE {flight}
DropAccounting == rejected = {e \in Events : phase[e] = "rejected"}
SubmissionsResolve == \A e \in Events : phase[e] = "checked" ~> phase[e] # "checked"
AcceptedEventsResolve == \A e \in Events : phase[e] \in {"queued", "running"}
                         ~> phase[e] \in {"delivered", "abandoned"}
=============================================================================
