---------------- MODULE Mailbox ----------------
EXTENDS Naturals, FiniteSets
CONSTANTS Frames, NoFrame, Capacity, Bounded, Recheck, SingleRelease
VARIABLES phase, queue, flight, worker, closed, releases
vars == <<phase, queue, flight, worker, closed, releases>>
Slots == {f \in Frames : phase[f] \in {"retained", "queued"}}
Init == /\ phase = [f \in Frames |-> "new"] /\ queue = {} /\ flight = NoFrame
        /\ worker = "idle" /\ closed = FALSE /\ releases = [f \in Frames |-> 0]
Admit(f) == /\ phase[f] = "new" /\ ~closed
            /\ (~Bounded \/ Cardinality(Slots) < Capacity)
            /\ phase' = [phase EXCEPT ![f] = "retained"]
            /\ UNCHANGED <<queue, flight, worker, closed, releases>>
Reject(f) == /\ phase[f] = "new"
             /\ (closed \/ (Bounded /\ Cardinality(Slots) >= Capacity))
             /\ phase' = [phase EXCEPT ![f] = "dropped"]
             /\ UNCHANGED <<queue, flight, worker, closed, releases>>
Publish(f) == /\ phase[f] = "retained" /\ queue' = queue \cup {f}
              /\ phase' = [phase EXCEPT ![f] = "queued"]
              /\ worker' = IF worker \in {"idle", "cleared"} THEN "busy" ELSE worker
              /\ UNCHANGED <<flight, closed, releases>>
Take(f) == /\ ~closed /\ worker = "busy" /\ flight = NoFrame /\ f \in queue
           /\ queue' = queue \ {f} /\ flight' = f
           /\ phase' = [phase EXCEPT ![f] = "inflight"]
           /\ UNCHANGED <<worker, closed, releases>>
Return == /\ flight # NoFrame
          /\ releases' = [releases EXCEPT ![flight] = @ + 1]
          /\ phase' = [phase EXCEPT ![flight] = "released"] /\ flight' = NoFrame
          /\ UNCHANGED <<queue, worker, closed>>
ObserveEmpty == /\ worker = "busy" /\ flight = NoFrame /\ queue = {}
                /\ worker' = "emptySeen" /\ UNCHANGED <<phase, queue, flight, closed, releases>>
ClearWorker == /\ worker = "emptySeen" /\ worker' = "cleared"
               /\ UNCHANGED <<phase, queue, flight, closed, releases>>
RecheckQueue == /\ worker = "cleared"
                /\ worker' = IF Recheck /\ queue # {} THEN "busy" ELSE "idle"
                /\ UNCHANGED <<phase, queue, flight, closed, releases>>
Close == /\ ~closed /\ closed' = TRUE /\ UNCHANGED <<phase, queue, flight, worker, releases>>
Drain(f) == /\ closed /\ f \in queue /\ queue' = queue \ {f}
            /\ phase' = [phase EXCEPT ![f] = "released"]
            /\ releases' = [releases EXCEPT ![f] = @ + 1]
            /\ UNCHANGED <<flight, worker, closed>>
Duplicate(f) == /\ ~SingleRelease /\ phase[f] = "released" /\ releases[f] = 1
                /\ releases' = [releases EXCEPT ![f] = @ + 1]
                /\ UNCHANGED <<phase, queue, flight, worker, closed>>
Next == Return \/ ObserveEmpty \/ ClearWorker \/ RecheckQueue \/ Close
        \/ (\E f \in Frames : Admit(f) \/ Reject(f) \/ Publish(f) \/ Take(f) \/ Drain(f) \/ Duplicate(f))
Spec == Init /\ [][Next]_vars /\ WF_vars(Return) /\ WF_vars(ObserveEmpty)
        /\ WF_vars(ClearWorker) /\ WF_vars(RecheckQueue)
        /\ (\A f \in Frames : WF_vars(Publish(f)) /\ WF_vars(Take(f)) /\ WF_vars(Drain(f)))
TypeOK == /\ phase \in [Frames -> {"new", "retained", "queued", "inflight", "released", "dropped"}]
          /\ queue \subseteq Frames /\ flight \in Frames \cup {NoFrame}
          /\ worker \in {"idle", "busy", "emptySeen", "cleared"} /\ closed \in BOOLEAN
          /\ releases \in [Frames -> Nat]
QueueOwnership == queue = {f \in Frames : phase[f] = "queued"}
FlightOwnership == {f \in Frames : phase[f] = "inflight"} =
                   IF flight = NoFrame THEN {} ELSE {flight}
CapacityBound == Bounded => Cardinality(Slots) <= Capacity
NoDoubleRelease == \A f \in Frames : releases[f] <= 1
NoStrandedWork == ~closed /\ queue # {} => worker # "idle"
ReferenceAccounting == \A f \in Frames : (phase[f] = "released") <=> (releases[f] = 1)
FramesResolve == \A f \in Frames : phase[f] \in {"retained", "queued", "inflight"} ~> phase[f] = "released"
ClosedDrains == closed ~> queue = {} /\ flight = NoFrame /\ Slots = {}
=============================================================================
