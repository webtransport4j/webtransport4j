------------------------- MODULE SessionAdmission -------------------------
EXTENDS Naturals, Integers, FiniteSets
CONSTANTS Requests, Connections, ConnectionOf, GlobalLimit, ConnectionLimit, SingleWinner
RequestConnection == [r \in Requests |-> IF r = CHOOSE x \in Requests : TRUE
                                           THEN CHOOSE c \in Connections : TRUE
                                           ELSE CHOOSE c \in Connections : c # (CHOOSE x \in Connections : TRUE)]
VARIABLES mode, phase, localSlots, globalSlots, releases, drained
vars == <<mode, phase, localSlots, globalSlots, releases, drained>>
LocalHeld == {r \in Requests : phase[r] \in {"local", "pending", "active"}}
GlobalHeld == {r \in Requests : phase[r] \in {"pending", "active"}}
Init == /\ mode = "accepting" /\ phase = [r \in Requests |-> "idle"]
        /\ localSlots = [c \in Connections |-> 0] /\ globalSlots = 0
        /\ releases = [r \in Requests |-> 0] /\ drained = {}
\* Gate check and reservation are separate: a checked request can race drain.
Gate(r) == /\ mode = "accepting" /\ phase[r] = "idle"
           /\ phase' = [phase EXCEPT ![r] = "checked"]
           /\ UNCHANGED <<mode, localSlots, globalSlots, releases, drained>>
ReserveLocal(r) == /\ phase[r] = "checked"
                   /\ localSlots[ConnectionOf[r]] < ConnectionLimit
                   /\ phase' = [phase EXCEPT ![r] = "local"]
                   /\ localSlots' = [localSlots EXCEPT ![ConnectionOf[r]] = @ + 1]
                   /\ UNCHANGED <<mode, globalSlots, releases, drained>>
ReserveGlobal(r) == /\ phase[r] = "local" /\ globalSlots < GlobalLimit
                    /\ phase' = [phase EXCEPT ![r] = "pending"]
                    /\ globalSlots' = globalSlots + 1
                    /\ UNCHANGED <<mode, localSlots, releases, drained>>
Commit(r) == /\ phase[r] = "pending"
             /\ phase' = [phase EXCEPT ![r] = "active"]
             /\ drained' = IF mode = "accepting" THEN drained ELSE drained \cup {r}
             /\ UNCHANGED <<mode, localSlots, globalSlots, releases>>
\* Failed handshakes and close callbacks share the pending CAS ownership rule.
Cancel(r) == /\ phase[r] \in {"checked", "local", "pending", "active"}
             /\ localSlots' = IF r \in LocalHeld
                 THEN [localSlots EXCEPT ![ConnectionOf[r]] = @ - 1] ELSE localSlots
             /\ globalSlots' = IF r \in GlobalHeld THEN globalSlots - 1 ELSE globalSlots
             /\ releases' = IF r \in LocalHeld
                 THEN [releases EXCEPT ![r] = @ + 1] ELSE releases
             /\ phase' = [phase EXCEPT ![r] = "finished"]
             /\ drained' = drained \ {r}
             /\ UNCHANGED <<mode>>
\* Negative control: a duplicate completion releases an already released slot.
Duplicate(r) == /\ ~SingleWinner /\ phase[r] = "finished" /\ releases[r] = 1
                /\ globalSlots' = globalSlots - 1
                /\ releases' = [releases EXCEPT ![r] = @ + 1]
                /\ UNCHANGED <<mode, phase, localSlots, drained>>
Drain == /\ mode = "accepting" /\ mode' = "draining"
         /\ drained' = {r \in Requests : phase[r] = "active"}
         /\ UNCHANGED <<phase, localSlots, globalSlots, releases>>
FinishStop == /\ mode = "draining"
              /\ \A r \in Requests : phase[r] \in {"idle", "finished"}
              /\ mode' = "stopped"
              /\ UNCHANGED <<phase, localSlots, globalSlots, releases, drained>>
Next == Drain \/ FinishStop \/ (\E r \in Requests : Gate(r) \/ ReserveLocal(r)
        \/ ReserveGlobal(r) \/ Commit(r) \/ Cancel(r) \/ Duplicate(r))
Spec == Init /\ [][Next]_vars /\ WF_vars(FinishStop)
        /\ (\A r \in Requests : WF_vars(Cancel(r)))
TypeOK == /\ mode \in {"accepting", "draining", "stopped"}
          /\ phase \in [Requests -> {"idle", "checked", "local", "pending", "active", "finished"}]
          /\ localSlots \in [Connections -> Int] /\ globalSlots \in Int
          /\ releases \in [Requests -> Nat] /\ drained \subseteq Requests
Capacity == /\ globalSlots \in 0..GlobalLimit
            /\ \A c \in Connections : localSlots[c] \in 0..ConnectionLimit
Accounting == /\ globalSlots = Cardinality(GlobalHeld)
              /\ \A c \in Connections :
                  localSlots[c] = Cardinality({r \in LocalHeld : ConnectionOf[r] = c})
SingleRelease == \A r \in Requests : releases[r] <= 1
DrainedSessions == mode # "accepting" => {r \in Requests : phase[r] = "active"} \subseteq drained
StoppedEmpty == mode = "stopped" => LocalHeld = {} /\ GlobalHeld = {}
ShutdownCompletes == mode = "draining" ~> mode = "stopped"
ReservationsResolve == \A r \in Requests : phase[r] = "pending" ~> phase[r] = "finished"
=============================================================================
