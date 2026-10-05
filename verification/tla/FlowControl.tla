---------------- MODULE FlowControl ----------------
EXTENDS Naturals, FiniteSets
CONSTANTS Operations, InitialCredit, MaxCredit, AtomicReservation
VARIABLES limit, used, phase, closed
vars == <<limit, used, phase, closed>>
Init == /\ limit = InitialCredit /\ used = 0 /\ closed = FALSE
        /\ phase = [o \in Operations |-> "new"]
Check(o) == /\ ~closed /\ phase[o] = "new" /\ used < limit
            /\ phase' = [phase EXCEPT ![o] = "checked"]
            /\ UNCHANGED <<limit, used, closed>>
Reserve(o) == /\ phase[o] = "checked"
              /\ (~AtomicReservation \/ (~closed /\ used < limit))
              /\ used' = used + 1 /\ phase' = [phase EXCEPT ![o] = "reserved"]
              /\ UNCHANGED <<limit, closed>>
Finish(o) == /\ phase[o] = "reserved" /\ phase' = [phase EXCEPT ![o] = "finished"]
             /\ UNCHANGED <<limit, used, closed>>
\* Failed stream creation consumes a cumulative index in the current Java code.
Fail(o) == /\ phase[o] = "reserved" /\ phase' = [phase EXCEPT ![o] = "failed"]
           /\ UNCHANGED <<limit, used, closed>>
Cancel(o) == /\ phase[o] = "checked" /\ phase' = [phase EXCEPT ![o] = "cancelled"]
             /\ UNCHANGED <<limit, used, closed>>
Grant(n) == /\ n \in (limit + 1)..MaxCredit /\ limit' = n
            /\ UNCHANGED <<used, phase, closed>>
Close == /\ ~closed /\ closed' = TRUE /\ UNCHANGED <<limit, used, phase>>
Next == Close \/ (\E n \in 0..MaxCredit : Grant(n))
        \/ (\E o \in Operations : Check(o) \/ Reserve(o) \/ Finish(o) \/ Fail(o) \/ Cancel(o))
Spec == Init /\ [][Next]_vars
        /\ (\A o \in Operations : WF_vars(Cancel(o)) /\ WF_vars(Finish(o) \/ Fail(o)))
TypeOK == /\ limit \in InitialCredit..MaxCredit /\ used \in Nat /\ closed \in BOOLEAN
          /\ phase \in [Operations -> {"new", "checked", "reserved", "finished", "failed", "cancelled"}]
CreditBound == used <= limit
CumulativeAccounting == used = Cardinality({o \in Operations : phase[o] \in {"reserved", "finished", "failed"}})
CreditNeverDecreases == limit' >= limit
MonotonicCredit == [][CreditNeverDecreases]_vars
OperationsResolve == \A o \in Operations : phase[o] \in {"checked", "reserved"}
                      ~> phase[o] \in {"finished", "failed", "cancelled"}
=============================================================================
