---------------- MODULE ReactivePublisher ----------------
EXTENDS Naturals, FiniteSets
CONSTANTS Items, MaxRequests, RecheckEmit, SingleTerminal, RespectDemand
VARIABLES phase, requested, delivered, cancelled, completed, terminal, work, subscribed
vars == <<phase, requested, delivered, cancelled, completed, terminal, work, subscribed>>
Queue == {i \in Items : phase[i] = "queued"}
Init == /\ phase = [i \in Items |-> "new"] /\ requested = 0 /\ delivered = 0
        /\ cancelled = FALSE /\ completed = FALSE /\ terminal = 0
        /\ work = FALSE /\ subscribed = FALSE
Subscribe == /\ ~subscribed /\ subscribed' = TRUE
             /\ UNCHANGED <<phase, requested, delivered, cancelled, completed, terminal, work>>
Request == /\ subscribed /\ ~cancelled /\ terminal = 0 /\ requested < MaxRequests
           /\ requested' = requested + 1 /\ work' = TRUE
           /\ UNCHANGED <<phase, delivered, cancelled, completed, terminal, subscribed>>
BeginEmit(i) == /\ phase[i] = "new" /\ ~cancelled /\ ~completed
                /\ phase' = [phase EXCEPT ![i] = "checked"]
                /\ UNCHANGED <<requested, delivered, cancelled, completed, terminal, work, subscribed>>
Offer(i) == /\ phase[i] = "checked"
            /\ phase' = [phase EXCEPT ![i] =
                IF RecheckEmit /\ (cancelled \/ completed \/ terminal > 0) THEN "discarded" ELSE "queued"]
            /\ work' = IF cancelled \/ terminal > 0 THEN work ELSE TRUE
            /\ UNCHANGED <<requested, delivered, cancelled, completed, terminal, subscribed>>
Deliver(i) == /\ work /\ subscribed /\ ~cancelled /\ terminal = 0
              /\ i \in Queue /\ (~RespectDemand \/ requested > delivered)
              /\ phase' = [phase EXCEPT ![i] = "delivered"] /\ delivered' = delivered + 1
              /\ UNCHANGED <<requested, cancelled, completed, terminal, work, subscribed>>
Complete == /\ ~completed /\ completed' = TRUE /\ work' = TRUE
            /\ UNCHANGED <<phase, requested, delivered, cancelled, terminal, subscribed>>
Cancel == /\ ~cancelled /\ cancelled' = TRUE /\ work' = TRUE
          /\ UNCHANGED <<phase, requested, delivered, completed, terminal, subscribed>>
Discard(i) == /\ i \in Queue /\ (cancelled \/ (completed /\ ~subscribed))
              /\ phase' = [phase EXCEPT ![i] = "discarded"]
              /\ UNCHANGED <<requested, delivered, cancelled, completed, terminal, work, subscribed>>
Terminate == /\ work /\ subscribed /\ completed /\ ~cancelled /\ Queue = {}
             /\ (terminal = 0 \/ (~SingleTerminal /\ terminal = 1))
             /\ terminal' = terminal + 1 /\ work' = FALSE
             /\ UNCHANGED <<phase, requested, delivered, cancelled, completed, subscribed>>
DuplicateTerminal == /\ ~SingleTerminal /\ terminal = 1 /\ terminal' = 2
                     /\ UNCHANGED <<phase, requested, delivered, cancelled, completed, work, subscribed>>
Next == Subscribe \/ Request \/ Complete \/ Cancel \/ Terminate \/ DuplicateTerminal
        \/ (\E i \in Items : BeginEmit(i) \/ Offer(i) \/ Deliver(i) \/ Discard(i))
Spec == Init /\ [][Next]_vars /\ WF_vars(Terminate)
        /\ (\A i \in Items : WF_vars(Offer(i)) /\ WF_vars(Deliver(i)) /\ WF_vars(Discard(i)))
TypeOK == /\ phase \in [Items -> {"new", "checked", "queued", "delivered", "discarded"}]
          /\ requested \in 0..MaxRequests /\ delivered \in Nat /\ terminal \in 0..2
          /\ cancelled \in BOOLEAN /\ completed \in BOOLEAN /\ subscribed \in BOOLEAN /\ work \in BOOLEAN
DemandRespected == delivered <= requested
DeliveryAccounting == delivered = Cardinality({i \in Items : phase[i] = "delivered"})
SingleTerminalSignal == terminal <= 1
NoPostTerminalQueue == terminal > 0 => Queue = {}
CancelledItemsResolve == \A i \in Items : cancelled /\ phase[i] \in {"checked", "queued"}
                          ~> phase[i] \in {"delivered", "discarded"}
CompletionProgress == subscribed /\ completed /\ Queue = {} ~> terminal > 0 \/ cancelled
=============================================================================
