# Integrated demo walkthrough

Build from the repository root with Java 24: `mvnw.cmd clean verify`.
The README supplies process launch commands and CSV/manual input examples.
The combined JAR is only a convenient distribution: bootstrap, every worker and
each client run in separate Java processes. Source projects remain separate.

## Follow the code

1. `worker-node/.../WorkerNode.java`: `start()` creates the gRPC server, registers with
   bootstrap, attaches to a random neighbour, and schedules heartbeats/monitoring.
   `bootstrap-node/.../BootstrapServiceImpl.java` assigns IDs and expires silent workers.
2. `worker-node/.../WorkerElectionMetadata.java`: `begin()` prepares a numbered election
   round. The lowest active ID serializes round proposals. It is not automatically
   the coordinator. Workers retain the previous ready replica group for recovery.
3. `worker-node/.../ElectionRecovery.java`: `receiveElection()` processes a round once
   and floods neighbours. Candidacy reaches the initiator. `conclude()` waits for
   all current candidates, then picks lowest JAC, breaking ties by highest ID.
   The coordinator announcement is flooded. `recover()` restores checkpoints
   before the elected worker is allowed to serve jobs.
4. `worker-node/.../JobGrpcServiceImpl.java`: `submitJob()` validates/stores the request,
   partitions ranges/lists, and schedules tasks. `persist()` checkpoints admission,
   assignment and completed results before publication. The same request ID refers
   to the same logical job. `ensureRecovered()` retains completed results and
   restarts unfinished attempts after a coordinator change.
5. `worker-node/.../TaskExecution.java`: numerical loops produce measured percentages.
   Separate heartbeat messages prove transport liveness without increasing work.
   `TaskProgressWatch.java` distinguishes advancing, slow, silent and stalled tasks.
6. `client/.../ClientGUI.java`: each manual/CSV job gets its own progress row.
   `ResilientJobClient.java` discovers workers and reconnects using the original
   request ID after an unavailable coordinator. `common/src/main/proto/` defines
   every distributed message and RPC.

## Explain epochs and quorums in the demo

An epoch is the numbered coordinator round. A replica refuses an older round's
write after promising a newer one. This prevents delayed messages from an old
coordinator replacing newer state.

A data quorum is a majority. Three previous replicas need two responses for
recovery, and writes require a majority before acknowledgement. A majority read
intersects a majority write, so acknowledged checkpoints are not simply forgotten.
This data quorum is different from election configuration/readiness agreement,
which currently requires every current registered voter. A failed voter is removed
by bootstrap heartbeat expiry before a replacement configuration completes.

## Demonstrate failure

Start bootstrap and three workers, then a client. Submit PRIMESUM and observe
real intermediate progress. Abruptly stop the coordinator process. Remaining
workers suspect it, bootstrap expires it, a new coordinator is elected, state is
recovered, and the client reconnects. Completed results remain; unfinished attempts
may repeat. The system deduplicates accepted task results rather than promising
that computation physically executes exactly once.

`powershell -NoProfile -ExecutionPolicy Bypass -File scripts/Smoke-Test-Integrated.ps1 -RealJob`
automates an abrupt coordinator JVM kill and verifies the exact numerical result,
checkpoint survival, and a new worker joining. The integration suite separately
injects a worker join during active numerical job dispatch.

## Active, slow and failed are observations

Advancing measured work is Active. A task with no advancement for 10 seconds is
Slow. Workers emit task heartbeats once per second; five seconds without heartbeat
or progress makes an attempt unreachable. Sixty seconds without work advancement
makes it stalled even if heartbeats continue. Unfinished work is reassigned. A
progressing task has no fixed 60-second total runtime limit. These are timeout
suspicions, not proof of a crash; a pause or network delay can resemble failure.

## State and limitations to state honestly

Preserved: original request/list order/duplicates, stable job ID, task boundaries,
assignments, attempt counters, completed task IDs/results, and final job status.
Partial in-flight percentages can go backwards when unfinished work restarts.
JAC increments for each actual allocation to another worker, using an allocation
ID to deduplicate retries. Five admitted jobs end a coordinator term; old request-ID
retries do not count as new jobs.

Use at least three workers for the one-failure demo. A surviving majority of the
previous replica group and the bootstrap registry are required. State is in RAM;
full-cluster restart recovery, arbitrary partition-safe consensus and replicated
bootstrap are outside this implementation. Endpoints currently advertise localhost,
so the documented demonstration uses separate processes on one computer.
