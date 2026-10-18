# Integrated election and job recovery contract

## Working code

Use `cs324-A2` as the shared project. Networking, election and checkpoint recovery
run on the same worker server, using the existing `cs324.assignment2.grpc` types.
No fixed worker-list stub is used. Existing worker election RPC names/field tags
are retained; epochs and recovery messages are additional fields/methods.
All members must rebuild generated stubs from the same proto files.

## Bootstrap and membership

Bootstrap assigns worker IDs and removes workers after 5000 ms without a heartbeat;
cleanup runs every 1000 ms. Heartbeats are sent every 1500 ms. Worker control RPCs
have 600 ms deadlines. Monotonic time avoids wall-clock adjustments changing expiry.

Bootstrap exposes only RegisterWorker, DeregisterWorker, GetActiveWorkers and
Heartbeat. It stores no election epoch, coordinator endpoint or readiness flag.
Workers own those records through WorkerService. The lowest registered worker ID
serializes round proposals; this role does not determine the coordinator. Election
messages still flood the neighbour overlay, and the initiating worker collects
EVERY current voter's candidacy before choosing lowest JAC/highest ID.

A preparation vote installs a strictly increasing epoch on current workers and
carries the previous recovered replica configuration. All current voters must
participate before candidacy collection can complete. Coordinator selection and
post-recovery readiness are replicated to current workers. The elected worker
serves only after every current voter acknowledges readiness. Clients discover
worker endpoints through bootstrap and query WorkerService for the newest view.
Concurrent triggers are forwarded to the round proposer; old epoch votes cannot
replace a newer local view. A failed proposer is replaced after membership expiry.

Membership changes invalidate readiness and start a new round. Failure of an
election initiator is recovered through heartbeat expiry or a 12-second election
metadata timeout. Candidate collection never concludes merely because a timer
expired; a missing voter must report or be removed by a membership transition.
Neighbour links retain random attachment and are repaired with a reciprocal ring
over current voters, so losing a tree articulation worker does not strand survivors.

This architecture deliberately relies on one available bootstrap for membership
and discovery. If bootstrap is unreachable, new checkpoint admission
fails and workers stop advertising ready leadership. Registry state is in RAM;
its restart is outside the supported failure model. It must not be silently
replaced with a fresh empty registry while workers continue accepting jobs.

## Recovery across configurations

Before a new coordinator becomes ready it reads a majority of the PREVIOUS ready
voters. Each read atomically raises that replica's promised epoch before returning
records, so it will not subsequently accept an older coordinator's checkpoint.
The coordinator merges the newest epoch/revision per key and writes those records
to a majority of the NEW voters before publishing readiness to the current workers.

A shrinking active-worker list does not lower the required OLD read quorum.
Three previous replicas need two previous responses, even if only one remains
registered. This prevents silently starting an empty replacement and losing jobs.
It can reduce availability, especially if a singleton initial coordinator dies
before its state is transitioned to a larger replica configuration.

RPC loss can leave an uncertain write outcome. Some stored-but-unacknowledged
records may be recovered; callers must reconcile stable IDs rather than assume
an unsuccessful response means no record exists. This is not a general consensus
protocol or a proof of linearizable behaviour during arbitrary partitions.

## Integrated job engine

JobGrpcServiceImpl is now registered on the same worker server. Its PersistedJob
protobuf stores original submission, stable job ID, status and each task request,
attempt counter, worker identity, completion state and accepted result. Records use
the `job/` key prefix. Other checkpoint fixtures remain opaque to the job engine.

Admission is replicated before acknowledgement. Task assignment, completion and
failure transitions are checkpointed before publication; partial streamed progress
does not generate a checkpoint for every percentage update. On a new coordinator,
completed results are retained and unfinished attempts are rescheduled. Old-owner
streams close with UNAVAILABLE. ResilientJobClient rediscovers the coordinator and
resubmits/reconnects with the original request_id, preventing a second logical job.
Clients must use distinct request IDs for distinct inputs.

`SaveCheckpointRequest` accepts a stable key and opaque bytes. Include the real
job type/full input, client request ID, task boundaries, assignments/attempt IDs,
accepted partial results, completed task IDs, progress and final result in those
bytes. Prefer a dedicated protobuf payload in your job module. The recovery
service does not interpret or execute it.

For a NEW client job, set `new_job=true` and `remote_assignments` to the number
of actual allocations to OTHER workers. Use `WorkerNode.checkpointJob(request)`
locally, or the `SaveCheckpoint` RPC. A true reply means majority replication.
An already acknowledged/recovered admitted ID is not counted again and is not
overwritten by a new admission request: retrieve its existing state instead.
After five distinct successful admissions, the coordinator starts a new term.
For real dispatch, prefer `remote_assignments=0` at admission and call
`WorkerNode.recordRemoteAssignment(jobTaskAttemptId)` for each actual allocation
or reassignment to another worker. Repeated allocation IDs count once. This records
JAC separately from the five-job term count. Do not both pass an admission allocation
count and record those same allocations through the hook. Do not count planned but
never-issued work as dispatched. The CLI's optional allocation count is for demos.

For later state updates set `new_job=false`. Preserve the admitted marker and
serialize state transitions per job. A false reply or RPC error requires retry/
reconciliation with the SAME job ID. These operations are guarded across concurrent
local admissions; snapshots/replication are protected by a separate state lock.

When the ready coordinator changes, call `WorkerNode.recoveredJobs()` or
`ReadRecovery` with the current epoch, decode the saved states and reconcile live
workers before reassigning pending work. Deduplicate task results by task/attempt
identity; reconnect client progress streams using the original job ID. Retain
completed job records for request deduplication. No exactly-once task execution is
provided by the checkpoint layer itself.

Do not block election/heartbeat threads with numerical computation. Use a separate
executor. WorkerService execute RPCs now run SUM/PRIMESUM; JobService handles
submission/progress. The old WorkerService SubmitJob remains UNIMPLEMENTED and
is not used by the GUI. The range-only jobs still need true list input support
for MAX and PRIMECOUNT, CSV samples/loading and multiple concurrent GUI job rows.
Worker details can be opened by clicking the progress bar or its details button.

## Remaining boundaries

In-memory replica survival covers process loss while enough previous voters and
bootstrap remain available. Persistent JAC/epoch storage, bootstrap replication,
full restart recovery and authentication/TLS are not implemented. A worker whose
registration expires must stop and rejoin with a new ID; it does not silently
resume using its old identity. Live joins are tested with checkpoint state;
coordinator failover is also tested during a real numerical job with retained
completed subtasks and correct final aggregation.
