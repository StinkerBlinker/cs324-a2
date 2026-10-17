# Integration contract and limits

## Temporary demo stubs

`cs324.election.demo.StaticMembershipStub` is the replaceable configuration parser
used by `WorkerMain`; it does not call a bootstrap node. Josh should replace this
launcher boundary with discovery once the membership contract is agreed.

`cs324.election.demo.JobCheckpointStub` supplies the `save-demo` CLI fixture. Its
payload is explicitly prefixed `STUB_ONLY`. Aveshal should use the actual job
engine state through the existing checkpoint API; there is no simulated execution
or claim of end-to-end job recovery behind this fixture. The automated smoke test
checks replication/recovery of these exact stub bytes after killing the leader.

## Josh: bootstrap and membership

Construct `ElectionNode` with a unique worker endpoint, a consistent full voting
membership list, reciprocal neighbour IDs and that worker's current JAC. Start the
service once per worker JVM. The service presently owns its port. To share one
worker gRPC server, register the ElectionNode service with the same ServerBuilder
as the job service and separate server ownership from monitoring startup.

The demo uses configured membership, not the old RMI bootstrap. Discovery RPCs and
dynamic registration remain Josh's implementation. Do not remove a crashed node
from the voting denominator independently on each worker: doing so can allow
incompatible groups to each believe they have a majority.

Online changes to the voting membership are NOT implemented. An added worker can
initially be a job worker/non-voting participant; safely promoting new voters
needs an agreed configuration transition. Broken neighbour paths also require
bootstrap-assisted overlay repair. The election module does not currently do that.

## Aveshal: job engine

Use stable client request/job IDs and task IDs, including retry attempt IDs.
Before acknowledging a new job, save a full checkpoint under its job ID via
`SaveCheckpoint` or `ElectionNode.checkpoint`. A true result means a majority
stored it. False, timeout or connection loss is an uncertain outcome: the record
may survive recovery. Retry the SAME ID and reconcile its recorded status.

The payload is opaque bytes so the job service can define its own protobuf state.
Include at least job type and full inputs, client request ID, task boundaries,
assignments/attempts, accepted partial results, completion/progress and final result.
Use completed records as deduplication entries; do not discard them immediately.
Checkpoint each state transition necessary for safe recovery. Apply the same task
result once even if a worker resends it. Reads and writes must be serialized with
the job engine's state changes to avoid overwriting a newer state with stale data.

Wait for `GetStatus.ready` on the coordinator before submitting new jobs. After
failover, `ReadRecovery` on that ready coordinator provides the recovered records.
Resume pending tasks; reconcile in-flight tasks/results; reconnect progress streams
using the same job ID. A checkpoint string alone is not a running job.

Call `recordAcceptedJob(remoteAssignments)` exactly once per new accepted client
job, after its acceptance has been checkpointed. Pass the count of chunks assigned
to OTHER workers. This updates JAC and starts a new election on the fifth job.
The engine must serialize acceptance/checkpoint/count operations so concurrent jobs
cannot exceed the term limit. Recovery must not count an old job as a new one.
This admission transaction is an integration responsibility, not an atomic public RPC.

## Failure model and limitations

* Crash-stop coordinator failures with a surviving majority of the original voting
  configuration. Three voters tolerate one failure; five tolerate two.
* Heartbeat RPC deadline: 600 ms, monitor delay: 400 ms, three unsuccessful checks
  cause suspicion. Timing is not proof of death; a slow/unreachable process can be
  suspected. Recovery time varies with RPC timeouts, rounds and checkpoint count.
* Candidate collection lasts 1400 ms. All reachable active candidates are considered
  only if their flood/candidacy messages arrive in this interval. This is a bounded
  timing assumption; the code does not prove termination detection on arbitrary
  delayed networks. There is no full Raft/Paxos consensus protocol here.
* Logical rounds suppress duplicate/older election processing and older coordinator
  announcements. Concurrent starts converge in a healthy connected overlay.
* Majority replication/read repair reduces checkpoint loss under the stated crash
  model. It does NOT establish linearizability under arbitrary network partitions,
  overlapping leaders or reconfiguration. Do not claim general split-brain safety.
* State is replicated in RAM. A full cluster restart loses checkpoints, JACs and
  rounds. Restarting a crashed voter in place is not supported safely: persistent
  epochs and state, or a controlled rejoin protocol, are needed first.
* Plaintext trusted local demo network, no authentication/TLS.
* Client/worker/bootstrap gRPC migration, GUI/progress, sample CSVs and job
  reassignment are the other team portions and are not implemented in this module.

These limitations must be reconciled with the group's final A2 requirements.
