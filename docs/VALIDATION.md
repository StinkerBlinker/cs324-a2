# Bootstrap and election integration validation

Validation completed on 18 October 2026 using JDK 24 and Maven 3.9.11.

## Build and service integration tests

Command: `mvnw.cmd -f cs324-A2/pom.xml clean verify`.
The packaged build passed. `IntegratedClusterTest` ran 11 tests and
`JobGrpcServiceImplTest` ran 5 tests: 16 total, zero failures, errors or skipped
tests. The integration tests run actual localhost gRPC bootstrap and worker
servers; they do not replace bootstrap discovery with a static membership stub.

1. Registration/discovery and neighbour links; lowest JAC and highest-ID tie-break.
2. Abrupt coordinator loss; heartbeat expiry, replacement election and checkpoint recovery.
3. A worker joins a running cluster; recovered state survives the configuration
   transition and another coordinator loss.
4. Duplicate admitted job IDs count once; five distinct admissions rotate the
   coordinator and preserve their checkpoint records.
5. A registered endpoint that does not report candidacy is not omitted by a fixed
   election timer; election completes after heartbeat expiry removes that endpoint.
6. Election initiator loss; retry and overlay repair let remaining workers agree.
7. Previous replica majority loss; the remaining worker refuses to start an empty
   replacement or acknowledge an unsafe checkpoint.
8. Older coordinator messages do not roll back the epoch; worker range execution
   returns its actual computed result.
9. Bootstrap loss; the coordinator stops advertising readiness and cannot admit
   new checkpoints without authoritative metadata.
10. Actual-allocation accounting deduplicates the same job/task/attempt ID while
    counting a distinct reassignment separately from the five-job term counter.
11. A real PRIMESUM job is interrupted after completed subtasks have been
    checkpointed. The coordinator crashes, the replacement recovers those task
    results and resumes the same job ID. PRIMESUM(1,2000000) returns 142913828922;
    acknowledged completed task identities remain in the recovered final state.

The five job-service tests cover numerical submission/progress, rejection by a
non-coordinator, monotonic measured progress during normal execution, failed-worker
task reassignment, and request-ID deduplication/rejection of changed inputs.

## Independent process demonstration

Command: `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/Smoke-Test-Integrated.ps1`.

The script started a bootstrap JVM and three worker JVMs, discovered worker 3 as
coordinator, replicated a checkpoint, and force-killed that coordinator JVM.
Bootstrap expired worker 3, worker 2 became ready and the checkpoint payload was
read back. A fourth worker then registered into the still-running cluster, became
coordinator and recovered the existing checkpoint through the configuration change.
Both smoke-test checks passed. The script stopped its own remaining processes.

With `-RealJob`, the process demo additionally starts a reconnecting client and
force-kills the coordinator after the client observes completed tasks. The client
must rediscover the replacement and finish with the exact PRIMESUM result; an
already-completed job before crash injection is rejected as an invalid demo.

The job service, measured streaming, checkpoint recovery and client reconnect
path are integrated. CSV loading/samples, MAX and PRIMECOUNT list jobs, and multiple
concurrent jobs inside one GUI remain incomplete. The Swing layout has not been
visually exercised by this test suite; GUI use shares the tested client logic.
The earlier Word report describes the standalone module; its protocol/state details
must be updated to this integrated version before the final report is assembled.
