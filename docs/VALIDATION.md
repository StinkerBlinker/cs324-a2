# Bootstrap and election integration validation

Validation completed on 18 October 2026 using JDK 24 and Maven 3.9.11.

## Build and service integration tests

Command: `mvnw.cmd -f cs324-A2/pom.xml clean verify`.
The packaged build passed. `IntegratedClusterTest` ran 10 tests with zero failures,
zero errors and zero skipped tests. Tests run actual localhost gRPC bootstrap and
worker servers; they do not replace bootstrap discovery with a static membership stub.

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
8. Older coordinator messages do not roll back the epoch; unfinished job execution
   RPCs return UNIMPLEMENTED instead of reporting fabricated results.
9. Bootstrap loss; the coordinator stops advertising readiness and cannot admit
   new checkpoints without authoritative metadata.
10. Actual-allocation accounting deduplicates the same job/task/attempt ID while
    counting a distinct reassignment separately from the five-job term counter.

## Independent process demonstration

Command: `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/Smoke-Test-Integrated.ps1`.

The script started a bootstrap JVM and three worker JVMs, discovered worker 3 as
coordinator, replicated a checkpoint, and force-killed that coordinator JVM.
Bootstrap expired worker 3, worker 2 became ready and the checkpoint payload was
read back. A fourth worker then registered into the still-running cluster, became
coordinator and recovered the existing checkpoint through the configuration change.
Both smoke-test checks passed. The script stopped its own remaining processes.

These tests prove election, discovery and checkpoint integration. They do not prove
completion/resumption of an actual numerical job, task reassignment, CSV loading,
GUI progress or stream reconnection: those require Aveshal's pending job component.
The earlier Word report describes the standalone module; its protocol/state details
must be updated to this integrated version before the final report is assembled.
