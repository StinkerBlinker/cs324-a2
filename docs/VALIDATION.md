# Bootstrap and election integration validation

Validation completed on 18 October 2026 using JDK 24 and Maven 3.9.11.

## Build and service integration tests

Command: `mvnw.cmd clean verify` (root reactor, Java 24).
The worker-owned election build passed. `IntegratedClusterTest` ran 14 tests, `JobGrpcServiceImplTest` ran 7 tests and
`CsvJobLoaderTest` ran 3 tests: 24 integrated-application tests. The standalone
`election/` module adds 6 tests: 30 total, zero failures, errors or skipped tests. The integration tests run actual localhost gRPC bootstrap and worker
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
   new checkpoints without the active membership registry.
10. Actual-allocation accounting deduplicates the same job/task/attempt ID while
    counting a distinct reassignment separately from the five-job term counter.
11. A real PRIMESUM job is interrupted after completed subtasks have been
    checkpointed. The coordinator crashes, the replacement recovers those task
    results and resumes the same job ID. PRIMESUM(1,2000000) returns 142913828922;
    acknowledged completed task identities remain in the recovered final state.

12. Bootstrap's generated service descriptor exposes only registration, deregistration,
    discovery and heartbeat methods; election control belongs to WorkerService.
13. Concurrent election triggers at all workers produce one new epoch; every worker
    rejects a stale recovered-view vote after that round completes.

14. Concurrent MAX and PRIMECOUNT list jobs execute on real worker gRPC servers.
    Negative-only MAX produces -3; duplicate prime occurrences produce a count of 6.
    After the coordinator crashes, the same request IDs recover the same results;
    original list order, duplicates and partition inputs remain in replicated state.

The seven job-service tests cover numerical submission/progress, rejection by a
non-coordinator, monotonic measured progress during normal execution, failed-worker
task reassignment, list MAX/PRIMECOUNT and request-ID deduplication/rejection of
changed inputs. Three CSV tests cover range/list parsing, duplicate retention,
quoted comma-separated values and invalid-row rejection.

## Independent process demonstration

Command: `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/Smoke-Test-Integrated.ps1 -RealJob`.

The script started a bootstrap JVM and three worker JVMs, discovered worker 3 as
coordinator, replicated a checkpoint, and force-killed that coordinator JVM.
Bootstrap expired worker 3, worker 2 became ready and the checkpoint payload was
read back. A fourth worker then registered into the still-running cluster, became
coordinator and recovered the existing checkpoint through the configuration change.
The worker-owned election run passed all three checks, including the numerical job. The script stopped its own remaining processes.

With `-RealJob`, the process demo additionally starts a reconnecting client and
force-kills the coordinator after the client observes completed tasks. The client
must rediscover the replacement and finish with the exact PRIMESUM result; an
already-completed job before crash injection is rejected as an invalid demo.

The latest process logs are in `.demo/integrated-6f664d0c6bef40938a34c3259b2b06d8/`.

The job service, measured streaming, checkpoint recovery and client reconnect
path are integrated with PR #4 (fcfa3bb): CSV loading/sample_jobs.csv, MAX and
PRIMECOUNT list jobs, and multiple concurrent GUI progress rows. Each GUI job uses
the tested reconnecting client. The Swing layout has not been visually exercised
by this test suite.
The earlier Word report describes the standalone module; its protocol/state details
must be updated to this integrated version before the final report is assembled.

The standalone initiator-crash test now observes the injected round at a survivor
and waits for its replacement round before asserting readiness. This prevents an
old ready state from satisfying the assertion before the injected election arrives.
The integrated module passed `clean verify`; after this test synchronization fix,
the standalone module was revalidated with `mvnw.cmd -pl election -am verify`.
