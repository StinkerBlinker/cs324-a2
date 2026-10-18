# CS324 Assignment 2

This branch integrates Joshua's bootstrap/networking, Pranay's leader election
and recovery, and Aveshal's job execution/streaming/GUI inside `cs324-A2`.
The original Assignment 1 ZIP/report remain at the repository root.

## Build

Use JDK 24 and the supplied Maven wrapper. In PowerShell at this repository root:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-24'
.\mvnw.cmd -f cs324-A2\pom.xml clean verify
```

Adjust JAVA_HOME for your JDK location. The first build downloads dependencies.
Output: `cs324-A2/target/cs324-A2-1.0-SNAPSHOT.jar`.

## Start the integrated cluster

Open four VS Code terminals at the repository root. Start bootstrap first:

```powershell
java -cp cs324-A2/target/cs324-A2-1.0-SNAPSHOT.jar cs324.assignment2.bootstrap.BootstrapNode
```

In each of the other three terminals run:

```powershell
java -cp cs324-A2/target/cs324-A2-1.0-SNAPSHOT.jar cs324.assignment2.worker.WorkerNode
```

Workers receive unique IDs from bootstrap, bind free ports, choose random
neighbours, and send heartbeats. After membership settles, each registered voter
reports its candidacy. Lowest JAC wins, with highest ID breaking ties. For three
fresh workers the ready coordinator should be worker 3.

Optional arguments: `BootstrapNode PORT`; `WorkerNode BOOTSTRAP_HOST PORT
LISTEN_PORT INITIAL_JAC`. Defaults are localhost, bootstrap port 50051, an
automatically assigned worker port and JAC zero. Example with a fixed worker port:

```powershell
java -cp cs324-A2/target/cs324-A2-1.0-SNAPSHOT.jar cs324.assignment2.worker.WorkerNode localhost 50051 5101 0
```

## Observe and demonstrate recovery

Use another terminal. These CLI calls discover the leader through the real
bootstrap, without a static worker list:

```powershell
java -cp cs324-A2/target/cs324-A2-1.0-SNAPSHOT.jar cs324.assignment2.worker.ClusterDemo status
java -cp cs324-A2/target/cs324-A2-1.0-SNAPSHOT.jar cs324.assignment2.worker.ClusterDemo checkpoint localhost 50051 job-42 'input=1..100;task-A=done;task-B=pending'
```

Expect `CHECKPOINT_ACCEPTED=true`. Close/kill only the worker 3 process. A force
kill demonstrates bootstrap timeout detection; Ctrl+C uses clean deregistration.
After election/recovery, query again:

```powershell
java -cp cs324-A2/target/cs324-A2-1.0-SNAPSHOT.jar cs324.assignment2.worker.ClusterDemo status
java -cp cs324-A2/target/cs324-A2-1.0-SNAPSHOT.jar cs324.assignment2.worker.ClusterDemo read
```

Expect coordinator 2, `READY=true`, and the saved payload. Start another worker
while the remaining cluster is running: it registers, joins the overlay and
participates in a new election/configuration. Existing checkpoints are copied
from a majority of the previous configuration to a majority of the new one.
Checkpoint admission pauses during this transition; callers retry stable job IDs.

To exercise the five-job term hook, use `admit localhost 50051 JOB_ID PAYLOAD 2`
instead of `checkpoint`; five DISTINCT IDs end the term and increase the old
coordinator's JAC by ten. Repeating an acknowledged ID does not count twice.
This is a checkpoint/admission demonstration, not actual numerical computation.

Automated separate-process demo:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Smoke-Test-Integrated.ps1
 # Add -RealJob to kill the coordinator during a real PRIMESUM computation.
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Smoke-Test-Integrated.ps1 -RealJob
```

It starts bootstrap and three workers, saves a checkpoint, force-kills the leader,
verifies recovery, adds a joining worker and verifies state survives that change.
It stops only the processes it started and leaves logs under `.demo/`.

## Implementation and remaining integration

* `BootstrapServiceImpl`: registration, monotonic heartbeat timeouts and consistent
  election/configuration metadata. It neither reports candidacy nor selects a winner.
* `WorkerNode`: one gRPC server for networking, election, recovery and job execution;
  pooled channels, deadlines and shutdown cleanup.
* `ElectionRecovery`: duplicate-suppressed flooding, complete candidate collection,
  JAC selection, coordinator checks, replica recovery and job admission deduplication.
* `IntegratedClusterTest`: actual localhost gRPC tests using real bootstrap/workers.
* `docs/INTEGRATION.md`: contracts for Aveshal and the exact recovery assumptions.

## Run jobs and the GUI

After starting bootstrap and at least three workers, launch the client:

```powershell
java -cp cs324-A2/target/cs324-A2-1.0-SNAPSHOT.jar cs324.assignment2.client.ClientGUI
```

The GUI submits range jobs and displays measured streamed progress; click the
progress bar or the details button for the worker breakdown. It automatically
rediscovers the coordinator and reconnects using the same job ID after failover.
For a command-line computation with the same reconnect behaviour:

```powershell
java -cp cs324-A2/target/cs324-A2-1.0-SNAPSHOT.jar cs324.assignment2.worker.ClusterDemo job localhost 50051 PRIMESUM 1 50 demo-primes
```

Expected result: 328. Reusing `demo-primes` retrieves that same logical job; use
a different ID for a different submission. Do not reuse IDs with different inputs.
Admission, task assignment/completion and failure transitions are checkpointed.
On failover completed task results are retained; unfinished tasks restart with new
attempt identities. Partial in-flight progress can decrease when work is retried.

Aveshal's job service currently supports range-based SUM and PRIMESUM. The required
MAX and PRIMECOUNT over unsorted lists, sample CSV/loading, and multiple concurrent
jobs in one GUI still need work. The GUI presently monitors one job at a time;
separate clients and backend jobs can execute concurrently. The old WorkerService
SubmitJob RPC is deprecated/UNIMPLEMENTED; use the JobService API used by the GUI.

Bootstrap must remain available and retain its metadata. Full-cluster/bootstrap
restart recovery, TLS/authentication and replicated bootstrap are not implemented.
Checkpoint bytes are kept in worker RAM. Recovery deliberately stops if a majority
of the PREVIOUS replica set is unavailable; shrinking the live registry does not
make loss of acknowledged work acceptable. Do not claim arbitrary partition-safe
consensus. End-to-end job recovery is verified under the documented crash model.
