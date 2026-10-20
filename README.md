# CS324 Assignment 2

This branch integrates Joshua's bootstrap-node/networking, Pranay's leader election
and recovery, and Aveshal's job execution/streaming/GUI across separate Maven component projects.
Historical project files are kept outside the active project in a local backup.

## Run Code in VS Code (Windows and macOS)

One checkout supports both operating systems; no alternate settings or source tree is needed.

1. Install **JDK 24**, make `java` available on PATH, and install VS Code's **Code Runner** extension (`formulahendry.code-runner`). The shared build/run launcher checks `JAVA_HOME` and the current Java installation, then checks standard Windows JDK installation directories or macOS `/usr/libexec/java_home -v 24`. If JDK 24 is installed elsewhere, set `JAVA_HOME` to that folder. It sets the correct JDK for Maven even when the inherited `JAVA_HOME` points to an older JDK. Restart VS Code after environment changes.
2. Extract the ZIP or pull the repository, then open the **a2** folder containing the root `pom.xml` and `.vscode`. Trust the folder after reviewing it.
3. Build with **Ctrl+Shift+B** (Windows) or **Cmd+Shift+B** (macOS). Wait for **BUILD SUCCESS**. Initial Maven dependencies require internet access. Rebuild after changing Java or protobuf source. Automatic IDE compilation is disabled so the IDE and Maven do not overwrite each other's generated protobuf classes.
4. Open `BootstrapNode.java` and click **Run Code**.
5. Open `WorkerNode.java` and click **Run Code** three times, once for each worker.
6. Open `ClientGUI.java` and click **Run Code**.

Every click launches only the selected component in its own process. Windows opens a PowerShell console that stays open after the component exits and saves its output under `.demo/launch/`; macOS opens a Terminal session (allow VS Code to control Terminal if prompted). Stop bootstrap/workers with Ctrl+C in their respective windows. `.vscode/RunComponent.java` is a source-mode Java launcher that chooses the OS and JDK; it does not modify the application. Supporting classes without a main method cannot be run independently. All team members use the same files and the same Run Code buttons.

## Build

Use JDK 24 and the supplied Maven wrapper. In PowerShell at this repository root:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-24'
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
.\mvnw.cmd clean verify
```

Adjust JAVA_HOME for your JDK location. The first build downloads dependencies.
The parent builds four modules, each with its own POM:

| Project | Contents |
|---|---|
| `common/` | Shared .proto definitions and generated gRPC classes |
| `bootstrap-node/` | Registration, discovery and heartbeat expiry |
| `worker-node/` | Election, checkpoint recovery and job execution |
| `client/` | GUI, CSV loader, reconnecting client and diagnostic CLI |

Bootstrap, worker and client each produce a JAR containing their runtime dependencies under their own `target/` folder. They run as separate Java processes. There are five POM files in total, including the root parent. The old standalone election demo and combined packaging module are no longer part of this project.

## Start the integrated cluster

Open four VS Code terminals at the repository root. Start bootstrap first:

```powershell
java -cp bootstrap-node/target/bootstrap-node-1.0-SNAPSHOT.jar cs324.assignment2.bootstrap.BootstrapNode
```

In each of the other three terminals run:

```powershell
java -cp worker-node/target/worker-node-1.0-SNAPSHOT.jar cs324.assignment2.worker.WorkerNode
```

Workers receive unique IDs from bootstrap, bind free ports, choose random
neighbours, and send heartbeats. After membership settles, each registered voter
reports its candidacy. Lowest JAC wins, with highest ID breaking ties. For three
fresh workers the ready coordinator should be worker 3.

Optional arguments: `BootstrapNode PORT`; `WorkerNode BOOTSTRAP_HOST PORT
LISTEN_PORT INITIAL_JAC`. Defaults are localhost, bootstrap port 50051, an
automatically assigned worker port and JAC zero. Example with a fixed worker port:

```powershell
java -cp worker-node/target/worker-node-1.0-SNAPSHOT.jar cs324.assignment2.worker.WorkerNode localhost 50051 5101 0
```

## Observe and demonstrate recovery

Use another terminal. These CLI calls discover the leader through the real
bootstrap, without a static worker list:

```powershell
java -cp client/target/client-1.0-SNAPSHOT.jar cs324.assignment2.client.ClusterDemo status
java -cp client/target/client-1.0-SNAPSHOT.jar cs324.assignment2.client.ClusterDemo checkpoint localhost 50051 job-42 'input=1..100;task-A=done;task-B=pending'
```

Expect `CHECKPOINT_ACCEPTED=true`. Close/kill only the worker 3 process. A force
kill demonstrates bootstrap timeout detection; Ctrl+C uses clean deregistration.
After election/recovery, query again:

```powershell
java -cp client/target/client-1.0-SNAPSHOT.jar cs324.assignment2.client.ClusterDemo status
java -cp client/target/client-1.0-SNAPSHOT.jar cs324.assignment2.client.ClusterDemo read
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

* `BootstrapServiceImpl`: registration, discovery and monotonic heartbeat timeouts only.
* `WorkerElectionMetadata`: worker-owned epochs, configuration votes and readiness;
  the lowest active ID serializes proposals, while candidacy chooses the coordinator.
* `WorkerNode`: one gRPC server for networking, election, recovery and job execution;
  pooled channels, deadlines and shutdown cleanup.
* `ElectionRecovery`: duplicate-suppressed flooding, complete candidate collection,
  JAC selection, coordinator checks, replica recovery and job admission deduplication.
* `IntegratedClusterTest`: actual localhost gRPC tests using real bootstrap-node/workers.
* `docs/INTEGRATION.md`: contracts for Aveshal and the exact recovery assumptions.

## Run jobs and the GUI

After starting bootstrap and at least three workers, launch the client:

```powershell
java -cp client/target/client-1.0-SNAPSHOT.jar cs324.assignment2.client.ClientGUI
```

The GUI supports SUM, PRIMESUM, MAX and PRIMECOUNT. For a CSV dataset:

1. Choose **MAX** or **PRIMECOUNT**.
2. Click **Load CSV** and select `sample_values.csv` (100,000 unsorted numbers).
3. Click **Submit Job**. Every number in the entire file belongs to this one job; the coordinator partitions it among workers.
4. Compare the result with `sample_values_expected.txt`. Duplicate primes count separately.

The CSV contains plain numbers separated by commas, optionally across multiple rows. There is no header, job type, start/end column, or separate job per row. The GUI shows a short preview and retains the complete dataset for submission. Switch job type to reuse the loaded data, or click **Use manual input** to return to typing numbers. SUM and PRIMESUM still use their Start/End range inputs. Loading the CSV selects MAX if a range job was selected.

MAX can finish very quickly even on a large list; PRIMECOUNT performs more work. No artificial delay or invented progress is added. Loading and parsing happen on the existing background executor.
Each job has its own measured progress bar; up to four client monitors run at once.
Click a progress bar or the details button to expand the worker breakdown.
Every monitor rediscovers and reconnects with its original request ID after failover.
For a command-line computation with the same reconnect behaviour:

```powershell
java -cp client/target/client-1.0-SNAPSHOT.jar cs324.assignment2.client.ClusterDemo job localhost 50051 PRIMESUM 1 50 demo-primes
```

Expected result: 328. Reusing `demo-primes` retrieves that same logical job; use
a different ID for a different submission. Do not reuse IDs with different inputs.
Admission, task assignment/completion and failure transitions are checkpointed.
On failover completed task results are retained; unfinished tasks restart with new
attempt identities. Partial in-flight progress can decrease when work is retried.

The old WorkerService SubmitJob RPC remains UNIMPLEMENTED; the GUI uses JobService.
All processes must rebuild from the merged proto files: job submission uses
`values = 4` and `request_id = 5`. Mixed older stubs are incompatible.
The root Maven build includes only the four component modules.

Bootstrap must remain available and retain its worker registry. Full-cluster/bootstrap
restart recovery, TLS/authentication and replicated bootstrap are not implemented.
Checkpoint bytes are kept in worker RAM. Recovery deliberately stops if a majority
of the PREVIOUS replica set is unavailable; shrinking the live registry does not
make loss of acknowledged work acceptable. Do not claim arbitrary partition-safe
consensus. End-to-end job recovery is verified under the documented crash model.

## Active, slow and unavailable tasks

Workers send task heartbeats once per second, separately from actual work updates.
Heartbeats never increase completion percentage. Advancing measured work means
Active. A reachable task without advancement for 10 seconds is shown as Slow.
Five seconds without task heartbeat/progress means unreachable; 60 seconds without
measured advancement means stalled. Unfinished work is reassigned. These are
failure suspicions, not proof that a machine crashed. A task can run longer than
60 seconds provided actual work continues advancing. Bootstrap independently
expires worker registrations after missed heartbeats. The coordinator's checkpoint
RPCs are independent of task-stream cancellation, and uncertain checkpoint writes
are retried with the original job/task IDs.
