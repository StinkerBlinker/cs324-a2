# CS324 A2 - leader election and coordinator recovery

Java 24 / Maven. This branch implements Pranay's election service and checkpoint
recovery boundary. The submitted A1 ZIP and report remain at the repository root.
This is a standalone election demo, not the completed A2 job application.

## Temporary integration stubs

The `cs324.election.demo` package makes the missing team integrations explicit:

| Stub | Temporary behaviour | Replace with |
| --- | --- | --- |
| `StaticMembershipStub` | Parses the fixed worker list passed on the command line | Josh's bootstrap discovery and agreed membership configuration |
| `JobCheckpointStub` | Supplies a fabricated job checkpoint marked `STUB_ONLY` | Aveshal's actual job/task state and recovery integration |

Election RPCs, coordinator monitoring and checkpoint replication/recovery are real.
These stubs do not implement bootstrap registration, computation or resumed jobs.
Report Q1/Q4/Q5, dynamic membership and the documented protocol limitations remain
outstanding; this branch is work in progress, not a complete assignment submission.

## Build in VS Code / PowerShell

Open this repository folder (the folder containing this README and `pom.xml`).
Use JDK 24. On Pranay's machine JAVA_HOME currently points to Java 17 even though
`java -version` prints 24. Set JAVA_HOME in each build terminal first (adjust the
path on other machines). No global Maven installation is needed:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-24'
.\mvnw.cmd clean verify
```

The first build requires internet access to Maven Central. Output:
`election/target/election-1.0-SNAPSHOT.jar`.

## Run three independent worker processes

Open three VS Code terminals at the repository root. Run one command in each.
Keep the membership list identical. Neighbour links in this demo are reciprocal.

Terminal 1:
```powershell
java -jar election/target/election-1.0-SNAPSHOT.jar 1 '1:localhost:5101,2:localhost:5102,3:localhost:5103' '2,3'
```
Terminal 2:
```powershell
java -jar election/target/election-1.0-SNAPSHOT.jar 2 '1:localhost:5101,2:localhost:5102,3:localhost:5103' '1,3'
```
Terminal 3:
```powershell
java -jar election/target/election-1.0-SNAPSHOT.jar 3 '1:localhost:5101,2:localhost:5102,3:localhost:5103' '1,2'
```

Start all three within a few seconds. Wait for `COORDINATOR 3` and worker 3's
`READY` message. All initial JACs are zero, so highest ID wins. An optional fourth
argument sets the initial JAC, useful for demonstrating that lowest JAC takes priority.

## Crash and recover demo

In a fourth terminal, store an illustrative job checkpoint on coordinator 3:

```powershell
java -jar election/target/election-1.0-SNAPSHOT.jar save-demo localhost 5103 job-42
```

Expect `Checkpoint replicated to a majority`. Stop worker 3 with Ctrl+C, or kill
that worker process to demonstrate an abrupt crash. Workers 1 and 2 should log
three missed coordinator checks, an election, `COORDINATOR 2`, and recovery.

```powershell
java -jar election/target/election-1.0-SNAPSHOT.jar status localhost 5102
java -jar election/target/election-1.0-SNAPSHOT.jar read localhost 5102
```

Expect `ready: true`, coordinator ID 2, and the original `job-42` payload.
This proves checkpoint survival. It does not execute/resume a numerical job:
Aveshal's job engine must consume the recovered records and resume pending tasks.
Stop a second worker: the last worker must not acknowledge new checkpoints,
because two of the original three voters are required.

For an automated independent-process smoke test (starts three hidden worker JVMs,
force-kills the coordinator, verifies recovery, and stops its own processes):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Smoke-Test.ps1
```

## Source map

* `election/src/main/proto/election.proto`: election messages, RPCs, recovery records.
* `ElectionNode.java`: flooding, duplicate suppression, candidate ranking, health
  checks, logical rounds, replication and recovery.
* `WorkerMain.java`: process launcher and status/save/read demo commands.
* `demo/StaticMembershipStub.java` and `demo/JobCheckpointStub.java`: replaceable
  demo fixtures, explicitly labelled as temporary rather than teammate work.
* `ElectionNodeTest.java`: real localhost gRPC tests for selection, stale messages,
  duplicates, crash recovery, five-job rotation and quorum loss.
* `docs/INTEGRATION.md`: contracts for Josh and Aveshal; limits to explain honestly.
* `docs/WALKTHROUGH.md`: explanation of the demo and its limitations.

Generated protobuf/stub classes and Maven output are build artifacts and should
not be hand edited or committed.
