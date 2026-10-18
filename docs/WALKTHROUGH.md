> This walkthrough describes the older standalone `election/` demonstration. For the integrated application and GUI, use the repository README and docs/INTEGRATION.md.

# Understand the demo

These notes explain the election and recovery demo, its integration points, and
the limitations to understand when running it.

1. Each worker is a separate Java process running a gRPC service. Protocol Buffers
   specify messages and generate the Java types and RPC stubs during Maven builds.
2. A worker without a coordinator starts a logical election round. A round is a
   counter plus initiator ID, providing a deterministic order without comparing
   computers' wall clocks. A worker processes only strictly newer election rounds.
3. Election messages spread along neighbour links. Every participating worker
   reports its identity and JAC directly to the initiator. Forwarding runs on an
   executor, so inbound RPC handlers do not wait on recursive neighbour callbacks.
4. After the candidate window, the initiator needs a configured majority and
   compares candidates: lowest JAC first, then highest worker ID. The result floods
   along neighbour links. Explain the candidate timing limitation honestly.
5. The selected worker reads recovery records from a majority, merges the newest
   version of each key, and replicates recovered records again before becoming
   ready. Round and revision identify record versions. It does not yet know how to
   execute the opaque payload: that belongs to the job engine integration.
6. Followers periodically call GetStatus on the coordinator with an RPC deadline.
   Three failed/inconsistent responses trigger suspicion and a new election.
   A coordinator still recovering can answer heartbeats but cannot admit jobs.
7. Checkpoint writes return success only after a majority acknowledges. If the
   caller times out, the outcome is uncertain rather than guaranteed absent.
   Stable IDs and reconciliation prevent accidental duplicate job submissions.
8. A leader without a reachable majority stops being ready. This improves the
   simple crash demo's behaviour, but is not a general distributed-consensus proof.
9. The job engine notifies this module of each new accepted job and its remote
   allocations. JAC persists in that living worker across terms; the fifth job
   triggers another election. Test coverage exercises this hook without a GUI.

Questions to practise: Why lowest JAC? Why highest ID for ties? Why suppress
duplicates? Why can't a timeout distinguish a crash from a network delay? What
state does an in-flight job need? Why are two copies enough in three voters? What
happens if the coordinator crashes before replying to a successful checkpoint?
What still needs integration before an actual computation resumes?
