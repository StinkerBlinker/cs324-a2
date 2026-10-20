package cs324.assignment2.worker;

import cs324.assignment2.grpc.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Worker-only election, view agreement and replicated checkpoint recovery.
 * Bootstrap is used solely to discover the registered active worker endpoints. */
final class ElectionRecovery {
    private final WorkerNode node;
    private final Object state = new Object(), admission = new Object();
    private final Map<Integer, CandidacyReport> candidates = new HashMap<>();
    private final Map<String, RecoveryRecord> records = new HashMap<>();
    private final Set<String> acknowledgedAdmissions = new HashSet<>();
    private final Set<String> countedAllocations = new HashSet<>();
    private final AtomicBoolean recovering = new AtomicBoolean();
    private volatile ClusterView view = ClusterView.getDefaultInstance();
    private volatile boolean bootstrapReachable;
    private long seenElection, seenAnnouncement, repairedOverlay, promisedEpoch, recoveredEpoch, revision;
    private CandidacyReport ownCandidate;
    private int jobsThisTerm, missedChecks;

    ElectionRecovery(WorkerNode node) { this.node = node; }
    void unavailable() { bootstrapReachable = false; }
    private ClusterView syncView() {
        ClusterView current = node.metadata().current();
        return adopt(current);
    }
    private ClusterView adopt(ClusterView current) {
        synchronized (state) {
            if (current.getEpoch() < view.getEpoch()) return view;
            if (current.getEpoch() == view.getEpoch() && current.getMembershipVersion() < view.getMembershipVersion()) return view;
            if (current.getEpoch() > view.getEpoch()) {
                candidates.clear(); ownCandidate = null; jobsThisTerm = 0; missedChecks = 0;
            }
            view = current; bootstrapReachable = true;
            node.getElectionInProgress().set(current.getElectionInProgress());
            return current;
        }
    }
    void tick() {
        ClusterView current = syncView();
        if (node.stopped()) return;
        if (!current.getReady() && !current.getElectionInProgress()) {
            current = begin(current, false);
        }
        if (current.getEpoch() == 0) return;
        if (repairedOverlay != current.getEpoch()) {
            repairedOverlay = current.getEpoch();
            List<WorkerInfo> voters = current.getVotersList();
            node.dispatch(() -> node.repairOverlay(voters));
        }
        if (current.getElectionInProgress() && !current.hasCoordinator()) {
            ElectionMessage message = ElectionMessage.newBuilder().setEpoch(current.getEpoch())
                    .setElectionId(Long.toString(current.getEpoch())).setInitiator(current.getInitiator()).build();
            if (current.getInitiator().getId() == node.getMyId()) {
                receiveElection(message);
                // Retransmit along the overlay; duplicates never repeat processing.
                floodElection(message);
                conclude(current.getEpoch());
            }
            CandidacyReport report;
            synchronized (state) { report = ownCandidate; }
            if (report != null && report.getEpoch() == current.getEpoch()) report(report, current.getInitiator());
            return;
        }
        if (!current.hasCoordinator()) return;
        if (current.getCoordinator().getId() == node.getMyId()) {
            if (current.getReady() && jobsThisTerm >= 5) { begin(current, true); return; }
            if (!current.getReady() && recovering.compareAndSet(false, true)) {
                ClusterView recoveryView = current;
                node.dispatch(() -> {
                    try { recover(recoveryView); }
                    catch (RuntimeException error) { node.log("Recovery not ready: " + error.getMessage()); }
                    finally { recovering.set(false); }
                });
            }
        } else if (current.getReady()) {
            boolean healthy = false;
            try {
                WorkerStatus status = node.peer(current.getCoordinator()).getWorkerStatus(GetWorkerStatusRequest.getDefaultInstance());
                healthy = status.getEpoch() == current.getEpoch() && status.getReady()
                        && status.getCoordinatorId() == current.getCoordinator().getId();
            } catch (StatusRuntimeException ignored) { }
            missedChecks = healthy ? 0 : missedChecks + 1;
            if (missedChecks >= 3) { node.log("Coordinator suspected after 3 missed checks"); begin(current, true); missedChecks = 0; }
        }
    }
    private ClusterView begin(ClusterView observed, boolean force) {
        return adopt(node.metadata().begin(BeginElectionRequest.newBuilder().setWorkerId(node.getMyId())
                .setObservedEpoch(observed.getEpoch()).setForce(force).build()));
    }
    void receiveElection(ElectionMessage message) {
        ClusterView current = syncView();
        if (!current.getElectionInProgress() || current.hasCoordinator() || message.getEpoch() != current.getEpoch()
                || !message.getInitiator().equals(current.getInitiator())
                || !message.getElectionId().equals(Long.toString(current.getEpoch()))) return;
        CandidacyReport report;
        synchronized (state) {
            if (message.getEpoch() <= seenElection || !current.getVotersList().contains(node.selfInfo())) return;
            seenElection = message.getEpoch();
            ownCandidate = CandidacyReport.newBuilder().setEpoch(message.getEpoch()).setElectionId(message.getElectionId())
                    .setCandidate(node.selfInfo()).setJac(node.getJac().get()).build();
            report = ownCandidate;
        }
        node.log("ELECTION epoch=" + current.getEpoch());
        report(report, current.getInitiator()); floodElection(message);
    }
    private void report(CandidacyReport report, WorkerInfo initiator) {
        if (initiator.getId() == node.getMyId()) candidate(report);
        else node.dispatch(() -> {
            try { node.peer(initiator).reportCandidacy(report); } catch (StatusRuntimeException ignored) { }
        });
    }
    private void floodElection(ElectionMessage message) {
        for (WorkerInfo neighbour : List.copyOf(node.getNeighbours())) node.dispatch(() -> {
            try { node.peer(neighbour).receiveElectionMessage(message); } catch (StatusRuntimeException ignored) { }
        });
    }
    void candidate(CandidacyReport report) {
        ClusterView current = view;
        synchronized (state) {
            if (!current.getElectionInProgress() || current.hasCoordinator() || current.getEpoch() != report.getEpoch()
                    || current.getInitiator().getId() != node.getMyId() || report.getJac() < 0
                    || !report.getElectionId().equals(Long.toString(current.getEpoch()))
                    || !current.getVotersList().contains(report.getCandidate())) return;
            candidates.putIfAbsent(report.getCandidate().getId(), report);
        }
    }
    private void conclude(long epoch) {
        WorkerInfo winner;
        synchronized (state) {
            if (view.getEpoch() != epoch || view.hasCoordinator() || !view.getElectionInProgress()) return;
            // Wait for EVERY member of this round, not a fixed candidate timer.
            // A failed member is removed by bootstrap; that invalidates this round.
            if (view.getVotersList().stream().anyMatch(w -> !candidates.containsKey(w.getId()))) return;
            winner = candidates.values().stream().min(Comparator.comparingInt(CandidacyReport::getJac)
                    .thenComparing(Comparator.comparingInt((CandidacyReport c) -> c.getCandidate().getId()).reversed()))
                    .orElseThrow().getCandidate();
        }
        ClusterView published = adopt(node.metadata().publish(PublishCoordinatorRequest.newBuilder()
                .setEpoch(epoch).setInitiatorId(node.getMyId()).setCoordinatorId(winner.getId()).build()));
        if (published.getEpoch() == epoch && published.hasCoordinator())
            announcement(CoordinatorMessage.newBuilder().setEpoch(epoch).setElectionId(Long.toString(epoch))
                    .setCoordinator(published.getCoordinator()).build());
    }
    void announcement(CoordinatorMessage message) {
        ClusterView current = syncView();
        if (current.getEpoch() != message.getEpoch() || !current.hasCoordinator()
                || !current.getCoordinator().equals(message.getCoordinator())
                || !message.getElectionId().equals(Long.toString(current.getEpoch()))) return;
        synchronized (state) {
            if (seenAnnouncement >= message.getEpoch()) return;
            seenAnnouncement = message.getEpoch();
        }
        node.log("COORDINATOR " + message.getCoordinator().getId() + " epoch=" + message.getEpoch());
        for (WorkerInfo neighbour : List.copyOf(node.getNeighbours())) node.dispatch(() -> {
            try { node.peer(neighbour).receiveCoordinatorMessage(message); } catch (StatusRuntimeException ignored) { }
        });
    }
    private static int compare(RecoveryRecord a, RecoveryRecord b) {
        int order = Long.compare(a.getEpoch(), b.getEpoch());
        return order == 0 ? Long.compare(a.getRevision(), b.getRevision()) : order;
    }
    private static void merge(Map<String, RecoveryRecord> into, RecoveryRecord record) {
        into.merge(record.getKey(), record, (a,b) -> compare(a,b) >= 0 ? a : b);
    }
    private static int majority(int size) { return size / 2 + 1; }
    private void requireEpoch(ClusterView actual, long epoch) {
        if (node.stopped() || actual.getEpoch() != epoch || !actual.hasCoordinator()
                || (!actual.getReady() && !actual.getElectionInProgress()))
            throw new IllegalStateException("Election/configuration changed; retry after discovery");
    }
    RecoverySnapshot readRecovery(long epoch) {
        ClusterView actual = syncView(); requireEpoch(actual, epoch);
        synchronized (state) {
            if (epoch < promisedEpoch) throw new IllegalStateException("Older recovery epoch");
            // Atomic barrier: once this snapshot is read, this replica will not
            // accept any write from an earlier coordinator epoch.
            promisedEpoch = epoch;
            return snapshot();
        }
    }
    RecoverySnapshot snapshot() {
        synchronized (state) { return RecoverySnapshot.newBuilder().addAllRecords(records.values()).build(); }
    }
    CheckpointAck replicate(RecoveryRecord record) {
        ClusterView actual = syncView(); requireEpoch(actual, record.getEpoch());
        synchronized (state) {
            if (record.getKey().isBlank() || record.getRevision() <= 0 || record.getEpoch() < promisedEpoch) return ack(false, "Stale/invalid record");
            promisedEpoch = record.getEpoch();
            RecoveryRecord previous = records.get(record.getKey());
            if (previous != null && compare(previous, record) > 0) return ack(false, "Newer record retained");
            if (previous != null && compare(previous, record) == 0) return ack(previous.equals(record), "Duplicate version");
            records.put(record.getKey(), record); return ack(true, "Stored");
        }
    }
    private int replicateToVoters(RecoveryRecord record, ClusterView target) {
        int copies = 0;
        for (WorkerInfo worker : target.getVotersList()) try {
            CheckpointAck answer = worker.getId() == node.getMyId() ? replicate(record) : node.peer(worker).replicateRecord(record);
            if (answer.getAccepted()) copies++;
        } catch (StatusRuntimeException | IllegalStateException ignored) { }
        return copies;
    }
    private void recover(ClusterView target) {
        synchronized (admission) {
            long epoch = target.getEpoch();
            Map<String, RecoveryRecord> merged = new HashMap<>();
            int oldResponses = 0;
            for (WorkerInfo worker : target.getPreviousVotersList()) try {
                RecoverySnapshot snapshot = worker.getId() == node.getMyId() ? readRecovery(epoch)
                        : node.peer(worker).readRecovery(RecoveryRequest.newBuilder().setEpoch(epoch).build());
                oldResponses++;
                snapshot.getRecordsList().forEach(r -> merge(merged, r));
            } catch (StatusRuntimeException | IllegalStateException ignored) { }
            if (!target.getPreviousVotersList().isEmpty() && oldResponses < majority(target.getPreviousVotersCount()))
                throw new IllegalStateException("Previous replica majority unavailable; refusing to lose acknowledged jobs");
            ClusterView actual = syncView(); requireEpoch(actual, epoch);
            if (actual.getCoordinator().getId() != node.getMyId()) return;
            synchronized (state) {
                promisedEpoch = Math.max(promisedEpoch, epoch);
                records.clear(); records.putAll(merged);
                revision = records.values().stream().mapToLong(RecoveryRecord::getRevision).max().orElse(0);
            }
            for (RecoveryRecord old : merged.values()) {
                RecoveryRecord repaired;
                synchronized (state) { repaired = old.toBuilder().setEpoch(epoch).setRevision(++revision).build(); }
                if (replicateToVoters(repaired, target) < majority(target.getVotersCount()))
                    throw new IllegalStateException("Cannot repair recovered records to the new majority");
            }
            synchronized (state) {
                recoveredEpoch = epoch;
                acknowledgedAdmissions.clear();
                merged.values().stream().filter(RecoveryRecord::getAdmittedJob)
                        .forEach(r -> acknowledgedAdmissions.add(r.getKey()));
            }
            ClusterView ready = adopt(node.metadata().ready(MarkCoordinatorReadyRequest.newBuilder()
                    .setEpoch(epoch).setCoordinatorId(node.getMyId()).build()));
            if (ready.getEpoch() == epoch && ready.getReady())
                node.log("READY epoch=" + epoch + " recovered=" + merged.size() + " previousResponses=" + oldResponses);
        }
    }
    CheckpointAck checkpoint(SaveCheckpointRequest request) {
        if (request.getKey().isBlank() || request.getRemoteAssignments() < 0)
            throw new IllegalArgumentException("Stable checkpoint key and nonnegative assignment count required");
        synchronized (admission) {
            ClusterView actual = syncView();
            long epoch = actual.getEpoch();
            requireEpoch(actual, epoch);
            synchronized (state) {
                if (!actual.getReady() || actual.getCoordinator().getId() != node.getMyId() || recoveredEpoch != epoch)
                    throw new IllegalStateException("Coordinator not ready; rediscover and retry");
                RecoveryRecord existing = records.get(request.getKey());
                if (request.getNewJob() && existing != null && acknowledgedAdmissions.contains(request.getKey()))
                    return ack(true, "Existing job ID; retrieve its state rather than counting/resubmitting it");
                if (request.getNewJob() && jobsThisTerm >= 5) throw new IllegalStateException("Five-job term ended");
                if (request.getNewJob() && request.getRemoteAssignments() > Integer.MAX_VALUE - node.getJac().get())
                    throw new IllegalArgumentException("JAC would overflow");
            }
            RecoveryRecord record;
            synchronized (state) {
                RecoveryRecord previous = records.get(request.getKey());
                record = RecoveryRecord.newBuilder().setKey(request.getKey()).setPayload(request.getPayload())
                        .setEpoch(epoch).setRevision(++revision)
                        .setAdmittedJob(request.getNewJob() || (previous != null && previous.getAdmittedJob())).build();
            }
            int copies = replicateToVoters(record, actual);
            ClusterView after = syncView();
            if (copies < majority(actual.getVotersCount()) || after.getEpoch() != epoch || !after.getReady()
                    || !after.hasCoordinator() || after.getCoordinator().getId() != node.getMyId())
                return ack(false, "Outcome uncertain; retry using the same job ID");
            boolean rotate = false;
            if (request.getNewJob()) synchronized (state) {
                acknowledgedAdmissions.add(request.getKey());
                node.getJac().addAndGet(request.getRemoteAssignments()); rotate = ++jobsThisTerm == 5;
            }
            if (rotate) {
                node.log("Five jobs accepted; ending term");
                node.dispatch(() -> { try { begin(after, true); } catch (StatusRuntimeException ignored) { } });
            }
            return ack(true, "Checkpoint stored on a majority");
        }
    }
    private static CheckpointAck ack(boolean accepted, String detail) { return CheckpointAck.newBuilder().setAccepted(accepted).setDetail(detail).build(); }
    void recordRemoteAssignment(String allocationId) {
        if (allocationId.isBlank()) throw new IllegalArgumentException("Unique allocation ID required");
        synchronized (admission) {
            ClusterView actual = syncView();
            synchronized (state) {
                if (!actual.getReady() || !actual.hasCoordinator() || actual.getCoordinator().getId() != node.getMyId()
                        || recoveredEpoch != actual.getEpoch()) throw new IllegalStateException("Only the ready coordinator can allocate work");
                if (countedAllocations.contains(allocationId)) return;
                if (node.getJac().get() == Integer.MAX_VALUE) throw new IllegalStateException("JAC exhausted");
                countedAllocations.add(allocationId); node.getJac().incrementAndGet();
            }
        }
    }
    int coordinatorId() {
        ClusterView current = view;
        return bootstrapReachable && current.getReady() && current.hasCoordinator() ? current.getCoordinator().getId() : -1;
    }
    WorkerStatus status() {
        synchronized (state) {
            return WorkerStatus.newBuilder().setSelf(node.selfInfo()).setEpoch(view.getEpoch()).setJac(node.getJac().get())
                    .setJobsThisTerm(jobsThisTerm).setCoordinatorId(coordinatorId()).setElectionInProgress(view.getElectionInProgress())
                    .setReady(bootstrapReachable && view.getReady() && view.hasCoordinator()
                            && view.getCoordinator().getId() == node.getMyId() && recoveredEpoch == view.getEpoch()).build();
        }
    }
}
