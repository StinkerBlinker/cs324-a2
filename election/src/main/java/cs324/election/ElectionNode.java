package cs324.election;

import cs324.election.proto.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** Worker-side election and replicated checkpoint service. Membership is a fixed
 * voting configuration; the bootstrap adapter must give every node the same list.
 * Designed for crash-stop failures with a surviving majority, not arbitrary partitions.
 */
public final class ElectionNode extends LeaderElectionGrpc.LeaderElectionImplBase implements AutoCloseable {
    public static final String leaderman = "cs324";
    private static final long RPC_MS = 600, ELECTION_MS = 1400;
    private final Worker self;
    private final Map<Integer, Worker> members;
    private final List<Worker> neighbours;
    private final int quorum;
    private final Object lock = new Object();
    private final ConcurrentMap<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timer = Executors.newScheduledThreadPool(2);
    private final ExecutorService network = Executors.newCachedThreadPool();
    private final AtomicLong jac = new AtomicLong();
    private final Map<String, RecoveryRecord> records = new HashMap<>();
    private final Map<Integer, Candidate> candidates = new HashMap<>();
    private Round round = Round.getDefaultInstance();
    private Worker leader;
    private boolean ready, closed;
    private int missedHeartbeats, jobsThisTerm;
    private long electionDeadline, revision;
    private Server server;

    public ElectionNode(Worker self, List<Worker> membership, List<Integer> neighbourIds, long initialJac) {
        this.self = self;
        Map<Integer, Worker> configured = new TreeMap<>();
        for (Worker w : membership) {
            if (w.getId() <= 0 || w.getPort() <= 0 || w.getPort() > 65535 || w.getHost().isBlank()
                    || configured.putIfAbsent(w.getId(), w) != null)
                throw new IllegalArgumentException("Invalid/duplicate worker configuration");
        }
        if (!self.equals(configured.get(self.getId())) || initialJac < 0)
            throw new IllegalArgumentException("Self must be in membership; JAC must be nonnegative");
        members = Map.copyOf(configured);
        neighbours = neighbourIds.stream().distinct().map(id -> {
            Worker w = members.get(id);
            if (w == null || id == self.getId()) throw new IllegalArgumentException("Invalid neighbour " + id);
            return w;
        }).toList();
        quorum = members.size() / 2 + 1;
        jac.set(initialJac);
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(self.getPort()).addService(this).build().start();
        // Jitter reduces competing starts; correctness uses ordered rounds, not timing.
        timer.scheduleWithFixedDelay(this::safeTick, 300 + self.getId() * 40L, 400, TimeUnit.MILLISECONDS);
        log("listening on " + self.getPort() + "; quorum=" + quorum + "; leaderman=" + leaderman);
    }

    private LeaderElectionGrpc.LeaderElectionBlockingStub peer(Worker w) {
        ManagedChannel channel = channels.computeIfAbsent(w.getId(), id ->
                ManagedChannelBuilder.forAddress(w.getHost(), w.getPort()).usePlaintext().build());
        return LeaderElectionGrpc.newBlockingStub(channel).withDeadlineAfter(RPC_MS, TimeUnit.MILLISECONDS);
    }

    static int compare(Round a, Round b) {
        int order = Long.compare(a.getCounter(), b.getCounter());
        return order != 0 ? order : Integer.compare(a.getInitiatorId(), b.getInitiatorId());
    }

    public void startElection() {
        Round next;
        synchronized (lock) {
            if (closed) return;
            next = Round.newBuilder().setCounter(round.getCounter() + 1).setInitiatorId(self.getId()).build();
        }
        acceptElection(Election.newBuilder().setRound(next).setInitiator(self).build());
    }

    private boolean acceptElection(Election message) {
        if (!message.getInitiator().equals(members.get(message.getRound().getInitiatorId()))
                || message.getRound().getCounter() <= 0) return false;
        synchronized (lock) {
            // One processing per ordered round; equal messages are duplicates.
            if (closed || compare(message.getRound(), round) <= 0) return false;
            round = message.getRound(); leader = null; ready = false;
            candidates.clear(); missedHeartbeats = 0;
            electionDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ELECTION_MS + 1200);
        }
        log("ELECTION " + label(message.getRound()));
        Candidate candidate = Candidate.newBuilder().setRound(message.getRound()).setWorker(self).setJac(jac.get()).build();
        if (message.getInitiator().getId() == self.getId()) {
            acceptCandidate(candidate);
            timer.schedule(() -> conclude(message.getRound()), ELECTION_MS, TimeUnit.MILLISECONDS);
        } else network.execute(() -> attempt(() -> peer(message.getInitiator()).reportCandidacy(candidate)));
        for (Worker w : neighbours)
            network.execute(() -> attempt(() -> peer(w).receiveElection(message)));
        return true;
    }

    private boolean acceptCandidate(Candidate candidate) {
        synchronized (lock) {
            if (!candidate.getWorker().equals(members.get(candidate.getWorker().getId())) || candidate.getJac() < 0
                    || compare(candidate.getRound(), round) != 0 || round.getInitiatorId() != self.getId() || leader != null)
                return false;
            candidates.putIfAbsent(candidate.getWorker().getId(), candidate);
            return true;
        }
    }

    private void conclude(Round expected) {
        Coordinator announcement;
        synchronized (lock) {
            if (closed || compare(round, expected) != 0 || leader != null) return;
            if (candidates.size() < quorum) {
                log("election has no majority; retrying after timeout");
                return;
            }
            Candidate best = candidates.values().stream().min(Comparator.comparingLong(Candidate::getJac)
                    .thenComparing(Comparator.comparingInt((Candidate c) -> c.getWorker().getId()).reversed())).orElseThrow();
            announcement = Coordinator.newBuilder().setRound(round).setWorker(best.getWorker()).build();
        }
        acceptCoordinator(announcement);
    }

    private boolean acceptCoordinator(Coordinator message) {
        synchronized (lock) {
            if (closed || !message.getWorker().equals(members.get(message.getWorker().getId()))
                    || compare(message.getRound(), round) < 0 || message.getRound().getCounter() <= 0) return false;
            if (compare(message.getRound(), round) == 0 && leader != null) return leader.equals(message.getWorker());
            round = message.getRound(); leader = message.getWorker(); ready = false;
            jobsThisTerm = 0; missedHeartbeats = 0;
        }
        log("COORDINATOR " + message.getWorker().getId() + " round=" + label(message.getRound()));
        for (Worker w : neighbours)
            network.execute(() -> attempt(() -> peer(w).announceCoordinator(message)));
        if (message.getWorker().equals(self)) network.execute(() -> recover(message.getRound()));
        return true;
    }

    private void recover(Round expected) {
        Map<String, RecoveryRecord> merged = new HashMap<>();
        int responses = 0;
        for (Worker w : members.values()) {
            try {
                RecoverySnapshot snapshot = w.equals(self) ? snapshot() : peer(w).readRecovery(Empty.getDefaultInstance());
                responses++;
                for (RecoveryRecord record : snapshot.getRecordsList()) merged.merge(record.getKey(), record,
                        (a,b) -> newer(a,b) >= 0 ? a : b);
            } catch (StatusRuntimeException ignored) { }
        }
        synchronized (lock) {
            if (closed || compare(round, expected) != 0 || !self.equals(leader)) return;
            if (responses < quorum) { leader = null; ready = false; electionDeadline = 0; return; }
            for (RecoveryRecord record : merged.values()) records.merge(record.getKey(), record,
                    (a,b) -> newer(a,b) >= 0 ? a : b);
            revision = records.values().stream().mapToLong(RecoveryRecord::getRevision).max().orElse(0);
        }
        // Repair recovered records onto a majority before accepting new jobs.
        // Otherwise a later crash could destroy the sole surviving recovery copy.
        for (RecoveryRecord old : merged.values()) {
            RecoveryRecord repaired;
            synchronized (lock) {
                if (closed || compare(round, expected) != 0 || !self.equals(leader)) return;
                repaired = old.toBuilder().setRound(expected).setRevision(++revision).build();
                records.put(repaired.getKey(), repaired);
            }
            int copies = 1;
            for (Worker w : members.values()) if (!w.equals(self)) {
                try { if (peer(w).replicateRecord(repaired).getAccepted()) copies++; }
                catch (StatusRuntimeException ignored) { }
            }
            if (copies < quorum) {
                synchronized (lock) {
                    if (compare(round, expected) == 0) { leader = null; ready = false; electionDeadline = 0; }
                }
                return;
            }
        }
        synchronized (lock) {
            if (closed || compare(round, expected) != 0 || !self.equals(leader)) return;
            ready = true;
        }
        log("READY recovered " + merged.size() + " checkpoints from " + responses + " replicas");
    }

    private static int newer(RecoveryRecord a, RecoveryRecord b) {
        int order = compare(a.getRound(), b.getRound());
        return order != 0 ? order : Long.compare(a.getRevision(), b.getRevision());
    }

    private void safeTick() {
        try { tick(); } catch (Exception error) { log("monitor: " + error.getMessage()); }
    }

    private void tick() {
        Worker current; Round expected;
        synchronized (lock) {
            if (closed) return;
            current = leader; expected = round;
            if (current == null && System.nanoTime() < electionDeadline) return;
        }
        if (current == null) { startElection(); return; }
        if (current.equals(self)) {
            int reachable = 1;
            for (Worker w : members.values()) if (!w.equals(self)) {
                try {
                    NodeStatus status = peer(w).getStatus(Empty.getDefaultInstance());
                    if (compare(status.getRound(), expected) > 0) {
                        synchronized (lock) {
                            if (compare(round, expected) == 0) { ready = false; leader = null; electionDeadline = 0; }
                        }
                        return;
                    }
                    if (compare(status.getRound(), expected) == 0 && status.hasCoordinator()
                            && status.getCoordinator().equals(self)) reachable++;
                } catch (StatusRuntimeException ignored) { }
            }
            if (reachable < quorum) synchronized (lock) {
                if (compare(round, expected) == 0) { ready = false; leader = null; electionDeadline = 0; }
            }
            return;
        }
        boolean healthy = false;
        try {
            NodeStatus status = peer(current).getStatus(Empty.getDefaultInstance());
            // A live coordinator may still be recovering. Its admission-ready flag
            // is not a heartbeat: treating recovery as a crash causes election storms.
            healthy = status.getCoordinator().equals(current)
                    && compare(status.getRound(), expected) == 0;
        } catch (StatusRuntimeException ignored) { }
        boolean retry;
        synchronized (lock) {
            if (compare(round, expected) != 0 || !current.equals(leader)) return;
            missedHeartbeats = healthy ? 0 : missedHeartbeats + 1;
            retry = missedHeartbeats >= 3;
        }
        if (retry) { log("coordinator suspected after 3 missed checks"); startElection(); }
    }

    /** Call only for a NEW accepted job, not retries/chunks. Ends term at five.
     * Job engine must checkpoint acceptance before invoking this method. */
    public void recordAcceptedJob(int remoteAssignments) {
        if (remoteAssignments < 0) throw new IllegalArgumentException("Negative assignment count");
        boolean end;
        synchronized (lock) {
            requireLeader();
            if (jobsThisTerm >= 5) throw new IllegalStateException("Term ended");
            jac.addAndGet(remoteAssignments);
            end = ++jobsThisTerm == 5;
        }
        if (end) startElection();
    }

    private void requireLeader() {
        if (closed || !ready || !self.equals(leader)) throw new IllegalStateException("Coordinator not ready; rediscover and retry");
    }

    /** Majority replication before success. Serialize callers to preserve local revision order. */
    public synchronized boolean checkpoint(String key, com.google.protobuf.ByteString payload) {
        if (key.isBlank()) throw new IllegalArgumentException("Checkpoint key required");
        RecoveryRecord record;
        synchronized (lock) {
            requireLeader();
            record = RecoveryRecord.newBuilder().setKey(key).setPayload(payload).setRound(round).setRevision(++revision).build();
            records.put(key, record);
        }
        int acknowledgements = 1;
        for (Worker w : members.values()) if (!w.equals(self)) {
            try { if (peer(w).replicateRecord(record).getAccepted()) acknowledgements++; }
            catch (StatusRuntimeException ignored) { }
        }
        synchronized (lock) {
            return acknowledgements >= quorum && ready && self.equals(leader) && compare(round, record.getRound()) == 0;
        }
    }

    private boolean replicate(RecoveryRecord record) {
        synchronized (lock) {
            if (closed || leader == null || compare(record.getRound(), round) != 0 || record.getKey().isBlank()
                    || record.getRevision() <= 0) return false;
            RecoveryRecord previous = records.get(record.getKey());
            if (previous != null && newer(previous, record) > 0) return false;
            if (previous != null && newer(previous, record) == 0) return previous.equals(record);
            records.put(record.getKey(), record);
            return true;
        }
    }

    public NodeStatus status() {
        synchronized (lock) {
            NodeStatus.Builder result = NodeStatus.newBuilder().setSelf(self).setRound(round).setJac(jac.get())
                    .setJobsThisTerm(jobsThisTerm).setReady(ready);
            if (leader != null) result.setCoordinator(leader);
            return result.build();
        }
    }
    public RecoverySnapshot snapshot() {
        synchronized (lock) { return RecoverySnapshot.newBuilder().addAllRecords(records.values()).build(); }
    }
    private static void reply(StreamObserver<Ack> observer, boolean accepted) {
        observer.onNext(Ack.newBuilder().setAccepted(accepted).build()); observer.onCompleted();
    }
    @Override public void receiveElection(Election request, StreamObserver<Ack> observer) { reply(observer, acceptElection(request)); }
    @Override public void reportCandidacy(Candidate request, StreamObserver<Ack> observer) { reply(observer, acceptCandidate(request)); }
    @Override public void announceCoordinator(Coordinator request, StreamObserver<Ack> observer) { reply(observer, acceptCoordinator(request)); }
    @Override public void getStatus(Empty request, StreamObserver<NodeStatus> observer) { observer.onNext(status()); observer.onCompleted(); }
    @Override public void readRecovery(Empty request, StreamObserver<RecoverySnapshot> observer) { observer.onNext(snapshot()); observer.onCompleted(); }
    @Override public void replicateRecord(RecoveryRecord request, StreamObserver<Ack> observer) { reply(observer, replicate(request)); }
    @Override public void saveCheckpoint(Checkpoint request, StreamObserver<Ack> observer) {
        try { reply(observer, checkpoint(request.getKey(), request.getPayload())); }
        catch (IllegalStateException e) { observer.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException()); }
        catch (IllegalArgumentException e) { observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException()); }
    }
    private static void attempt(Supplier<?> operation) { try { operation.get(); } catch (StatusRuntimeException ignored) { } }
    private static String label(Round r) { return r.getCounter() + ":" + r.getInitiatorId(); }
    private void log(String text) { System.out.println("[Worker " + self.getId() + "] " + text); }
    @Override public void close() {
        synchronized (lock) { closed = true; ready = false; }
        if (server != null) server.shutdownNow();
        timer.shutdownNow(); network.shutdownNow();
        channels.values().forEach(ManagedChannel::shutdownNow);
    }
}
