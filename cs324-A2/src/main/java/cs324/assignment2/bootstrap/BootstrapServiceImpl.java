package cs324.assignment2.bootstrap;

import cs324.assignment2.grpc.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.*;
import java.util.concurrent.*;

/** Registration, failure monitoring and a single authoritative view of election
 * metadata. Bootstrap does not rank candidates or execute computational jobs.
 * Bootstrap itself must remain running: metadata is not durable/replicated. */
public class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {
    public static final long HEARTBEAT_TIMEOUT_MS = 5000;
    private final Map<Integer, WorkerRecord> activeWorkers = new TreeMap<>();
    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();
    private final long timeoutMs, electionTimeoutMs;
    private int nextId = 1;
    private long membershipVersion, electionDeadline;
    private List<WorkerInfo> lastReadyVoters = List.of();
    private ClusterView view = ClusterView.getDefaultInstance();

    public BootstrapServiceImpl() { this(HEARTBEAT_TIMEOUT_MS, 1000, 12000); }
    public BootstrapServiceImpl(long timeoutMs, long cleanupMs, long electionTimeoutMs) {
        this.timeoutMs = timeoutMs; this.electionTimeoutMs = electionTimeoutMs;
        cleaner.scheduleWithFixedDelay(this::removeDeadWorkers, cleanupMs, cleanupMs, TimeUnit.MILLISECONDS);
    }
    private static <T> void reply(StreamObserver<T> out, T value) { out.onNext(value); out.onCompleted(); }
    private void membershipChanged() {
        membershipVersion++;
        view = view.toBuilder().setMembershipVersion(membershipVersion).setReady(false).setElectionInProgress(false).clearCoordinator().build();
    }
    @Override public synchronized void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> out) {
        if (request.getHost().isBlank() || request.getPort() <= 0 || request.getPort() > 65535) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Valid worker endpoint required").asRuntimeException()); return;
        }
        int id = nextId++;
        WorkerInfo info = WorkerInfo.newBuilder().setId(id).setHost(request.getHost()).setPort(request.getPort()).build();
        activeWorkers.put(id, new WorkerRecord(info)); membershipChanged();
        System.out.println("[Bootstrap] Registered worker " + id + " at " + info.getHost() + ":" + info.getPort());
        reply(out, RegisterWorkerResponse.newBuilder().setWorkerId(id).build());
    }
    @Override public synchronized void deregisterWorker(DeregisterWorkerRequest request, StreamObserver<DeregisterWorkerResponse> out) {
        if (activeWorkers.remove(request.getWorkerId()) != null) {
            membershipChanged(); System.out.println("[Bootstrap] Deregistered worker " + request.getWorkerId());
        }
        reply(out, DeregisterWorkerResponse.getDefaultInstance());
    }
    @Override public synchronized void getActiveWorkers(GetActiveWorkersRequest request, StreamObserver<GetActiveWorkersResponse> out) {
        reply(out, GetActiveWorkersResponse.newBuilder().addAllWorkers(activeWorkers.values().stream().map(r -> r.info).toList()).build());
    }
    @Override public synchronized void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> out) {
        WorkerRecord rec = activeWorkers.get(request.getWorkerId());
        if (rec != null) rec.lastHeartbeatNanos = System.nanoTime();
        reply(out, HeartbeatResponse.newBuilder().setAcknowledged(rec != null).build());
    }
    private synchronized void removeDeadWorkers() {
        long now = System.nanoTime();
        boolean changed = activeWorkers.entrySet().removeIf(entry -> {
            boolean dead = now - entry.getValue().lastHeartbeatNanos > TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            if (dead) System.out.println("[Bootstrap] Worker " + entry.getKey() + " heartbeat timed out");
            return dead;
        });
        if (changed) membershipChanged();
    }
    private void expireElection() {
        if (view.getElectionInProgress() && System.nanoTime() > electionDeadline)
            view = view.toBuilder().setElectionInProgress(false).setReady(false).clearCoordinator().build();
    }
    @Override public synchronized void getClusterView(GetClusterViewRequest request, StreamObserver<ClusterView> out) {
        expireElection(); reply(out, view);
    }
    @Override public synchronized void beginElection(BeginElectionRequest request, StreamObserver<ClusterView> out) {
        expireElection();
        WorkerRecord initiator = activeWorkers.get(request.getWorkerId());
        if (initiator == null) { out.onError(Status.FAILED_PRECONDITION.withDescription("Worker not registered").asRuntimeException()); return; }
        // Concurrent requests attach to the same round. Stale requests cannot
        // interrupt a newer round. Membership changes invalidate the old view.
        if (view.getElectionInProgress() || request.getObservedEpoch() != view.getEpoch()
                || (view.getReady() && !request.getForce())) { reply(out, view); return; }
        view = ClusterView.newBuilder().setEpoch(view.getEpoch() + 1).setInitiator(initiator.info)
                .setMembershipVersion(membershipVersion).setElectionInProgress(true)
                .addAllVoters(activeWorkers.values().stream().map(r -> r.info).toList())
                .addAllPreviousVoters(lastReadyVoters).build();
        electionDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(electionTimeoutMs);
        System.out.println("[Bootstrap] Election metadata epoch=" + view.getEpoch() + " voters=" + view.getVotersCount());
        reply(out, view);
    }
    @Override public synchronized void publishCoordinator(PublishCoordinatorRequest request, StreamObserver<ClusterView> out) {
        expireElection();
        if (view.getElectionInProgress() && request.getEpoch() == view.getEpoch()
                && request.getInitiatorId() == view.getInitiator().getId()
                && view.getMembershipVersion() == membershipVersion && !view.hasCoordinator()) {
            view.getVotersList().stream().filter(w -> w.getId() == request.getCoordinatorId()).findFirst()
                    .ifPresent(w -> view = view.toBuilder().setCoordinator(w).build());
        }
        reply(out, view);
    }
    @Override public synchronized void markCoordinatorReady(MarkCoordinatorReadyRequest request, StreamObserver<ClusterView> out) {
        expireElection();
        if (view.getElectionInProgress() && view.hasCoordinator() && request.getEpoch() == view.getEpoch()
                && request.getCoordinatorId() == view.getCoordinator().getId() && view.getMembershipVersion() == membershipVersion) {
            lastReadyVoters = List.copyOf(view.getVotersList());
            view = view.toBuilder().setReady(true).setElectionInProgress(false).build();
        }
        reply(out, view);
    }
    public void shutdown() { cleaner.shutdownNow(); }
    private static final class WorkerRecord {
        final WorkerInfo info;
        long lastHeartbeatNanos = System.nanoTime();
        WorkerRecord(WorkerInfo info) { this.info = info; }
    }
}
