package cs324.assignment2.bootstrap;

import cs324.assignment2.grpc.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.*;
import java.util.concurrent.*;

/** Registration, discovery and heartbeat expiry only. No election state,
 * coordinator selection, readiness control or computational job processing. */
public class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {
    public static final long HEARTBEAT_TIMEOUT_MS = 5000;
    private final Map<Integer, WorkerRecord> activeWorkers = new TreeMap<>();
    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();
    private final long timeoutMs;
    private int nextId = 1;

    public BootstrapServiceImpl() { this(HEARTBEAT_TIMEOUT_MS, 1000); }
    public BootstrapServiceImpl(long timeoutMs, long cleanupMs) {
        this.timeoutMs = timeoutMs;
        cleaner.scheduleWithFixedDelay(this::removeDeadWorkers, cleanupMs, cleanupMs, TimeUnit.MILLISECONDS);
    }
    private static <T> void reply(StreamObserver<T> out, T value) { out.onNext(value); out.onCompleted(); }
    @Override public synchronized void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> out) {
        if (request.getHost().isBlank() || request.getPort() <= 0 || request.getPort() > 65535) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Valid worker endpoint required").asRuntimeException()); return;
        }
        int id = nextId++;
        WorkerInfo info = WorkerInfo.newBuilder().setId(id).setHost(request.getHost()).setPort(request.getPort()).build();
        activeWorkers.put(id, new WorkerRecord(info));
        System.out.println("[Bootstrap] Registered worker " + id + " at " + info.getHost() + ":" + info.getPort());
        reply(out, RegisterWorkerResponse.newBuilder().setWorkerId(id).build());
    }
    @Override public synchronized void deregisterWorker(DeregisterWorkerRequest request, StreamObserver<DeregisterWorkerResponse> out) {
        if (activeWorkers.remove(request.getWorkerId()) != null) {
            System.out.println("[Bootstrap] Deregistered worker " + request.getWorkerId());
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
        activeWorkers.entrySet().removeIf(entry -> {
            boolean dead = now - entry.getValue().lastHeartbeatNanos > TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            if (dead) System.out.println("[Bootstrap] Worker " + entry.getKey() + " heartbeat timed out");
            return dead;
        });
    }
    public void shutdown() { cleaner.shutdownNow(); }
    private static final class WorkerRecord {
        final WorkerInfo info;
        long lastHeartbeatNanos = System.nanoTime();
        WorkerRecord(WorkerInfo info) { this.info = info; }
    }
}
