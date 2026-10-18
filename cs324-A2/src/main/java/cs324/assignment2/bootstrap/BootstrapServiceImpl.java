package cs324.assignment2.bootstrap;

import cs324.assignment2.grpc.*;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {

    public static final long HEARTBEAT_TIMEOUT_MS = 5000L;
    private static final long CLEANUP_INTERVAL_MS = 1000L;

    private final Map<Integer, WorkerRecord> activeWorkers = new ConcurrentHashMap<>();
    private final AtomicInteger idCounter = new AtomicInteger(1);
    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "bootstrap-cleaner");
        t.setDaemon(true);
        return t;
    });

    public BootstrapServiceImpl() {
        cleaner.scheduleAtFixedRate(this::removeDeadWorkers,
                CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void registerWorker(RegisterWorkerRequest request,
                               StreamObserver<RegisterWorkerResponse> responseObserver) {
        int newId = idCounter.getAndIncrement();
        long now = System.currentTimeMillis();

        WorkerInfo info = WorkerInfo.newBuilder()
                .setId(newId)
                .setHost(request.getHost())
                .setPort(request.getPort())
                .build();

        activeWorkers.put(newId, new WorkerRecord(info, now));
        System.out.println("[Bootstrap] Registered worker " + newId
                + " at " + request.getHost() + ":" + request.getPort());

        responseObserver.onNext(RegisterWorkerResponse.newBuilder()
                .setWorkerId(newId)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deregisterWorker(DeregisterWorkerRequest request,
                                 StreamObserver<DeregisterWorkerResponse> responseObserver) {
        int id = request.getWorkerId();
        if (activeWorkers.remove(id) != null) {
            System.out.println("[Bootstrap] Deregistered worker " + id + " (clean shutdown)");
        }
        responseObserver.onNext(DeregisterWorkerResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getActiveWorkers(GetActiveWorkersRequest request,
                                 StreamObserver<GetActiveWorkersResponse> responseObserver) {
        List<WorkerInfo> list = new ArrayList<>();
        for (WorkerRecord rec : activeWorkers.values()) {
            list.add(rec.info);
        }
        responseObserver.onNext(GetActiveWorkersResponse.newBuilder()
                .addAllWorkers(list)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest request,
                          StreamObserver<HeartbeatResponse> responseObserver) {
        int id = request.getWorkerId();
        WorkerRecord rec = activeWorkers.get(id);
        if (rec != null) {
            rec.lastHeartbeatMs = System.currentTimeMillis();
            responseObserver.onNext(HeartbeatResponse.newBuilder()
                    .setAcknowledged(true)
                    .build());
        } else {
            responseObserver.onNext(HeartbeatResponse.newBuilder()
                    .setAcknowledged(false)
                    .build());
        }
        responseObserver.onCompleted();
    }

    private void removeDeadWorkers() {
        long now = System.currentTimeMillis();
        for (Map.Entry<Integer, WorkerRecord> entry : activeWorkers.entrySet()) {
            WorkerRecord rec = entry.getValue();
            if (now - rec.lastHeartbeatMs > HEARTBEAT_TIMEOUT_MS) {
                if (activeWorkers.remove(entry.getKey(), rec)) {
                    System.out.println("[Bootstrap] Worker " + entry.getKey()
                            + " timed out (no heartbeat for > "
                            + HEARTBEAT_TIMEOUT_MS + " ms) — removed from registry");
                }
            }
        }
    }

    private static class WorkerRecord {
        final WorkerInfo info;
        volatile long lastHeartbeatMs;

        WorkerRecord(WorkerInfo info, long lastHeartbeatMs) {
            this.info = info;
            this.lastHeartbeatMs = lastHeartbeatMs;
        }
    }

    public void shutdown() {
        cleaner.shutdownNow();
    }
}