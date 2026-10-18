package cs324.assignment2.worker;

import cs324.assignment2.grpc.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

/** Joshua's registration/neighbour service with integrated election and recovery.
 * Each instance owns one gRPC server; job RPCs remain explicit integration hooks. */
public class WorkerNode extends WorkerServiceGrpc.WorkerServiceImplBase implements AutoCloseable {
    private final String leaderman = "cs324";
    private final String bootstrapHost;
    private final int bootstrapPort, requestedPort;
    private final long rpcMs, heartbeatMs, monitorMs;
    private volatile int id = -1, port = -1;
    private volatile boolean stopped, registrationLost;
    private final List<WorkerInfo> neighbours = new CopyOnWriteArrayList<>();
    private final AtomicInteger jac = new AtomicInteger();
    private final AtomicBoolean electionInProgress = new AtomicBoolean();
    private final ConcurrentMap<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final ExecutorService floodPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ManagedChannel bootstrapChannel;
    private Server server;
    private volatile ElectionRecovery election;

    public WorkerNode() { this("localhost", 50051, 0, 0); }
    public WorkerNode(String bootstrapHost, int bootstrapPort, int listenPort, int initialJac) {
        this(bootstrapHost, bootstrapPort, listenPort, initialJac, 600, 1500, 400);
    }
    public WorkerNode(String bootstrapHost, int bootstrapPort, int listenPort, int initialJac,
                      long rpcMs, long heartbeatMs, long monitorMs) {
        if (initialJac < 0) throw new IllegalArgumentException("JAC must be nonnegative");
        this.bootstrapHost = bootstrapHost; this.bootstrapPort = bootstrapPort;
        this.requestedPort = listenPort; this.rpcMs = rpcMs;
        this.heartbeatMs = heartbeatMs; this.monitorMs = monitorMs; jac.set(initialJac);
    }
    public static void main(String[] args) throws Exception {
        WorkerNode node = new WorkerNode(args.length > 0 ? args[0] : "localhost",
                args.length > 1 ? Integer.parseInt(args[1]) : 50051,
                args.length > 2 ? Integer.parseInt(args[2]) : 0,
                args.length > 3 ? Integer.parseInt(args[3]) : 0);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close));
        try { node.start(); node.awaitTermination(); }
        finally { node.close(); }
    }
    public void start() throws Exception {
        try {
            bootstrapChannel = ManagedChannelBuilder.forAddress(bootstrapHost, bootstrapPort).usePlaintext().build();
            server = ServerBuilder.forPort(requestedPort).addService(this).build().start();
            port = server.getPort();
            id = bootstrap().registerWorker(RegisterWorkerRequest.newBuilder().setHost("localhost").setPort(port).build()).getWorkerId();
            election = new ElectionRecovery(this);
            log("Registered; gRPC port=" + port + "; leaderman=" + leaderman);
            scheduler.scheduleWithFixedDelay(this::sendHeartbeat, 0, heartbeatMs, TimeUnit.MILLISECONDS);
            joinNetwork();
            scheduler.scheduleWithFixedDelay(() -> {
                try { if (!stopped && !registrationLost) election.tick(); }
                catch (Exception e) { election.unavailable(); log("monitor: " + e.getMessage()); }
            }, 100, monitorMs, TimeUnit.MILLISECONDS);
        } catch (Exception failure) { close(); throw failure; }
    }
    public void awaitTermination() throws InterruptedException { server.awaitTermination(); }
    BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap() {
        return BootstrapServiceGrpc.newBlockingStub(bootstrapChannel).withDeadlineAfter(rpcMs, TimeUnit.MILLISECONDS);
    }
    WorkerServiceGrpc.WorkerServiceBlockingStub peer(WorkerInfo worker) {
        ManagedChannel channel = channels.computeIfAbsent(worker.getId(), ignored ->
                ManagedChannelBuilder.forAddress(worker.getHost(), worker.getPort()).usePlaintext().build());
        return WorkerServiceGrpc.newBlockingStub(channel).withDeadlineAfter(rpcMs, TimeUnit.MILLISECONDS);
    }
    private void sendHeartbeat() {
        try {
            if (!bootstrap().heartbeat(HeartbeatRequest.newBuilder().setWorkerId(id).build()).getAcknowledged()) {
                registrationLost = true; election.unavailable();
                log("Registration expired; stop this process and rejoin with a new worker ID");
            }
        } catch (StatusRuntimeException failure) { election.unavailable(); log("Heartbeat unavailable"); }
    }
    private void joinNetwork() {
        List<WorkerInfo> others = new ArrayList<>(getActiveWorkersFromBootstrap());
        Collections.shuffle(others);
        for (WorkerInfo worker : others) if (worker.getId() != id) {
            try {
                peer(worker).addNeighbour(AddNeighbourRequest.newBuilder().setNeighbour(selfInfo()).build());
                addNeighbourLocal(worker); log("Connected to neighbour " + worker.getId()); return;
            } catch (StatusRuntimeException ignored) { }
        }
    }
    // A failed articulation node can split the random overlay. Add a reciprocal
    // ring over the current voters while preserving existing random neighbours.
    void repairOverlay(List<WorkerInfo> voters) {
        List<WorkerInfo> sorted = voters.stream().sorted(Comparator.comparingInt(WorkerInfo::getId)).toList();
        neighbours.removeIf(w -> sorted.stream().noneMatch(active -> active.getId() == w.getId()));
        int index = -1;
        for (int i = 0; i < sorted.size(); i++) if (sorted.get(i).getId() == id) index = i;
        if (index < 0 || sorted.size() < 2) return;
        Set<WorkerInfo> adjacent = new HashSet<>(List.of(sorted.get((index + 1) % sorted.size()),
                sorted.get((index + sorted.size() - 1) % sorted.size())));
        for (WorkerInfo worker : adjacent) try {
            peer(worker).addNeighbour(AddNeighbourRequest.newBuilder().setNeighbour(selfInfo()).build());
            addNeighbourLocal(worker);
        } catch (StatusRuntimeException ignored) { }
    }
    private void addNeighbourLocal(WorkerInfo worker) {
        if (worker.getId() > 0 && worker.getId() != id && neighbours.stream().noneMatch(w -> w.getId() == worker.getId()))
            neighbours.add(worker);
    }
    public WorkerInfo selfInfo() { return WorkerInfo.newBuilder().setId(id).setHost("localhost").setPort(port).build(); }
    boolean stopped() { return stopped || registrationLost; }
    void log(String message) { System.out.println("[Worker " + id + "] " + message); }
    void dispatch(Runnable operation) {
        if (!stopped) try { floodPool.execute(operation); } catch (RejectedExecutionException ignored) { }
    }
    private ElectionRecovery controller() {
        if (election == null || stopped()) throw Status.UNAVAILABLE.withDescription("Worker not registered/ready").asRuntimeException();
        return election;
    }
    private static <T> void respond(StreamObserver<T> out, Supplier<T> action) {
        try { out.onNext(action.get()); out.onCompleted(); }
        catch (StatusRuntimeException error) { out.onError(error); }
        catch (IllegalArgumentException error) { out.onError(Status.INVALID_ARGUMENT.withDescription(error.getMessage()).asRuntimeException()); }
        catch (IllegalStateException error) { out.onError(Status.FAILED_PRECONDITION.withDescription(error.getMessage()).asRuntimeException()); }
    }
    @Override public void addNeighbour(AddNeighbourRequest req, StreamObserver<AddNeighbourResponse> out) {
        addNeighbourLocal(req.getNeighbour()); out.onNext(AddNeighbourResponse.getDefaultInstance()); out.onCompleted();
    }
    @Override public void getId(GetIdRequest req, StreamObserver<GetIdResponse> out) {
        respond(out, () -> GetIdResponse.newBuilder().setId(id).build());
    }
    @Override public void getCoordinatorId(GetCoordinatorIdRequest req, StreamObserver<GetCoordinatorIdResponse> out) {
        respond(out, () -> GetCoordinatorIdResponse.newBuilder().setCoordinatorId(controller().coordinatorId()).build());
    }
    @Override public void getJac(GetJacRequest req, StreamObserver<GetJacResponse> out) {
        respond(out, () -> GetJacResponse.newBuilder().setJac(jac.get()).build());
    }
    @Override public void receiveElectionMessage(ElectionMessage req, StreamObserver<Empty> out) {
        respond(out, () -> { controller().receiveElection(req); return Empty.getDefaultInstance(); });
    }
    @Override public void reportCandidacy(CandidacyReport req, StreamObserver<Empty> out) {
        respond(out, () -> { controller().candidate(req); return Empty.getDefaultInstance(); });
    }
    @Override public void receiveCoordinatorMessage(CoordinatorMessage req, StreamObserver<Empty> out) {
        respond(out, () -> { controller().announcement(req); return Empty.getDefaultInstance(); });
    }
    @Override public void getWorkerStatus(GetWorkerStatusRequest req, StreamObserver<WorkerStatus> out) {
        respond(out, () -> controller().status());
    }
    @Override public void saveCheckpoint(SaveCheckpointRequest req, StreamObserver<CheckpointAck> out) {
        respond(out, () -> controller().checkpoint(req));
    }
    @Override public void readRecovery(RecoveryRequest req, StreamObserver<RecoverySnapshot> out) {
        respond(out, () -> controller().readRecovery(req.getEpoch()));
    }
    @Override public void replicateRecord(RecoveryRecord req, StreamObserver<CheckpointAck> out) {
        respond(out, () -> controller().replicate(req));
    }
    @Override public void executeJob(ExecuteJobRequest req, StreamObserver<ExecuteJobResponse> out) {
        // Aveshal's integration hook: never report a fabricated successful result.
        out.onError(Status.UNIMPLEMENTED.withDescription("Job execution awaits job-engine integration").asRuntimeException());
    }
    @Override public void submitJob(SubmitJobRequest req, StreamObserver<JobProgressUpdate> out) {
        out.onError(Status.UNIMPLEMENTED.withDescription("Job submission/progress awaits job-engine integration").asRuntimeException());
    }
    /** Job-engine hook: call with a stable ID and full input/task/result payload.
     * A repeated admitted ID returns its existing state without counting a new job. */
    public CheckpointAck checkpointJob(SaveCheckpointRequest request) { return controller().checkpoint(request); }
    /** Call for each actual allocation/reassignment to another worker. Use stable
     * job/task/attempt IDs; set admission remote_assignments=0 when using this. */
    public void recordRemoteAssignment(String allocationId) { controller().recordRemoteAssignment(allocationId); }
    public RecoverySnapshot recoveredJobs() { return controller().snapshot(); }
    public List<WorkerInfo> getActiveWorkersFromBootstrap() {
        return bootstrap().getActiveWorkers(GetActiveWorkersRequest.getDefaultInstance()).getWorkersList();
    }
    public int getMyId() { return id; }
    public int getMyCoordinatorId() { return controller().coordinatorId(); }
    public List<WorkerInfo> getNeighbours() { return neighbours; }
    public AtomicInteger getJac() { return jac; }
    public AtomicBoolean getElectionInProgress() { return electionInProgress; }
    public ExecutorService getFloodPool() { return floodPool; }
    public void crash() { stop(false); }
    @Override public void close() { stop(true); }
    private synchronized void stop(boolean deregister) {
        if (stopped) return;
        stopped = true;
        scheduler.shutdownNow();
        if (election != null) election.unavailable();
        if (deregister && id > 0) try { bootstrap().deregisterWorker(DeregisterWorkerRequest.newBuilder().setWorkerId(id).build()); }
        catch (StatusRuntimeException ignored) { }
        if (server != null) server.shutdownNow();
        floodPool.shutdownNow(); channels.values().forEach(ManagedChannel::shutdownNow);
        if (bootstrapChannel != null) bootstrapChannel.shutdownNow();
    }
}
