package cs324.assignment2.worker;

import cs324.assignment2.bootstrap.BootstrapServiceImpl;
import cs324.assignment2.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.*;
import org.junit.jupiter.api.*;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

class IntegratedClusterTest {
    private BootstrapServiceImpl bootstrapService;
    private Server bootstrapServer;
    private ManagedChannel bootstrapChannel;
    private BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;
    private final List<WorkerNode> workers = new ArrayList<>();
    private final List<ManagedChannel> clients = new ArrayList<>();

    @BeforeEach void setup() throws Exception {
        bootstrapService = new BootstrapServiceImpl(1800, 150);
        bootstrapServer = ServerBuilder.forPort(0).addService(bootstrapService).build().start();
        bootstrapChannel = ManagedChannelBuilder.forAddress("localhost", bootstrapServer.getPort()).usePlaintext().build();
        bootstrap = BootstrapServiceGrpc.newBlockingStub(bootstrapChannel);
    }
    @AfterEach void close() {
        workers.forEach(WorkerNode::crash); clients.forEach(ManagedChannel::shutdownNow);
        bootstrapChannel.shutdownNow(); bootstrapServer.shutdownNow(); bootstrapService.shutdown();
    }
    private WorkerNode add(int jac) throws Exception {
        WorkerNode worker = new WorkerNode("localhost", bootstrapServer.getPort(), 0, jac, 400, 200, 150);
        workers.add(worker); worker.start(); return worker;
    }
    private WorkerServiceGrpc.WorkerServiceBlockingStub stub(WorkerNode worker) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", worker.selfInfo().getPort()).usePlaintext().build();
        clients.add(channel); return WorkerServiceGrpc.newBlockingStub(channel).withDeadlineAfter(8, TimeUnit.SECONDS);
    }
    private BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap() {
        return bootstrap.withDeadlineAfter(2, TimeUnit.SECONDS);
    }
    private ClusterView view() { return cs324.assignment2.client.WorkerDiscovery.discover(bootstrap()); }
    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
        while (System.nanoTime() < deadline) {
            try { if (condition.getAsBoolean()) return; } catch (StatusRuntimeException ignored) { }
            Thread.sleep(40);
        }
        fail("Integrated cluster did not reach expected state in 25 seconds");
    }
    private void ready(int leader, int voters) throws Exception {
        await(() -> { ClusterView v = view(); return v.getReady() && v.getCoordinator().getId() == leader && v.getVotersCount() == voters; });
        WorkerNode elected = workers.stream().filter(w -> w.getMyId() == leader).findFirst().orElseThrow();
        await(() -> stubOnce(elected).getWorkerStatus(GetWorkerStatusRequest.getDefaultInstance()).getReady());
    }
    private WorkerServiceGrpc.WorkerServiceBlockingStub stubOnce(WorkerNode worker) {
        // Reuse each worker's pooled local client channel for polling.
        return worker.peer(worker.selfInfo());
    }
    private CheckpointAck save(WorkerNode worker, String id, boolean admit) {
        return stubOnce(worker).saveCheckpoint(SaveCheckpointRequest.newBuilder().setKey(id)
                .setPayload(ByteString.copyFromUtf8("input=1..100;task-A=done;task-B=pending"))
                .setNewJob(admit).setRemoteAssignments(admit ? 2 : 0).build());
    }
    private RecoverySnapshot read(WorkerNode worker) {
        return stub(worker).readRecovery(RecoveryRequest.newBuilder().setEpoch(view().getEpoch()).build());
    }
    @Test void discoversWorkersAndUsesLowestJacThenHighestId() throws Exception {
        add(8); add(2); add(2); ready(3, 3);
        assertEquals(3, bootstrap().getActiveWorkers(GetActiveWorkersRequest.getDefaultInstance()).getWorkersCount());
        for (WorkerNode worker : workers) {
            await(() -> worker.getMyCoordinatorId() == 3);
            assertFalse(worker.getNeighbours().isEmpty());
        }
    }
    @Test void bootstrapExposesOnlyRegistrationDiscoveryAndHeartbeats() {
        var methods = BootstrapServiceGrpc.getServiceDescriptor().getMethods().stream()
                .map(io.grpc.MethodDescriptor::getBareMethodName).collect(java.util.stream.Collectors.toSet());
        assertEquals(Set.of("RegisterWorker", "DeregisterWorker", "GetActiveWorkers", "Heartbeat"), methods);
    }
    @Test void concurrentWorkerTriggersAgreeAndRejectStaleViewVotes() throws Exception {
        add(0); add(0); add(0); ready(3, 3);
        ClusterView old = view();
        var pool = java.util.concurrent.Executors.newFixedThreadPool(3);
        try {
            List<java.util.concurrent.Callable<ClusterView>> triggers = workers.stream()
                    .<java.util.concurrent.Callable<ClusterView>>map(worker -> () -> stubOnce(worker).beginElection(
                            BeginElectionRequest.newBuilder().setWorkerId(worker.getMyId())
                                    .setObservedEpoch(old.getEpoch()).setForce(true).build())).toList();
            for (var result : pool.invokeAll(triggers)) assertEquals(old.getEpoch() + 1, result.get().getEpoch());
        } finally { pool.shutdownNow(); }
        ready(3, 3);
        ClusterView elected = view();
        assertEquals(old.getEpoch() + 1, elected.getEpoch());
        for (WorkerNode worker : workers) {
            var rejected = stubOnce(worker).voteView(ViewVoteRequest.newBuilder().setView(old)
                    .setPhase(ViewPhase.RECOVERED).build());
            assertFalse(rejected.getAccepted());
            assertEquals(elected.getEpoch(), rejected.getCurrent().getEpoch());
            assertEquals(3, rejected.getCurrent().getCoordinator().getId());
        }
    }
    @Test void coordinatorCrashRecoversAndBootstrapEvictsIt() throws Exception {
        add(0); add(0); WorkerNode leader = add(0); ready(3, 3);
        assertTrue(save(leader, "job-42", false).getAccepted());
        leader.crash(); ready(2, 2);
        assertEquals(2, bootstrap().getActiveWorkers(GetActiveWorkersRequest.getDefaultInstance()).getWorkersCount());
        assertTrue(read(workers.get(1)).getRecordsList().stream().anyMatch(r -> r.getKey().equals("job-42") && r.getPayload().toStringUtf8().contains("task-B=pending")));
    }
    @Test void joiningWorkerTriggersSafeReplicaTransition() throws Exception {
        add(0); add(0); WorkerNode oldLeader = add(0); ready(3, 3);
        assertTrue(save(oldLeader, "before-join", false).getAccepted());
        WorkerNode joined = add(0); ready(4, 4);
        assertTrue(read(joined).getRecordsList().stream().anyMatch(r -> r.getKey().equals("before-join")));
        assertTrue(save(joined, "after-join", false).getAccepted());
        joined.crash(); ready(3, 3);
        assertEquals(2, read(oldLeader).getRecordsCount());
    }
    @Test void admissionDeduplicatesAndFifthJobRotates() throws Exception {
        add(0); add(0); WorkerNode leader = add(0); ready(3, 3);
        assertTrue(save(leader, "j1", true).getAccepted());
        assertTrue(save(leader, "j1", true).getAccepted());
        assertEquals(1, stub(leader).getWorkerStatus(GetWorkerStatusRequest.getDefaultInstance()).getJobsThisTerm());
        for (int i = 2; i <= 5; i++) assertTrue(save(leader, "j" + i, true).getAccepted());
        ready(2, 3);
        assertEquals(10, leader.getJac().get());
        assertEquals(5, read(workers.get(1)).getRecordsCount());
        assertTrue(save(workers.get(1), "j1", true).getAccepted());
        assertEquals(0, stub(workers.get(1)).getWorkerStatus(GetWorkerStatusRequest.getDefaultInstance()).getJobsThisTerm());
    }
    @Test void missingCandidateCannotBeOmittedByACollectionTimer() throws Exception {
        add(0); add(0); add(0); ready(3, 3);
        try (ServerSocket unused = new ServerSocket(0)) {
            bootstrap().registerWorker(RegisterWorkerRequest.newBuilder().setHost("localhost").setPort(unused.getLocalPort()).build());
            // The endpoint is not a gRPC worker and sends no candidacy or heartbeat.
            Thread.sleep(350);
            assertFalse(view().getReady(), "A round cannot elect before all configured candidates report");
        }
        ready(3, 3);
    }
    @Test void initiatorCrashRetriesAfterMembershipRepair() throws Exception {
        WorkerNode initiator = add(0); add(0); add(0); ready(3, 3);
        stubOnce(initiator).beginElection(BeginElectionRequest.newBuilder().setWorkerId(1).setObservedEpoch(view().getEpoch()).setForce(true).build());
        initiator.crash(); ready(3, 2);
        assertTrue(save(workers.get(2), "continued", false).getAccepted());
    }
    @Test void losingPreviousMajorityDoesNotSilentlyDiscardJobs() throws Exception {
        add(0); add(0); WorkerNode leader = add(0); ready(3, 3);
        assertTrue(save(leader, "retained", false).getAccepted());
        workers.get(0).crash(); workers.get(1).crash();
        await(() -> bootstrap().getActiveWorkers(GetActiveWorkersRequest.getDefaultInstance()).getWorkersCount() == 1);
        Thread.sleep(700);
        assertFalse(view().getReady());
        StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, () -> save(leader, "unsafe", false));
        assertEquals(Status.Code.FAILED_PRECONDITION, failure.getStatus().getCode());
    }
    @Test void rejectsOldEpochMessagesAndExecutesRangeTasks() throws Exception {
        WorkerNode worker = add(0); ready(1, 1);
        var client = stub(worker);
        long epoch = view().getEpoch();
        client.receiveCoordinatorMessage(CoordinatorMessage.newBuilder().setEpoch(epoch - 1).setElectionId("old").setCoordinator(worker.selfInfo()).build());
        assertEquals(epoch, client.getWorkerStatus(GetWorkerStatusRequest.getDefaultInstance()).getEpoch());
        assertEquals(55, client.executeJob(ExecuteJobRequest.newBuilder().setJobType("SUM").setStart(1).setEnd(10).build()).getResult());
    }
    @Test void bootstrapLossStopsCoordinatorAdmission() throws Exception {
        WorkerNode worker = add(0); ready(1, 1);
        var client = stub(worker);
        bootstrapServer.shutdownNow(); bootstrapService.shutdown();
        await(() -> !client.getWorkerStatus(GetWorkerStatusRequest.getDefaultInstance()).getReady());
        assertThrows(StatusRuntimeException.class, () -> client.saveCheckpoint(SaveCheckpointRequest.newBuilder().setKey("x").build()));
    }
    @Test void actualAllocationHookCountsRetriesOnce() throws Exception {
        WorkerNode worker = add(0); ready(1, 1);
        worker.recordRemoteAssignment("job/task/attempt-1");
        worker.recordRemoteAssignment("job/task/attempt-1");
        worker.recordRemoteAssignment("job/task/attempt-2");
        assertEquals(2, worker.getJac().get());
        assertEquals(0, stub(worker).getWorkerStatus(GetWorkerStatusRequest.getDefaultInstance()).getJobsThisTerm());
    }
    @Test void concurrentListJobsPreserveInputsAndResultsAcrossCoordinatorChange() throws Exception {
        add(0); add(0); WorkerNode leader = add(0); ready(3, 3);
        var maximum = cs324.assignment2.job.grpc.SubmitJobRequest.newBuilder().setJobType("MAX")
                .setRequestId("negative-list-max").addAllValues(List.of(-9L, -4L, -21L, -3L, -21L)).build();
        var count = cs324.assignment2.job.grpc.SubmitJobRequest.newBuilder().setJobType("PRIMECOUNT")
                .setRequestId("duplicate-primes").addAllValues(List.of(2L, 3L, 3L, 4L, 5L, 5L, 5L, -7L)).build();
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var maxResult = pool.submit(() -> cs324.assignment2.client.ResilientJobClient.run(
                    "localhost", bootstrapServer.getPort(), maximum, update -> {}, 20000));
            var countResult = pool.submit(() -> cs324.assignment2.client.ResilientJobClient.run(
                    "localhost", bootstrapServer.getPort(), count, update -> {}, 20000));
            assertEquals(-3, maxResult.get(25, TimeUnit.SECONDS).getResult());
            assertEquals(6, countResult.get(25, TimeUnit.SECONDS).getResult());
            long oldEpoch = view().getEpoch();
            leader.crash(); ready(2, 2);
            assertTrue(view().getEpoch() > oldEpoch);
            for (var request : List.of(maximum, count)) {
                var repeated = cs324.assignment2.client.ResilientJobClient.run("localhost", bootstrapServer.getPort(),
                        request, update -> {}, 20000);
                assertEquals(request.getRequestId(), repeated.getJobId());
                assertEquals(request.getJobType().equals("MAX") ? -3 : 6, repeated.getResult());
                var record = read(workers.get(1)).getRecordsList().stream()
                        .filter(r -> r.getKey().equals("job/" + request.getRequestId())).findFirst().orElseThrow();
                var saved = cs324.assignment2.job.grpc.PersistedJob.parseFrom(record.getPayload());
                assertEquals(request, saved.getOriginalRequest());
                assertEquals(request.getValuesList(), saved.getTasksList().stream()
                        .flatMap(t -> t.getRequest().getValuesList().stream()).toList());
            }
        } finally { pool.shutdownNow(); }
    }
    @Test void realPrimeJobResumesAfterCoordinatorCrashWithoutLosingCompletedTasks() throws Exception {
        add(0); add(0); WorkerNode oldLeader = add(0); ready(3, 3);
        ManagedChannel oldChannel = ManagedChannelBuilder.forAddress("localhost", oldLeader.selfInfo().getPort()).usePlaintext().build();
        clients.add(oldChannel);
        var request = cs324.assignment2.job.grpc.SubmitJobRequest.newBuilder().setJobType("PRIMESUM")
                .setRequestId("recover-real-primes").putParameters("start", "1").putParameters("end", "2000000").build();
        var accepted = cs324.assignment2.job.grpc.JobServiceGrpc.newBlockingStub(oldChannel).withDeadlineAfter(8, TimeUnit.SECONDS).submitJob(request);
        var killed = new java.util.concurrent.atomic.AtomicBoolean();
        var completedIds = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
        cs324.assignment2.job.grpc.JobServiceGrpc.newStub(oldChannel).withDeadlineAfter(20, TimeUnit.SECONDS)
                .streamJobProgress(cs324.assignment2.job.grpc.StreamJobProgressRequest.newBuilder().setJobId(accepted.getJobId()).build(),
                        new io.grpc.stub.StreamObserver<cs324.assignment2.job.grpc.JobProgressUpdate>() {
                            @Override public void onNext(cs324.assignment2.job.grpc.JobProgressUpdate update) {
                                var done = update.getSubTasksList().stream().filter(t -> t.getState() == cs324.assignment2.job.grpc.SubTaskState.SUB_TASK_STATE_COMPLETED).toList();
                                if (!done.isEmpty() && update.getStatus() != cs324.assignment2.job.grpc.JobStatus.JOB_STATUS_COMPLETED && killed.compareAndSet(false, true)) {
                                    done.forEach(t -> completedIds.add(t.getSubTaskId()));
                                    new Thread(oldLeader::crash).start();
                                }
                            }
                            @Override public void onError(Throwable error) { }
                            @Override public void onCompleted() { }
                        });
        await(killed::get); ready(2, 2);
        WorkerNode replacement = workers.get(1);
        ManagedChannel nextChannel = ManagedChannelBuilder.forAddress("localhost", replacement.selfInfo().getPort()).usePlaintext().build();
        clients.add(nextChannel);
        var next = cs324.assignment2.job.grpc.JobServiceGrpc.newBlockingStub(nextChannel);
        var resumed = next.withDeadlineAfter(8, TimeUnit.SECONDS).submitJob(request);
        assertEquals(accepted.getJobId(), resumed.getJobId());
        var progress = next.withDeadlineAfter(25, TimeUnit.SECONDS).streamJobProgress(
                cs324.assignment2.job.grpc.StreamJobProgressRequest.newBuilder().setJobId(accepted.getJobId()).build());
        cs324.assignment2.job.grpc.JobProgressUpdate terminal = null;
        while (progress.hasNext()) terminal = progress.next();
        assertNotNull(terminal);
        assertEquals(cs324.assignment2.job.grpc.JobStatus.JOB_STATUS_COMPLETED, terminal.getStatus());
        assertEquals(142913828922L, terminal.getResult());
        var record = read(replacement).getRecordsList().stream().filter(r -> r.getKey().equals("job/" + accepted.getJobId())).findFirst().orElseThrow();
        var saved = cs324.assignment2.job.grpc.PersistedJob.parseFrom(record.getPayload());
        assertTrue(saved.getTasksList().stream().filter(t -> t.getState() == cs324.assignment2.job.grpc.SubTaskState.SUB_TASK_STATE_COMPLETED)
                .map(t -> t.getRequest().getTaskId()).collect(java.util.stream.Collectors.toSet()).containsAll(completedIds));
        assertEquals(0, stub(replacement).getWorkerStatus(GetWorkerStatusRequest.getDefaultInstance()).getJobsThisTerm());
    }
}
