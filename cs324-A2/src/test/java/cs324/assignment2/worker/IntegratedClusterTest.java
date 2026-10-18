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
        bootstrapService = new BootstrapServiceImpl(1800, 150, 6000);
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
    private ClusterView view() { return bootstrap().getClusterView(GetClusterViewRequest.getDefaultInstance()); }
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
        bootstrap().beginElection(BeginElectionRequest.newBuilder().setWorkerId(1).setObservedEpoch(view().getEpoch()).setForce(true).build());
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
    @Test void rejectsOldEpochMessagesAndReportsJobsAsUnimplemented() throws Exception {
        WorkerNode worker = add(0); ready(1, 1);
        var client = stub(worker);
        long epoch = view().getEpoch();
        client.receiveCoordinatorMessage(CoordinatorMessage.newBuilder().setEpoch(epoch - 1).setElectionId("old").setCoordinator(worker.selfInfo()).build());
        assertEquals(epoch, client.getWorkerStatus(GetWorkerStatusRequest.getDefaultInstance()).getEpoch());
        assertEquals(Status.Code.UNIMPLEMENTED, assertThrows(StatusRuntimeException.class,
                () -> client.executeJob(ExecuteJobRequest.getDefaultInstance())).getStatus().getCode());
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
}
