package cs324.assignment2.worker;

import cs324.assignment2.grpc.ExecuteJobProgress;
import cs324.assignment2.grpc.ExecuteJobRequest;
import cs324.assignment2.grpc.WorkerInfo;
import cs324.assignment2.grpc.WorkerServiceGrpc;
import cs324.assignment2.job.grpc.JobProgressUpdate;
import cs324.assignment2.job.grpc.JobServiceGrpc;
import cs324.assignment2.job.grpc.JobStatus;
import cs324.assignment2.job.grpc.StreamJobProgressRequest;
import cs324.assignment2.job.grpc.SubTaskState;
import cs324.assignment2.job.grpc.SubmitJobRequest;
import cs324.assignment2.job.grpc.SubmitJobResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JobGrpcServiceImplTest {

    private static final int COORDINATOR_ID = 1;

    private final List<Server> workerServers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final List<WorkerInfo> activeWorkers = new ArrayList<>();

    private WorkerNode coordinatorWorker;
    private JobGrpcServiceImpl jobService;
    private Server coordinatorServer;
    private JobServiceGrpc.JobServiceBlockingStub jobStub;

    @BeforeEach
    void setUp() throws Exception {
        coordinatorWorker = Mockito.mock(WorkerNode.class);
        when(coordinatorWorker.isReadyCoordinator()).thenReturn(true);
        when(coordinatorWorker.recoveredJobs()).thenReturn(cs324.assignment2.grpc.RecoverySnapshot.getDefaultInstance());
        when(coordinatorWorker.checkpointJob(Mockito.any())).thenReturn(cs324.assignment2.grpc.CheckpointAck.newBuilder().setAccepted(true).build());
        when(coordinatorWorker.getMyId()).thenReturn(COORDINATOR_ID);
        when(coordinatorWorker.getMyCoordinatorId()).thenReturn(COORDINATOR_ID);
        when(coordinatorWorker.getActiveWorkersFromBootstrap())
                .thenAnswer(invocation -> List.copyOf(activeWorkers));

        jobService = new JobGrpcServiceImpl(coordinatorWorker);
        ProgressWorker coordinatorExecutor = new ProgressWorker(false, false);
        coordinatorServer = ServerBuilder.forPort(0)
                .addService(coordinatorExecutor)
                .addService(jobService)
                .build()
                .start();
        workerServers.add(coordinatorServer);
        activeWorkers.add(workerInfo(COORDINATOR_ID, coordinatorServer));

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", coordinatorServer.getPort())
                .usePlaintext()
                .build();
        channels.add(channel);
        jobStub = JobServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(20, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        for (ManagedChannel channel : channels) {
            channel.shutdownNow();
        }
        jobService.close();
        for (Server server : workerServers) {
            server.shutdownNow();
        }
    }

    @Test
    @Timeout(30)
    void submitJobReturnsIdAndStreamsSuccessfulExecution() {
        var response = jobStub.submitJob(jobRequest("SUM", 1, 5));

        assertFalse(response.getJobId().isBlank());
        assertEquals(JobStatus.JOB_STATUS_QUEUED, response.getStatus());

        List<JobProgressUpdate> updates = readProgress(response.getJobId());
        JobProgressUpdate terminal = updates.get(updates.size() - 1);

        assertEquals(JobStatus.JOB_STATUS_COMPLETED, terminal.getStatus());
        assertEquals(15L, terminal.getResult());
        assertEquals(5, terminal.getSubTasksCount());
        assertTrue(terminal.getSubTasksList().stream()
                .allMatch(task -> task.getState() == SubTaskState.SUB_TASK_STATE_COMPLETED));
    }

    @Test
    @Timeout(30)
    void maxAndPrimeCountHandleUnsortedValuesAndPreserveDuplicates() throws Exception {
        var maxResponse = jobStub.submitJob(valueJobRequest("MAX", 9, -4, 21, 3, 21));
        var primeCountResponse = jobStub.submitJob(
                valueJobRequest("PRIMECOUNT", 2, 3, 3, 4, 5, 5, 5));

        JobProgressUpdate maxResult = lastProgress(maxResponse.getJobId());
        JobProgressUpdate primeCountResult = lastProgress(primeCountResponse.getJobId());

        assertEquals(JobStatus.JOB_STATUS_COMPLETED, maxResult.getStatus());
        assertEquals(21L, maxResult.getResult());
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, primeCountResult.getStatus());
        assertEquals(6L, primeCountResult.getResult());
    }

    @Test
    @Timeout(15)
    void workerProcessesUnsortedMaxAndDuplicatePrimeValues() throws Exception {
        WorkerNode worker = new WorkerNode();
        Server workerServer = ServerBuilder.forPort(0).addService(worker).build().start();
        workerServers.add(workerServer);
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", workerServer.getPort())
                .usePlaintext()
                .build();
        channels.add(channel);
        WorkerServiceGrpc.WorkerServiceBlockingStub workerStub = WorkerServiceGrpc
                .newBlockingStub(channel)
                .withDeadlineAfter(5, TimeUnit.SECONDS);

        ExecuteJobProgress maxResult = lastWorkerProgress(workerStub,
                workerRequest("MAX", 19, -2, 7, 31, 7));
        ExecuteJobProgress primeCountResult = lastWorkerProgress(workerStub,
                workerRequest("PRIMECOUNT", 2, 3, 3, 4, 5, 5, 5));

        assertEquals(31L, maxResult.getResult());
        assertEquals(6L, primeCountResult.getResult());
    }

    @Test
    void rejectsSubmissionWhenThisWorkerIsNotCoordinator() {
        WorkerNode nonCoordinator = Mockito.mock(WorkerNode.class);
        when(nonCoordinator.getMyId()).thenReturn(2);
        when(nonCoordinator.getMyCoordinatorId()).thenReturn(COORDINATOR_ID);
        JobGrpcServiceImpl service = new JobGrpcServiceImpl(nonCoordinator);
        @SuppressWarnings("unchecked")
        StreamObserver<SubmitJobResponse> observer = mock(StreamObserver.class);

        try {
            service.submitJob(jobRequest("SUM", 1, 10), observer);

            ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
            verify(observer).onError(error.capture());
            assertEquals(Status.Code.FAILED_PRECONDITION,
                    Status.fromThrowable(error.getValue()).getCode());
        } finally {
            service.close();
        }
    }

    @Test
    void stableRequestIdCannotBeReusedForDifferentInputs() {
        var first = jobRequest("SUM", 1, 5).toBuilder().setRequestId("stable-request").build();
        assertEquals("stable-request", jobStub.submitJob(first).getJobId());
        assertEquals("stable-request", jobStub.submitJob(first).getJobId());
        var changed = jobRequest("SUM", 1, 10).toBuilder().setRequestId("stable-request").build();
        var error = org.junit.jupiter.api.Assertions.assertThrows(io.grpc.StatusRuntimeException.class, () -> jobStub.submitJob(changed));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    @Timeout(30)
    void progressStreamReportsMonotonicMeasuredCompletionUntilDone() throws Exception {
        ProgressWorker coordinatorExecutor = new ProgressWorker(false, true);
        replaceCoordinatorExecutor(coordinatorExecutor);

        var response = jobStub.submitJob(jobRequest("SUM", 1, 4));
        assertTrue(coordinatorExecutor.awaitFirstTask(5, TimeUnit.SECONDS),
                "Coordinator worker did not start task execution");

        Iterator<JobProgressUpdate> stream = jobStub.streamJobProgress(
                StreamJobProgressRequest.newBuilder().setJobId(response.getJobId()).build());
        assertTrue(stream.hasNext());
        JobProgressUpdate initial = stream.next();
        assertEquals(0.0, initial.getCompletionPercentage());

        coordinatorExecutor.releaseFirstTask();
        List<JobProgressUpdate> updates = new ArrayList<>();
        updates.add(initial);
        while (stream.hasNext()) {
            updates.add(stream.next());
        }

        double previous = -1.0;
        boolean sawIntermediateProgress = false;
        for (JobProgressUpdate update : updates) {
            double percentage = update.getCompletionPercentage();
            assertTrue(percentage >= previous, "Progress percentage must not decrease");
            assertTrue(percentage >= 0.0 && percentage <= 100.0);
            sawIntermediateProgress |= percentage > 0.0 && percentage < 100.0;
            previous = percentage;
        }

        JobProgressUpdate terminal = updates.get(updates.size() - 1);
        assertTrue(sawIntermediateProgress, "Expected updates before task completion");
        assertEquals(100.0, terminal.getCompletionPercentage());
        assertEquals(10L, terminal.getResult());
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, terminal.getStatus());
    }

    @Test
    @Timeout(30)
    void failedWorkerTaskIsReassignedAndJobCompletes() throws Exception {
        ProgressWorker failingExecutor = new ProgressWorker(true, false);
        Server failingServer = ServerBuilder.forPort(0)
                .addService(failingExecutor)
                .build()
                .start();
        workerServers.add(failingServer);
        activeWorkers.add(workerInfo(2, failingServer));

        var response = jobStub.submitJob(jobRequest("SUM", 1, 40));
        List<JobProgressUpdate> updates = readProgress(response.getJobId());
        JobProgressUpdate terminal = updates.get(updates.size() - 1);

        assertEquals(1, failingExecutor.getCalls(),
                "The failed worker should be removed from this job's retry candidates");
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, terminal.getStatus());
        assertEquals(820L, terminal.getResult());
        assertEquals(40, terminal.getSubTasksCount());
        assertTrue(terminal.getSubTasksList().stream()
                .allMatch(task -> task.getState() == SubTaskState.SUB_TASK_STATE_COMPLETED));
        assertTrue(updates.stream().anyMatch(update ->
                        update.getMessage().contains("retrying unfinished sub-task")),
                "The progress stream should report the failed-worker retry");
    }

    @Test @Timeout(10)
    void progressingTaskMayRunLongerThanTheIdleTimeout() throws Exception {
        jobService.close();
        jobService = new JobGrpcServiceImpl(coordinatorWorker, 200, 800);
        replaceCoordinatorExecutor(new WorkerServiceGrpc.WorkerServiceImplBase() {
            @Override public void executeJobWithProgress(ExecuteJobRequest request, StreamObserver<ExecuteJobProgress> out) {
                try {
                    for (int step = 1; step <= 15; step++) {
                        Thread.sleep(100);
                        out.onNext(ExecuteJobProgress.newBuilder().setTaskId(request.getTaskId())
                                .setCompletionPercentage(step * 100.0 / 15).setResult(1).setIsFinished(step == 15).build());
                    }
                    out.onCompleted();
                } catch (InterruptedException error) { Thread.currentThread().interrupt(); out.onError(error); }
            }
        });
        var accepted = jobStub.submitJob(jobRequest("SUM", 1, 1));
        assertEquals(1, lastProgress(accepted.getJobId()).getResult());
    }
    @Test @Timeout(10)
    void stalledStreamIsReportedSlowThenReassigned() throws Exception {
        jobService.close();
        jobService = new JobGrpcServiceImpl(coordinatorWorker, 100, 500);
        replaceCoordinatorExecutor(new ProgressWorker(false, false));
        Server stalled = ServerBuilder.forPort(0).addService(new WorkerServiceGrpc.WorkerServiceImplBase() {
            @Override public void executeJobWithProgress(ExecuteJobRequest request, StreamObserver<ExecuteJobProgress> out) {
                out.onNext(ExecuteJobProgress.newBuilder().setTaskId(request.getTaskId()).setCompletionPercentage(0).build());
                // Keep transport open without advancing work, as a stalled computation would.
            }
        }).build().start();
        workerServers.add(stalled); activeWorkers.add(workerInfo(2, stalled));
        var accepted = jobStub.submitJob(jobRequest("SUM", 1, 40));
        var updates = readProgress(accepted.getJobId());
        assertTrue(updates.stream().anyMatch(u -> u.getWorkersList().stream()
                .anyMatch(w -> w.getStatus() == cs324.assignment2.job.grpc.WorkerStatus.WORKER_STATUS_SLOW)));
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, updates.getLast().getStatus());
        assertEquals(820, updates.getLast().getResult());
    }
    private void replaceCoordinatorExecutor(WorkerServiceGrpc.WorkerServiceImplBase replacement) throws Exception {
        coordinatorServer.shutdownNow();
        coordinatorServer.awaitTermination();
        workerServers.remove(coordinatorServer);
        coordinatorServer = ServerBuilder.forPort(0)
                .addService(replacement)
                .addService(jobService)
                .build()
                .start();
        workerServers.add(coordinatorServer);

        activeWorkers.clear();
        activeWorkers.add(workerInfo(COORDINATOR_ID, coordinatorServer));

        channels.forEach(ManagedChannel::shutdownNow);
        channels.clear();
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", coordinatorServer.getPort())
                .usePlaintext()
                .build();
        channels.add(channel);
        jobStub = JobServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(20, TimeUnit.SECONDS);
    }

    private List<JobProgressUpdate> readProgress(String jobId) {
        Iterator<JobProgressUpdate> stream = jobStub.streamJobProgress(
                StreamJobProgressRequest.newBuilder().setJobId(jobId).build());
        List<JobProgressUpdate> updates = new ArrayList<>();
        while (stream.hasNext()) {
            updates.add(stream.next());
        }
        assertFalse(updates.isEmpty(), "The server should send at least one progress update");
        return updates;
    }

    private JobProgressUpdate lastProgress(String jobId) {
        List<JobProgressUpdate> updates = readProgress(jobId);
        return updates.get(updates.size() - 1);
    }

    private static SubmitJobRequest jobRequest(String type, int start, int end) {
        return SubmitJobRequest.newBuilder()
                .setJobType(type)
                .putParameters("start", Integer.toString(start))
                .putParameters("end", Integer.toString(end))
                .build();
    }

    private static SubmitJobRequest valueJobRequest(String type, long... values) {
        SubmitJobRequest.Builder request = SubmitJobRequest.newBuilder().setJobType(type);
        for (long value : values) {
            request.addValues(value);
        }
        return request.build();
    }

    private static cs324.assignment2.grpc.ExecuteJobRequest workerRequest(
            String type, long... values) {
        cs324.assignment2.grpc.ExecuteJobRequest.Builder request =
                cs324.assignment2.grpc.ExecuteJobRequest.newBuilder().setJobType(type);
        for (long value : values) {
            request.addValues(value);
        }
        return request.build();
    }

    private static ExecuteJobProgress lastWorkerProgress(
            WorkerServiceGrpc.WorkerServiceBlockingStub workerStub,
            cs324.assignment2.grpc.ExecuteJobRequest request) {
        Iterator<ExecuteJobProgress> stream = workerStub.executeJobWithProgress(request);
        ExecuteJobProgress latest = null;
        while (stream.hasNext()) {
            latest = stream.next();
        }
        return latest;
    }

    private static WorkerInfo workerInfo(int id, Server server) {
        return WorkerInfo.newBuilder()
                .setId(id)
                .setHost("localhost")
                .setPort(server.getPort())
                .build();
    }

    private static final class ProgressWorker extends WorkerServiceGrpc.WorkerServiceImplBase {
        private final boolean failFirstCall;
        private final boolean blockFirstCall;
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch firstTaskStarted = new CountDownLatch(1);
        private final CountDownLatch releaseTask = new CountDownLatch(1);

        private ProgressWorker(boolean failFirstCall, boolean blockFirstCall) {
            this.failFirstCall = failFirstCall;
            this.blockFirstCall = blockFirstCall;
        }

        @Override
        public void executeJobWithProgress(ExecuteJobRequest request,
                                           StreamObserver<ExecuteJobProgress> responseObserver) {
            int callNumber = calls.incrementAndGet();
            firstTaskStarted.countDown();

            if (failFirstCall && callNumber == 1) {
                responseObserver.onNext(ExecuteJobProgress.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setCompletionPercentage(25.0)
                        .setResult(1L)
                        .build());
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("Simulated worker disconnect")
                        .asRuntimeException());
                return;
            }

            if (blockFirstCall && callNumber == 1) {
                try {
                    if (!releaseTask.await(5, TimeUnit.SECONDS)) {
                        responseObserver.onError(Status.DEADLINE_EXCEEDED
                                .withDescription("Test did not release the blocked task")
                                .asRuntimeException());
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    responseObserver.onError(Status.CANCELLED
                            .withDescription("Test worker was interrupted")
                            .withCause(e)
                            .asRuntimeException());
                    return;
                }
            }

            responseObserver.onNext(ExecuteJobProgress.newBuilder()
                    .setTaskId(request.getTaskId())
                    .setCompletionPercentage(50.0)
                    .setResult(calculate(request))
                    .build());
            responseObserver.onNext(ExecuteJobProgress.newBuilder()
                    .setTaskId(request.getTaskId())
                    .setCompletionPercentage(75.0)
                    .setResult(calculate(request))
                    .build());
            responseObserver.onNext(ExecuteJobProgress.newBuilder()
                    .setTaskId(request.getTaskId())
                    .setCompletionPercentage(100.0)
                    .setResult(calculate(request))
                    .setIsFinished(true)
                    .build());
            responseObserver.onCompleted();
        }

        private boolean awaitFirstTask(long timeout, TimeUnit unit) throws InterruptedException {
            return firstTaskStarted.await(timeout, unit);
        }

        private void releaseFirstTask() {
            releaseTask.countDown();
        }

        private int getCalls() {
            return calls.get();
        }

        private static long sum(int start, int end) {
            long total = 0L;
            for (long value = start; value <= end; value++) {
                total = Math.addExact(total, value);
            }
            return total;
        }

        private static long calculate(ExecuteJobRequest request) {
            if (request.getValuesCount() == 0) {
                if ("MAX".equalsIgnoreCase(request.getJobType())) {
                    return request.getStart() <= request.getEnd()
                            ? request.getEnd() : Long.MIN_VALUE;
                }
                if ("PRIMECOUNT".equalsIgnoreCase(request.getJobType())) {
                    long count = 0;
                    for (long value = request.getStart(); value <= request.getEnd(); value++) {
                        if (isPrime(value)) {
                            count++;
                        }
                    }
                    return count;
                }
                return sum(request.getStart(), request.getEnd());
            }
            if ("MAX".equalsIgnoreCase(request.getJobType())) {
                return request.getValuesList().stream().mapToLong(Long::longValue).max()
                        .orElseThrow();
            }
            if ("PRIMECOUNT".equalsIgnoreCase(request.getJobType())) {
                return request.getValuesList().stream()
                        .filter(ProgressWorker::isPrime)
                        .count();
            }
            return request.getValuesList().stream().mapToLong(Long::longValue).sum();
        }

        private static boolean isPrime(long value) {
            if (value < 2) {
                return false;
            }
            for (long divisor = 2; divisor <= value / divisor; divisor++) {
                if (value % divisor == 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
