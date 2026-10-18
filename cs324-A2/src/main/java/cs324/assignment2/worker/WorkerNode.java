package cs324.assignment2.worker;

import cs324.assignment2.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerNode extends WorkerServiceGrpc.WorkerServiceImplBase {

    private static final int TERM_JOB_LIMIT = 5;
    private static final int PROGRESS_UPDATE_TARGET = 1000;
    private static final long HEARTBEAT_INTERVAL_MS = 1500L;
    private static final String BOOTSTRAP_HOST = "localhost";
    private static final int BOOTSTRAP_PORT = 50051;

    private final String leaderman = "cs324";

    private int id = -1;
    private int port = -1;

    private final List<WorkerInfo> neighbours = new CopyOnWriteArrayList<>();
    private final AtomicInteger jac = new AtomicInteger(0);
    private final AtomicInteger jobsThisTerm = new AtomicInteger(0);
    private volatile int coordinatorId = -1;
    private volatile long lastAcceptedElectionTimestamp = -1L;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final ExecutorService jobPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "worker-job-" + id);
        t.setDaemon(true);
        return t;
    });
    private final JobGrpcServiceImpl jobGrpcService = new JobGrpcServiceImpl(this);

    private BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrapStub;
    private ManagedChannel bootstrapChannel;

    private final Set<String> seenElections = ConcurrentHashMap.newKeySet();
    private final Set<String> seenCoordinations = ConcurrentHashMap.newKeySet();
    private final Map<String, Map<Integer, Integer>> jacByWorkerId = new ConcurrentHashMap<>();

    private final ExecutorService floodPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "flood-" + id);
        t.setDaemon(true);
        return t;
    });

    private final ScheduledExecutorService heartbeatScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "heartbeat");
                t.setDaemon(true);
                return t;
            });

    public static void main(String[] args) throws Exception {
        WorkerNode node = new WorkerNode();
        node.start();
    }

    public void start() throws Exception {
        bootstrapChannel = ManagedChannelBuilder
                .forAddress(BOOTSTRAP_HOST, BOOTSTRAP_PORT)
                .usePlaintext()
                .build();
        bootstrapStub = BootstrapServiceGrpc.newBlockingStub(bootstrapChannel);

        Server server = null;
        Random random = new Random();
        for (int attempt = 0; attempt < 40 && server == null; attempt++) {
            this.port = 2000 + random.nextInt(1000);
            try {
                server = ServerBuilder.forPort(this.port)
                        .addService(this)
                        .addService(jobGrpcService)
                        .build()
                        .start();
            } catch (IOException e) {
                server = null;
            }
        }
        if (server == null) {
            throw new IllegalStateException("Could not bind a free gRPC port for this worker");
        }

        RegisterWorkerResponse reg = bootstrapStub.registerWorker(
                RegisterWorkerRequest.newBuilder()
                        .setHost("localhost")
                        .setPort(this.port)
                        .build());
        this.id = reg.getWorkerId();
        System.out.println("[Worker " + id + "] Registered successfully. leaderman=" + leaderman);
        System.out.println("[Worker " + id + "] Listening on gRPC port " + this.port);

        heartbeatScheduler.scheduleAtFixedRate(this::sendHeartbeat,
                0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        joinNetwork();

        server.awaitTermination();
    }

    private void joinNetwork() {
        GetActiveWorkersResponse resp = bootstrapStub.getActiveWorkers(
                GetActiveWorkersRequest.newBuilder().build());

        List<WorkerInfo> others = resp.getWorkersList().stream()
                .filter(w -> w.getId() != this.id)
                .toList();

        if (!others.isEmpty()) {
            WorkerInfo randomNeighbour = others.get(new Random().nextInt(others.size()));
            addNeighbourLocal(randomNeighbour);
            System.out.println("[Worker " + id + "] Connected to neighbour: Worker "
                    + randomNeighbour.getId());

            try {
                WorkerServiceGrpc.WorkerServiceBlockingStub neighbourStub =
                        createWorkerStub(randomNeighbour);
                neighbourStub.addNeighbour(AddNeighbourRequest.newBuilder()
                        .setNeighbour(selfInfo())
                        .build());

                int existingCoordinator = neighbourStub.getCoordinatorId(
                        GetCoordinatorIdRequest.newBuilder().build()).getCoordinatorId();

                if (existingCoordinator == -1) {
                    System.out.println("[Worker " + id + "] No coordinator known yet — starting election.");
                    // TODO (Pranay): startElection();
                } else {
                    this.coordinatorId = existingCoordinator;
                    System.out.println("[Worker " + id + "] Learned current coordinator: Worker "
                            + coordinatorId);
                }
            } catch (Exception e) {
                System.err.println("[Worker " + id + "] Could not contact neighbour: " + e.getMessage());
            }
        } else {
            System.out.println("[Worker " + id + "] First worker in the network — self-electing as coordinator.");
            this.coordinatorId = this.id;
            this.jobsThisTerm.set(0);
        }
    }

    private void sendHeartbeat() {
        try {
            HeartbeatResponse resp = bootstrapStub.heartbeat(
                    HeartbeatRequest.newBuilder().setWorkerId(this.id).build());
            if (!resp.getAcknowledged()) {
                System.err.println("[Worker " + id + "] Bootstrap did not acknowledge heartbeat");
            }
        } catch (Exception e) {
            System.err.println("[Worker " + id + "] Heartbeat failed: " + e.getMessage());
        }
    }

    private WorkerInfo selfInfo() {
        return WorkerInfo.newBuilder()
                .setId(this.id)
                .setHost("localhost")
                .setPort(this.port)
                .build();
    }

    private void addNeighbourLocal(WorkerInfo neighbour) {
        if (neighbour == null || neighbour.getId() == this.id) return;
        for (WorkerInfo existing : neighbours) {
            if (existing.getId() == neighbour.getId()) return;
        }
        neighbours.add(neighbour);
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub createWorkerStub(WorkerInfo w) {
        ManagedChannel ch = ManagedChannelBuilder
                .forAddress(w.getHost(), w.getPort())
                .usePlaintext()
                .build();
        return WorkerServiceGrpc.newBlockingStub(ch);
    }

    @Override
    public void addNeighbour(AddNeighbourRequest request,
                             StreamObserver<AddNeighbourResponse> responseObserver) {
        addNeighbourLocal(request.getNeighbour());
        System.out.println("[Worker " + id + "] New neighbour added: Worker "
                + request.getNeighbour().getId());
        responseObserver.onNext(AddNeighbourResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getId(GetIdRequest request, StreamObserver<GetIdResponse> responseObserver) {
        responseObserver.onNext(GetIdResponse.newBuilder().setId(this.id).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getCoordinatorId(GetCoordinatorIdRequest request,
                                 StreamObserver<GetCoordinatorIdResponse> responseObserver) {
        responseObserver.onNext(GetCoordinatorIdResponse.newBuilder()
                .setCoordinatorId(this.coordinatorId)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getJac(GetJacRequest request, StreamObserver<GetJacResponse> responseObserver) {
        responseObserver.onNext(GetJacResponse.newBuilder().setJac(jac.get()).build());
        responseObserver.onCompleted();
    }

    // ---- Pranay TODOs ----
    @Override
    public void receiveElectionMessage(ElectionMessage request,
                                       StreamObserver<Empty> responseObserver) {
        // TODO (Pranay)
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void reportCandidacy(CandidacyReport request,
                                StreamObserver<Empty> responseObserver) {
        // TODO (Pranay)
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void receiveCoordinatorMessage(CoordinatorMessage request,
                                          StreamObserver<Empty> responseObserver) {
        // TODO (Pranay)
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    // ---- Aveshal TODOs ----
    @Override
    public void executeJob(ExecuteJobRequest request,
                           StreamObserver<ExecuteJobResponse> responseObserver) {
        if (!isSupportedJobType(request.getJobType())) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Supported job types are SUM and PRIMESUM")
                    .asRuntimeException());
            return;
        }
        if (request.getStart() > request.getEnd()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("start must be less than or equal to end")
                    .asRuntimeException());
            return;
        }

        jobPool.execute(() -> {
            try {
                long result = calculateRange(request.getJobType(), request.getStart(),
                        request.getEnd(), (percentage, partialResult) -> { });
                responseObserver.onNext(ExecuteJobResponse.newBuilder()
                        .setResult(result)
                        .setTaskId(request.getTaskId())
                        .build());
                responseObserver.onCompleted();
            } catch (ArithmeticException e) {
                responseObserver.onError(Status.OUT_OF_RANGE
                        .withDescription("The job result exceeds the supported 64-bit range")
                        .withCause(e)
                        .asRuntimeException());
            }
        });
    }

    @Override
    public void executeJobWithProgress(ExecuteJobRequest request,
                                       StreamObserver<ExecuteJobProgress> responseObserver) {
        if (!isSupportedJobType(request.getJobType())) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Supported job types are SUM and PRIMESUM")
                    .asRuntimeException());
            return;
        }
        if (request.getStart() > request.getEnd()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("start must be less than or equal to end")
                    .asRuntimeException());
            return;
        }

        jobPool.execute(() -> {
            try {
                long result = calculateRange(request.getJobType(), request.getStart(), request.getEnd(),
                        (percentage, partialResult) -> responseObserver.onNext(
                                ExecuteJobProgress.newBuilder()
                                        .setTaskId(request.getTaskId())
                                        .setCompletionPercentage(percentage)
                                        .setResult(partialResult)
                                        .build()));
                responseObserver.onNext(ExecuteJobProgress.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setCompletionPercentage(100.0)
                        .setResult(result)
                        .setIsFinished(true)
                        .build());
                responseObserver.onCompleted();
            } catch (ArithmeticException e) {
                responseObserver.onError(Status.OUT_OF_RANGE
                        .withDescription("The job result exceeds the supported 64-bit range")
                        .withCause(e)
                        .asRuntimeException());
            }
        });
    }

    private static boolean isSupportedJobType(String jobType) {
        return "SUM".equalsIgnoreCase(jobType) || "PRIMESUM".equalsIgnoreCase(jobType);
    }

    private static long calculateRange(String jobType, int start, int end,
                                       JobProgressListener progressListener) {
        long itemCount = (long) end - start + 1;
        long progressInterval = Math.max(1L,
                (itemCount + PROGRESS_UPDATE_TARGET - 1) / PROGRESS_UPDATE_TARGET);
        long result = 0L;
        long completed = 0L;
        boolean primeSum = "PRIMESUM".equalsIgnoreCase(jobType);

        for (long value = start; value <= end; value++) {
            if (!primeSum || isPrime(value)) {
                result = Math.addExact(result, value);
            }
            completed++;
            if (completed % progressInterval == 0 || completed == itemCount) {
                progressListener.onProgress(completed * 100.0 / itemCount, result);
            }
        }
        return result;
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

    @FunctionalInterface
    private interface JobProgressListener {
        void onProgress(double percentage, long partialResult);
    }

    @Override
    public void submitJob(SubmitJobRequest request,
                          StreamObserver<JobProgressUpdate> responseObserver) {
        cs324.assignment2.job.grpc.SubmitJobRequest jobRequest =
                cs324.assignment2.job.grpc.SubmitJobRequest.newBuilder()
                        .setJobType(request.getJobType())
                        .putParameters("start", Integer.toString(request.getStart()))
                        .putParameters("end", Integer.toString(request.getEnd()))
                        .build();

        jobGrpcService.submitJob(jobRequest,
                new StreamObserver<cs324.assignment2.job.grpc.SubmitJobResponse>() {
                    @Override
                    public void onNext(cs324.assignment2.job.grpc.SubmitJobResponse response) {
                        jobGrpcService.streamJobProgress(
                                cs324.assignment2.job.grpc.StreamJobProgressRequest.newBuilder()
                                        .setJobId(response.getJobId())
                                        .build(),
                                new StreamObserver<cs324.assignment2.job.grpc.JobProgressUpdate>() {
                                    @Override
                                    public void onNext(
                                            cs324.assignment2.job.grpc.JobProgressUpdate update) {
                                        JobProgressUpdate.Builder legacyUpdate =
                                                JobProgressUpdate.newBuilder()
                                                        .setOverallPercent(
                                                                update.getCompletionPercentage())
                                                        .setFinalResult(update.getResult())
                                                        .setIsFinished(update.getStatus()
                                                                == cs324.assignment2.job.grpc.JobStatus.JOB_STATUS_COMPLETED
                                                                || update.getStatus()
                                                                == cs324.assignment2.job.grpc.JobStatus.JOB_STATUS_FAILED);
                                        for (cs324.assignment2.job.grpc.WorkerStatusMetric worker
                                                : update.getWorkersList()) {
                                            legacyUpdate.addWorkers(WorkerProgress.newBuilder()
                                                    .setWorkerId(worker.getWorkerId())
                                                    .setPercentDone(
                                                            worker.getCompletionPercentage())
                                                    .setStatus(worker.getStatus().name()));
                                        }
                                        responseObserver.onNext(legacyUpdate.build());
                                    }

                                    @Override
                                    public void onError(Throwable error) {
                                        responseObserver.onError(error);
                                    }

                                    @Override
                                    public void onCompleted() {
                                        responseObserver.onCompleted();
                                    }
                                });
                    }

                    @Override
                    public void onError(Throwable error) {
                        responseObserver.onError(error);
                    }

                    @Override
                    public void onCompleted() {
                        // The legacy response stream completes when the job progress stream does.
                    }
                });
    }

    public List<WorkerInfo> getActiveWorkersFromBootstrap() {
        return bootstrapStub.getActiveWorkers(
                GetActiveWorkersRequest.newBuilder().build()).getWorkersList();
    }

    public int getMyId() { return id; }
    public int getMyCoordinatorId() { return coordinatorId; }
    public List<WorkerInfo> getNeighbours() { return neighbours; }
    public AtomicInteger getJac() { return jac; }
    public AtomicBoolean getElectionInProgress() { return electionInProgress; }
    public ExecutorService getFloodPool() { return floodPool; }
}