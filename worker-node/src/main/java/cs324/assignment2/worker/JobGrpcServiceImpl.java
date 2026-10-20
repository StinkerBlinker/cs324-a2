package cs324.assignment2.worker;

import cs324.assignment2.grpc.ExecuteJobProgress;
import cs324.assignment2.grpc.SaveCheckpointRequest;
import cs324.assignment2.job.grpc.PersistedJob;
import cs324.assignment2.job.grpc.PersistedTask;
import cs324.assignment2.grpc.ExecuteJobRequest;
import cs324.assignment2.grpc.WorkerInfo;
import cs324.assignment2.grpc.WorkerServiceGrpc;
import cs324.assignment2.job.grpc.JobProgressUpdate;
import cs324.assignment2.job.grpc.JobServiceGrpc;
import cs324.assignment2.job.grpc.JobStatus;
import cs324.assignment2.job.grpc.StreamJobProgressRequest;
import cs324.assignment2.job.grpc.SubTaskState;
import cs324.assignment2.job.grpc.SubTaskStatus;
import cs324.assignment2.job.grpc.SubmitJobRequest;
import cs324.assignment2.job.grpc.SubmitJobResponse;
import cs324.assignment2.job.grpc.WorkerStatus;
import cs324.assignment2.job.grpc.WorkerStatusMetric;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class JobGrpcServiceImpl extends JobServiceGrpc.JobServiceImplBase
        implements AutoCloseable {

    private static final int TASKS_PER_WORKER = 20;
    private static final int MAX_TASK_COUNT = 10_000;
    private final long slowMs, stalledMs;
    private final java.util.concurrent.ScheduledExecutorService taskMonitor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "task-progress-monitor"); thread.setDaemon(true); return thread;
    });

    private final WorkerNode localWorker;
    private final java.util.function.Function<WorkerInfo, ManagedChannel> taskChannels;
    private final Object leadership = new Object();
    private volatile long loadedEpoch = -1;
    private volatile boolean closed, observedReady;
    private final Set<ManagedChannel> activeChannels = ConcurrentHashMap.newKeySet();
    private final Map<String, JobExecution> jobs = new ConcurrentHashMap<>();
    private final ExecutorService dispatchPool = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r, "job-dispatch");
        thread.setDaemon(true);
        return thread;
    });

    public JobGrpcServiceImpl(WorkerNode localWorker) {
        this(localWorker, 10_000, 60_000);
    }

    JobGrpcServiceImpl(WorkerNode localWorker, long slowMs, long stalledMs) {
        this(localWorker, slowMs, stalledMs, worker -> ManagedChannelBuilder
                .forAddress(worker.getHost(), worker.getPort()).usePlaintext().build());
    }

    JobGrpcServiceImpl(WorkerNode localWorker, long slowMs, long stalledMs,
                       java.util.function.Function<WorkerInfo, ManagedChannel> taskChannels) {
        this.localWorker = localWorker;
        this.taskChannels = taskChannels;
        this.slowMs = slowMs;
        this.stalledMs = stalledMs;
    }

    @Override
    public synchronized void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
        if (localWorker.getMyId() < 0 || localWorker.getMyCoordinatorId() != localWorker.getMyId()) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("This worker is not the current coordinator")
                    .asRuntimeException());
            return;
        }

        try { ensureRecovered(); }
        catch (RuntimeException error) { responseObserver.onError(Status.UNAVAILABLE.withDescription(error.getMessage()).asRuntimeException()); return; }
        String requestedId = request.getRequestId().isBlank() ? UUID.randomUUID().toString() : request.getRequestId();
        JobExecution existing = jobs.get(requestedId);
        if (existing != null) {
            if (!existing.originalRequest.toBuilder().clearRequestId().build().equals(request.toBuilder().clearRequestId().build())) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("request_id already belongs to different job inputs").asRuntimeException()); return;
            }
            responseObserver.onNext(SubmitJobResponse.newBuilder().setJobId(requestedId).setStatus(existing.status)
                    .setMessage("Existing request ID; continuing the same job").build());
            responseObserver.onCompleted(); return;
        }
        String jobType = request.getJobType().trim().toUpperCase();
        if (jobType.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("job_type must not be empty")
                    .asRuntimeException());
            return;
        }
        if (!isSupportedJobType(jobType)) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Supported job types are SUM, PRIMESUM, MAX and PRIMECOUNT")
                    .asRuntimeException());
            return;
        }

        int start;
        int end;
        if (request.getValuesCount() == 0) {
            try {
                start = Integer.parseInt(request.getParametersMap().getOrDefault("start", ""));
                end = Integer.parseInt(request.getParametersMap().getOrDefault("end", ""));
            } catch (NumberFormatException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Provide numeric values or integer start and end parameters")
                        .asRuntimeException());
                return;
            }
            if (start > end) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("start must be less than or equal to end")
                        .asRuntimeException());
                return;
            }
        } else {
            start = 0;
            end = -1;
        }

        List<WorkerInfo> workers;
        try {
            workers = localWorker.getActiveWorkersFromBootstrap().stream()
                    .filter(worker -> worker.getId() > 0 && worker.getPort() > 0
                            && !worker.getHost().isBlank())
                    .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        } catch (RuntimeException e) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Could not retrieve active workers from Bootstrap")
                    .withCause(e)
                    .asRuntimeException());
            return;
        }
        workers.sort((left, right) -> Integer.compare(left.getId(), right.getId()));
        if (workers.stream().noneMatch(worker -> worker.getId() == localWorker.getMyId())) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("The coordinator is not present in Bootstrap's active worker list")
                    .asRuntimeException());
            return;
        }

        List<TaskAssignment> assignments = createAssignments(
                request, jobType, start, end, workers);
        String jobId = requestedId;
        JobExecution job = new JobExecution(jobId, assignments, workers, request);
        try { persist(job, true); }
        catch (RuntimeException error) { responseObserver.onError(Status.UNAVAILABLE.withDescription("Admission uncertain; retry the same request_id: " + error.getMessage()).asRuntimeException()); return; }
        jobs.put(jobId, job);

        responseObserver.onNext(SubmitJobResponse.newBuilder()
                .setJobId(jobId)
                .setStatus(JobStatus.JOB_STATUS_QUEUED)
                .setMessage("Job accepted and queued")
                .build());
        responseObserver.onCompleted();

        dispatchPool.execute(() -> {
            job.start();
            schedule(job);
        });
    }

    private static boolean isSupportedJobType(String jobType) {
        return "SUM".equals(jobType) || "PRIMESUM".equals(jobType)
                || "MAX".equals(jobType) || "PRIMECOUNT".equals(jobType);
    }

    @Override
    public void streamJobProgress(StreamJobProgressRequest request,
                                  StreamObserver<JobProgressUpdate> responseObserver) {
        try { ensureRecovered(); }
        catch (RuntimeException error) { responseObserver.onError(Status.UNAVAILABLE.withDescription(error.getMessage()).asRuntimeException()); return; }
        JobExecution job = jobs.get(request.getJobId());
        if (job == null) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Unknown job ID: " + request.getJobId())
                    .asRuntimeException());
            return;
        }
        job.subscribe(responseObserver);
    }

    private List<TaskAssignment> createAssignments(SubmitJobRequest originalRequest, String jobType,
                                                   int start, int end, List<WorkerInfo> workers) {
        boolean hasExplicitValues = originalRequest.getValuesCount() > 0;
        long rangeSize = hasExplicitValues
                ? originalRequest.getValuesCount()
                : (long) end - start + 1;
        long requestedTaskCount = Math.min((long) workers.size() * TASKS_PER_WORKER,
                MAX_TASK_COUNT);
        int taskCount = (int) Math.min(rangeSize, requestedTaskCount);
        long baseSize = rangeSize / taskCount;
        long extra = rangeSize % taskCount;
        long nextIndex = 0L;
        List<TaskAssignment> assignments = new ArrayList<>(taskCount);

        for (int i = 0; i < taskCount; i++) {
            long taskSize = baseSize + (i < extra ? 1 : 0);
            String taskId = UUID.randomUUID().toString();
            ExecuteJobRequest.Builder taskRequest = ExecuteJobRequest.newBuilder()
                    .setJobType(jobType)
                    .setTaskId(taskId)
                    .putAllParameters(originalRequest.getParametersMap())
                    .setPayload(originalRequest.getPayload());
            if (hasExplicitValues) {
                int taskStart = Math.toIntExact(nextIndex);
                int taskEnd = Math.toIntExact(nextIndex + taskSize);
                taskRequest.addAllValues(originalRequest.getValuesList()
                        .subList(taskStart, taskEnd));
            } else {
                taskRequest.setStart(Math.toIntExact((long) start + nextIndex))
                        .setEnd(Math.toIntExact((long) start + nextIndex + taskSize - 1));
            }
            assignments.add(new TaskAssignment(taskRequest.build()));
            nextIndex += taskSize;
        }
        return assignments;
    }

    private void dispatch(JobExecution job, TaskProgress task) {
        if (closed || job.paused) return;
        TaskAttempt attempt = job.currentAttempt(task);
        if (attempt == null) {
            return;
        }
        ManagedChannel channel;
        try {
            channel = taskChannels.apply(attempt.worker);
        } catch (RuntimeException e) {
            handleWorkerFailure(job, attempt,
                    "Could not contact worker " + attempt.worker.getId()
                            + ": " + e.getMessage());
            return;
        }

        // Register ownership before creating the monitor. close() and this check
        // share the lock, so a dispatch racing shutdown cannot orphan a channel.
        synchronized (activeChannels) {
            if (closed) { channel.shutdownNow(); return; }
            activeChannels.add(channel);
        }

        io.grpc.Context.CancellableContext context = io.grpc.Context.ROOT.withCancellation();
        TaskProgressWatch watch = new TaskProgressWatch(System::nanoTime, slowMs, stalledMs, Math.min(5_000, stalledMs));
        var finished = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.ScheduledFuture<?> alarm;
        try { alarm = taskMonitor.scheduleWithFixedDelay(() -> {
            if (finished.get()) return;
            TaskProgressWatch.State health = watch.state();
            if ((health == TaskProgressWatch.State.STALLED || health == TaskProgressWatch.State.UNREACHABLE) && finished.compareAndSet(false, true)) {
                String reason = health == TaskProgressWatch.State.UNREACHABLE ? "No task heartbeat/progress received"
                        : "No increase in measured work for " + stalledMs + " ms";
                handleWorkerFailure(job, attempt, reason + "; retrying unfinished task");
                context.cancel(Status.DEADLINE_EXCEEDED.withDescription("Task stalled").asRuntimeException());
            } else if (watch.state() == TaskProgressWatch.State.SLOW) job.markSlow(attempt);
        }, Math.min(500, slowMs), Math.min(500, slowMs), TimeUnit.MILLISECONDS); }
        catch (RuntimeException stopping) {
            activeChannels.remove(channel); channel.shutdownNow(); context.close();
            if (!closed) handleWorkerFailure(job, attempt, "Task monitor unavailable: " + stopping.getMessage());
            return;
        }
        try {
            if (!localWorker.isReadyCoordinator() || loadedEpoch != job.epoch || job.paused) {
                alarm.cancel(false); context.close(); activeChannels.remove(channel); channel.shutdownNow(); return;
            }
            if (attempt.worker.getId() != localWorker.getMyId())
                localWorker.recordRemoteAssignment(job.jobId + "/" + task.taskId + "/" + job.epoch + "/" + attempt.attemptNumber);
            context.run(() -> WorkerServiceGrpc.newStub(channel).executeJobWithProgress(task.taskRequest,
                    new StreamObserver<ExecuteJobProgress>() {
                        @Override public void onNext(ExecuteJobProgress update) {
                            if (finished.get()) return;
                            if (update.getHeartbeat()) { watch.heartbeat(); return; }
                            watch.advance(update.getCompletionPercentage());
                            if (!update.getErrorMessage().isEmpty()) {
                                finished.set(true); alarm.cancel(false);
                                handleWorkerFailure(job, attempt, update.getErrorMessage()); context.close();
                            } else if (update.getIsFinished()) {
                                if (finished.compareAndSet(false, true)) {
                                    alarm.cancel(false); job.completeTask(attempt, update.getResult()); schedule(job);
                                }
                            } else job.updateProgress(attempt, update.getCompletionPercentage(), update.getResult());
                        }
                        @Override public void onError(Throwable error) {
                            alarm.cancel(false);
                            if (finished.compareAndSet(false, true)) handleWorkerFailure(job, attempt,
                                    "Worker " + attempt.worker.getId() + " unavailable: " + error.getMessage());
                            activeChannels.remove(channel); channel.shutdown(); context.close();
                        }
                        @Override public void onCompleted() {
                            alarm.cancel(false); finished.set(true);
                            activeChannels.remove(channel); channel.shutdown(); context.close();
                            if (job.failIfIncomplete(attempt)) refreshWorkersAndSchedule(job);
                            else schedule(job);
                        }
                    }));
        } catch (RuntimeException error) {
            alarm.cancel(false); finished.set(true); context.close();
            handleWorkerFailure(job, attempt, "Could not contact worker " + attempt.worker.getId() + ": " + error.getMessage());
            activeChannels.remove(channel); channel.shutdown();
        }
    }

    private void handleWorkerFailure(JobExecution job, TaskAttempt attempt, String errorMessage) {
        if (job.requeueAfterWorkerFailure(attempt, errorMessage)) {
            refreshWorkersAndSchedule(job);
        }
    }

    private void refreshWorkersAndSchedule(JobExecution job) {
        if (closed || job.paused) return;
        dispatchPool.execute(() -> {
            try {
                job.addAvailableWorkers(localWorker.getActiveWorkersFromBootstrap());
            } catch (RuntimeException refreshError) {
                System.err.println("Could not refresh workers after task failure: "
                        + refreshError.getMessage());
            }
            job.failIfNoWorkersCanContinue();
            schedule(job);
        });
    }

    private void schedule(JobExecution job) {
        if (closed || job.paused || !localWorker.isReadyCoordinator() || job.epoch != localWorker.getMyEpoch()) { job.pause(); return; }
        for (TaskProgress task : job.assignPendingTasks()) {
            dispatchPool.execute(() -> dispatch(job, task));
        }
    }

    @Override
    public void close() {
        synchronized (activeChannels) {
            closed = true;
            activeChannels.forEach(ManagedChannel::shutdownNow);
        }
        jobs.values().forEach(JobExecution::pause);
        dispatchPool.shutdownNow(); taskMonitor.shutdownNow();
    }

    private void persist(JobExecution job, boolean admission) {
        if (closed || job.epoch != localWorker.getMyEpoch() || !localWorker.isReadyCoordinator())
            throw new IllegalStateException("Coordinator changed; reconnect using the same job ID");
        // A task transport finishing/cancelling must not cancel its checkpoint RPCs.
        var saved = SaveCheckpointRequest.newBuilder().setKey("job/" + job.jobId)
                .setPayload(job.checkpoint().toByteString()).setNewJob(admission).build();
        var answer = new java.util.concurrent.atomic.AtomicReference<cs324.assignment2.grpc.CheckpointAck>();
        io.grpc.Context.ROOT.run(() -> answer.set(localWorker.checkpointJob(saved)));
        var ack = answer.get();
        if (!ack.getAccepted()) throw new IllegalStateException(ack.getDetail());
    }

    public void refreshLeadership() {
        if (closed) return;
        boolean ready = localWorker.isReadyCoordinator();
        if (!ready && observedReady) {
            observedReady = false;
            dispatchPool.execute(() -> { synchronized (leadership) {
                if (!localWorker.isReadyCoordinator()) {
                    jobs.values().forEach(JobExecution::pause); activeChannels.forEach(ManagedChannel::shutdownNow); loadedEpoch = -1;
                }
            } });
        } else if (ready && loadedEpoch != localWorker.getMyEpoch()) {
            dispatchPool.execute(() -> { try { ensureRecovered(); } catch (RuntimeException ignored) {} });
        }
    }

    private void ensureRecovered() {
        synchronized (leadership) {
            if (closed || !localWorker.isReadyCoordinator()) throw new IllegalStateException("Coordinator is not ready");
            long epoch = localWorker.getMyEpoch();
            if (loadedEpoch == epoch) return;
            jobs.values().forEach(JobExecution::pause);
            jobs.clear();
            for (var record : localWorker.recoveredJobs().getRecordsList()) {
                if (!record.getKey().startsWith("job/")) continue;
                try {
                    PersistedJob saved = PersistedJob.parseFrom(record.getPayload());
                    if (!record.getKey().equals("job/" + saved.getJobId())) throw new IllegalArgumentException("Mismatched job checkpoint ID");
                    List<TaskAssignment> assignments = saved.getTasksList().stream().map(t -> new TaskAssignment(t.getRequest())).toList();
                    JobExecution job = new JobExecution(saved.getJobId(), assignments,
                            localWorker.getActiveWorkersFromBootstrap(), saved.getOriginalRequest());
                    job.pendingTasks.clear();
                    for (PersistedTask task : saved.getTasksList()) {
                        TaskProgress restored = job.tasks.get(task.getRequest().getTaskId());
                        restored.attemptNumber = task.getAttemptNumber();
                        if (task.getState() == SubTaskState.SUB_TASK_STATE_COMPLETED) {
                            restored.state = task.getState(); restored.result = task.getResult(); restored.completionPercentage = 100;
                            restored.workerStatus = WorkerStatus.WORKER_STATUS_IDLE;
                            if (task.hasWorker()) restored.worker = task.getWorker();
                        } else { job.pendingTasks.add(restored); }
                    }
                    job.status = saved.getStatus(); job.message = saved.getMessage();
                    jobs.put(job.jobId, job);
                } catch (com.google.protobuf.InvalidProtocolBufferException error) {
                    throw new IllegalStateException("Cannot decode checkpoint " + record.getKey(), error);
                }
            }
            loadedEpoch = epoch; observedReady = true;
            for (JobExecution job : jobs.values()) if (!job.isTerminal()) dispatchPool.execute(() -> { job.start(); schedule(job); });
        }
    }

    private static final class TaskAssignment {
        private final ExecuteJobRequest taskRequest;

        private TaskAssignment(ExecuteJobRequest taskRequest) {
            this.taskRequest = taskRequest;
        }
    }

    private static final class TaskProgress {
        private WorkerInfo worker;
        private final String taskId;
        private final ExecuteJobRequest taskRequest;
        private final long totalWork;
        private SubTaskState state = SubTaskState.SUB_TASK_STATE_PENDING;
        private WorkerStatus workerStatus = WorkerStatus.WORKER_STATUS_IDLE;
        private double completionPercentage;
        private int attemptNumber;
        private long result;
        private String message = "Waiting for assignment";

        private TaskProgress(TaskAssignment assignment) {
            taskRequest = assignment.taskRequest;
            taskId = assignment.taskRequest.getTaskId();
            totalWork = assignment.taskRequest.getValuesCount() > 0
                    ? assignment.taskRequest.getValuesCount()
                    : (long) assignment.taskRequest.getEnd()
                    - assignment.taskRequest.getStart() + 1;
        }
    }

    private static final class TaskAttempt {
        private final TaskProgress task;
        private final WorkerInfo worker;
        private final int attemptNumber;

        private TaskAttempt(TaskProgress task, WorkerInfo worker, int attemptNumber) {
            this.task = task;
            this.worker = worker;
            this.attemptNumber = attemptNumber;
        }
    }

    private final class JobExecution {
        private final long epoch = localWorker.getMyEpoch();
        private final SubmitJobRequest originalRequest;
        private volatile boolean paused;
        private final String jobId;
        private final String jobType;
        private final Map<String, TaskProgress> tasks = new LinkedHashMap<>();
        private final ArrayDeque<TaskProgress> pendingTasks = new ArrayDeque<>();
        private final List<StreamObserver<JobProgressUpdate>> subscribers = new ArrayList<>();
        private final Map<Integer, WorkerInfo> workers = new LinkedHashMap<>();
        private final Map<Integer, String> activeTasksByWorker = new LinkedHashMap<>();
        private final Set<Integer> failedWorkerIds = new HashSet<>();
        private JobStatus status = JobStatus.JOB_STATUS_QUEUED;
        private String message = "Job accepted and queued";

        private JobExecution(String jobId, List<TaskAssignment> assignments,
                             List<WorkerInfo> availableWorkers, SubmitJobRequest originalRequest) {
            this.originalRequest = originalRequest;
            this.jobId = jobId;
            this.jobType = originalRequest.getJobType().trim().toUpperCase();
            addAvailableWorkers(availableWorkers);
            for (TaskAssignment assignment : assignments) {
                TaskProgress task = new TaskProgress(assignment);
                if ("MAX".equals(jobType)) {
                    task.result = Long.MIN_VALUE;
                }
                tasks.put(task.taskId, task);
                pendingTasks.addLast(task);
            }
        }

        private synchronized void addAvailableWorkers(List<WorkerInfo> availableWorkers) {
            for (WorkerInfo worker : availableWorkers) {
                if (worker.getId() > 0 && worker.getPort() > 0 && !worker.getHost().isBlank()
                        && !failedWorkerIds.contains(worker.getId())) {
                    workers.put(worker.getId(), worker);
                }
            }
        }

        private synchronized void subscribe(StreamObserver<JobProgressUpdate> observer) {
            if (paused) { observer.onError(Status.UNAVAILABLE.withDescription("Job ownership changed; reconnect").asRuntimeException()); return; }
            subscribers.add(observer);
            observer.onNext(snapshot());
            if (isTerminal()) {
                subscribers.remove(observer);
                observer.onCompleted();
            }
        }

        private synchronized void start() {
            if (isTerminal()) {
                return;
            }
            status = JobStatus.JOB_STATUS_RUNNING;
            message = "Job is running";
            publish();
        }

        private synchronized List<TaskProgress> assignPendingTasks() {
            List<TaskProgress> assigned = new ArrayList<>();
            if (isTerminal()) {
                return assigned;
            }
            for (WorkerInfo worker : workers.values()) {
                if (failedWorkerIds.contains(worker.getId())
                        || activeTasksByWorker.containsKey(worker.getId())) {
                    continue;
                }
                TaskProgress task = pendingTasks.pollFirst();
                if (task == null) {
                    break;
                }
                task.worker = worker;
                task.state = SubTaskState.SUB_TASK_STATE_RUNNING;
                task.workerStatus = WorkerStatus.WORKER_STATUS_ACTIVE;
                task.message = "Executing";
                activeTasksByWorker.put(worker.getId(), task.taskId);
                task.attemptNumber++;
                assigned.add(task);
            }
            if (!assigned.isEmpty()) {
                publish();
            }
            return assigned;
        }

        private synchronized TaskAttempt currentAttempt(TaskProgress task) {
            if (task.state != SubTaskState.SUB_TASK_STATE_RUNNING || task.worker == null) {
                return null;
            }
            return new TaskAttempt(task, task.worker, task.attemptNumber);
        }

        private synchronized void updateProgress(TaskAttempt attempt,
                                                 double percentage, long result) {
            if (isTerminal()) {
                return;
            }
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (!isCurrentAttempt(task, attempt)) {
                return;
            }
            task.completionPercentage = Math.max(0.0, Math.min(100.0, percentage));
            task.workerStatus = WorkerStatus.WORKER_STATUS_ACTIVE;
            task.state = SubTaskState.SUB_TASK_STATE_RUNNING;
            task.message = "Executing";
            updateTerminalStatus();
            task.result = result;
            publish(false);
        }

        private synchronized void markSlow(TaskAttempt attempt) {
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (isTerminal() || !isCurrentAttempt(task, attempt) || task.workerStatus == WorkerStatus.WORKER_STATUS_SLOW) return;
            task.workerStatus = WorkerStatus.WORKER_STATUS_SLOW;
            task.state = SubTaskState.SUB_TASK_STATE_SLOW;
            status = JobStatus.JOB_STATUS_SLOW;
            task.message = "Slow: waiting for measured work to advance";
            publish(false);
        }

        private synchronized void completeTask(TaskAttempt attempt, long result) {
            if (isTerminal()) {
                return;
            }
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (!isCurrentAttempt(task, attempt)) {
                return;
            }
            task.result = result;
            task.completionPercentage = 100.0;
            task.state = SubTaskState.SUB_TASK_STATE_COMPLETED;
            task.workerStatus = WorkerStatus.WORKER_STATUS_IDLE;
            task.message = "Completed";
            activeTasksByWorker.remove(attempt.worker.getId(), task.taskId);
            failIfNoWorkersCanContinue();
            updateTerminalStatus();
            publish();
        }

        private synchronized boolean requeueAfterWorkerFailure(TaskAttempt attempt,
                                                               String errorMessage) {
            if (isTerminal()) {
                return false;
            }
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (!isCurrentAttempt(task, attempt)) {
                return false;
            }
            int failedWorkerId = attempt.worker.getId();
            failedWorkerIds.add(failedWorkerId);
            workers.remove(failedWorkerId);
            activeTasksByWorker.remove(failedWorkerId, task.taskId);
            task.worker = null;
            task.state = SubTaskState.SUB_TASK_STATE_PENDING;
            task.workerStatus = WorkerStatus.WORKER_STATUS_FAILED;
            task.completionPercentage = 0.0;
            task.result = "MAX".equals(jobType) ? Long.MIN_VALUE : 0L;
            task.message = errorMessage;
            message = "Worker " + failedWorkerId + " failed; retrying unfinished sub-task";
            pendingTasks.addFirst(task);
            publish();
            return !isTerminal();
        }

        private synchronized boolean failIfIncomplete(TaskAttempt attempt) {
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (isCurrentAttempt(task, attempt)) {
                return requeueAfterWorkerFailure(attempt,
                        "Worker stream ended before the sub-task completed");
            }
            return false;
        }

        private boolean isCurrentAttempt(TaskProgress task, TaskAttempt attempt) {
            return task != null
                    && (task.state == SubTaskState.SUB_TASK_STATE_RUNNING || task.state == SubTaskState.SUB_TASK_STATE_SLOW)
                    && task.attemptNumber == attempt.attemptNumber
                    && task.worker != null
                    && task.worker.getId() == attempt.worker.getId();
        }

        private synchronized void failIfNoWorkersCanContinue() {
            failIfNoWorkersCanContinueLocked();
            if (isTerminal()) {
                publish();
            }
        }

        private void failPendingTasks(String reason) {
            status = JobStatus.JOB_STATUS_FAILED;
            message = reason;
            pendingTasks.clear();
            for (TaskProgress task : tasks.values()) {
                if (task.state == SubTaskState.SUB_TASK_STATE_PENDING) {
                    task.state = SubTaskState.SUB_TASK_STATE_FAILED;
                    task.workerStatus = WorkerStatus.WORKER_STATUS_UNAVAILABLE;
                    task.message = reason;
                }
            }
        }

        private void updateTerminalStatus() {
            failIfNoWorkersCanContinueLocked();
            boolean allTerminal = tasks.values().stream().allMatch(task -> isTaskTerminal(task.state));
            if (!allTerminal) {
                status = tasks.values().stream().anyMatch(t -> t.state == SubTaskState.SUB_TASK_STATE_SLOW)
                        ? JobStatus.JOB_STATUS_SLOW : JobStatus.JOB_STATUS_RUNNING;
                return;
            }
            boolean failed = tasks.values().stream()
                    .anyMatch(task -> task.state == SubTaskState.SUB_TASK_STATE_FAILED);
            status = failed ? JobStatus.JOB_STATUS_FAILED : JobStatus.JOB_STATUS_COMPLETED;
            message = failed ? "One or more sub-tasks failed" : "Job completed";
        }

        private void failIfNoWorkersCanContinueLocked() {
            if (!pendingTasks.isEmpty() && workers.isEmpty() && activeTasksByWorker.isEmpty()) {
                failPendingTasks("No healthy workers remain to retry unfinished sub-tasks");
            }
        }

        private boolean isTerminal() {
            return paused || status == JobStatus.JOB_STATUS_COMPLETED
                    || status == JobStatus.JOB_STATUS_FAILED
                    || status == JobStatus.JOB_STATUS_CANCELLED;
        }

        private static boolean isTaskTerminal(SubTaskState state) {
            return state == SubTaskState.SUB_TASK_STATE_COMPLETED
                    || state == SubTaskState.SUB_TASK_STATE_FAILED;
        }

        private JobProgressUpdate snapshot() {
            JobProgressUpdate.Builder update = JobProgressUpdate.newBuilder()
                    .setJobId(jobId)
                    .setStatus(status)
                    .setMessage(message)
                    .setUpdatedAtUnixMs(System.currentTimeMillis());
            long totalWork = 0;
            double completedWork = 0.0;
            long totalResult = "MAX".equals(jobType) ? Long.MIN_VALUE : 0L;
            boolean hasMaximum = false;
            int activeWorkers = activeTasksByWorker.size();

            for (TaskProgress task : tasks.values()) {
                totalWork += task.totalWork;
                completedWork += task.totalWork * task.completionPercentage / 100.0;
                if ("MAX".equals(jobType)) {
                    if (task.state == SubTaskState.SUB_TASK_STATE_COMPLETED
                            || (task.state == SubTaskState.SUB_TASK_STATE_RUNNING || task.state == SubTaskState.SUB_TASK_STATE_SLOW)
                            && task.completionPercentage > 0.0) {
                        totalResult = Math.max(totalResult, task.result);
                        hasMaximum = true;
                    }
                } else {
                    totalResult = Math.addExact(totalResult, task.result);
                }
                if (task.worker != null) {
                    update.addWorkers(WorkerStatusMetric.newBuilder()
                            .setWorkerId(task.worker.getId())
                            .setStatus(task.workerStatus)
                            .setCompletionPercentage(task.completionPercentage)
                            .setActiveSubTaskCount(
                                    task.state == SubTaskState.SUB_TASK_STATE_RUNNING || task.state == SubTaskState.SUB_TASK_STATE_SLOW ? 1 : 0)
                            .setCompletedSubTaskCount(
                                    task.state == SubTaskState.SUB_TASK_STATE_COMPLETED ? 1 : 0)
                            .setStatusMessage(task.message));
                }
                SubTaskStatus.Builder subTask = SubTaskStatus.newBuilder()
                        .setSubTaskId(task.taskId)
                        .setState(task.state)
                        .setCompletionPercentage(task.completionPercentage)
                        .setMessage(task.message);
                if (task.worker != null) {
                    subTask.setWorkerId(task.worker.getId());
                }
                update.addSubTasks(subTask);
            }
            double completionPercentage = completedWork * 100.0 / totalWork;
            if ("MAX".equals(jobType) && !hasMaximum) {
                totalResult = 0L;
            }
            if (status == JobStatus.JOB_STATUS_COMPLETED) {
                completionPercentage = 100.0;
            }
            update.setCompletionPercentage(completionPercentage)
                    .setActiveWorkerCount(activeWorkers)
                    .setResult(totalResult);
            return update.build();
        }

        private synchronized PersistedJob checkpoint() {
            PersistedJob.Builder saved = PersistedJob.newBuilder().setJobId(jobId).setOriginalRequest(originalRequest)
                    .setStatus(status).setMessage(message);
            for (TaskProgress task : tasks.values()) {
                PersistedTask.Builder entry = PersistedTask.newBuilder().setRequest(task.taskRequest).setState(task.state)
                        .setResult(task.result).setAttemptNumber(task.attemptNumber);
                if (task.worker != null) entry.setWorker(task.worker);
                saved.addTasks(entry);
            }
            return saved.build();
        }
        private synchronized void pause() {
            if (paused) return;
            paused = true;
            for (StreamObserver<JobProgressUpdate> observer : List.copyOf(subscribers)) {
                try { observer.onError(Status.UNAVAILABLE.withDescription("Coordinator changed; reconnect with job ID " + jobId).asRuntimeException()); }
                catch (RuntimeException ignored) {}
            }
            subscribers.clear();
        }
        private void publish() { publish(true); }
        private void publish(boolean durable) {
            if (paused) return;
            if (durable) try { persist(this, false); }
            catch (RuntimeException failure) { localWorker.log("Checkpoint retry needed for " + jobId + ": " + failure.getMessage()); pause(); loadedEpoch = -1; return; }
            JobProgressUpdate update = snapshot();
            for (StreamObserver<JobProgressUpdate> subscriber : List.copyOf(subscribers)) {
                try {
                    subscriber.onNext(update);
                    if (isTerminal()) subscriber.onCompleted();
                } catch (RuntimeException cancelled) { subscribers.remove(subscriber); }
            }
            if (isTerminal()) subscribers.clear();
        }
    }
}
