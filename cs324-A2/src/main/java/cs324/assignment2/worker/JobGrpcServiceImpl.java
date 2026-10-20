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
import cs324.assignment2.job.grpc.SubTaskStatus;
import cs324.assignment2.job.grpc.SubmitJobRequest;
import cs324.assignment2.job.grpc.SubmitJobResponse;
import cs324.assignment2.job.grpc.WorkerStatus;
import cs324.assignment2.job.grpc.WorkerStatusMetric;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class JobGrpcServiceImpl extends JobServiceGrpc.JobServiceImplBase
        implements AutoCloseable {

    private static final int TASKS_PER_WORKER = 20;
    private static final int MAX_TASK_COUNT = 10_000;
    private static final long TASK_DEADLINE_MS = TimeUnit.SECONDS.toMillis(60);
    private static final long TASK_SLOW_AFTER_MS = TimeUnit.SECONDS.toMillis(45);
    private static final long TASK_HEARTBEAT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

    private final WorkerNode localWorker;
    private final long taskDeadlineNanos;
    private final long taskSlowAfterNanos;
    private final long taskHeartbeatTimeoutNanos;
    private final Map<String, JobExecution> jobs = new ConcurrentHashMap<>();
    private final ExecutorService dispatchPool = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r, "job-dispatch");
        thread.setDaemon(true);
        return thread;
    });
    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "job-watchdog");
        thread.setDaemon(true);
        return thread;
    });

    public JobGrpcServiceImpl(WorkerNode localWorker) {
        this(localWorker, TASK_DEADLINE_MS, TASK_SLOW_AFTER_MS, TASK_HEARTBEAT_TIMEOUT_MS);
    }

    JobGrpcServiceImpl(WorkerNode localWorker, long taskDeadlineMs,
                       long taskSlowAfterMs, long taskHeartbeatTimeoutMs) {
        if (taskDeadlineMs <= 0 || taskSlowAfterMs <= 0
                || taskSlowAfterMs >= taskDeadlineMs || taskHeartbeatTimeoutMs <= 0) {
            throw new IllegalArgumentException("Task monitoring thresholds must be positive "
                    + "and the slow threshold must be below the deadline");
        }
        this.localWorker = localWorker;
        taskDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(taskDeadlineMs);
        taskSlowAfterNanos = TimeUnit.MILLISECONDS.toNanos(taskSlowAfterMs);
        taskHeartbeatTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(taskHeartbeatTimeoutMs);
        watchdog.scheduleAtFixedRate(this::checkTaskHealth, 100, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
        if (localWorker.getMyId() < 0 || localWorker.getMyCoordinatorId() != localWorker.getMyId()) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("This worker is not the current coordinator")
                    .asRuntimeException());
            return;
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
        String jobId = UUID.randomUUID().toString();
        JobExecution job = new JobExecution(jobId, jobType, assignments, workers,
                taskDeadlineNanos, taskSlowAfterNanos);
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
        TaskAttempt attempt = job.currentAttempt(task);
        if (attempt == null) {
            return;
        }
        if (!job.markStarted(attempt, System.nanoTime())) {
            return;
        }
        ManagedChannel channel;
        try {
            channel = ManagedChannelBuilder.forAddress(
                            attempt.worker.getHost(), attempt.worker.getPort())
                    .usePlaintext()
                    .build();
        } catch (RuntimeException e) {
            handleWorkerFailure(job, attempt,
                    "Could not contact worker " + attempt.worker.getId()
                            + ": " + e.getMessage());
            return;
        }

        try {
            AtomicReference<ClientCallStreamObserver<ExecuteJobRequest>> call =
                    new AtomicReference<>();
            job.setCancellationAction(attempt, () -> {
                ClientCallStreamObserver<ExecuteJobRequest> activeCall = call.get();
                if (activeCall != null) {
                    activeCall.cancel("Task deadline or progress timeout exceeded", null);
                }
                channel.shutdownNow();
            });
            WorkerServiceGrpc.newStub(channel)
                    .withDeadlineAfter(TASK_DEADLINE_MS + TimeUnit.SECONDS.toMillis(5),
                            TimeUnit.MILLISECONDS)
                    .executeJobWithProgress(
                    task.taskRequest,
                    new ClientResponseObserver<ExecuteJobRequest, ExecuteJobProgress>() {
                        @Override
                        public void beforeStart(
                                ClientCallStreamObserver<ExecuteJobRequest> requestStream) {
                            call.set(requestStream);
                        }

                        @Override
                        public void onNext(ExecuteJobProgress update) {
                            if (!update.getErrorMessage().isEmpty()) {
                                handleWorkerFailure(job, attempt, update.getErrorMessage());
                            } else if (update.getHeartbeat()) {
                                if (job.updateHeartbeat(attempt, System.nanoTime())) {
                                    schedule(job);
                                }
                            } else if (update.getIsFinished()) {
                                if (job.completeTask(attempt, update.getResult(),
                                        System.nanoTime())) {
                                    schedule(job);
                                }
                            } else {
                                if (job.updateProgress(attempt,
                                        update.getCompletionPercentage(), update.getResult(),
                                        System.nanoTime())) {
                                    schedule(job);
                                }
                            }
                        }

                        @Override
                        public void onError(Throwable error) {
                            if (Status.fromThrowable(error).getCode() == Status.Code.DEADLINE_EXCEEDED) {
                                if (job.failTask(attempt, "Task exceeded its 60-second deadline",
                                        false, System.nanoTime())) {
                                    schedule(job);
                                }
                            } else {
                                handleWorkerFailure(job, attempt,
                                        "Worker " + attempt.worker.getId()
                                                + " failed: " + error.getMessage());
                            }
                            channel.shutdown();
                        }

                        @Override
                        public void onCompleted() {
                            channel.shutdown();
                            if (job.clearCancellationAction(attempt)
                                    && job.failIfIncomplete(attempt)) {
                                refreshWorkersAndSchedule(job);
                            } else {
                                schedule(job);
                            }
                        }
                    });
        } catch (RuntimeException e) {
            handleWorkerFailure(job, attempt,
                    "Could not contact worker " + attempt.worker.getId()
                            + ": " + e.getMessage());
            channel.shutdown();
        }
    }

    private void handleWorkerFailure(JobExecution job, TaskAttempt attempt, String errorMessage) {
        if (job.requeueAfterWorkerFailure(attempt, errorMessage)) {
            refreshWorkersAndSchedule(job);
        }
    }

    private void checkTaskHealth() {
        long now = System.nanoTime();
        for (JobExecution job : jobs.values()) {
            if (job.checkTaskHealth(now, taskDeadlineNanos, taskSlowAfterNanos,
                    taskHeartbeatTimeoutNanos)) {
                schedule(job);
            }
        }
    }

    private void refreshWorkersAndSchedule(JobExecution job) {
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
        for (TaskProgress task : job.assignPendingTasks()) {
            dispatchPool.execute(() -> dispatch(job, task));
        }
    }

    @Override
    public void close() {
        watchdog.shutdownNow();
        dispatchPool.shutdownNow();
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
        private long startedAtNanos;
        private long lastProgressAtNanos;
        private long lastHeartbeatAtNanos;
        private Runnable cancellationAction;

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

    private static final class JobExecution {
        private final String jobId;
        private final String jobType;
        private final long taskDeadlineNanos;
        private final long taskSlowAfterNanos;
        private final Map<String, TaskProgress> tasks = new LinkedHashMap<>();
        private final ArrayDeque<TaskProgress> pendingTasks = new ArrayDeque<>();
        private final List<StreamObserver<JobProgressUpdate>> subscribers = new ArrayList<>();
        private final Map<Integer, WorkerInfo> workers = new LinkedHashMap<>();
        private final Map<Integer, String> activeTasksByWorker = new LinkedHashMap<>();
        private final Set<Integer> failedWorkerIds = new HashSet<>();
        private JobStatus status = JobStatus.JOB_STATUS_QUEUED;
        private String message = "Job accepted and queued";

        private JobExecution(String jobId, String jobType, List<TaskAssignment> assignments,
                             List<WorkerInfo> availableWorkers, long taskDeadlineNanos,
                             long taskSlowAfterNanos) {
            this.jobId = jobId;
            this.jobType = jobType;
            this.taskDeadlineNanos = taskDeadlineNanos;
            this.taskSlowAfterNanos = taskSlowAfterNanos;
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
                task.startedAtNanos = 0L;
                task.lastProgressAtNanos = 0L;
                task.lastHeartbeatAtNanos = 0L;
                task.cancellationAction = null;
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

        private synchronized boolean markStarted(TaskAttempt attempt, long nowNanos) {
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (!isCurrentAttempt(task, attempt)) {
                return false;
            }
            task.startedAtNanos = nowNanos;
            task.lastProgressAtNanos = nowNanos;
            task.lastHeartbeatAtNanos = nowNanos;
            return true;
        }

        private synchronized void setCancellationAction(TaskAttempt attempt, Runnable action) {
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (isCurrentAttempt(task, attempt)) {
                task.cancellationAction = action;
            } else {
                action.run();
            }
        }

        private synchronized boolean clearCancellationAction(TaskAttempt attempt) {
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (!isCurrentAttempt(task, attempt)) {
                return false;
            }
            task.cancellationAction = null;
            return true;
        }

        private synchronized boolean updateProgress(TaskAttempt attempt,
                                                    double percentage, long result,
                                                    long nowNanos) {
            if (isTerminal()) {
                return false;
            }
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (!isCurrentAttempt(task, attempt)) {
                return false;
            }
            if (nowNanos - task.startedAtNanos >= taskDeadlineNanos) {
                return failTaskLocked(task, "Task exceeded its 60-second deadline", false);
            }
            task.completionPercentage = Math.max(0.0, Math.min(100.0, percentage));
            task.result = result;
            task.lastProgressAtNanos = nowNanos;
            task.lastHeartbeatAtNanos = nowNanos;
            boolean approachingDeadline = nowNanos - task.startedAtNanos >= taskSlowAfterNanos;
            task.state = approachingDeadline
                    ? SubTaskState.SUB_TASK_STATE_SLOW : SubTaskState.SUB_TASK_STATE_RUNNING;
            task.workerStatus = approachingDeadline
                    ? WorkerStatus.WORKER_STATUS_SLOW : WorkerStatus.WORKER_STATUS_ACTIVE;
            task.message = approachingDeadline
                    ? "Progress is slower than expected" : "Executing";
            updateTerminalStatus();
            publish();
            return false;
        }

        private synchronized boolean updateHeartbeat(TaskAttempt attempt, long nowNanos) {
            if (isTerminal()) {
                return false;
            }
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (!isCurrentAttempt(task, attempt)) {
                return false;
            }
            if (nowNanos - task.startedAtNanos >= taskDeadlineNanos) {
                return failTaskLocked(task, "Task exceeded its 60-second deadline", false);
            }
            task.lastHeartbeatAtNanos = nowNanos;
            return false;
        }

        private synchronized boolean completeTask(TaskAttempt attempt, long result,
                                                  long nowNanos) {
            if (isTerminal()) {
                return false;
            }
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (!isCurrentAttempt(task, attempt)) {
                return false;
            }
            if (nowNanos - task.startedAtNanos >= taskDeadlineNanos) {
                return failTaskLocked(task, "Task exceeded its 60-second deadline", false);
            }
            task.result = result;
            task.completionPercentage = 100.0;
            task.state = SubTaskState.SUB_TASK_STATE_COMPLETED;
            task.workerStatus = WorkerStatus.WORKER_STATUS_IDLE;
            task.message = "Completed";
            task.cancellationAction = null;
            activeTasksByWorker.remove(attempt.worker.getId(), task.taskId);
            failIfNoWorkersCanContinue();
            updateTerminalStatus();
            publish();
            return true;
        }

        private synchronized boolean failTask(TaskAttempt attempt, String reason,
                                              boolean workerUnavailable, long nowNanos) {
            TaskProgress task = tasks.get(attempt.task.taskId);
            if (!isCurrentAttempt(task, attempt)) {
                return false;
            }
            String failureReason = nowNanos - task.startedAtNanos >= taskDeadlineNanos
                    ? "Task exceeded its 60-second deadline" : reason;
            return failTaskLocked(task, failureReason, workerUnavailable);
        }

        private boolean failTaskLocked(TaskProgress task, String reason,
                                       boolean workerUnavailable) {
            Runnable cancel = task.cancellationAction;
            task.cancellationAction = null;
            task.state = SubTaskState.SUB_TASK_STATE_FAILED;
            task.workerStatus = WorkerStatus.WORKER_STATUS_FAILED;
            task.message = reason;
            if (task.worker != null) {
                int workerId = task.worker.getId();
                activeTasksByWorker.remove(workerId, task.taskId);
                if (workerUnavailable) {
                    failedWorkerIds.add(workerId);
                    workers.remove(workerId);
                }
            }
            failIfNoWorkersCanContinue();
            updateTerminalStatus();
            publish();
            if (cancel != null) {
                cancel.run();
            }
            return true;
        }

        private synchronized boolean checkTaskHealth(long nowNanos, long deadlineNanos,
                                                     long slowAfterNanos,
                                                     long heartbeatTimeoutNanos) {
            if (isTerminal()) {
                return false;
            }
            boolean changed = false;
            long progressLagThresholdNanos = Math.min(slowAfterNanos,
                    heartbeatTimeoutNanos / 2);
            for (TaskProgress task : tasks.values()) {
                if (task.state != SubTaskState.SUB_TASK_STATE_RUNNING
                        && task.state != SubTaskState.SUB_TASK_STATE_SLOW) {
                    continue;
                }
                if (task.startedAtNanos == 0L) {
                    continue;
                }
                long elapsedNanos = nowNanos - task.startedAtNanos;
                if (elapsedNanos >= deadlineNanos) {
                    changed |= failTaskLocked(task, "Task exceeded its 60-second deadline", false);
                    continue;
                }
                if (nowNanos - task.lastHeartbeatAtNanos >= heartbeatTimeoutNanos) {
                    changed |= failTaskLocked(task,
                            "Worker stopped sending task heartbeats", true);
                    continue;
                }
                boolean slow = elapsedNanos >= slowAfterNanos
                        || nowNanos - task.lastProgressAtNanos >= progressLagThresholdNanos;
                if (slow && task.state != SubTaskState.SUB_TASK_STATE_SLOW) {
                    task.state = SubTaskState.SUB_TASK_STATE_SLOW;
                    task.workerStatus = WorkerStatus.WORKER_STATUS_SLOW;
                    task.message = "Progress is slower than expected";
                    changed = true;
                } else if (!slow && task.state == SubTaskState.SUB_TASK_STATE_SLOW) {
                    task.state = SubTaskState.SUB_TASK_STATE_RUNNING;
                    task.workerStatus = WorkerStatus.WORKER_STATUS_ACTIVE;
                    task.message = "Executing";
                    changed = true;
                }
            }
            if (changed) {
                updateTerminalStatus();
                publish();
            }
            return changed;
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
            Runnable cancel = task.cancellationAction;
            task.cancellationAction = null;
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
            if (cancel != null) {
                cancel.run();
            }
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
                    && (task.state == SubTaskState.SUB_TASK_STATE_RUNNING
                    || task.state == SubTaskState.SUB_TASK_STATE_SLOW)
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
                boolean slow = tasks.values().stream()
                        .anyMatch(task -> task.state == SubTaskState.SUB_TASK_STATE_SLOW);
                status = slow ? JobStatus.JOB_STATUS_SLOW : JobStatus.JOB_STATUS_RUNNING;
                message = slow ? "One or more tasks are progressing slowly" : "Job is running";
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
            return status == JobStatus.JOB_STATUS_COMPLETED
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
                            || task.state == SubTaskState.SUB_TASK_STATE_RUNNING
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
                                    task.state == SubTaskState.SUB_TASK_STATE_RUNNING
                                            || task.state == SubTaskState.SUB_TASK_STATE_SLOW
                                            ? 1 : 0)
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

        private void publish() {
            JobProgressUpdate update = snapshot();
            for (StreamObserver<JobProgressUpdate> subscriber : List.copyOf(subscribers)) {
                subscriber.onNext(update);
                if (isTerminal()) {
                    subscriber.onCompleted();
                }
            }
            if (isTerminal()) {
                subscribers.clear();
            }
        }
    }
}
