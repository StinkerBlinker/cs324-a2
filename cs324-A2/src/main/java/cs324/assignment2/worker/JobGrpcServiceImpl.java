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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class JobGrpcServiceImpl extends JobServiceGrpc.JobServiceImplBase
        implements AutoCloseable {

    private final WorkerNode localWorker;
    private final Map<String, JobExecution> jobs = new ConcurrentHashMap<>();
    private final ExecutorService dispatchPool = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r, "job-dispatch");
        thread.setDaemon(true);
        return thread;
    });

    public JobGrpcServiceImpl(WorkerNode localWorker) {
        this.localWorker = localWorker;
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
        if (!"SUM".equals(jobType) && !"PRIMESUM".equals(jobType)) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Supported job types are SUM and PRIMESUM")
                    .asRuntimeException());
            return;
        }

        int start;
        int end;
        try {
            start = Integer.parseInt(request.getParametersMap().getOrDefault("start", ""));
            end = Integer.parseInt(request.getParametersMap().getOrDefault("end", ""));
        } catch (NumberFormatException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("parameters must include integer start and end values")
                    .asRuntimeException());
            return;
        }
        if (start > end) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("start must be less than or equal to end")
                    .asRuntimeException());
            return;
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

        long rangeSize = (long) end - start + 1;
        int assignmentCount = (int) Math.min(rangeSize, workers.size());
        List<TaskAssignment> assignments = createAssignments(
                request, jobType, start, end, workers.subList(0, assignmentCount));
        String jobId = UUID.randomUUID().toString();
        JobExecution job = new JobExecution(jobId, assignments);
        jobs.put(jobId, job);

        responseObserver.onNext(SubmitJobResponse.newBuilder()
                .setJobId(jobId)
                .setStatus(JobStatus.JOB_STATUS_QUEUED)
                .setMessage("Job accepted and queued")
                .build());
        responseObserver.onCompleted();

        dispatchPool.execute(() -> {
            job.start();
            for (TaskAssignment assignment : assignments) {
                dispatchPool.execute(() -> dispatch(job, assignment));
            }
        });
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
        long rangeSize = (long) end - start + 1;
        long baseSize = rangeSize / workers.size();
        long extra = rangeSize % workers.size();
        long nextStart = start;
        List<TaskAssignment> assignments = new ArrayList<>(workers.size());

        for (int i = 0; i < workers.size(); i++) {
            long taskSize = baseSize + (i < extra ? 1 : 0);
            int taskStart = Math.toIntExact(nextStart);
            int taskEnd = Math.toIntExact(nextStart + taskSize - 1);
            String taskId = UUID.randomUUID().toString();
            ExecuteJobRequest taskRequest = ExecuteJobRequest.newBuilder()
                    .setJobType(jobType)
                    .setStart(taskStart)
                    .setEnd(taskEnd)
                    .setTaskId(taskId)
                    .putAllParameters(originalRequest.getParametersMap())
                    .setPayload(originalRequest.getPayload())
                    .build();
            assignments.add(new TaskAssignment(workers.get(i), taskRequest));
            nextStart += taskSize;
        }
        return assignments;
    }

    private void dispatch(JobExecution job, TaskAssignment assignment) {
        job.markRunning(assignment.taskRequest.getTaskId());
        ManagedChannel channel;
        try {
            channel = ManagedChannelBuilder
                    .forAddress(assignment.worker.getHost(), assignment.worker.getPort())
                    .usePlaintext()
                    .build();
        } catch (RuntimeException e) {
            job.markFailed(assignment.taskRequest.getTaskId(),
                    "Could not contact worker " + assignment.worker.getId()
                            + ": " + e.getMessage());
            return;
        }

        try {
            WorkerServiceGrpc.newStub(channel).executeJobWithProgress(
                    assignment.taskRequest,
                    new StreamObserver<ExecuteJobProgress>() {
                        @Override
                        public void onNext(ExecuteJobProgress update) {
                            if (!update.getErrorMessage().isEmpty()) {
                                job.markFailed(assignment.taskRequest.getTaskId(),
                                        update.getErrorMessage());
                            } else if (update.getIsFinished()) {
                                job.markCompleted(assignment.taskRequest.getTaskId(),
                                        update.getResult());
                            } else {
                                job.updateProgress(assignment.taskRequest.getTaskId(),
                                        update.getCompletionPercentage(), update.getResult());
                            }
                        }

                        @Override
                        public void onError(Throwable error) {
                            job.markFailed(assignment.taskRequest.getTaskId(),
                                    "Worker " + assignment.worker.getId()
                                            + " failed: " + error.getMessage());
                            channel.shutdown();
                        }

                        @Override
                        public void onCompleted() {
                            job.failIfIncomplete(assignment.taskRequest.getTaskId());
                            channel.shutdown();
                        }
                    });
        } catch (RuntimeException e) {
            job.markFailed(assignment.taskRequest.getTaskId(),
                    "Could not contact worker " + assignment.worker.getId()
                            + ": " + e.getMessage());
            channel.shutdown();
        }
    }

    @Override
    public void close() {
        dispatchPool.shutdownNow();
    }

    private static final class TaskAssignment {
        private final WorkerInfo worker;
        private final ExecuteJobRequest taskRequest;

        private TaskAssignment(WorkerInfo worker, ExecuteJobRequest taskRequest) {
            this.worker = worker;
            this.taskRequest = taskRequest;
        }
    }

    private static final class TaskProgress {
        private final WorkerInfo worker;
        private final String taskId;
        private SubTaskState state = SubTaskState.SUB_TASK_STATE_PENDING;
        private WorkerStatus workerStatus = WorkerStatus.WORKER_STATUS_IDLE;
        private double completionPercentage;
        private long result;
        private String message = "Waiting for assignment";

        private TaskProgress(TaskAssignment assignment) {
            worker = assignment.worker;
            taskId = assignment.taskRequest.getTaskId();
        }
    }

    private static final class JobExecution {
        private final String jobId;
        private final Map<String, TaskProgress> tasks = new LinkedHashMap<>();
        private final List<StreamObserver<JobProgressUpdate>> subscribers = new ArrayList<>();
        private JobStatus status = JobStatus.JOB_STATUS_QUEUED;
        private String message = "Job accepted and queued";

        private JobExecution(String jobId, List<TaskAssignment> assignments) {
            this.jobId = jobId;
            for (TaskAssignment assignment : assignments) {
                TaskProgress task = new TaskProgress(assignment);
                tasks.put(task.taskId, task);
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
            status = JobStatus.JOB_STATUS_RUNNING;
            message = "Job is running";
            publish();
        }

        private synchronized void markRunning(String taskId) {
            TaskProgress task = tasks.get(taskId);
            if (task == null || task.state != SubTaskState.SUB_TASK_STATE_PENDING) {
                return;
            }
            task.state = SubTaskState.SUB_TASK_STATE_RUNNING;
            task.workerStatus = WorkerStatus.WORKER_STATUS_ACTIVE;
            task.message = "Executing";
            publish();
        }

        private synchronized void updateProgress(String taskId, double percentage, long result) {
            TaskProgress task = tasks.get(taskId);
            if (task == null || task.state != SubTaskState.SUB_TASK_STATE_RUNNING) {
                return;
            }
            task.completionPercentage = Math.max(0.0, Math.min(100.0, percentage));
            task.result = result;
            publish();
        }

        private synchronized void markCompleted(String taskId, long result) {
            TaskProgress task = tasks.get(taskId);
            if (task == null || isTaskTerminal(task.state)) {
                return;
            }
            task.result = result;
            task.completionPercentage = 100.0;
            task.state = SubTaskState.SUB_TASK_STATE_COMPLETED;
            task.workerStatus = WorkerStatus.WORKER_STATUS_IDLE;
            task.message = "Completed";
            updateTerminalStatus();
            publish();
        }

        private synchronized void markFailed(String taskId, String errorMessage) {
            TaskProgress task = tasks.get(taskId);
            if (task == null || isTaskTerminal(task.state)) {
                return;
            }
            task.state = SubTaskState.SUB_TASK_STATE_FAILED;
            task.workerStatus = WorkerStatus.WORKER_STATUS_FAILED;
            task.message = errorMessage;
            message = errorMessage;
            updateTerminalStatus();
            publish();
        }

        private synchronized void failIfIncomplete(String taskId) {
            TaskProgress task = tasks.get(taskId);
            if (task != null && !isTaskTerminal(task.state)) {
                markFailed(taskId, "Worker stream ended before the sub-task completed");
            }
        }

        private void updateTerminalStatus() {
            boolean allTerminal = tasks.values().stream().allMatch(task -> isTaskTerminal(task.state));
            if (!allTerminal) {
                status = JobStatus.JOB_STATUS_RUNNING;
                return;
            }
            boolean failed = tasks.values().stream()
                    .anyMatch(task -> task.state == SubTaskState.SUB_TASK_STATE_FAILED);
            status = failed ? JobStatus.JOB_STATUS_FAILED : JobStatus.JOB_STATUS_COMPLETED;
            message = failed ? "One or more sub-tasks failed" : "Job completed";
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
            double totalPercentage = 0.0;
            long totalResult = 0L;
            int activeWorkers = 0;

            for (TaskProgress task : tasks.values()) {
                totalPercentage += task.completionPercentage;
                totalResult += task.result;
                if (task.workerStatus == WorkerStatus.WORKER_STATUS_ACTIVE) {
                    activeWorkers++;
                }
                update.addWorkers(WorkerStatusMetric.newBuilder()
                        .setWorkerId(task.worker.getId())
                        .setStatus(task.workerStatus)
                        .setCompletionPercentage(task.completionPercentage)
                        .setActiveSubTaskCount(
                                task.state == SubTaskState.SUB_TASK_STATE_RUNNING ? 1 : 0)
                        .setCompletedSubTaskCount(
                                task.state == SubTaskState.SUB_TASK_STATE_COMPLETED ? 1 : 0)
                        .setStatusMessage(task.message));
                update.addSubTasks(SubTaskStatus.newBuilder()
                        .setSubTaskId(task.taskId)
                        .setState(task.state)
                        .setCompletionPercentage(task.completionPercentage)
                        .setWorkerId(task.worker.getId())
                        .setMessage(task.message));
            }
            update.setCompletionPercentage(totalPercentage / tasks.size())
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
