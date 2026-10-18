package cs324.assignment2.client;

import cs324.assignment2.grpc.*;
import cs324.assignment2.job.grpc.JobProgressUpdate;
import cs324.assignment2.job.grpc.JobServiceGrpc;
import cs324.assignment2.job.grpc.JobStatus;
import cs324.assignment2.job.grpc.StreamJobProgressRequest;
import cs324.assignment2.job.grpc.SubmitJobRequest;
import io.grpc.*;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Reconnects discovery/progress using the original stable request ID. A retry
 * never creates a second logical job on a replacement coordinator. */
public final class ResilientJobClient {
    private ResilientJobClient() {}
    public static JobProgressUpdate run(String host, int port, SubmitJobRequest request,
                                        Consumer<JobProgressUpdate> progress, long timeoutMs) throws InterruptedException {
        String id = request.getRequestId().isBlank() ? UUID.randomUUID().toString() : request.getRequestId();
        SubmitJobRequest stable = request.toBuilder().setRequestId(id).build();
        ManagedChannel discovery = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        try {
            while (System.nanoTime() < deadline) {
                if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Job monitoring cancelled");
                ManagedChannel channel = null;
                try {
                    ClusterView view = WorkerDiscovery.discover(BootstrapServiceGrpc.newBlockingStub(discovery).withDeadlineAfter(2, TimeUnit.SECONDS));
                    if (!view.getReady() || !view.hasCoordinator()) { Thread.sleep(200); continue; }
                    WorkerInfo leader = view.getCoordinator();
                    channel = ManagedChannelBuilder.forAddress(leader.getHost(), leader.getPort()).usePlaintext().build();
                    var jobs = JobServiceGrpc.newBlockingStub(channel);
                    var accepted = jobs.withDeadlineAfter(10, TimeUnit.SECONDS).submitJob(stable);
                    long remaining = Math.max(1, deadline - System.nanoTime());
                    var stream = jobs.withDeadlineAfter(remaining, TimeUnit.NANOSECONDS).streamJobProgress(
                            StreamJobProgressRequest.newBuilder().setJobId(accepted.getJobId()).build());
                    while (stream.hasNext()) {
                        JobProgressUpdate update = stream.next(); progress.accept(update);
                        if (update.getStatus() == JobStatus.JOB_STATUS_COMPLETED) return update;
                        if (update.getStatus() == JobStatus.JOB_STATUS_FAILED || update.getStatus() == JobStatus.JOB_STATUS_CANCELLED)
                            throw new IllegalStateException(update.getMessage());
                    }
                } catch (StatusRuntimeException transientFailure) {
                    switch (transientFailure.getStatus().getCode()) {
                        case UNAVAILABLE, FAILED_PRECONDITION, NOT_FOUND, DEADLINE_EXCEEDED, ABORTED, CANCELLED -> { }
                        default -> throw transientFailure;
                    }
                } finally { if (channel != null) channel.shutdownNow(); }
                Thread.sleep(200);
            }
            throw new IllegalStateException("Job monitoring timed out; reconnect using request ID " + id);
        } finally { discovery.shutdownNow(); }
    }
}
