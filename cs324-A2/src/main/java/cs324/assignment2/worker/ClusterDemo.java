package cs324.assignment2.worker;

import cs324.assignment2.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.*;
import java.util.concurrent.TimeUnit;

/** Discovery-based CLI for observing the real integrated services. Checkpoint
 * commands exercise recovery bytes only, not Aveshal's numerical job engine. */
public final class ClusterDemo {
    private ClusterDemo() {}
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("status [BOOTSTRAP_HOST PORT] | checkpoint HOST PORT KEY PAYLOAD | admit HOST PORT JOB_ID PAYLOAD [REMOTE_ASSIGNMENTS] | read [HOST PORT]");
            return;
        }
        if (args[0].equals("job")) {
            if (args.length < 6) throw new IllegalArgumentException("job HOST PORT TYPE START END [REQUEST_ID]");
            var request = cs324.assignment2.job.grpc.SubmitJobRequest.newBuilder().setJobType(args[3])
                    .putParameters("start", args[4]).putParameters("end", args[5])
                    .setRequestId(args.length > 6 ? args[6] : java.util.UUID.randomUUID().toString()).build();
            java.util.concurrent.atomic.AtomicInteger previous = new java.util.concurrent.atomic.AtomicInteger(-1);
            var result = cs324.assignment2.client.ResilientJobClient.run(args[1], Integer.parseInt(args[2]), request, update -> {
                int done = (int) update.getSubTasksList().stream().filter(t -> t.getState() == cs324.assignment2.job.grpc.SubTaskState.SUB_TASK_STATE_COMPLETED).count();
                if (previous.getAndSet(done) != done) System.out.println("JOB_TASKS_DONE=" + done + " PROGRESS=" + update.getCompletionPercentage());
            }, TimeUnit.MINUTES.toMillis(2));
            System.out.println("JOB_ID=" + result.getJobId());
            System.out.println("JOB_RESULT=" + result.getResult());
            return;
        }
        String host = args.length > 1 ? args[1] : "localhost";
        int port = args.length > 2 ? Integer.parseInt(args[2]) : 50051;
        ManagedChannel bootstrapChannel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        try {
            var bootstrap = BootstrapServiceGrpc.newBlockingStub(bootstrapChannel).withDeadlineAfter(3, TimeUnit.SECONDS);
            ClusterView view = cs324.assignment2.client.WorkerDiscovery.discover(bootstrap);
            if (args[0].equals("status")) {
                System.out.println("ACTIVE_WORKERS=" + bootstrap.getActiveWorkers(GetActiveWorkersRequest.getDefaultInstance()).getWorkersCount());
                System.out.println("EPOCH=" + view.getEpoch());
                System.out.println("COORDINATOR=" + (view.hasCoordinator() ? view.getCoordinator().getId() : -1));
                System.out.println("READY=" + view.getReady());
                System.out.println("VOTERS=" + view.getVotersList().stream().map(WorkerInfo::getId).toList());
                return;
            }
            if (!view.getReady() || !view.hasCoordinator()) throw new IllegalStateException("No ready coordinator; wait for election/recovery");
            WorkerInfo coordinator = view.getCoordinator();
            ManagedChannel workerChannel = ManagedChannelBuilder.forAddress(coordinator.getHost(), coordinator.getPort()).usePlaintext().build();
            try {
                var worker = WorkerServiceGrpc.newBlockingStub(workerChannel).withDeadlineAfter(10, TimeUnit.SECONDS);
                switch (args[0]) {
                    case "read" -> {
                        var snapshot = worker.readRecovery(RecoveryRequest.newBuilder().setEpoch(view.getEpoch()).build());
                        for (RecoveryRecord record : snapshot.getRecordsList()) {
                            if (record.getKey().startsWith("job/")) {
                                var job = cs324.assignment2.job.grpc.PersistedJob.parseFrom(record.getPayload());
                                System.out.println(record.getKey() + " = " + job.getStatus() + "; tasks=" + job.getTasksCount());
                            } else System.out.println(record.getKey() + " = " + record.getPayload().toStringUtf8());
                        }
                        System.out.println("RECORDS=" + snapshot.getRecordsCount());
                    }
                    case "checkpoint", "admit" -> {
                        if (args.length < 5) throw new IllegalArgumentException("Provide key/job ID and checkpoint payload");
                        var request = SaveCheckpointRequest.newBuilder().setKey(args[3]).setPayload(ByteString.copyFromUtf8(args[4]))
                                .setNewJob(args[0].equals("admit")).setRemoteAssignments(args.length > 5 ? Integer.parseInt(args[5]) : 0).build();
                        CheckpointAck ack = worker.saveCheckpoint(request);
                        System.out.println("CHECKPOINT_ACCEPTED=" + ack.getAccepted());
                        System.out.println(ack.getDetail());
                        System.out.println("Checkpoint demo only; no numerical job was executed.");
                    }
                    default -> throw new IllegalArgumentException("Unknown command " + args[0]);
                }
            } finally { workerChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); }
        } finally { bootstrapChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); }
    }
}
