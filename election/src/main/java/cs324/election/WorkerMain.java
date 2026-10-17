package cs324.election;

import cs324.election.proto.*;
import cs324.election.demo.JobCheckpointStub;
import cs324.election.demo.StaticMembershipStub;
import com.google.protobuf.ByteString;
import io.grpc.*;
import java.util.*;
import java.util.concurrent.*;

/** Standalone election demo; bootstrap/job/GUI adapters are separate team work. */
public final class WorkerMain {
    private WorkerMain() {}
    public static void main(String[] args) throws Exception {
        if (args.length == 0) { usage(); return; }
        if (args[0].equals("status") || args[0].equals("save") || args[0].equals("save-demo") || args[0].equals("read")) {
            client(args); return;
        }
        if (args.length < 3 || args.length > 4) { usage(); return; }
        int id = Integer.parseInt(args[0]);
        System.out.println("[DEMO STUB] Static membership; bootstrap discovery is not connected.");
        List<Worker> membership = StaticMembershipStub.parse(args[1]);
        Worker self = membership.stream().filter(w -> w.getId() == id).findFirst().orElseThrow();
        List<Integer> neighbours = args[2].equals("-") ? List.of()
                : Arrays.stream(args[2].split(",")).map(Integer::parseInt).toList();
        ElectionNode node = new ElectionNode(self, membership, neighbours, args.length == 4 ? Long.parseLong(args[3]) : 0);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close));
        node.start();
        new CountDownLatch(1).await();
    }
    private static void client(String[] args) throws Exception {
        if (args.length < 3) { usage(); return; }
        ManagedChannel channel = ManagedChannelBuilder.forAddress(args[1], Integer.parseInt(args[2])).usePlaintext().build();
        try {
            var stub = LeaderElectionGrpc.newBlockingStub(channel).withDeadlineAfter(8, TimeUnit.SECONDS);
            switch (args[0]) {
                case "status" -> System.out.println(stub.getStatus(Empty.getDefaultInstance()));
                case "read" -> {
                    RecoverySnapshot snapshot = stub.readRecovery(Empty.getDefaultInstance());
                    for (RecoveryRecord record : snapshot.getRecordsList())
                        System.out.println(record.getKey() + " = " + record.getPayload().toStringUtf8());
                    System.out.println("Records: " + snapshot.getRecordsCount());
                }
                case "save" -> {
                    if (args.length != 5) throw new IllegalArgumentException("save HOST PORT KEY PAYLOAD");
                    Ack ack = stub.saveCheckpoint(Checkpoint.newBuilder().setKey(args[3])
                            .setPayload(ByteString.copyFromUtf8(args[4])).build());
                    System.out.println(ack.getAccepted() ? "Checkpoint replicated to a majority" : "Not acknowledged; outcome uncertain, retry same job ID");
                }
                case "save-demo" -> {
                    if (args.length != 4) throw new IllegalArgumentException("save-demo HOST PORT JOB_ID");
                    System.out.println("[DEMO STUB] Saving fabricated job state; no numerical job is executed.");
                    Ack ack = stub.saveCheckpoint(JobCheckpointStub.example(args[3]));
                    System.out.println(ack.getAccepted() ? "Checkpoint replicated to a majority" : "Not acknowledged; outcome uncertain, retry same job ID");
                }
                default -> usage();
            }
        } finally { channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); }
    }
    private static void usage() {
        System.out.println("Worker: ID id:host:port,id:host:port NEIGHBOUR_IDS [INITIAL_JAC]");
        System.out.println("Client: status HOST PORT | read HOST PORT | save HOST PORT KEY PAYLOAD");
        System.out.println("Temporary job stub: save-demo HOST PORT JOB_ID");
    }
}
