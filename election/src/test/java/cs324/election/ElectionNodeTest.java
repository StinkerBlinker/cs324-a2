package cs324.election;

import cs324.election.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.*;
import org.junit.jupiter.api.*;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

class ElectionNodeTest {
    private final List<ElectionNode> nodes = new ArrayList<>();
    private final List<Worker> workers = new ArrayList<>();
    @AfterEach void cleanup() { nodes.forEach(ElectionNode::close); }
    private void cluster(long... counters) throws Exception {
        for (int i = 0; i < counters.length; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                workers.add(Worker.newBuilder().setId(i + 1).setHost("localhost").setPort(socket.getLocalPort()).build());
            }
        }
        for (int i = 0; i < counters.length; i++) {
            // Ring: not an all-to-all overlay; survives one node disappearing.
            int previous = (i + counters.length - 1) % counters.length + 1;
            int next = (i + 1) % counters.length + 1;
            List<Integer> neighbours = counters.length == 1 ? List.of() : List.of(previous, next);
            ElectionNode node = new ElectionNode(workers.get(i), workers, neighbours, counters[i]);
            nodes.add(node); node.start();
        }
    }
    private void agree(int leader, int... alive) throws Exception {
        await(() -> {
            for (int id : alive) {
                NodeStatus s = nodes.get(id - 1).status();
                if (!s.hasCoordinator() || s.getCoordinator().getId() != leader) return false;
            }
            return nodes.get(leader - 1).status().getReady();
        });
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        fail("Cluster did not converge within 25 seconds");
    }
    @Test void lowestJacAndHighestIdTieBreak() throws Exception {
        cluster(4, 1, 1); agree(3, 1, 2, 3);
    }
    @Test void crashCoordinatorRecoversCheckpointOverGrpc() throws Exception {
        cluster(0, 0, 0); agree(3, 1, 2, 3);
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", workers.get(2).getPort()).usePlaintext().build();
        try {
            Ack ack = LeaderElectionGrpc.newBlockingStub(channel).withDeadlineAfter(8, TimeUnit.SECONDS)
                    .saveCheckpoint(Checkpoint.newBuilder().setKey("job-42")
                            .setPayload(ByteString.copyFromUtf8("input=1..100;task-1=done;task-2=pending")).build());
            assertTrue(ack.getAccepted());
        } finally { channel.shutdownNow(); }
        nodes.get(2).close(); agree(2, 1, 2);
        assertEquals("job-42", nodes.get(1).snapshot().getRecords(0).getKey());
        assertTrue(nodes.get(1).snapshot().getRecords(0).getPayload().toStringUtf8().contains("task-2=pending"));
    }
    @Test void staleAnnouncementAndDuplicateElectionAreRejected() throws Exception {
        cluster(0, 0, 0); agree(3, 1, 2, 3);
        Round round = nodes.get(0).status().getRound();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", workers.get(0).getPort()).usePlaintext().build();
        try {
            var stub = LeaderElectionGrpc.newBlockingStub(channel).withDeadlineAfter(2, TimeUnit.SECONDS);
            assertFalse(stub.receiveElection(Election.newBuilder().setRound(round)
                    .setInitiator(workers.get(round.getInitiatorId() - 1)).build()).getAccepted());
            assertFalse(stub.announceCoordinator(Coordinator.newBuilder().setRound(Round.getDefaultInstance())
                    .setWorker(workers.get(0)).build()).getAccepted());
            assertEquals(3, nodes.get(0).status().getCoordinator().getId());
        } finally { channel.shutdownNow(); }
    }
    @Test void fiveAcceptedJobsRotateLeaderUsingUpdatedJac() throws Exception {
        cluster(0, 0, 0); agree(3, 1, 2, 3);
        for (int i = 0; i < 5; i++) nodes.get(2).recordAcceptedJob(2);
        agree(2, 1, 2, 3);
        assertEquals(10, nodes.get(2).status().getJac());
    }
    @Test void noMajorityStopsWrites() throws Exception {
        cluster(0, 0, 0); agree(3, 1, 2, 3);
        nodes.get(0).close(); nodes.get(1).close();
        // Even before monitoring demotes the leader, replication must not acknowledge.
        try { assertFalse(nodes.get(2).checkpoint("x", ByteString.copyFromUtf8("y"))); }
        catch (IllegalStateException alreadyDemoted) { assertFalse(nodes.get(2).status().getReady()); }
        await(() -> !nodes.get(2).status().getReady());
    }
    @Test void fiveWorkerOverlayRecoversAfterCoordinatorAndInitiatorCrash() throws Exception {
        cluster(0, 0, 0, 0, 0); agree(5, 1, 2, 3, 4, 5);
        assertTrue(nodes.get(4).checkpoint("retained", ByteString.copyFromUtf8("result=328")));
        // Break the five-node ring at one point; four survivors remain connected.
        nodes.get(4).close(); agree(4, 1, 2, 3, 4);
        assertEquals("result=328", nodes.get(3).snapshot().getRecords(0).getPayload().toStringUtf8());
        nodes.get(0).startElection();
        nodes.get(0).close();
        // The election initiator never announces a result; the three survivors retry.
        agree(4, 2, 3, 4);
        assertTrue(nodes.get(3).checkpoint("second", ByteString.copyFromUtf8("continued")));
    }
}
