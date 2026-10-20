package cs324.assignment2.worker;

import cs324.assignment2.bootstrap.BootstrapServiceImpl;
import cs324.assignment2.grpc.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class WorkerLifecycleTest {
    @Test void coldRegistrationHasMoreTimeThanSteadyStateProbes() throws Exception {
        BootstrapServiceImpl registry = new BootstrapServiceImpl() {
            @Override public void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> out) {
                try { Thread.sleep(500); }
                catch (InterruptedException error) { Thread.currentThread().interrupt(); out.onError(error); return; }
                super.registerWorker(request, out);
            }
        };
        Server server = ServerBuilder.forPort(0).addService(registry).build().start();
        WorkerNode worker = new WorkerNode("localhost", server.getPort(), 0, 0, 200, 1000, 400);
        try {
            worker.start();
            assertEquals(1, worker.getMyId(), "Cold registration must survive a delay longer than a 200ms health probe");
            assertTrue(worker.bootstrap().getCallOptions().getDeadline().timeRemaining(TimeUnit.MILLISECONDS) <= 200);
        } finally { worker.crash(); server.shutdownNow(); registry.shutdown(); }
    }
    @Test void stoppedWorkerCannotCreateAnotherPeerChannel() {
        WorkerNode worker = new WorkerNode();
        worker.crash();
        WorkerInfo peer = WorkerInfo.newBuilder().setId(2).setHost("localhost").setPort(54321).build();
        StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, () -> worker.peer(peer));
        assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
    }
}
