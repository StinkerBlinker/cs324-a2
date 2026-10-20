package cs324.assignment2.client;

import cs324.assignment2.grpc.*;
import io.grpc.*;
import java.util.concurrent.TimeUnit;

/** Bootstrap supplies endpoints; election metadata comes exclusively from workers. */
public final class WorkerDiscovery {
    private WorkerDiscovery() {}
    public static ClusterView discover(BootstrapServiceGrpc.BootstrapServiceBlockingStub registry) {
        var workers = registry.getActiveWorkers(GetActiveWorkersRequest.getDefaultInstance()).getWorkersList();
        ClusterView newest = ClusterView.getDefaultInstance();
        for (WorkerInfo worker : workers) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(worker.getHost(), worker.getPort()).usePlaintext().build();
            try {
                ClusterView answer = WorkerServiceGrpc.newBlockingStub(channel).withDeadlineAfter(700, TimeUnit.MILLISECONDS)
                        .getClusterView(GetClusterViewRequest.getDefaultInstance());
                if (answer.getEpoch() > newest.getEpoch()
                        || (answer.getEpoch() == newest.getEpoch() && answer.getReady())) newest = answer;
            } catch (StatusRuntimeException ignored) { /* Consult the remaining active endpoints. */ }
            finally { channel.shutdownNow(); }
        }
        return newest;
    }
}
