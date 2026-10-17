package cs324.assignment2.bootstrap;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class BootstrapNode {

    public static final int GRPC_PORT = 50051;

    public static void main(String[] args) throws IOException, InterruptedException {
        BootstrapServiceImpl service = new BootstrapServiceImpl();

        Server server = ServerBuilder.forPort(GRPC_PORT)
                .addService(service)
                .build()
                .start();

        System.out.println("[Bootstrap] gRPC server started on port " + GRPC_PORT);
        System.out.println("[Bootstrap] Waiting for workers to register...");
        System.out.println("[Bootstrap] Heartbeat timeout = "
                + BootstrapServiceImpl.HEARTBEAT_TIMEOUT_MS + " ms");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[Bootstrap] Shutting down...");
            service.shutdown();
            server.shutdown();
            try {
                server.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));

        server.awaitTermination();
    }
}