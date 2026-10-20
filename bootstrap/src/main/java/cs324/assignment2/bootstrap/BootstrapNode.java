package cs324.assignment2.bootstrap;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class BootstrapNode {

    public static final int GRPC_PORT = 50051;

    public static void main(String[] args) throws IOException, InterruptedException {
        BootstrapServiceImpl service = new BootstrapServiceImpl();

        int listenPort = args.length == 0 ? GRPC_PORT : Integer.parseInt(args[0]);
        Server server = ServerBuilder.forPort(listenPort)
                .addService(service)
                .build()
                .start();

        System.out.println("[Bootstrap] gRPC server started on port " + listenPort);
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
