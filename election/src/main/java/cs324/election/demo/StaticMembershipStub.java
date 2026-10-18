package cs324.election.demo;

import cs324.election.proto.Worker;
import java.util.Arrays;
import java.util.List;

/** TEMPORARY DEMO STUB for Josh's bootstrap discovery. No registration,
 * health monitoring, dynamic joining, or membership reconfiguration occurs here. */
public final class StaticMembershipStub {
    private StaticMembershipStub() {}

    public static List<Worker> parse(String configuration) {
        return Arrays.stream(configuration.split(",")).map(value -> {
            String[] parts = value.split(":");
            if (parts.length != 3) throw new IllegalArgumentException("Use id:host:port");
            return Worker.newBuilder().setId(Integer.parseInt(parts[0])).setHost(parts[1])
                    .setPort(Integer.parseInt(parts[2])).build();
        }).toList();
    }
}
