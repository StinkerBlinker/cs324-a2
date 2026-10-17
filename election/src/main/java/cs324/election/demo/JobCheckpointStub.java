package cs324.election.demo;

import com.google.protobuf.ByteString;
import cs324.election.proto.Checkpoint;

/** TEMPORARY DEMO STUB for Aveshal's job state. This is a fabricated checkpoint
 * fixture: no job is submitted, executed, reassigned or resumed by this class. */
public final class JobCheckpointStub {
    private JobCheckpointStub() {}

    public static Checkpoint example(String jobId) {
        if (jobId.isBlank()) throw new IllegalArgumentException("Demo job ID required");
        return Checkpoint.newBuilder().setKey(jobId).setPayload(ByteString.copyFromUtf8(
                "STUB_ONLY;operation=PRIMESUM;input=1..100;task-1=done;task-2=pending"))
                .build();
    }
}
