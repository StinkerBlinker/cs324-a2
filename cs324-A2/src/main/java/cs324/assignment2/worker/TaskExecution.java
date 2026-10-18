package cs324.assignment2.worker;

import cs324.assignment2.grpc.*;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/** Aveshal's measured range computation, shared by unary and streaming RPCs. */
final class TaskExecution {
    private TaskExecution() {}
    private interface Progress { void update(double percentage, long value); }
    private static void validate(ExecuteJobRequest req) {
        if (!req.getJobType().equalsIgnoreCase("SUM") && !req.getJobType().equalsIgnoreCase("PRIMESUM"))
            throw new IllegalArgumentException("Supported range job types are SUM and PRIMESUM");
        if (req.getStart() > req.getEnd()) throw new IllegalArgumentException("start must not exceed end");
    }
    static void execute(ExecuteJobRequest req, StreamObserver<ExecuteJobResponse> out) {
        try {
            validate(req);
            long result = calculate(req, (percentage,value) -> {});
            out.onNext(ExecuteJobResponse.newBuilder().setTaskId(req.getTaskId()).setResult(result).build()); out.onCompleted();
        } catch (RuntimeException error) { fail(out,error); }
    }
    static void stream(ExecuteJobRequest req, StreamObserver<ExecuteJobProgress> out) {
        try {
            validate(req);
            long result = calculate(req, (percentage,value) -> out.onNext(ExecuteJobProgress.newBuilder()
                    .setTaskId(req.getTaskId()).setCompletionPercentage(percentage).setResult(value).build()));
            out.onNext(ExecuteJobProgress.newBuilder().setTaskId(req.getTaskId()).setCompletionPercentage(100)
                    .setResult(result).setIsFinished(true).build()); out.onCompleted();
        } catch (RuntimeException error) { fail(out,error); }
    }
    private static long calculate(ExecuteJobRequest req, Progress progress) {
        long total=(long)req.getEnd()-req.getStart()+1;
        long interval=Math.max(1,(total+99)/100), done=0, result=0;
        boolean primes=req.getJobType().equalsIgnoreCase("PRIMESUM");
        for (long number=req.getStart(); number<=req.getEnd(); number++) {
            if (Thread.currentThread().isInterrupted()) throw new IllegalStateException("Worker computation interrupted");
            if (!primes || isPrime(number)) result=Math.addExact(result,number);
            if (++done%interval==0 || done==total) progress.update(done*100.0/total,result);
        }
        return result;
    }
    private static boolean isPrime(long value) {
        if(value<2)return false;
        if(value%2==0)return value==2;
        if(value%3==0)return value==3;
        for(long divisor=5;divisor<=value/divisor;divisor+=6)
            if(value%divisor==0 || value%(divisor+2)==0)return false;
        return true;
    }
    private static void fail(StreamObserver<?> out, RuntimeException error) {
        Status status=error instanceof IllegalArgumentException ? Status.INVALID_ARGUMENT
                : error instanceof ArithmeticException ? Status.OUT_OF_RANGE : Status.CANCELLED;
        out.onError(status.withDescription(error.getMessage()).asRuntimeException());
    }
}
