package cs324.assignment2.worker;

import cs324.assignment2.grpc.*;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/** Measured range/list computation, shared by unary and streaming RPCs. */
final class TaskExecution {
    private TaskExecution() {}
    private interface Progress { void update(double percentage, long value); }
    private static void validate(ExecuteJobRequest req) {
        if (!java.util.Set.of("SUM", "PRIMESUM", "MAX", "PRIMECOUNT").contains(req.getJobType().toUpperCase()))
            throw new IllegalArgumentException("Supported job types are SUM, PRIMESUM, MAX and PRIMECOUNT");
        if (req.getValuesCount() == 0 && req.getStart() > req.getEnd()) throw new IllegalArgumentException("start must not exceed end");
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
        boolean list=req.getValuesCount()>0;
        long total=list ? req.getValuesCount() : (long)req.getEnd()-req.getStart()+1;
        boolean maximum=req.getJobType().equalsIgnoreCase("MAX");
        boolean count=req.getJobType().equalsIgnoreCase("PRIMECOUNT");
        long interval=Math.max(1,(total+99)/100), done=0, result=maximum ? Long.MIN_VALUE : 0;
        boolean primes=req.getJobType().equalsIgnoreCase("PRIMESUM");
        for (long index=0; index<total; index++) {
            if (Thread.currentThread().isInterrupted() || Context.current().isCancelled()) throw new IllegalStateException("Worker computation interrupted");
            long number=list ? req.getValues(Math.toIntExact(index)) : (long)req.getStart()+index;
            if (maximum) result=Math.max(result,number);
            else if (count) { if (isPrime(number)) result=Math.addExact(result,1); }
            else if (!primes || isPrime(number)) result=Math.addExact(result,number);
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
