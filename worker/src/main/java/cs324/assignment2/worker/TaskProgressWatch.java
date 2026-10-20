package cs324.assignment2.worker;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Time since measured work advanced, rather than total task duration.
 * Slow/stalled are suspicions; a timeout cannot prove a machine crashed. */
final class TaskProgressWatch {
    enum State { ACTIVE, SLOW, STALLED }
    private final LongSupplier clock;
    private final long slowNanos, stalledNanos;
    private long lastAdvance;
    private double highest = -1;

    TaskProgressWatch() { this(System::nanoTime, 10_000, 60_000); }
    TaskProgressWatch(LongSupplier clock, long slowMs, long stalledMs) {
        if (slowMs <= 0 || stalledMs <= slowMs) throw new IllegalArgumentException("Invalid progress timeouts");
        this.clock = clock;
        slowNanos = TimeUnit.MILLISECONDS.toNanos(slowMs);
        stalledNanos = TimeUnit.MILLISECONDS.toNanos(stalledMs);
        lastAdvance = clock.getAsLong();
    }
    synchronized void advance(double percentage) {
        if (Double.isFinite(percentage) && percentage > highest) {
            highest = percentage;
            lastAdvance = clock.getAsLong();
        }
    }
    synchronized State state() {
        long idle = clock.getAsLong() - lastAdvance;
        return idle >= stalledNanos ? State.STALLED : idle >= slowNanos ? State.SLOW : State.ACTIVE;
    }
}
