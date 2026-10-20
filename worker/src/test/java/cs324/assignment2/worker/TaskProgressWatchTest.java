package cs324.assignment2.worker;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class TaskProgressWatchTest {
    @Test void continuedWorkCanRunBeyondTheStallWindow() {
        AtomicLong clock = new AtomicLong();
        TaskProgressWatch watch = new TaskProgressWatch(clock::get, 10, 60);
        for (int percent = 1; percent <= 100; percent++) {
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(5));
            watch.advance(percent);
            assertEquals(TaskProgressWatch.State.ACTIVE, watch.state());
        }
    }
    @Test void slowAndStalledDependOnAdvancementNotRepeatedMessages() {
        AtomicLong clock = new AtomicLong();
        TaskProgressWatch watch = new TaskProgressWatch(clock::get, 10, 60);
        watch.advance(25);
        clock.set(TimeUnit.MILLISECONDS.toNanos(15));
        watch.advance(25);
        assertEquals(TaskProgressWatch.State.SLOW, watch.state());
        clock.set(TimeUnit.MILLISECONDS.toNanos(61));
        assertEquals(TaskProgressWatch.State.STALLED, watch.state());
        watch.advance(26);
        assertEquals(TaskProgressWatch.State.ACTIVE, watch.state());
    }
}
