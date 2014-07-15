package buls.util.concurrent.research;

import buls.util.concurrent.SimpleConcurrentArrayQueue;
import org.jetbrains.annotations.NotNull;

import java.io.PrintStream;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class SimpleConcurrentArrayQueueWithStatistic<E> extends SimpleConcurrentArrayQueue<E> implements QueueWithStatistic<E> {

    protected final LongAdder successSet = new LongAdder();
    protected final LongAdder failSet = new LongAdder();
    protected final LongAdder failLockedSet = new LongAdder();
    protected final LongAdder successGet = new LongAdder();
    protected final LongAdder failGet = new LongAdder();

    protected final LongAdder failNextTail = new LongAdder();
    protected final LongAdder failNextHead = new LongAdder();

    protected final boolean writeStatistic;

    public SimpleConcurrentArrayQueueWithStatistic(int capacity, boolean writeStatistic) {
        super(capacity);
        this.writeStatistic = writeStatistic;
    }

    protected final void failGet() {
        if (writeStatistic) failGet.increment();
    }

    protected final void successGet() {
        if (writeStatistic) successGet.increment();
    }

    protected final void failSet() {
        if (writeStatistic) failSet.increment();
    }

    protected final void successSet() {
        if (writeStatistic) successSet.increment();
    }

    @Override
    public void printStatistic(@NotNull PrintStream printStream) {
        if (writeStatistic) {
            printStream.println("success sets " + successSet);
            printStream.println("fail sets " + failSet);
            printStream.println("fail locked sets " + failLockedSet);
            printStream.println("success gets " + successGet);
            printStream.println("fail gets " + failGet);

            printStream.println("fail next tail " + failNextTail);
            printStream.println("fail next head " + failNextHead);
        }
    }
}
