package buls.util.concurrent.research;

import org.jetbrains.annotations.NotNull;

import java.io.PrintStream;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class SimpleConcurrentArrayQueueWithStatistic<E> extends SimpleConcurrentArrayQueue<E> implements QueueWithStatistic<E> {

    protected final LongAdder successSet = new LongAdder();
    protected final LongAdder failSet = new LongAdder();
    protected final LongAdder fullSet = new LongAdder();
    protected final LongAdder successGet = new LongAdder();
    protected final LongAdder failGet = new LongAdder();
    protected final LongAdder fullGet = new LongAdder();
    protected final LongAdder emptyGet = new LongAdder();
    protected final LongAdder tealStealing = new LongAdder();
    protected final LongAdder failSetRepeat = new LongAdder();
    protected final LongAdder successSetRepeat = new LongAdder();

    protected final boolean writeStatistic;

    public SimpleConcurrentArrayQueueWithStatistic(int capacity, boolean writeStatistic) {
        super(capacity);
        this.writeStatistic = writeStatistic;
    }

    @Override
    protected final void failGet() {
        if (writeStatistic) failGet.increment();
    }

    @Override
    protected void fullGet() {
        if (writeStatistic) fullGet.increment();
    }
    @Override
    protected void emptyGet() {
        if (writeStatistic) emptyGet.increment();
    }
    @Override
    protected final void successGet() {
        if (writeStatistic) successGet.increment();
    }

    @Override
    protected final void failSet() {
        if (writeStatistic) failSet.increment();
    }

    @Override
    protected void fullSet() {
        if (writeStatistic) fullSet.increment();
    }

    @Override
    protected final void successSet() {
        if (writeStatistic) successSet.increment();
    }

    @Override
    protected final void tailStealing() {
        if (writeStatistic) tealStealing.increment();
    }


    protected void failSetRepeat(long attempts) {
        if (writeStatistic) failSetRepeat.increment();
    }

    protected void successSetRepeat(long attempts) {
        if (writeStatistic) successSetRepeat.increment();
    }

    @Override
    public void printStatistic(@NotNull PrintStream printStream) {
        if (writeStatistic) {
            printStream.println("success sets " + successSet);
            printStream.println("fail sets " + failSet);
            printStream.println("full sets " + fullSet);
            printStream.println("success gets " + successGet);
            printStream.println("success set repeats " + successSetRepeat);
            printStream.println("fail set repeats " + failSetRepeat);
            printStream.println("fail gets " + failGet);
            printStream.println("full gets " + fullGet);
            printStream.println("empty gets " + emptyGet);
            printStream.println("teal stealing " + tealStealing);
        }
    }
}
