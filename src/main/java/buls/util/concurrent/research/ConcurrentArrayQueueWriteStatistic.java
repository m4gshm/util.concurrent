package buls.util.concurrent.research;

import buls.util.concurrent.AbstractConcurrentArrayQueue;
import buls.util.concurrent.ConcurrentArrayQueue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueueWriteStatistic<E> extends ConcurrentArrayQueue<E> implements QueueWithStatistic<E> {

    protected final LongAdder successSet = new LongAdder();
    protected final LongAdder failSet = new LongAdder();
    protected final LongAdder failLockedSet = new LongAdder();
    protected final LongAdder successGet = new LongAdder();
    protected final LongAdder failGet = new LongAdder();

    protected final boolean writeStatistic;

    public ConcurrentArrayQueueWriteStatistic(int capacity, boolean writeStatistic) {
        super(capacity);
        this.writeStatistic = writeStatistic;
    }

    @Override
    protected boolean setElement(@NotNull final E e, final long tail, final long head) {
        long currentTail = tail;
        final int capacity = capacity();

        while (isNotInterrupted()) {
            final int res = set(e, tail, currentTail, head);
            if (res == SUCCESS) {
                successSet();
                return true;
            } else {
                failSet();
                currentTail = computeTail(currentTail, res);

                boolean overflow = checkTailOverflow(currentTail, capacity);
                if (overflow) {
                    return false;
                }
            }
        }
        return false;
    }


    @Nullable
    @Override
    protected E getElement(final long head, final long tail) {
        assert delta(head, tail) > 0 : head + " " + tail + ", size " + delta(head, tail);
        long currentHead = head;
        long currentTail = tail;
        long fails = 0;
        while (isNotInterrupted()) {
            E e;
            if ((e = get(head, currentHead, currentTail, fails)) != null) {
                successGet();
                return e;
            } else {
                fails++;
                failGet();

                currentHead = computeHead(currentHead);
                long t = getTail();

                if (checkHeadOverflow(currentHead, t)) {
                    return null;
                }
                assert delta(currentHead, t) > 0 : currentHead + " " + t + ", delta " + delta(currentHead, t);
                currentTail = t;
            }
        }
        return null;
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
        }
    }
}
