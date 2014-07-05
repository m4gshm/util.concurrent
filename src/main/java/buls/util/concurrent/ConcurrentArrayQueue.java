package buls.util.concurrent;

import buls.util.concurrent.research.QueueWithStatistic;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueue<E> extends AbstractConcurrentArrayQueue<E> implements QueueWithStatistic<E> {

    protected final LongAdder successSet = new LongAdder();
    protected final LongAdder failSet = new LongAdder();
    protected final LongAdder failLockedSet = new LongAdder();
    protected final LongAdder successGet = new LongAdder();
    protected final LongAdder failGet = new LongAdder();

    protected final LongAdder failNextTail = new LongAdder();
    protected final LongAdder failNextHead = new LongAdder();

    protected final boolean writeStatistic;

    public ConcurrentArrayQueue(int capacity, boolean writeStatistic) {
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

    protected long computeTail(long currentTail, int calculateType) {
        if (calculateType == GO_NEXT) {
            currentTail++;
        } else {
            currentTail = getTail();
            assert calculateType == GET_CURRENT_TAIL;
        }
        return currentTail;
    }

    protected final boolean checkTailOverflow(long tailForInserting, int capacity) {
        long head = getHead();
        long amount = tailForInserting - head;
        return amount > capacity;
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

    protected final boolean checkHeadOverflow(long newHead, long tail) {
        boolean overflow;
        if (newHead == tail) {

            overflow = true;
        } else {
            int delta = delta(newHead, tail);
            int capacity = capacity();
            overflow = delta > capacity;
        }
        return overflow;
    }

    protected long computeHead(long head) {
        final long h = getHead();
        if (head < h) {
            head = h;
        } else {
            head++;
        }
        return head;
    }

    @Override
    protected final boolean setNextHead(long oldHead, long currentHead) {
        boolean result = super.setNextHead(oldHead, currentHead);
        if (!result) {
            failNextHead.increment();
        }
        return result;
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
