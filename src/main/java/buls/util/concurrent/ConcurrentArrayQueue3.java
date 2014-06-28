package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueue3<E> extends AbstractConcurrentArrayQueue2<E> implements QueueWithStatistic<E> {

    protected final LongAdder successSet = new LongAdder();
    protected final LongAdder failSet = new LongAdder();
    protected final LongAdder failLockedSet = new LongAdder();
    protected final LongAdder successGet = new LongAdder();
    protected final LongAdder failGet = new LongAdder();

    protected final LongAdder failNextTail = new LongAdder();
    protected final LongAdder failNextHead = new LongAdder();

    protected final boolean writeStatistic;

    public ConcurrentArrayQueue3(int capacity, boolean writeStatistic) {
        super(capacity);
        this.writeStatistic = writeStatistic;
    }

    @Override
    protected boolean setElement(@NotNull final E e, final long tail, final long head) {
        long currentTail = tail;
        final int capacity = capacity();

        while (true) {
            final int res = set(e, tail, currentTail);
            if (res == SUCCESS) {
                successSet();
                return true;
            } else {
                failSet();
                currentTail = computeTail(currentTail, res);

                boolean overflow;
                long headIteration = getHeadIteration();
                long tailIteration = computeIteration(currentTail);
                if (headIteration - tailIteration == 1) {
                    overflow = false;
                } else {
                    overflow = checkTailOverflow(currentTail, capacity);
                }
                if (overflow) {
                    return false;
                }
            }
        }
    }

    protected long computeTail(long currentTail, int calculateType) {
        if (calculateType == GO_NEXT) {
            currentTail++;
        } else {
            currentTail = getTail();
            assert calculateType == GET_CURRENT;
        }
        return currentTail;
    }

    protected final boolean checkTailOverflow(long tailForInserting, int capacity) {
        long amount = tailForInserting - getHead();
        return amount > capacity;
    }

    @Nullable
    @Override
    protected E getElement(final long head, long tail) {
        long currentHead = head;
        while (true) {
            E e;
            if ((e = get(head, currentHead)) != null) {
                successGet();
                return e;
            } else {
                failGet();

                currentHead = computeHead(currentHead);

                boolean overflow;
                long tailIteration = getTailIteration();
                long headIteration = computeIteration(currentHead);
                if (tailIteration - headIteration == 1) {
                    overflow = false;
                } else {
                    long t = getTail();
                    overflow = checkHeadOverflow(currentHead, t);
                }

                if (overflow) {
                    return null;
                }
            }
        }
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
        return newHead >= tail;
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

            printStream.println("tailOffset " + tailOffset);
            printStream.println("headOffset " + headOffset);
            printStream.println("tail - head offsets = " + (tailOffset - headOffset));
        }
    }
}
