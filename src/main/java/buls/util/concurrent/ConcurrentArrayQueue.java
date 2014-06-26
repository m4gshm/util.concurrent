package buls.util.concurrent;

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

    protected final LongAdder setBehindHead = new LongAdder();

    protected final boolean writeStatistic;

    public ConcurrentArrayQueue(int capacity, boolean writeStatistic) {
        super(capacity);
        this.writeStatistic = writeStatistic;
    }

    @Override
    protected boolean setElement(final E e, final long tail, final long head) {
        assert e != null;
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

                boolean overflow = checkTailOverflow(currentTail, capacity);
                if (overflow) {
                    return false;
                }
            }
        }
    }

    @Override
    protected final boolean checkBehindHead(long currentTail, long head) {
        final boolean result = super.checkBehindHead(currentTail, head);
        if (writeStatistic && result) {
            setBehindHead.increment();
        }
        return result;
    }

    protected long computeTail(long currentTail, int calculateType) {
        if (calculateType == GO_NEXT) {
            currentTail++;
        } else {
            currentTail = getTail();
        }
        return currentTail;
    }

    protected final boolean checkTailOverflow(long tail, int capacity) {
        return checkTailOverflow(tail, capacity, getHead());
    }

    protected final boolean checkTailOverflow(long tail, int capacity, long head) {
        long amount = tail - head;
        return amount >= capacity;
    }

    @Override
    protected E getElement(final long head, long tail) {
        long currentHead = head;
        while (true) {
            E e;
            if ((e = get(head, currentHead)) != null) {
                //checkHeadTailConsistency(currentHead, getTail());
                successGet();
                return e;
            } else {
                failGet();

                long t = getTail();
                currentHead = computeHead(currentHead, t);
                if (checkHeadOverflow(currentHead, t)) {
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
        if (newHead >= tail) {
            //assert newHead == tail: newHead +" <> " + tail;
            return true;
        }
        return false;
    }

    protected long computeHead(long head, long tail) {
        final int size = size(head, tail);
        final int capacity = capacity();
        if (size > capacity) {
            final long h = getHead();
            if (h < head) {
                head++;
            } else {
                head = h;
            }
        } else {
            head++;
        }
        return head;
    }

    protected long computeHead(long head) {
        return computeHead(head, getTail());
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
    public void printStatistic(PrintStream printStream) {
        if (writeStatistic) {
            printStream.println("success sets " + successSet);
            printStream.println("fail sets " + failSet);
            printStream.println("fail locked sets " + failLockedSet);
            printStream.println("success gets " + successGet);
            printStream.println("fail gets " + failGet);

            printStream.println("fail next tail " + failNextTail);
            printStream.println("fail next head " + failNextHead);

            printStream.println("set behind head " + setBehindHead);
        }
    }
}
