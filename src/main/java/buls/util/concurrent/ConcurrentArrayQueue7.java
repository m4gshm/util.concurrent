package buls.util.concurrent;

import java.io.PrintStream;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueue7<E> extends AbstractConcurrentArrayQueue1<E> implements QueueWithStatistic<E> {

    protected final LongAdder successSet = new LongAdder();
    protected final LongAdder failSet = new LongAdder();
    protected final LongAdder failLockedSet = new LongAdder();
    protected final LongAdder successGet = new LongAdder();
    protected final LongAdder failGet = new LongAdder();

    protected final LongAdder failNextTail = new LongAdder();
    protected final LongAdder failNextHead = new LongAdder();

    protected final LongAdder setBehindHead = new LongAdder();

    protected final boolean writeStatistic;

    public ConcurrentArrayQueue7(int capacity, boolean writeStatistic) {
        super(capacity);
        this.writeStatistic = writeStatistic;
    }

    @Override
    protected boolean setElement(final E e, final long tail, final long head) {
        if (e == null) {
            throw new NullPointerException("e cannot be null");
        }
        long currentTail = tail;
        final int capacity = capacity();

        long attempt = 0;
        while (true) {
            final int res = set(e, tail, currentTail, head, ++attempt);
            if (res == SUCCESS) {

                //checkHeadTailConsistency(getHead(), currentTail);

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
        //throw new IllegalStateException("setElement");
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
        return getTail();
//        if (calculateType == GO_NEXT) {
//            currentTail++;
//        } else {
//            currentTail = getTail();
//            //long h = getHead();
//            //assert h - head >= capacity;
//        }
//        return currentTail;
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
        long attempts = 0;
        while (true) {
            E e;
            if ((e = get(head, currentHead, tail, ++attempts)) != null) {
                checkHeadTailConsistency(currentHead, getTail());
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
        //throw new IllegalStateException("getElement");
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
            assert newHead == tail;
            return true;
        }
        return false;
    }

    protected long computeHead(long head, long tail) {
        return getHead();
//        if (tail - head > capacity()) {
//            final long h = getHead();
//            if (h < head) {
//                head++;
//            } else {
//                head = h;
//            }
//        } else {
//            head++;
//        }
//        return head;
    }

    @Override
    protected final boolean setNextHead(long oldHead, long currentHead) {
        boolean result = super.setNextHead(oldHead, currentHead);
        if (!result) {
            failNextHead.increment();
        }
        return result;
    }

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
