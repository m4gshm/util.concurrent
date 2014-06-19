package util.concurrent;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueue<E> extends AbstractConcurrentArrayQueue<E> {

    protected final AtomicLong successSet = new AtomicLong();
    protected final AtomicLong failSet = new AtomicLong();
    protected final AtomicLong failLockedSet = new AtomicLong();

    protected final AtomicLong successGet = new AtomicLong();
    protected final AtomicLong failGet = new AtomicLong();

    protected final boolean writeStatistic;

    public ConcurrentArrayQueue(int capacity, boolean writeStatistic) {
        super(capacity);
        this.writeStatistic = writeStatistic;
    }

    @Override
    protected boolean setElement(final E e, final long tail) {
        long currentTail = tail;
        int capacity = capacity();
        int index = (int) (currentTail % capacity);

        while (true) {
            if (set(e, index)) {

                setNextTail(tail, currentTail);

                checkHeadTailConsistency();

                successSet();

                return true;
            } else {

                failSet();

                currentTail = computeCurrentTail(currentTail);
                if (checkTailOwerflow(currentTail, capacity)) {
                    return false;
                }
                index = calcIndex(currentTail);
            }
        }
        //throw new IllegalStateException("setElement");
    }

    private void checkHeadTailConsistency() {
        long h = headSequence.get();
        long t = tailSequence.get();
        assert h <= t;
    }

    protected int calcIndex(long counter) {
        return (int) (counter % capacity());
    }

    protected boolean checkTailOwerflow(long tail, int capacity) {
        long head = getHead();
        long amount = tail - head;
        return amount >= capacity;
    }

    protected long computeCurrentTail(long tail) {
        long currentTail = getTail();
        if (tail < currentTail) {
            tail = currentTail;
        } else {
            tail++;
        }
        return tail;
    }

    @Override
    protected E getElement(final long head) {
        long currentHead = head;
        int index = (int) (currentHead % capacity());

        while (true) {
            E e;
            if ((e = get(index)) != null) {

                setNextHead(head, currentHead);

                checkHeadTailConsistency();

                successGet();

                return e;
            } else {
                failGet();

                currentHead = getNewHead(currentHead);
                if (checkHeadOwerflow(currentHead)) {
                    return null;
                }
                index = calcIndex(currentHead);
            }
        }
        //throw new IllegalStateException("getElement");
    }

    protected void failGet() {
        if (writeStatistic) failGet.incrementAndGet();
    }

    protected void successGet() {
        if (writeStatistic) successGet.incrementAndGet();
    }


    protected void failSet() {
        if (writeStatistic) failSet.incrementAndGet();
    }

    protected void successSet() {
        if (writeStatistic) successSet.incrementAndGet();
    }

    protected boolean checkHeadOwerflow(long newHead) {
        long tail = getTail();
        if (newHead >= tail) {
            assert newHead == tail;
            return true;
        }
        return false;
    }

    protected long getNewHead(long head) {
        long currentHead = getHead();
        if (head < currentHead) {
            head = currentHead;
        } else {
            head++;
        }
        return head;
    }

    public void printStatistic(PrintStream printStream) {
        if (writeStatistic) {
            printStream.println("success sets " + successSet.get());
            printStream.println("fail sets " + failSet.get());
            printStream.println("fail locked sets " + failLockedSet.get());
            printStream.println("success gets " + successGet.get());
            printStream.println("fail gets " + failGet.get());
        }
    }

}
