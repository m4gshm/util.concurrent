package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Alex on 25.06.2014.
 */
public abstract class AbstractHeadTailArrayQueue<E> extends AbstractArrayQueue<E> {

    public final static int SUCCESS = 0;
    public final static int GO_NEXT = 1;
    public final static int GET_CURRENT_TAIL = 2;
    public final static int TRY_AGAIN = 4;
    public final static int INTERRUPTED = 5;

    static final int MAX_VALUE = Integer.MAX_VALUE;

    protected final AtomicLong tailSequence = new AtomicLong();
    protected final AtomicLong headSequence = new AtomicLong();
    protected final boolean checkInterruption;

    private final int MAX_TAIL;

    protected AbstractHeadTailArrayQueue(int capacity, boolean checkInterruption) {
        super(capacity);
        MAX_TAIL = capacity == 0 ? 0 : (MAX_VALUE - Integer.MAX_VALUE % capacity) - 1;
        this.checkInterruption = checkInterruption;
    }

    @NotNull
    @Override
    public String toString() {
        final long h = getHead();
        final long t = getTail();
        return "h: " + h + " (" + computeIndex(h) + ")"
                + ", t: " + t + " (" + computeIndex(t) + ")"
                + ", c:" + capacity()
                + ", mT:" + max_tail()
                + "\n" + super.toString();
    }

    protected final int delta(final long head, final long tail) {
        final long delta;
        if (tail >= head) delta = tail - head;
        else delta = tail - 0 + max_tail() - head + 1;
        return (int) delta;
    }

    protected final long getTail() {
        return tailSequence.get();
    }

    protected final long getHead() {
        return headSequence.get();
    }

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

                boolean overflow = checkTail(currentTail, capacity);
                if (overflow) return false;
            }
        }
        return false;
    }

    protected abstract int set(E e, long tail, long currentTail, long head);

    @Nullable
    protected E getElement(final long head, final long tail) {
        long currentHead = head;
        while (isNotInterrupted()) {
            E e;
            if ((e = get(head, currentHead)) != null) {
                successGet();
                return e;
            } else {
                failGet();

                currentHead = computeHead(currentHead);
                long t = getTail();
                if (checkHead(currentHead, t)) return null;
            }
        }
        return null;
    }

    protected final int max_tail() {
        return MAX_TAIL;
    }

    @Override
    public final int size() {
        long tail = getTail();
        long head = getHead();
        return delta(head, tail);
    }

    protected final int computeIndex(long counter) {
        return (int) (counter % capacity());
    }

    @Override
    public boolean offer(@Nullable E e) {
        if (e == null) throw new IllegalArgumentException("element cannot be null");

        final int capacity = capacity();
        if (capacity == 0) return false;

        final long tail = getTail();
        final long head = getHead();
        final long size = delta(head, tail);

        return (size < capacity) && setElement(e, tail, head);
    }

    @Nullable
    @Override
    public E poll() {
        final int capacity = capacity();

        if (capacity == 0) return null;

        final long tail = getTail();
        final long head = getHead();
        int size = delta(head, tail);

        boolean notEmpty = size > 0;
        return notEmpty ? getElement(head, tail) : null;
    }

    protected boolean setNextHead(long oldHead, long insertedHead) {
        return next(oldHead, insertedHead, headSequence);
    }

    protected boolean setNextTail(long oldTail, long insertedTail) {
        return next(oldTail, insertedTail, tailSequence);
    }

    private boolean next(long oldVal, long insertedVal, @NotNull AtomicLong sequence) {
        assert insertedVal >= 0;
        assert insertedVal <= max_tail();

        boolean ivOverflow = oldVal > insertedVal;
        long newValue = _increment(insertedVal);
        boolean nvOverflow = newValue == 0 && insertedVal == max_tail();

        assert !(nvOverflow && ivOverflow) : oldVal + " " + newValue + " " + insertedVal + " " + max_tail();

        boolean set = cas(sequence, oldVal, newValue);
        while (!set) {
            long currentValue = sequence.get();

            boolean tailRange = currentValue > oldVal
                    && currentValue <= max_tail();
            boolean headRange = currentValue < newValue;

            if (ivOverflow || nvOverflow) {
                assertRange(oldVal, insertedVal, newValue, currentValue, tailRange, headRange);
                if (tailRange || headRange) set = cas(sequence, currentValue, newValue);
                else break;
            } else if (currentValue < newValue) set = cas(sequence, currentValue, newValue);
            else break;
        }
        return set;
    }

    private void assertRange(long oldVal, long insertedVal, long newValue, long currentValue, boolean tailRange, boolean headRange) {
        assert !(tailRange && headRange) : tailRange + " " + headRange + " "
                + oldVal + " " + insertedVal + " " + newValue + " " + currentValue + " " + max_tail() + "\n" + this;
    }

    protected long _increment(long counter) {
        return (counter == max_tail()) ? 0 : counter + 1;
    }

    private boolean cas(@NotNull AtomicLong counter, long expected, long update) {
        return counter.compareAndSet(expected, update);
    }

    protected long computeTail(long currentTail, int calculateType) {
        if (calculateType == GO_NEXT) {
            currentTail = _increment(currentTail);
        } else {
            currentTail = getTail();
            assert calculateType == GET_CURRENT_TAIL;
        }
        return currentTail;
    }

    protected final boolean checkTail(long tailForInserting, int capacity) {
        long head = getHead();
        long amount = delta(head, tailForInserting);
        return amount > capacity;
    }

    protected final boolean checkHead(long headForGetting, long tail) {
        boolean overflow;
        if (headForGetting == tail) {
            overflow = true;
        } else {
            int delta = delta(headForGetting, tail);
            int capacity = capacity();
            overflow = delta > capacity;
        }
        return overflow;
    }

    protected long computeHead(long head) {
        long h = getHead();

        if (head < h) return h;
        else return _increment(head);
    }

    protected abstract E get(long head, long currentHead);

    protected void failGet() {
    }

    protected void successGet() {
    }

    protected void failSet() {
    }

    protected void successSet() {
    }

    protected final boolean isNotInterrupted() {
        return !(checkInterruption && Thread.interrupted());
    }

    protected final long computeLevel(long counter) {
        return counter / capacity();
    }
}
