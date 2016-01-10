package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    private final int MAX_SEQUENCE_VALUE;

    protected final boolean checkInterruption;

    protected AbstractHeadTailArrayQueue(int capacity, boolean checkInterruption) {
        super(capacity);
        MAX_SEQUENCE_VALUE = capacity == 0 ? 0 : (MAX_VALUE - (MAX_VALUE % capacity)) - 1;
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
                + ", msv:" + max_sequence_value()
                + "\n" + super.toString();
    }

    protected final int delta(final long head, final long tail) {
        final long delta;
        if (tail >= head) delta = tail - head;
        else delta = tail + max_sequence_value() - head + 1;
        return (int) delta;
    }

    protected final long getTail() {
        return tailSequence.get();
    }

    protected final long getHead() {
        return headSequence.get();
    }

    /**
     * set element to queue's end
     * @param e element
     * @param tail point to queue's end
     * @param head
     * @return true if element has been set
     */
    protected boolean setElement(@NotNull final E e, final long tail, final long head) {
        long insertingTail = tail;
        final int capacity = capacity();

        while (isNotInterrupted()) {
            final int res = set(e, tail, insertingTail, head);
            if (res == SUCCESS) {
                successSet();
                return true;
            } else {
                failSet();
                insertingTail = computeTail(insertingTail, res);

                boolean overflow = checkTail(insertingTail, capacity);
                if (overflow) return false;
            }
        }
        return false;
    }

    /**
     * trying to set element in the end of this queue and increment the tail's counter
     * @param e
     * @param oldTail tail value for first insert
     * @param insertingTail tail value for current insert
     * @param head
     * @return see childs
     */
    protected abstract int set(E e, long oldTail, long insertingTail, long head);

    /**
     * get a element from head of the queue
     * @param head
     * @param tail - not used
     * @return element or null if failed or interrupted
     */
    @Nullable
    protected E getElement(final long head, final long tail) {
        long readingHead = head;
        while (isNotInterrupted()) {
            E e;
            if ((e = get(head, readingHead)) != null) {
                successGet();
                return e;
            } else {
                failGet();

                readingHead = computeNextHead(readingHead);
                long t = getTail();
                if (checkHead(readingHead, t)) return null;
            }
        }
        return null;
    }

    protected final int max_sequence_value() {
        return MAX_SEQUENCE_VALUE;
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

    protected boolean incrementHead(long oldHead, long nextHead) {
        return incrementSequence(oldHead, nextHead, headSequence);
    }

    protected boolean incrementTail(long oldTail, long insertedTail) {
        return incrementSequence(oldTail, insertedTail, tailSequence);
    }

    /**
     * set next value in the tail or head sequence
     * @param oldVal
     * @param insertedVal
     * @param sequence
     * @return
     */
    private boolean incrementSequence(final long oldVal, final long insertedVal, final @NotNull AtomicLong sequence) {
        assert insertedVal >= 0;
        assert insertedVal <= max_sequence_value();

        final boolean ivOverflow = oldVal > insertedVal;
        final long newValue = _increment(insertedVal);
        //if the inserted value is last
        final boolean nvOverflow = newValue == 0 && insertedVal == max_sequence_value();

        assert !(nvOverflow && ivOverflow) : oldVal + " " + newValue + " " + insertedVal + " " + max_sequence_value();

        boolean set = cas(sequence, oldVal, newValue);
        while (!set) {
            final long currentValue = sequence.get();

            boolean tailRange = currentValue > oldVal
                    && currentValue <= max_sequence_value();
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
                + oldVal + " " + insertedVal + " " + newValue + " " + currentValue + " " + max_sequence_value() + "\n" + this;
    }

    protected long _increment(long counter) {
        return (counter == max_sequence_value()) ? 0 : counter + 1;
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

    protected long computeNextHead(long prevHead) {
        long currentHead = getHead();

        if (prevHead < currentHead) return currentHead;
        else return _increment(prevHead);
    }

    /**
     * get first element from this queue and increment head
     */
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

    /**
     * compute level for head or tail
     * @param counter
     * @return
     */
    protected final long computeLevel(long counter) {
        return counter / capacity();
    }
}
