package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by Alex on 25.06.2014.
 */
public abstract class AbstractArrayQueue<E> extends AbstractQueue<E> {

    public final static int SUCCESS = 0;
    public final static int GO_NEXT = 1;
    public final static int GET_CURRENT_TAIL = 2;
    public final static int TRY_AGAIN = 4;
    public final static int INTERRUPTED = 5;

    static final int MAX_VALUE = Integer.MAX_VALUE;

    protected final AtomicLong tailSequence = new AtomicLong();
    protected final AtomicLong headSequence = new AtomicLong();

    @NotNull
    protected final Object[] elements;
    private final int MAX_TAIL;

    protected AbstractArrayQueue(int capacity) {
        MAX_TAIL = capacity == 0 ? 0 : (MAX_VALUE - Integer.MAX_VALUE % capacity) - 1;
        this.elements = new Object[capacity];
    }

    @NotNull
    @Override
    public String toString() {
        final long h = getHead();
        final long t = getTail();
        return "h: " + h + " (" + computeIndex(h) + ")"
                +  ", t: " + t + " (" + computeIndex(t) + ")"
                + ", c:" + capacity()
                + ", mT:" + max_tail()
                + "\n" + _string();
    }

    @NotNull
    protected String _string() {
        int iMax = elements.length - 1;
        if (iMax == -1)
            return "[]";

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            b.append(_get(i));
            if (i == iMax)
                return b.append(']').toString();
            b.append(',').append(' ');
        }
    }

    @NotNull
    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    public final int capacity() {
        return elements.length;
    }

    protected final int delta(final long head, final long tail) {
        final long delta;
        if (tail >= head) {
            delta = tail - head;
        } else {
            delta = tail - 0 + max_tail() - head + 1;
        }
        assert delta >= 0 : delta + " " + head + " " + tail + "\n" + this;
        return (int) delta;
    }

    protected final long getTail() {
        return tailSequence.get();
    }

    protected final long getHead() {
        return headSequence.get();
    }

    protected abstract boolean setElement(E e, long tail, long head);

    protected abstract E getElement(long head, long tail);

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
        if (e == null) {
            throw new IllegalArgumentException("element cannot be null");
        }

        final int capacity = capacity();
        if (capacity == 0) {
            return false;
        }

        final long tail = getTail();
        final long head = getHead();
        final long size = delta(head, tail);

        return (size < capacity) && setElement(e, tail, head);
    }

    @Nullable
    @Override
    public E poll() {
        final int capacity = capacity();
        if (capacity == 0) {
            return null;
        }

        final long tail = getTail();
        final long head = getHead();
        int size = delta(head, tail);

        boolean notEmpty = size > 0;
        return notEmpty ? getElement(head, tail) : null;
    }

    @NotNull
    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }

    @Nullable
    protected E _get(int index) {
        return (E) elements[index];
    }

    protected boolean _insert(E e, int index) {
        elements[index] = e;
        return true;
    }

    protected E _retrieve(int index) {
        E e = _get(index);
        _insert(null, index);
        return e;
    }

    protected boolean setNextHead(long oldHead, long insertedHead) {
        boolean next = next(oldHead, insertedHead, headSequence);
        return next;
    }

    protected boolean setNextTail(long oldTail, long insertedTail) {
        return next(oldTail, insertedTail, tailSequence);
    }

    private boolean next(final long oldVal, final long insertedVal, final @NotNull AtomicLong sequence) {
        assert insertedVal >= 0;
        assert insertedVal <= max_tail();

        final boolean ivOverflow = oldVal > insertedVal;
        final long newValue = _increment(insertedVal);
        final boolean nvOverflow = newValue == 0 && insertedVal == max_tail();

        assert !(nvOverflow && ivOverflow) : oldVal + " " + newValue + " " + insertedVal + " " + max_tail();

        boolean set = cas(sequence, oldVal, newValue);
        while (!set) {
            final long currentValue = sequence.get();

            boolean tailRange = currentValue > oldVal
                    //&& currentValue > (max_tail() - capacity())
                    && currentValue <= max_tail();
            boolean headRange = currentValue < newValue;

            if (ivOverflow) {

                assert !(tailRange && headRange) : tailRange + " " + headRange + " "
                        + oldVal + " " + insertedVal + " " + newValue + " " + currentValue + " " + max_tail()
                        + "\n" + this;

                if (tailRange || headRange) {
                    set = cas(sequence, currentValue, newValue);
                } else {
                    set = false;
                    break;
                }
            } else if (nvOverflow) {
                assert insertedVal == max_tail() : insertedVal + " " + max_tail();

                assert !(tailRange && headRange) : tailRange + " " + headRange + " "
                        + oldVal + " " + insertedVal + " " + newValue + " " + currentValue + "\n" + this;

                if (tailRange || headRange) {
                    set = cas(sequence, currentValue, newValue);
                } else {
                    set = false;
                    break;
                }
            } else {
                if (currentValue < newValue) {
                    set = cas(sequence, currentValue, newValue);
                } else {
                    set = false;
                    break;
                }
            }
        }
        return set;
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
        final long h = getHead();
        if (head < h) {
            head = h;
        } else {
            head = _increment(head);
        }
        return head;
    }
}
