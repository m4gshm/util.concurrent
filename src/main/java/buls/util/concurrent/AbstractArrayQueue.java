package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractQueue;

/**
 * Created by Alex on 25.06.2014.
 */
public abstract class AbstractArrayQueue<E> extends AbstractQueue<E> {
    protected static final long PUTTING = Long.MIN_VALUE;
    protected static final long POOLING = Long.MAX_VALUE;
    protected static final int NEXT_LEVEL_SUMMAND = 1;
    static final int MAX_VALUE = Integer.MAX_VALUE;
    private static final int LOAD_FACTOR = 2;
    private final int MAX_TAIL;

    protected AbstractArrayQueue(int capacity) {
        MAX_TAIL = capacity == 0 ? 0 : (MAX_VALUE - Integer.MAX_VALUE % capacity) - 1;
    }

    public abstract int capacity();

    protected abstract long getTail();

    protected abstract long getHead();

    protected abstract boolean setElement(E e, long tail);

    protected abstract E getElement(long head);

    protected final int max_tail() {
        return MAX_TAIL;
    }

    @Override
    public final int size() {
        long tail = getTail();
        long head = getHead();
        return size(head, tail);
    }

    protected int size(long head, long tail) {
        long delta = tail - head;
        assert delta >= 0;
        return (int) delta;
    }

    protected final int computeIndex(long counter) {
        return (int) (counter % capacity());
    }

    protected final long computeNextLevel(long level) {
        return level + NEXT_LEVEL_SUMMAND;
    }

    protected final long computeLevel(long counter) {
        assertCounter(counter);
        return counter / capacity();
    }

    protected final long computeNextLevel2(long counter) {
        assertCounter(counter);
        long lfc = levelFirstCounter(counter);
        long nlc = nextLevelCounter(lfc);
        return computeLevel(nlc);
    }

    protected long levelFirstCounter(long counter) {
        return counter - computeIndex(counter);
    }

    protected long nextLevelCounter(long lfc) {
        if (lfc == MAX_VALUE) {
            return 0;
        }
        int capacity = capacity();
        return lfc - (lfc % capacity) + capacity;
    }

    private void assertCounter(long counter) {
        assert counter >= 0 : counter + "<0";
        assert counter < Long.MAX_VALUE : "counter = MAX_VALUE";
    }

    @Deprecated
    protected long computeIteration(long counter) {
        return counter / capacity() + 1;
    }

    protected final void yield() {
        Thread.yield();
    }

    protected final boolean isInterrupted() {
        return Thread.interrupted();
    }

    @Override
    public final boolean offer(@Nullable E e) {
        if (e == null) {
            throw new IllegalArgumentException("element cannot be null");
        }

        final int capacity = capacity();
        if (capacity == 0) {
            return false;
        }

        final long tail = getTail();
        final long head = getHead();

        final long amount = size(head, tail);
        return (amount < capacity) && setElement(e, tail);
    }

    @Nullable
    @Override
    public final E poll() {
        final int capacity = capacity();
        if (capacity == 0) {
            return null;
        }

        final long tail = getTail();
        final long head = getHead();
        int size = size(head, tail);
        boolean notEmpty = size > 0;
        return notEmpty ? getElement(head) : null;
    }

    @NotNull
    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }
}
