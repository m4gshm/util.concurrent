package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractQueue;

/**
 * Created by Alex on 25.06.2014.
 */
public abstract class AbstractArrayQueue<E> extends AbstractQueue<E> {

    public static final int LEVEL_FACTOR = 2;
    public static final int NEXT_LEVEL_SUMMAND = 1;

    public abstract int capacity();

    protected abstract long getTail();

    protected abstract long getHead();

    protected abstract boolean setElement(E e, long tail, @Deprecated long head);

    protected abstract E getElement(long head, @Deprecated long tail);

    @Override
    public final int size() {
        long tail = getTail();
        long head = getHead();
        return size(head, tail);
    }

    protected final int size(long head, long tail) {
        long delta = tail - head;
        return (int) ((int) delta < 0 ? -delta : delta);
    }

    protected final int computeIndex(long counter) {
        long l = counter % capacity();
        return (int) ((int) l < 0 ? -l : l);
    }

    protected final long computeNextLevel(long level) {
        return level + NEXT_LEVEL_SUMMAND;
    }

    protected final long computeLevel(long counter) {
        long l = counter / capacity();
        long result = l * LEVEL_FACTOR;
        if (counter < 0 && result < 0 && result > counter) {
            //перевалили за максимальное значение и начали с минимального
            //assert result < 0;
            result -= 2;
        }
        return result;
    }

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
        return (amount < capacity) && setElement(e, tail, head);
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
        return notEmpty ? getElement(head, tail) : null;
    }

    @NotNull
    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }
}
