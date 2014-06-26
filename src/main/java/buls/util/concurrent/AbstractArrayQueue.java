package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractQueue;

/**
 * Created by Alex on 25.06.2014.
 */
public abstract class AbstractArrayQueue<E> extends AbstractQueue<E> {

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
        return (int) (tail - head);
    }

    protected final int calcIndex(long counter) {
        return (int) (counter % capacity());
    }

    protected final long calcNextLevel(long level) {
        return level + 1;
    }

    protected final long calcLevel(long counter) {
        return counter / capacity() * 2;
    }

    protected boolean checkBehindHead(long currentTail, long head) {
        if (head > currentTail) {
            //headSequence.set(currentTail);
            return true;
        } else {
            return false;
        }
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

        final long head = getHead();
        long tail = getTail();

        final long amount = size(head, tail);
        return (amount < capacity) && setElement(e, tail, head);
    }

    @Nullable
    @Override
    public final E poll() {
        int capacity = capacity();
        if (capacity == 0) {
            return null;
        }

        long tail = getTail();
        long head = getHead();

        E result;

        if (head < tail) {
            result = getElement(head, tail);
        } else {
            result = null;
        }
        return result;
    }

    @NotNull
    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }
}
