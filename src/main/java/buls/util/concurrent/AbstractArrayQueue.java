package buls.util.concurrent;

import java.util.AbstractQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Alex on 25.06.2014.
 */
public abstract class AbstractArrayQueue<E> extends AbstractQueue<E> {
    //protected final AtomicReferenceArray<Thread> threads;
    protected final AtomicLong tailSequence = new AtomicLong(0);
    protected final AtomicLong headSequence = new AtomicLong(0);

    public abstract int capacity();

    @Override
    public final int size() {
        long tail = getTail();
        long head = getHead();
        return (int) (tail - head);
    }

    protected boolean checkBehindHead(long currentTail, long head) {
        if (head > currentTail) {
            //headSequence.set(currentTail);
            return true;
        } else {
            return false;
        }
    }

    protected final long getTail() {
        return tailSequence.get();
    }

    protected long getHead() {
        return headSequence.get();
    }

    protected boolean setNextHead(long oldHead, long insertedHead) {
        if (insertedHead < oldHead) {
            throw new RuntimeException("insertedHead < oldHead " + insertedHead + " < " + oldHead);
        }
        long newValue = insertedHead + 1;
        assert oldHead < newValue;
        AtomicLong sequence = headSequence;
        boolean set = sequence.compareAndSet(oldHead, newValue);
        while (!set) {
            long currentValue = sequence.get();
            if (currentValue < newValue) {
                set = sequence.compareAndSet(currentValue, newValue);
            } else if (currentValue == newValue) {
                break;
            } else {
                assert currentValue > newValue : oldHead + " " + currentValue + " " + newValue;
                break;
            }
        }
        return set;
    }

    protected final boolean setNextTail(long oldTail, long insertedTail) {
        long newValue = insertedTail + 1;
        assert oldTail < newValue;
        AtomicLong sequence = tailSequence;
        boolean set = sequence.compareAndSet(oldTail, newValue);
        while (!set) {
            long currentValue = sequence.get();
            if (currentValue < newValue) {
                set = sequence.compareAndSet(currentValue, newValue);
            } else if (currentValue == newValue) {
                return false;
            } else {
                assert currentValue > newValue : oldTail + " " + currentValue + " " + newValue;
                return true;
            }
        }
        return set;
    }

    protected final int calcIndex(long counter) {
        return (int) (counter % capacity());
    }

    protected final void checkHeadTailConsistency(long head, long tail) {
        if (head > tail) {
            throw new IllegalStateException("head <= tail, " + " head: " + head + ", tail: " + tail);
        }
    }

    protected final void yield() {
        Thread.yield();
    }

    protected final boolean isInterrupted() {
        return Thread.interrupted();
    }

    @Override
    public final boolean offer(E e) {
        if (e == null) {
            throw new IllegalArgumentException("element cannot be null");
        }

        final int capacity = capacity();
        if (capacity == 0) {
            return false;
        }

        final long head = getHead();
        long tail = getTail();

        boolean result;
        final long amount = tail - head;
        if (amount < capacity) {
            result = setElement(e, tail, head);
        } else {
            result = false;
        }

        return result;
    }

    protected abstract boolean setElement(E e, long tail, long head);

    protected abstract E getElement(long head, long tail);

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

    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }
}
