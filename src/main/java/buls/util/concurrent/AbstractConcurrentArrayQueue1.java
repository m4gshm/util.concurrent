package buls.util.concurrent;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractConcurrentArrayQueue1<E> extends AbstractArrayQueue<E> {
    public static final int EMPTY = 0;
    public static final int NOT_EMPTY = 1;

    public final static int SUCCESS = 0;
    public final static int GO_NEXT = 1;
    public final static int RECALCULATE = 2;

    protected final AtomicLong tailSequence = new AtomicLong(0);
    protected final AtomicLong headSequence = new AtomicLong(0);
    protected final AtomicReferenceArray<AtomicStampedReference<Object>> elements;

    public AbstractConcurrentArrayQueue1(int capacity) {
        this.elements = new AtomicReferenceArray<>(capacity);
        for (int i = 0; i < capacity; ++i) {
            elements.set(i, new AtomicStampedReference<>(null, 0));
        }
        //threads = new AtomicReferenceArray<>(capacity);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder().append("h: ").append(getHead()).append(", t:").append(getTail()).append(", c:").append(capacity()).append("\n");
        b.append("[");
        for (int i = 0; i < elements.length(); ++i) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(elements.get(i).getReference());

        }
        b.append("]\n[");
        for (int i = 0; i < elements.length(); ++i) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(elements.get(i).getStamp());

        }
        b.append("]");
        return b.toString();

    }

    @Override
    protected long getTail() {
        return tailSequence.get();
    }

    @Override
    protected long getHead() {
        return headSequence.get();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int capacity() {
        return elements.length();
    }

    protected final int set(final E e, final long tail, final long currentTail, final long head, final long attempt) {
        int index = calcIndex(currentTail);
        while (true) {
            AtomicStampedReference<Object> reference = elements.get(index);
            if (reference.compareAndSet(null, e, EMPTY, NOT_EMPTY)) {
                setNextTail(tail, currentTail);
                return SUCCESS;
            } else {
                final long l = reference.getStamp();
                if (l == NOT_EMPTY) {
                    return GO_NEXT;
                } else {
                    assert l == EMPTY;
                    //поток опоздал во всем
                    return RECALCULATE;
                }
            }
        }
        //return false;
    }


    @SuppressWarnings("unchecked")
    protected final E get(long oldHead, long currentHead, long tail, long attempt) {
        int index = calcIndex(currentHead);
        while (true) {
            //long l0 = levels.get(index);
            final AtomicStampedReference<Object> reference = elements.get(index);
            Object ref = reference.getReference();
            if (ref == null) {
                return null;
            }
            if (reference.compareAndSet(ref, null, NOT_EMPTY, EMPTY)) {
                setNextHead(oldHead, currentHead);
                return (E) ref;
            } else {
                return null;
            }
        }
        //return null;
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
}
