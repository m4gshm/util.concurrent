package buls.util.concurrent;

import java.util.Iterator;
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
        StringBuilder b = new StringBuilder().append("h: ").append(headSequence).append(", t:").append(tailSequence).append(", c:").append(capacity()).append("\n");
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

}
