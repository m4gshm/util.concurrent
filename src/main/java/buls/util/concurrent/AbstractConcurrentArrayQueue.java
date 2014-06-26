package buls.util.concurrent;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractConcurrentArrayQueue<E> extends AbstractArrayQueue<E> {
    public static final int EMPTY = 0;
    public static final int POOLING = -2;
    public static final int PUTTING = -1;
    public static final int NOT_EMPTY = 1;

    public final static int SUCCESS = 0;
    public final static int GO_NEXT = 1;
    public final static int RECALCULATE = 2;
    public final static int TRY_AGAIN = 3;
    protected final AtomicLongArray levels;
    protected final AtomicLong tailSequence = new AtomicLong();

    //protected final AtomicReferenceArray<Thread> threads;
    protected final AtomicLong headSequence = new AtomicLong();
    private final Object[] elements;

    public AbstractConcurrentArrayQueue(int capacity) {
        this.elements = new Object[capacity];
        levels = new AtomicLongArray(capacity);
        //threads = new AtomicReferenceArray<>(capacity);
    }

    @Override
    public String toString() {
        return "h: " + headSequence + ", t:" + tailSequence + ", c:" + capacity()
                + "\n" + _string()
                + "\n" + levels.toString();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int capacity() {
        return elements.length;
    }

    @Override
    protected final long getTail() {
        return tailSequence.get();
    }

    @Override
    protected final long getHead() {
        return headSequence.get();
    }

    @Deprecated
    protected final boolean set(E e, int index) {
        return _insert(e, index);
    }

    protected final int set(final E e, final long tail, final long currentTail) {
        int index = calcIndex(currentTail);
        while (true) {
            if (startPutting(index)) {
                boolean success = false;
                try {
                    _insert(e, index);
                    int result;

                    long h = getHead();
                    boolean behindHead = checkBehindHead(currentTail, h);
                    if (behindHead) {
                        _remove(e, index);
                        result = RECALCULATE;
                    } else {
                        checkHeadTailConsistency(h, currentTail);
                        setNextTail(tail, currentTail);
                        success = true;
                        result = SUCCESS;
                    }
                    return result;
                } finally {
                    finishPutting(index, success);
                }
            } else {
                int result;
                result = failStartPutting(index);
                if (result != TRY_AGAIN) {
                    return result;
                }
            }
        }
    }

    private int failStartPutting(int index) {
        int result;
        final long l = levels.get(index);
        if (l == PUTTING || l == NOT_EMPTY) {
            result = GO_NEXT;
        } else if (l == POOLING) {
            result = TRY_AGAIN;
        } else {
            assert l == EMPTY;
            //поток опоздал во всем
            result = RECALCULATE;
        }
        return result;
    }

    private void finishPutting(int index, boolean success) {
        int result = success ? NOT_EMPTY : EMPTY;
        levels.compareAndSet(index, PUTTING, result);
    }

    private boolean startPutting(int index) {
        return levels.compareAndSet(index, EMPTY, PUTTING);
    }

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

    protected boolean _remove(E e, int index) {
        _insert(null, index);
        return true;
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    protected final E get(int index) {
        E e = _get(index);
        if (e != null) {
            boolean set = _remove(e, index);
            if (set) {
                return e;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected final E get(long oldHead, long currentHead) {
        int index = calcIndex(currentHead);
        while (true) {
            if (startPooling(index)) {
                boolean success = false;
                try {
                    E e = _retrieve(index);
                    assert e != null;
                    setNextHead(oldHead, currentHead);
                    success = true;
                    return e;
                } finally {
                    finishPooling(index, success);
                }
            } else {
                if (failStartPooling(index)) return null;
            }
        }
    }

    private boolean failStartPooling(int index) {
        final long l = levels.get(index);
        if (l == PUTTING) {
            //завершается вставка в ячейку
            //continue;
            //} else if (l == POOLING || l == EMPTY) {
            //    //другой поток производит взятие
            //    return null;
        } else {
            return true;
        }
        return false;
    }

    private void finishPooling(int index, boolean success) {
        int result = success ? EMPTY : NOT_EMPTY;
        levels.compareAndSet(index, POOLING, result);
    }

    private boolean startPooling(int index) {
        return levels.compareAndSet(index, NOT_EMPTY, POOLING);
    }

    protected boolean setNextHead(long oldHead, long insertedHead) {
        AtomicLong sequence = headSequence;
        return next(oldHead, insertedHead, sequence);
    }

    protected final boolean setNextTail(long oldTail, long insertedTail) {
        AtomicLong sequence = tailSequence;
        return next(oldTail, insertedTail, sequence);
    }

    private boolean next(long oldVal, long insertedVal, AtomicLong sequence) {
        assert insertedVal >= oldVal;
        long newValue = insertedVal + 1;
        assert oldVal < newValue;
        boolean set = sequence.compareAndSet(oldVal, newValue);
        while (!set) {
            long currentValue = sequence.get();
            if (currentValue < newValue) {
                set = sequence.compareAndSet(currentValue, newValue);
            } else {
                break;
            }
        }
        return set;
    }
}
