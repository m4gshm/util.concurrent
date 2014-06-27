package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractConcurrentArrayQueue1<E> extends AbstractArrayQueue<E> {
    public static final int PUTTING = -1;
    public static final int POOLING = -2;

    public final static int SUCCESS = 0;
    public final static int GO_NEXT = 1;
    public final static int GET_CURRENT = 2;
    public final static int GET_CURRENT_GO_NEXT = 3;
    public final static int TRY_AGAIN = 4;

    @NotNull
    protected final AtomicLongArray levels;

    protected final AtomicLong tailSequence = new AtomicLong();
    protected final AtomicLong headSequence = new AtomicLong();

    @NotNull
    private final Object[] elements;

    public AbstractConcurrentArrayQueue1(int capacity) {
        this.elements = new Object[capacity];
        levels = new AtomicLongArray(capacity);
    }

    @NotNull
    @Override
    public String toString() {
        return "h: " + headSequence + ", t:" + tailSequence + ", c:" + capacity()
                + "\n" + _string()
                + "\n" + levels.toString();
    }

    @NotNull
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
        long level = calcLevel(currentTail);
        while (true) {
            if (startPutting(index, level)) {
                try {
                    _insert(e, index);
                    int result;

                    setNextTail(tail, currentTail);

                    result = SUCCESS;
                    return result;
                } finally {
                    finishPutting(index, level);
                }
            } else {
                int result = failPutting(index, level);
                if (result != TRY_AGAIN) {
                    return result;
                }
            }
        }
    }

    private int failPutting(int index, long level) {
        int result;
        final long l = levels.get(index);
        if (l == PUTTING) {
            result = GO_NEXT;
        } else if (l == POOLING) {
            result = TRY_AGAIN;
        } else if (isAlreadyPut(level, l)) {
            result = GO_NEXT;
        } else {
            result = GET_CURRENT;
        }
        return result;
    }

    private boolean isAlreadyPut(long level, long l) {
        return (l - level) == 1;
    }

    private void finishPutting(int index, long level) {
        boolean set = levels.compareAndSet(index, PUTTING, calcNextLevel(level));
        assert set : "finishPutting fail";
    }

    private boolean startPutting(int index, long level) {
        return levels.compareAndSet(index, level, PUTTING);
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

    @Nullable
    protected E _get(int index) {
        return (E) elements[index];
    }

    protected boolean _insert(E e, int index) {
        elements[index] = e;
        return true;
    }

    @NotNull
    protected E _retrieve(int index) {
        E e = _get(index);
        _insert(null, index);
        return e;
    }

    protected boolean _remove(E e, int index) {
        _insert(null, index);
        return true;
    }

    @Nullable
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

    @Nullable
    @SuppressWarnings("unchecked")
    protected final E get(long oldHead, long currentHead) {
        final int index = calcIndex(currentHead);
        final long level = calcNextLevel(calcLevel(currentHead));
        while (true) {
            if (startPooling(index, level)) {
                try {
                    final E e = _retrieve(index);
                    assert e != null;
                    setNextHead(oldHead, currentHead);
                    return e;
                } finally {
                    finishPooling(index, level);
                }
            } else if (stopTryPooling(index, level)) {
                return null;
            }
        }
    }

    private boolean stopTryPooling(int index, long level) {
        final long l = levels.get(index);
        if (l == PUTTING) {
            return false;
        } else if (l == POOLING) {
            return true;
        } else if (level == l) {
            return false;
        } else {
            assert level < l : "invalid level :" + level + " " + l;
            return true;
        }
    }

    private void finishPooling(int index, long level) {
        long nextLevel = calcNextLevel(level);
        levels.compareAndSet(index, POOLING, nextLevel);
    }

    private boolean startPooling(int index, long level) {
        return levels.compareAndSet(index, level, POOLING);
    }

    protected boolean setNextHead(long oldHead, long insertedHead) {
        AtomicLong sequence = headSequence;
        return next(oldHead, insertedHead, sequence);
    }

    protected final boolean setNextTail(long oldTail, long insertedTail) {
        AtomicLong sequence = tailSequence;
        return next(oldTail, insertedTail, sequence);
    }

    private boolean next(long oldVal, long insertedVal, @NotNull AtomicLong sequence) {
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
