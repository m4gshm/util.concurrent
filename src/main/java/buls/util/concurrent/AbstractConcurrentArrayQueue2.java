package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import sun.misc.Contended;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractConcurrentArrayQueue2<E> extends AbstractArrayQueue<E> {
    public static final int PUTTING = -1;
    public static final int POOLING = -2;

    public final static int SUCCESS = 0;
    public final static int GO_NEXT = 1;
    public final static int GET_CURRENT = 2;
    public final static int TRY_AGAIN = 4;
    protected static final long tailOffset;
    protected static final long headOffset;
    protected static final long tailIterationOffset;
    protected static final long headIterationOffset;

    private static final Unsafe unsafe;

    static {
        try {
            Field getUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            getUnsafe.setAccessible(true);
            unsafe = (Unsafe) getUnsafe.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new Error(e);
        }
        try {
            tailOffset = unsafe.objectFieldOffset
                    (AbstractConcurrentArrayQueue2.class.getDeclaredField("tailSequence"));
            headOffset = unsafe.objectFieldOffset
                    (AbstractConcurrentArrayQueue2.class.getDeclaredField("headSequence"));
            tailIterationOffset = unsafe.objectFieldOffset
                    (AbstractConcurrentArrayQueue2.class.getDeclaredField("tailIteration"));
            headIterationOffset = unsafe.objectFieldOffset
                    (AbstractConcurrentArrayQueue2.class.getDeclaredField("headIteration"));
        } catch (Exception ex) {
            throw new Error(ex);
        }

    }

    @NotNull
    protected final AtomicLongArray levels;
    @NotNull
    private final Object[] elements;
    @Contended
    protected volatile long tailSequence = 0;
    @Contended
    protected volatile long headSequence = 0;
    @Contended
    protected volatile long tailIteration = 1;
    @Contended
    protected volatile long headIteration = 1;

    public AbstractConcurrentArrayQueue2(int capacity) {
        this.elements = new Object[capacity];
        levels = new AtomicLongArray(capacity);
    }

    @NotNull
    @Override
    public String toString() {
        return "h: " + headSequence + ", t:" + tailSequence + ", c:" + capacity()
                + "\n" + _string()
                + "\n" + _levelsString();
    }

    @NotNull
    protected String _levelsString() {
        return levels.toString();
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

    @Override
    public final int capacity() {
        return elements.length;
    }

    @Override
    protected final long getTail() {
        return tailSequence;
    }

    @Override
    protected final long getHead() {
        return headSequence;
    }


    protected final long getTailIteration() {
        return tailIteration;
    }

    protected final long getHeadIteration() {
        return headIteration;
    }

    @Deprecated
    protected final boolean set(E e, int index) {
        return _insert(e, index);
    }

    protected final int set(final E e, final long tail, final long currentTail) {
        int index = calcIndex(currentTail);
        long level = computeLevel(currentTail);
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

                assert Arrays.asList(GO_NEXT, TRY_AGAIN, GET_CURRENT).contains(result);

                if (result != TRY_AGAIN) {
                    return result;
                }
            }
        }
    }

    private int failPutting(int index, long level) {
        int result;
        final long l = _level(index);
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

    private long _level(int index) {
        return levels.get(index);
    }

    private boolean _levelCas(int index, long expect, long update) {
        return levels.compareAndSet(index, expect, update);
    }

    private boolean isAlreadyPut(long level, long l) {
        return (l - level) == 1;
    }

    private void finishPutting(int index, long level) {
        long nextLevel = computeNextLevel(level);
        int expect = PUTTING;
        boolean set = _levelCas(index, expect, nextLevel);
        assert set : "finishPutting fail";
    }

    private boolean startPutting(int index, long level) {
        return _levelCas(index, level, PUTTING);
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
        final long level = computeNextLevel(computeLevel(currentHead));
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
        long nextLevel = computeNextLevel(level);
        boolean set = levels.compareAndSet(index, POOLING, nextLevel);
        assert set : "finishPooling fail";
    }

    private boolean startPooling(int index, long level) {
        return levels.compareAndSet(index, level, POOLING);
    }

    protected boolean setNextHead(long oldHead, long insertedHead) {
        return next(oldHead, insertedHead, headOffset, headIterationOffset);
    }

    protected final boolean setNextTail(long oldTail, long insertedTail) {
        return next(oldTail, insertedTail, tailOffset, tailIterationOffset);
    }

    private boolean next(long oldVal, long insertedVal, long sequenceOffset, long iteratorOffset) {
        assert insertedVal >= oldVal;
        long newValue = insertedVal + 1;
        assert oldVal < newValue;

        long currentIter = getLong(iteratorOffset);
        long nextIter = computeIteration(insertedVal);

        boolean iterated = false;
        boolean goNextIter = currentIter < nextIter;
        if (goNextIter) {
            assert nextIter - currentIter == 1 : "next iteration fail oldVal " + oldVal + ", newVal " + newValue;
            iterated = cas(iteratorOffset, currentIter, nextIter);
        } else {
            assert currentIter == nextIter : "check iteration fail oldVal " + oldVal + ", newVal " + newValue;
        }
        if (goNextIter && !iterated) {
            //todo нужна статистика!!!
            return false;
        }
        boolean set = cas(sequenceOffset, oldVal, newValue);
        while (!set) {
            long currentValue = getLong(sequenceOffset);
            if (currentValue < newValue) {
                set = cas(sequenceOffset, currentValue, newValue);
            } else {
                break;
            }
        }
        return set;
    }

    private long getLong(long offset) {
        return unsafe.getLong(this, offset);
    }

    private boolean cas(long valueOffset, long expectedValue, long newValue) {
        return unsafe.compareAndSwapLong(this, valueOffset, expectedValue, newValue);
    }
}
