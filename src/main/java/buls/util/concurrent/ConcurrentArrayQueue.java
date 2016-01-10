package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * It is an attempt to implement a bounded queue without using "fat" and spin locks.
 * <p/>
 * An bounded thread-safe {@linkplain java.util.Queue queue} backed by an array.
 * Consists from the array, a head counter, a tail counter and an auxiliary numeric array.
 * <p/>
 * Only atomic cas operations are used for the head and the tail counters.
 * {@linkplain java.util.concurrent.atomic.AtomicLongArray Atomic long array}
 * is used as the auxiliary array
 *
 * @author Bulgakov Alex
 */
public class ConcurrentArrayQueue<E> extends AbstractHeadTailArrayQueue<E> {

    protected static final long PUTTING = Long.MAX_VALUE;
    protected static final long POOLING = Long.MIN_VALUE;

    /**
     * auxiliary array is used as a lock per this queue's cell
     */
    @NotNull
    AtomicLongArray levels;

    public ConcurrentArrayQueue(int capacity) {
        this(capacity, false);
    }

    public ConcurrentArrayQueue(int capacity, boolean checkInterruption) {
        super(capacity, checkInterruption);
        levels = new AtomicLongArray(capacity);
    }

    @NotNull
    @Override
    public String toString() {
        return super.toString() + "\n" + _levelsString();
    }

    @NotNull
    protected String _levelsString() {
        return levels.toString();
    }

    /**
     * @return SUCCESS, INTERRUPTED or result from onPuttingFail
     */
    @Override
    protected final int set(final E e, final long oldTail, final long insertingTail, long head) {
        while (isNotInterrupted()) {
            final int index = computeIndex(insertingTail);
            final long level = getLevelBeforeSet(insertingTail);
            //level2 is needed for case when tail overflowed and reset to 0
            final long level2 = (level == 0) ? getLevelBeforeSet(max_sequence_value() + 1) : level;
            if (lockPutting(index, level, level2)) try {
                _insert(e, index);
                incrementTail(oldTail, insertingTail);
                return SUCCESS;
            } finally {
                releasePutting(insertingTail, index);
            }
            else {
                int result = onPuttingFail(index, level);
                if (result != TRY_AGAIN) return result;
            }
        }
        return INTERRUPTED;
    }

    /**
     * @return next operation after putting fail
     */
    private int onPuttingFail(int index, long level) {
        final long current = _level(index);

        if (current == PUTTING) return GO_NEXT;
        else if (current == POOLING || current == level) return TRY_AGAIN;
        else return GET_CURRENT_TAIL;
    }

    private long _level(int index) {
        return levels.get(index);
    }

    private boolean _levelCas(int index, long expect, long update) {
        return levels.compareAndSet(index, expect, update);
    }

    private void releasePutting(long currentTail, int index) {
        long nextLevel = getLevelAfterSet(currentTail);
        _levelCas(index, PUTTING, nextLevel);
    }

    private long getLevelBeforeSet(long currentTail) {
        return -computeLevel(currentTail);
    }

    private long getLevelAfterSet(long currentTail) {
        long nextLevelCounter = nextLevelCounter(currentTail);
        return computeLevel(nextLevelCounter);
    }

    private long beforeGetLevel(long currentHead) {
        return getLevelAfterSet(currentHead);
    }

    final long afterGetLevel(long currentHead) {
        return -beforeGetLevel(currentHead);
    }

    private boolean lockPutting(int index, long level, long level2) {
        return _levelCas(index, level, PUTTING) || _levelCas(index, level2, PUTTING);
    }

    @Nullable
    @Override
    protected final E get(final long head, final long currentHead) {
        while (isNotInterrupted()) {
            final int index = computeIndex(currentHead);
            final long level = beforeGetLevel(currentHead);
            if (lockPooling(index, level)) try {
                final E e = _retrieve(index);
                assert e != null;

                incrementHead(head, currentHead);
                return e;
            } finally {
                releasePooling(currentHead, index);
            }
            else if (isStopTryPooling(index, level)) {
                return null;
            }
        }
        return null;
    }

    private boolean isStopTryPooling(int index, long level) {
        final long current = _level(index);
        return !(current == PUTTING || level == current);
    }

    private void releasePooling(long currentHead, int index) {
        final long nextLevel = afterGetLevel(currentHead);
        _levelCas(index, POOLING, nextLevel);
    }

    private boolean lockPooling(int index, long level) {
        return _levelCas(index, level, POOLING);
    }

    protected long nextLevelCounter(long counter) {
        if (counter == MAX_VALUE) return 0;
        int capacity = capacity();
        return counter - (counter % capacity) + capacity;
    }
}
