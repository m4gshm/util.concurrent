package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueue<E> extends AbstractConcurrentArrayQueue<E> {

    protected static final long PUTTING = Long.MAX_VALUE;
    protected static final long POOLING = Long.MIN_VALUE;

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

    protected final int set(final E e, final long tail, final long currentTail) {
        while (isNotInterrupted()) {
            final int index = computeIndex(currentTail);
            final long level = beforeSetLevel(currentTail);
            final long level2 = (level == 0) ? beforeSetLevel(max_tail() + 1) : level;
            if (startPutting(index, level, level2)) try {
                _insert(e, index);
                setNextTail(tail, currentTail);
                return SUCCESS;
            } finally {
                finishPutting(currentTail, index);
            }
            else {
                int result = failPutting(index, level);
                if (result != TRY_AGAIN) return result;
            }
        }
        return INTERRUPTED;
    }

    private int failPutting(int index, long level) {
        assert level <= 0 : level;

        final long current = _level(index);
        assert current != 0 : current;

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

    private void finishPutting(long currentTail, int index) {
        long nextLevel = afterSetLevel(currentTail);
        _levelCas(index, PUTTING, nextLevel);
    }

    private long beforeSetLevel(long currentTail) {
        return -computeLevel(currentTail);
    }

    private long afterSetLevel(long currentTail) {
        long nextLevelCounter = nextLevelCounter(currentTail);
        return computeLevel(nextLevelCounter);
    }

    private long beforeGetLevel(long currentHead) {
        return afterSetLevel(currentHead);
    }

    final long afterGetLevel(long currentHead) {
        return -beforeGetLevel(currentHead);
    }

    private boolean startPutting(int index, long level, long level2) {
        return _levelCas(index, level, PUTTING) || _levelCas(index, level2, PUTTING);
    }

    @Nullable
    protected final E get(long head, long currentHead) {
        while (isNotInterrupted()) {
            final int index = computeIndex(currentHead);
            final long level = beforeGetLevel(currentHead);
            if (startPooling(index, level)) try {
                final E e = _retrieve(index);
                assert e != null;

                setNextHead(head, currentHead);
                return e;
            } finally {
                finishPooling(currentHead, index);
            }
            else if (stopTryPooling(index, level, currentHead))
                return null;
        }
        return null;
    }

    private boolean stopTryPooling(int index, long level, long currentHead) {
        assert level > 0;
        final long current = _level(index);
        //assert current != 0 : current + " " + level + " " + index + " " + currentHead + "\n" + this;
        return !(current == PUTTING || level == current);
    }

    private void finishPooling(long currentHead, int index) {
        final long nextLevel = afterGetLevel(currentHead);
        _levelCas(index, POOLING, nextLevel);
    }

    private boolean startPooling(int index, long level) {
        return _levelCas(index, level, POOLING);
    }
}
