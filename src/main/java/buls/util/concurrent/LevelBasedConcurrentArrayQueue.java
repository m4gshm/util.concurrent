package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class LevelBasedConcurrentArrayQueue<E> extends AbstractConcurrentArrayQueue<E> {

    protected static final long PUTTING = Long.MAX_VALUE;
    protected static final long POOLING = Long.MIN_VALUE;

    @NotNull
    AtomicLongArray levels;

    public LevelBasedConcurrentArrayQueue(int capacity) {
        this(capacity, false);
    }

    public LevelBasedConcurrentArrayQueue(int capacity, boolean checkInterruption) {
        super(capacity, checkInterruption);
        levels = new AtomicLongArray(capacity);
    }

    @NotNull
    @Override
    public String toString() {
        return super.toString()
                + "\n" + _levelsString();
    }

    @NotNull
    protected String _levelsString() {
        return levels.toString();
    }

    protected final int set(final E e, final long tail, final long currentTail, final long head) {
        while (isNotInterrupted()) {
            final int index = computeIndex(currentTail);
            final long level = beforeSetLevel(currentTail);
            final long level2 = (level == 0) ? beforeSetLevel(max_tail() + 1) : level;
            if (startPutting(index, level, level2)) {
                try {
                    _insert(e, index);
                    int result;

                    setNextTail(tail, currentTail);

                    result = SUCCESS;
                    return result;
                } finally {
                    finishPutting(currentTail, index);
                }
            } else {
                int result = failPutting(index, level/*, currentTail, head*/);

                assert Arrays.asList(GO_NEXT, TRY_AGAIN, GET_CURRENT_TAIL).contains(result);

                if (result != TRY_AGAIN) {
                    return result;
                }
            }
        }
        return INTERRUPTED;
    }

    private int failPutting(int index, long level/*, long currentTail, long head*/) {
        assert level <= 0 : level;

        final long current = _level(index);
        assert current != 0 : current;

        final int result;
        if (current == PUTTING) {
            result = GO_NEXT;
        } else if (current == POOLING) {
            result = TRY_AGAIN;
        } else if (current == level) {
            //pooled
            result = TRY_AGAIN;
        } else {
            result = GET_CURRENT_TAIL;
//            if (current > 0) {
//                long l = -level;
//                if (current == l) {
//                    //поток-вставщик заполнил текущую ячейку, прошел круг и пытается вставить в неё же на следующем уровне
//                    //todo exception
//                    return GET_CURRENT_TAIL;
//                } else if (current > l) {
//                    //поток опоздал на один уровень, выходим и запрашиваем текущий хвост
//                    return GET_CURRENT_TAIL;
//                }
//                throw new IllegalStateException(current + ", " + l);
//            } else if (current < 0) {
//                assert level > current : level + ">" + current;
//                result = GET_CURRENT_TAIL;
//            } else {
//                throw new IllegalStateException(current + ", " + level);
//            }
        }
        return result;
    }

    private long _level(int index) {
        return levels.get(index);
    }

    private boolean _levelCas(int index, long expect, long update) {
        return levels.compareAndSet(index, expect, update);
    }

    private void finishPutting(long currentTail, int index) {
        long nextLevel = afterSetLevel(currentTail);
        long expect = PUTTING;
        assert nextLevel >= 0;
        boolean set = _levelCas(index, expect, nextLevel);
        assert set : "finishPutting fail";
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
    protected final E get(long head, long currentHead, long tail, long fails) {

        assert delta(head, currentHead) >= 0 : head + " " + currentHead + ", delta " + delta(head, currentHead) + ", fails" + fails+ "\n" +this;
        assert delta(currentHead, tail) > 0 : currentHead + " " + tail + ", delta " + delta(currentHead, tail) + ", fails" + fails+ "\n" +this;

        while (isNotInterrupted()) {
            final int index = computeIndex(currentHead);
            final long level = beforeGetLevel(currentHead);
            if (startPooling(index, level)) {
                try {
                    final E e = _retrieve(index);
                    assert e != null;
                    setNextHead(head, currentHead);

                    return e;
                } finally {
                    finishPooling(currentHead, index);
                }
            } else if (stopTryPooling(index, level, currentHead, tail)) {
                return null;
            }
        }
        return null;
    }

    private boolean stopTryPooling(int index, long level, long currentHead, long tail) {
        assert level > 0;
        final long current = _level(index);
        assert current != 0: current +" "+level +" "+currentHead +" " +tail +"\n"+this;
        if (current == PUTTING) {
            return false;
        } else if (level == current) {
            return false;
        } else {
            return true;
//            if (current == POOLING) {
//                //return Pol.GET_CURRENT_HEAD;
//                return true;
//            } else if (current < 0) {
//                long c = -current;
//                if (c == level) {
//                    //нас обогнали в пуллинге
//                    //return Pol.GET_CURRENT_HEAD;
//                    return true;
//                } else if (c > level) {
//                    //нас обогнали в пуллинге как минимум на один уровень
//                    //return Pol.GET_CURRENT_HEAD;
//                    return true;
//                } else if (c < level) {
//                    //return Pol.GET_CURRENT_HEAD;
//                    //todo exception
//                    return true;
//                }
//                throw new IllegalStateException("invalid level :" + level + " " + current + ", index " + index + ", head " + currentHead + ", tail " + tail + "\n" + this);
//            } else if (level < current) {
//                return true;
//            } else {
//                assert level == -current : "invalid level :" + level + " " + current + ", index " + index + ", head " + currentHead + ", tail " + tail + "\n" + this;
//                return true;
//            }
        }
    }

    private void finishPooling(long currentHead, int index) {
        final long nextLevel = afterGetLevel(currentHead);
        assert nextLevel < 0;
        final boolean set = _levelCas(index, POOLING, nextLevel);
        assert set : "finishPooling fail";
    }

    private boolean startPooling(int index, long level) {
        return _levelCas(index, level, POOLING);
    }
}
