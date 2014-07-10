package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractConcurrentArrayQueue<E> extends AbstractArrayQueue<E> {

    public final static int SUCCESS = 0;
    public final static int GO_NEXT = 1;
    public final static int GET_CURRENT_TAIL = 2;
    public final static int TRY_AGAIN = 4;
    public final static int INTERRUPTED = 5;

    protected final AtomicLong tailSequence = new AtomicLong();
    protected final AtomicLong headSequence = new AtomicLong();

    @NotNull
    private final Object[] elements;
    @NotNull
    AtomicLongArray levels;

    private final boolean checkInterruption;

    protected AbstractConcurrentArrayQueue(int capacity, boolean checkInterruption) {
        super(capacity);
        this.checkInterruption = checkInterruption;
        this.elements = new Object[capacity];
        levels = new AtomicLongArray(capacity);
    }

    @NotNull
    @Override
    public String toString() {
        return "h: " + headSequence + ", t: " + tailSequence + ", c:" + capacity()
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
    protected final int delta(final long head, final long tail) {
        final long delta;
        if (tail >= head) {
            delta = tail - head;
        } else {
//            while (!tailOverflow) {
//               boolean interrupted = !isNotInterrupted();
//                if (interrupted) {
//                    break;
//                }
//            }
//            assert tailOverflow : "tailOverflow";
            //if (tailOverflow) {
            delta = tail - 0 + max_tail() - head + 1;
            //} else {
            //bullshit
            //    delta = tail - head;
            //}
        }
        assert delta >= 0 : delta + " " + head + " " + tail + "\n" + this;
        return (int) delta;
    }

    @Override
    protected final long getTail() {
        return tailSequence.get();
    }

    @Override
    protected final long getHead() {
        return headSequence.get();
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

                    setNextTail2(tail, currentTail);

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
    protected final E _get(int index) {
        return (E) elements[index];
    }

    protected final boolean _insert(E e, int index) {
        elements[index] = e;
        return true;
    }

    @NotNull
    protected final E _retrieve(int index) {
        E e = _get(index);
        _insert(null, index);
        return e;
    }

    @Nullable
    protected final E get(long oldHead, long currentHead, long tail, long fails) {

        assert delta(oldHead, currentHead) >= 0 : oldHead + " " + currentHead + ", delta " + delta(oldHead, currentHead) + ", fails" + fails+ "\n" +this;
        assert delta(currentHead, tail) > 0 : currentHead + " " + tail + ", delta " + delta(currentHead, tail) + ", fails" + fails+ "\n" +this;

        while (isNotInterrupted()) {
            final int index = computeIndex(currentHead);
            final long level = beforeGetLevel(currentHead);
            if (startPooling(index, level)) {
                try {
                    final E e = _retrieve(index);
                    assert e != null;
                    setNextHead2(oldHead, currentHead);

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

    protected final boolean isNotInterrupted() {
        return !checkInterruption || Thread.interrupted();
    }

    private boolean stopTryPooling(int index, long level, long currentHead, long tail) {
        assert level > 0;
        final long current = _level(index);
        assert current != 0;
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

    protected void setNextHead2(long oldHead, long insertedHead) {
        next(oldHead, insertedHead, headSequence);
    }

    protected void setNextTail2(long oldTail, long insertedTail) {
        next(oldTail, insertedTail, tailSequence);
    }

    private boolean next(final long oldVal, final long insertedVal, final @NotNull AtomicLong sequence) {
//        assert insertedVal >= oldVal || (insertedVal < capacity() && oldVal <= max_tail())
//                : insertedVal + " " + oldVal + " " + capacity() + " " + max_tail()+" " +sequence + "\n" + this;
        assert insertedVal >= 0;
        assert insertedVal <= max_tail();

        final long newValue = _increment(insertedVal);
        boolean overflow = newValue == 0 && insertedVal == max_tail();
        boolean set = cas(sequence, oldVal, newValue);
        while (!set) {
            final long currentValue = sequence.get();
            if (!overflow) {
                if (currentValue < newValue) {
                    set = cas(sequence, currentValue, newValue);
                } else {
                    break;
                }
            } else {
                //overflow detected
                assert insertedVal == max_tail() : insertedVal + " " + max_tail();
                if (!(currentValue > newValue) && currentValue <= insertedVal) {
                    set = cas(sequence, currentValue, newValue);
                } else {
                    break;
                }
            }
        }
        return set;
    }

    private long _increment(long counter) {
        return (counter == max_tail()) ? 0 : counter + 1;
    }

    private boolean cas(@NotNull AtomicLong counter, long expected, long update) {
        return counter.compareAndSet(expected, update);
    }

    protected long computeTail(long currentTail, int calculateType) {
        if (calculateType == GO_NEXT) {
            currentTail = _increment(currentTail);
        } else {
            currentTail = getTail();
            assert calculateType == GET_CURRENT_TAIL;
        }
        return currentTail;
    }

    protected final boolean checkTail(long tailForInserting, int capacity) {
        long head = getHead();
        long amount = delta(head, tailForInserting);
        return amount > capacity;
    }

    protected final boolean checkHead(long headForGetting, long tail) {
        boolean overflow;
        if (headForGetting == tail) {
            overflow = true;
        } else {
            int delta = delta(headForGetting, tail);
            int capacity = capacity();
            overflow = delta > capacity;
        }
        return overflow;
    }

    protected long computeHead(long head) {
        final long h = getHead();
        if (head < h) {
            head = h;
        } else {
            head = _increment(head);
        }
        return head;
    }
}
