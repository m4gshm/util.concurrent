package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractConcurrentArrayQueue1<E> extends AbstractConcurrentArrayQueue<E> {

    protected final AtomicLong headIteration = new AtomicLong(1);
    protected final AtomicLong tailIteration = new AtomicLong(1);

    public AbstractConcurrentArrayQueue1(int capacity) {
        super(capacity);
    }

    protected final long getTailIteration() {
        return tailIteration.get();
    }

    protected final long getHeadIteration() {
        return headIteration.get();
    }

    @Override
    protected boolean setNextHead(long oldHead, long insertedHead) {
        return next(oldHead, insertedHead, headSequence, headIteration);
    }

    @Override
    protected final boolean setNextTail(long oldTail, long insertedTail) {
        return next(oldTail, insertedTail, tailSequence, tailIteration);
    }

    private boolean next(long oldVal, long insertedVal, @NotNull AtomicLong sequence, @NotNull AtomicLong iteration) {
        assert insertedVal >= oldVal;
        long newValue = insertedVal + 1;
        assert oldVal < newValue;

        long currentIter = iteration.get();
        long nextIter = computeIteration(insertedVal);

        boolean iterated = false;
        boolean goNextIter = currentIter < nextIter;
        if (goNextIter) {
            assert nextIter - currentIter == 1 : "next iteration fail oldVal " + oldVal + ", newVal " + newValue;
            iterated = cas(iteration, currentIter, nextIter);
        } else {
            assert currentIter == nextIter : "check iteration fail oldVal " + oldVal + ", newVal " + newValue;
        }
        if (goNextIter && !iterated) {
            //todo нужна статистика!!!
            return false;
        }
        boolean set = cas(sequence, oldVal, newValue);
        while (!set) {
            long currentValue = sequence.get();
            if (currentValue < newValue) {
                set = cas(sequence, currentValue, newValue);
            } else {
                break;
            }
        }
        return set;
    }

    private boolean cas(AtomicLong counter, long expected, long update) {
        return counter.compareAndSet(expected, update);
    }
}
