package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public abstract class AbstractConcurrentArrayQueue<E> extends AbstractArrayQueue<E> {

    protected final boolean checkInterruption;

    public AbstractConcurrentArrayQueue(int capacity, boolean checkInterruption) {
        super(capacity);
        this.checkInterruption = checkInterruption;
    }

    @Override
    protected boolean setElement(@NotNull final E e, final long tail, final long head) {
        long currentTail = tail;
        final int capacity = capacity();

        while (isNotInterrupted()) {
            final int res = set(e, tail, currentTail);
            if (res == SUCCESS) {
                successSet();
                return true;
            } else {
                failSet();
                currentTail = computeTail(currentTail, res);

                boolean overflow = checkTail(currentTail, capacity);
                if (overflow) return false;
            }
        }
        return false;
    }

    protected abstract int set(E e, long tail, long currentTail);

    @Nullable
    @Override
    protected E getElement(final long head, final long tail) {
        long currentHead = head;
        while (isNotInterrupted()) {
            E e;
            if ((e = get(head, currentHead)) != null) {
                successGet();
                return e;
            } else {
                failGet();

                currentHead = computeHead(currentHead);
                long t = getTail();
                if (checkHead(currentHead, t)) return null;
            }
        }
        return null;
    }

    protected abstract E get(long head, long currentHead);

    protected void failGet() {
    }

    protected void successGet() {
    }

    protected void failSet() {
    }

    protected void successSet() {
    }

    protected final boolean isNotInterrupted() {
        return !(checkInterruption && Thread.interrupted());
    }

    protected final long computeLevel(long counter) {
        return counter / capacity();
    }

    protected long nextLevelCounter(long counter) {
        if (counter == MAX_VALUE) return 0;
        int capacity = capacity();
        return counter - (counter % capacity) + capacity;
    }
}
