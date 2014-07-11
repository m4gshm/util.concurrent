package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueue<E> extends AbstractConcurrentArrayQueue<E> {

    public ConcurrentArrayQueue(int capacity) {
        this(capacity, false);
    }

    public ConcurrentArrayQueue(int capacity, boolean checkInterruption) {
        super(capacity, checkInterruption);
    }

    @Override
    protected boolean setElement(@NotNull final E e, final long tail, final long head) {
        long currentTail = tail;
        final int capacity = capacity();

        while (isNotInterrupted()) {
            final int res = set(e, tail, currentTail, head);
            if (res == SUCCESS) {
                successSet();
                return true;
            } else {
                failSet();
                currentTail = computeTail(currentTail, res);

                boolean overflow = checkTail(currentTail, capacity);
                if (overflow) {
                    return false;
                }
            }
        }
        return false;
    }

    @Nullable
    @Override
    protected E getElement(final long head, final long tail) {
        assert delta(head, tail) > 0 : head + " " + tail + ", size " + delta(head, tail);
        long currentHead = head;
        long currentTail = tail;
        long fails = 0;
        while (isNotInterrupted()) {
            E e;
            if ((e = get(head, currentHead, currentTail, fails)) != null) {
                successGet();
                return e;
            } else {
                fails++;
                failGet();

                currentHead = computeHead(currentHead);
                long t = getTail();

                if (checkHead(currentHead, t)) {
                    return null;
                }
                assert delta(currentHead, t) > 0 : currentHead + " " + t + ", delta " + delta(currentHead, t);
                currentTail = t;
            }
        }
        return null;
    }

    protected void failGet() {
    }

    protected void successGet() {
    }

    protected void failSet() {
    }

    protected void successSet() {
    }
}
