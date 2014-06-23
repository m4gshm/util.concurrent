package buls.util.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueue4<E> extends ConcurrentArrayQueue<E> {

    public static final int FAIL_SETS = 5;
    public static final int FAIL_GETS = 5;

    private final Lock setLock = new ReentrantLock();
    private final Lock getLock = new ReentrantLock();

    public ConcurrentArrayQueue4(int capacity, boolean writeStatistic) {
        super(capacity, writeStatistic);
    }

    @Override
    protected boolean setElement(final E e, final long tail) {
        boolean locked;
        int iterations = 0;
        while (!(locked = setLock.tryLock())) {
            if (++iterations >= FAIL_SETS) {
                break;
            }
        }

        try {
            if (!locked) {
                setLock.lock();
            }

            long currentTail = getTail();
            int capacity = capacity();
            int index = calcIndex(currentTail);

            if (checkTailOverflow(currentTail, capacity)) {
                return false;
            }

            boolean set = set(e, index);
            assert set;

            boolean setNextTail = setNextTail(tail, currentTail);
            assert setNextTail;

            checkHeadTailConsistency();

            successSet();

            return true;

        } finally {
            setLock.unlock();
        }
    }

    @Override
    protected E getElement(final long head) {

        boolean locked;
        int iterations = 0;
        while (!(locked = getLock.tryLock())) {
            if (++iterations >= FAIL_GETS) {
                break;
            }
        }

        try {
            if (!locked) {
                getLock.lock();
            }
            long currentHead = getHead();
            if (checkHeadOverflow(currentHead)) {
                return null;
            }

            int index = calcIndex(currentHead);
            E e = get(index);
            assert e != null;

            boolean setNextHead = setNextHead(head, currentHead);
            assert setNextHead;
            checkHeadTailConsistency();

            successGet();

            return e;
        } finally {
            getLock.unlock();
        }

        //throw new IllegalStateException("getElement");
    }

}
