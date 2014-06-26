package buls.util.concurrent;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueue3<E> extends ConcurrentArrayQueue<E> {

    public static final int SET_FAILS = 20;
    public static final int GET_FAILS = 20;

    protected final LongAdder getLocks = new LongAdder();
    protected final LongAdder getLockRequests = new LongAdder();
    protected final LongAdder setLocks = new LongAdder();
    protected final LongAdder setLockRequests = new LongAdder();

    private final Lock getLock = new ReentrantLock();
    private final Lock setLock = new ReentrantLock();

    private final AtomicBoolean needSetLock = new AtomicBoolean();
    private final AtomicBoolean needGetLock = new AtomicBoolean();

    public ConcurrentArrayQueue3(int capacity, boolean writeStatistic) {
        super(capacity, writeStatistic);
    }

    @Override
    protected boolean setElement(final E e, final long tail, long head) {
        long currentTail = tail;
        int capacity = capacity();

        long fails = 0;
        boolean hasLock = false;
        AtomicBoolean needLockFlag = this.needSetLock;
        boolean needLock = needLockFlag.get();
        boolean lockRequested = false;
        try {
            final Lock lock = setLock;
            hasLock = acquireLock(needLock, lock);
            while (true) {
                final int res = set(e, tail, currentTail);
                if (res == SUCCESS) {
                    successSet();
                    return true;
                } else {
                    ++fails;

                    failSet(hasLock);

                    if (!hasLock) {
                        hasLock = acquireOnFail(fails, SET_FAILS, needLockFlag, lock);
                        lockRequested = hasLock;
                    }

                    currentTail = computeTail(currentTail, res);

                    boolean overflow = checkTailOverflow(currentTail, capacity);
                    if (overflow) {
                        return false;
                    }
                }
            }
        } finally {
            unmarkNeedLock(lockRequested, needLockFlag);
            releaseLock(hasLock);
        }
    }


    @Override
    protected E getElement(final long head, long tail) {
        long currentHead = head;
        long fails = 0;
        boolean hasLock = false;
        AtomicBoolean needLockFlag = this.needGetLock;
        boolean needLock = needLockFlag.get();
        boolean lockRequested = false;
        Lock lock = getLock;
        try {
            hasLock = acquireLock(needLock, lock);
            while (true) {
                E e;
                if ((e = get(head, currentHead)) != null) {
                    successGet();
                    return e;
                } else {
                    ++fails;
                    failGet();

                    if (!hasLock) {
                        hasLock = acquireOnFail(fails, GET_FAILS, needLockFlag, lock);
                        lockRequested = hasLock;
                    }

                    long t = getTail();
                    currentHead = computeHead(currentHead, t);
                    if (checkHeadOverflow(currentHead, t)) {
                        return null;
                    }
                }
            }
        } finally {
            unmarkNeedLock(lockRequested, needLockFlag);

            if (hasLock) {
                lock.unlock();
            }
        }
        //throw new IllegalStateException("getElement");
    }

    private boolean acquireOnFail(long fails, int threshold, AtomicBoolean flag, Lock lock) {
        boolean needLock = fails >= threshold;
        boolean hasLock = acquireLock(needLock, lock);
        if (hasLock) {
            markNeedLock(flag);
        }
        return hasLock;
    }

    private void unmarkNeedLock(boolean lockRequested, AtomicBoolean flag) {
        if (lockRequested) {
            boolean set = flag.compareAndSet(true, false);
            //if (!set) {
            //    throw new IllegalStateException("cannot set needSetLock false");
            //}
        }
    }

    private void releaseLock(boolean hasLock) {
        if (hasLock) {
            setLock.unlock();
        }
    }

    private void failSet(boolean hasLock) {
        if (hasLock && writeStatistic) {
            failLockedSet.increment();
        } else {
            failSet();
        }
    }

    private void markNeedLock(AtomicBoolean flag) {
        boolean set = flag.compareAndSet(false, true);
        if (!set) {
            throw new IllegalStateException("cannot set needLock  true");
        }
    }

    private boolean acquireLock(boolean needLock, Lock lock) {
        if (needLock) {
            //неудачник, просим блокировку
            lock.lock();

            if (writeStatistic) {
                if (lock == setLock) {
                    statSetLockRequest();
                    startSetLock();
                } else if (lock == getLock) {
                    statGetLockRequest();
                    statGetLock();
                }
            }
            return true;
        }
        return false;
    }


    @Deprecated
    protected final long computeTail(long tail) {
        long currentTail = getTail();
        if (tail < currentTail) {
            tail = currentTail;
        } else {
            tail++;
        }
        return tail;
    }

    private void statGetLockRequest() {
        if (writeStatistic) getLockRequests.increment();
    }

    private void statGetLock() {
        if (writeStatistic) getLocks.increment();
    }


    private void statSetLockRequest() {
        if (writeStatistic) setLockRequests.increment();
    }

    private void startSetLock() {
        if (writeStatistic) setLocks.increment();
    }

    @Override
    public void printStatistic(PrintStream printStream) {
        if (writeStatistic) {
            super.printStatistic(printStream);
            printStream.println("set locks " + setLocks);
            printStream.println("set lock requests " + setLockRequests);
            printStream.println("get locks " + getLocks);
            printStream.println("get lock requests " + getLockRequests);
        }
    }

}
