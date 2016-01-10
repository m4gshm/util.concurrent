package buls.util.concurrent.research;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
@Deprecated
public class ConcurrentArrayQueueWithLockByDemand<E> extends ConcurrentArrayQueueWithStatistic<E> {

    public static final int SET_FAILS = 100;
    public static final int GET_FAILS = 100;

    protected final LongAdder getLocks = new LongAdder();
    protected final LongAdder getLockRequests = new LongAdder();
    protected final LongAdder setLocks = new LongAdder();
    protected final LongAdder setLockRequests = new LongAdder();

    private final Lock getLock = new ReentrantLock();
    private final Lock setLock = new ReentrantLock();

    private final AtomicBoolean needSetLock = new AtomicBoolean();
    private final AtomicBoolean needGetLock = new AtomicBoolean();

    public ConcurrentArrayQueueWithLockByDemand(int capacity, boolean writeStatistic) {
        super(capacity, writeStatistic);
    }

    @Override
    protected boolean setElement(@NotNull final E e, final long tail, long head) {
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
                final int res = set(e, tail, currentTail, head);
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

                    boolean overflow = checkTail(currentTail, capacity);
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

    @Nullable
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
                    currentHead = computeNextHead(currentHead);
                    if (checkHead(currentHead, t)) {
                        return null;
                    }
                    assert delta(currentHead, t) > 0 : currentHead + " " + t + ", delta " + delta(currentHead, t);
                }
            }
        } finally {
            unmarkNeedLock(lockRequested, needLockFlag);

            if (hasLock) {
                lock.unlock();
            }
        }
    }

    private boolean acquireOnFail(long fails, int threshold, @NotNull AtomicBoolean flag, @NotNull Lock lock) {
        boolean needLock = fails >= threshold;
        boolean hasLock = acquireLock(needLock, lock);
        if (hasLock) {
            markNeedLock(flag);
        }
        return hasLock;
    }

    private void unmarkNeedLock(boolean lockRequested, @NotNull AtomicBoolean flag) {
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

    private void markNeedLock(@NotNull AtomicBoolean flag) {
        boolean set = flag.compareAndSet(false, true);
        if (!set) {
            throw new IllegalStateException("cannot set needLock  true");
        }
    }

    private boolean acquireLock(boolean needLock, @NotNull Lock lock) {
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
    public void printStatistic(@NotNull PrintStream printStream) {
        if (writeStatistic) {
            super.printStatistic(printStream);
            printStream.println("set locks " + setLocks);
            printStream.println("set lock requests " + setLockRequests);
            printStream.println("get locks " + getLocks);
            printStream.println("get lock requests " + getLockRequests);
        }
    }

}
