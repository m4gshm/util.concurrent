package buls.util.concurrent;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueue3<E> extends ConcurrentArrayQueue<E> {

    public static final int SET_FAILS = 20;
    public static final int GET_FAILS = 20;

    protected final AtomicLong getLocks = new AtomicLong();
    protected final AtomicLong getLockRequests = new AtomicLong();
    protected final AtomicLong setLocks = new AtomicLong();
    protected final AtomicLong setLockRequests = new AtomicLong();

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
        int index = calcIndex(currentTail);

        long fails = 0;
        boolean hasLock = false;
        AtomicBoolean needLockFlag = this.needSetLock;
        boolean needLock = needLockFlag.get();
        boolean lockRequested = false;
        try {
            if (needLock) {
                setLock.lock();
                startSetLock();
                hasLock = true;

                currentTail = computeTail(currentTail);
                if (checkTailOverflow(currentTail, capacity)) {
                    return false;
                }
                index = calcIndex(currentTail);
            }
            while (true) {
                if (set(e, index)) {
                    setNextTail(tail, currentTail);

                    successSet();
                    return true;
                } else {
                    ++fails;
                    if (hasLock) {
                        failLockedSet.incrementAndGet();
                    } else {
                        failSet();
                    }

                    if (!hasLock && fails >= SET_FAILS) {
                        //неудачник, просим блокировку
                        setLock.lock();
                        statSetLockRequest();
                        startSetLock();

                        hasLock = true;
                        //помечаем, что являемся реквестором лока
                        lockRequested = true;

                        boolean set = needLockFlag.compareAndSet(false, true);
                        if (!set) {
                            throw new IllegalStateException("cannot set needLock  true");
                        }
                    }

                    currentTail = computeTail(currentTail);
                    if (checkTailOverflow(currentTail, capacity)) {
                        return false;
                    }
                    index = calcIndex(currentTail);
                }
            }
        } finally {
            if (lockRequested) {
                boolean set = needLockFlag.compareAndSet(true, false);
                if (!set) {
                    throw new IllegalStateException("cannot set needSetLock false");
                }
            }

            if (hasLock) {
                setLock.unlock();
            }
        }
        //throw new IllegalStateException("setElement");
    }

    @Override
    protected E getElement(final long head, long tail) {
        long currentHead = head;
        int index = calcIndex(currentHead);

        long fails = 0;

        boolean hasLock = false;
        AtomicBoolean needLockFlag = this.needGetLock;
        boolean needLock = needLockFlag.get();
        boolean lockRequested = false;
        try {
            if (needLock) {
                getLock.lock();
                statGetLock();
                hasLock = true;

                currentHead = computeHead(currentHead);
                if (checkHeadOverflow(currentHead)) {
                    return null;
                }
                index = calcIndex(currentHead);

            }
            while (true) {
                E e;
                if ((e = get(index)) != null) {
                    setNextHead(head, currentHead);
                    successGet();
                    return e;
                } else {
                    //yield();
                    ++fails;

                    failGet();
                    if (!hasLock && fails >= GET_FAILS) {
                        getLock.lock();
                        statGetLock();
                        statGetLockRequest();
                        hasLock = true;
                        //помечаем, что являемся реквестором лока
                        lockRequested = true;

                        boolean set = needLockFlag.compareAndSet(false, true);
                        if (!set) {
                            throw new IllegalStateException("cannot set needGetLock true");
                        }
                    }

                    currentHead = computeHead(currentHead);
                    if (checkHeadOverflow(currentHead)) {
                        return null;
                    }
                    index = calcIndex(currentHead);
                }
            }
        } finally {
            if (lockRequested) {
                boolean set = needLockFlag.compareAndSet(true, false);
                if (!set) {
                    throw new IllegalStateException("cannot set needSetLock false");
                }
            }

            if (hasLock) {
                getLock.unlock();
            }
        }
        //throw new IllegalStateException("getElement");
    }

    private void statGetLockRequest() {
        if (writeStatistic) getLockRequests.incrementAndGet();
    }

    private void statGetLock() {
        if (writeStatistic) getLocks.incrementAndGet();
    }


    private void statSetLockRequest() {
        if (writeStatistic) setLockRequests.incrementAndGet();
    }

    private void startSetLock() {
        if (writeStatistic) setLocks.incrementAndGet();
    }

    @Override
    public void printStatistic(PrintStream printStream) {
        if (writeStatistic) {
            super.printStatistic(printStream);
            printStream.println("set locks " + setLocks.get());
            printStream.println("set lock requests " + setLockRequests.get());
            printStream.println("get locks " + getLocks.get());
            printStream.println("get lock requests " + getLockRequests.get());
        }
    }

}
