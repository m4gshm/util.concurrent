package buls.util.concurrent;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ArraySpinBlockedQueue<E> extends ConcurrentArrayQueue3<E> implements BlockingQueue<E> {
    private final boolean yield;

    public ArraySpinBlockedQueue(int capacity, boolean yield, boolean writeStatistic) {
        super(capacity, writeStatistic);
        this.yield = yield;
    }

    @Override
    public void put(E e) throws InterruptedException {
        while (!offer(e)) {
            if (isInterrupted()) {
                throw new InterruptedException();
            }
            yieldIfNeed();
        }
    }

    private void yieldIfNeed() {
        if (yield) {
            yield();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        long start = System.nanoTime();
        long end = start + unit.toNanos(timeout);
        while (System.nanoTime() < end) {
            boolean offer = offer(e);
            if (offer) {
                return true;
            }
            if (isInterrupted()) {
                throw new InterruptedException();
            }
            yieldIfNeed();
        }
        return false;
    }

    @Override
    public E take() throws InterruptedException {
        E result;
        while (null == (result = poll())) {
            if (isInterrupted()) {
                throw new InterruptedException();
            }
            yieldIfNeed();
        }
        return result;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long start = System.nanoTime();
        long end = start + unit.toNanos(timeout);
        while (System.nanoTime() < end) {
            E result = poll();
            if (result != null) {
                return result;
            }
            if (isInterrupted()) {
                throw new InterruptedException();
            }
            yieldIfNeed();
        }
        return null;
    }

    @Override
    public int remainingCapacity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException();
    }
}
