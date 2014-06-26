package buls.util.concurrent;

import java.util.concurrent.Semaphore;

/**
 * Created by Alex on 22.06.2014.
 */
public class ConcurrentArrayQueue5<E> extends ConcurrentArrayQueue7<E> {
    private final Semaphore setSemaphore;

    public ConcurrentArrayQueue5(int capacity, boolean writeStatistic, int concurrentSets) {
        super(capacity, writeStatistic);
        setSemaphore = new Semaphore(concurrentSets);
    }

    @Override
    protected boolean setElement(E e, long tail, long head) {
        try {
            setSemaphore.acquire();
            return super.setElement(e, tail, head);
        } catch (InterruptedException e1) {
            throw new RuntimeException(e1);
        } finally {
            setSemaphore.release();
        }
    }
}
