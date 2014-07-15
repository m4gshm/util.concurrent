package buls.util.concurrent.research;


import buls.util.concurrent.BaseArrayQueueTest;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueueWithSemaphoreTest extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueueWithSemaphore<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueueWithSemaphore<>(capacity, writeStatistic, 3);
    }
}
