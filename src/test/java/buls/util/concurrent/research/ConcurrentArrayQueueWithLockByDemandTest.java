package buls.util.concurrent.research;


import buls.util.concurrent.BaseArrayQueueTest;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueueWithLockByDemandTest extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueueWithLockByDemand<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueueWithLockByDemand<>(capacity, writeStatistic);
    }
}
