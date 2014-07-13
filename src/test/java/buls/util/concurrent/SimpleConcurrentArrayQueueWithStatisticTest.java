package buls.util.concurrent;

import buls.util.concurrent.research.SimpleConcurrentArrayQueueWithStatistic;


/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class SimpleConcurrentArrayQueueWithStatisticTest extends SimpleConcurrentArrayQueueTest {

    @Override
    protected SimpleConcurrentArrayQueueWithStatistic<String> createQueue(int capacity, boolean writeStatistic) {
        return new SimpleConcurrentArrayQueueWithStatistic<>(capacity, true);
    }
}
