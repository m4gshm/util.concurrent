package buls.util.concurrent;

import buls.util.concurrent.research.LevelBasedConcurrentArrayQueueWithStatistic;


/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class LevelBasedConcurrentArrayQueueWithStatisticTest extends LevelBasedConcurrentArrayQueueTest {

    @Override
    protected LevelBasedConcurrentArrayQueue<String> createQueue(int capacity, boolean writeStatistic) {
        return new LevelBasedConcurrentArrayQueueWithStatistic<>(capacity, true);
    }
}
