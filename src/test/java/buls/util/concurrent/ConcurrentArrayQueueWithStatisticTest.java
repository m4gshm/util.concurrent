package buls.util.concurrent;

import buls.util.concurrent.BaseArrayQueueTest;
import buls.util.concurrent.ConcurrentArrayQueue;
import buls.util.concurrent.research.ConcurrentArrayQueueWithStatistic;
import org.junit.Assert;
import org.junit.Test;


/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueueWithStatisticTest extends ConcurrentArrayQueueTest {

    @Override
    protected ConcurrentArrayQueue<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueueWithStatistic<>(capacity, true);
    }
}
