package buls.util.concurrent;

import org.testng.annotations.Test;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
@Test
public class ConcurrentArrayQueueTest extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueue<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueue<>(capacity, writeStatistic);
    }

}
