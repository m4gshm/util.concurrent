package buls.util.concurrent;


import org.testng.annotations.Test;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
@Test
public class ConcurrentArrayQueue5Test extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueue5<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueue5<>(capacity, writeStatistic, 3);
    }
}
