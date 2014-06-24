package buls.util.concurrent;


import org.testng.annotations.Test;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
@Test
public class ConcurrentArrayQueue6Test extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueue6<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueue6<>(capacity, writeStatistic);
    }
}
