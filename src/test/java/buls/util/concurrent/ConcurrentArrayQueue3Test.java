package buls.util.concurrent;


import org.testng.annotations.Test;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
@Test
public class ConcurrentArrayQueue3Test extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueue3<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueue3<>(capacity, writeStatistic);
    }
}
