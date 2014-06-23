package buls.util.concurrent;


import org.testng.annotations.Test;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
@Test
public class ConcurrentArrayQueue4Test extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueue4<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueue4<>(capacity, writeStatistic);
    }
}
