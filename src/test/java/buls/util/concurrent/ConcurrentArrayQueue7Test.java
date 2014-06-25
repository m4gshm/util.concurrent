package buls.util.concurrent;


import org.testng.annotations.Test;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
@Test
public class ConcurrentArrayQueue7Test extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueue7<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueue7<>(capacity, writeStatistic);
    }
}
