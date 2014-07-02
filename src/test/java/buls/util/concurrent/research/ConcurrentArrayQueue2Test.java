package buls.util.concurrent.research;


import buls.util.concurrent.BaseArrayQueueTest;
import org.testng.annotations.Test;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
@Test
public class ConcurrentArrayQueue2Test extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueue2<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueue2<>(capacity, writeStatistic);
    }
}
