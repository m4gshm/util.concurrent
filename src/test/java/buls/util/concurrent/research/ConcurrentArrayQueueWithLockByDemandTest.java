package buls.util.concurrent.research;


import buls.util.concurrent.BaseArrayQueueTest;
import org.testng.annotations.Test;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
@Test
public class ConcurrentArrayQueueWithLockByDemandTest extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueueWithLockByDemand<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueueWithLockByDemand<>(capacity, writeStatistic);
    }
}
