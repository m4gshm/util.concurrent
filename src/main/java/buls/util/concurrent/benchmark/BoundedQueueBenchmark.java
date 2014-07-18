package buls.util.concurrent.benchmark;

import org.openjdk.jmh.annotations.Param;

/**
 * Created by alexander on 19.07.14.
 */
public abstract class BoundedQueueBenchmark extends AbstractBenchmark {
    @Param({"10000", "1000000", "10000000"})
    public int capacity;
}
