package buls.util.concurrent.benchmark;

import buls.util.concurrent.ConcurrentArrayQueue;
import buls.util.concurrent.benchmark.impl.AbstractExecutor;
import buls.util.concurrent.benchmark.impl.AbstractNoBlockingQueueService;
import buls.util.concurrent.benchmark.impl.EmptyExecutor;
import buls.util.concurrent.benchmark.impl.Service;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class _CAQ_Benchmark1 extends BoundedQueueBenchmark {

    @Override @NotNull
    protected Queue<Runnable> createQueue() {
        return new ConcurrentArrayQueue<>(capacity);
    }

}
