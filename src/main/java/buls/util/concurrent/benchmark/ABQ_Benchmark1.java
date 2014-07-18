package buls.util.concurrent.benchmark;

import org.jetbrains.annotations.NotNull;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ABQ_Benchmark1 extends BoundedQueueBenchmark {
    @Override @NotNull
    protected Queue<Runnable> createQueue() {
        return new ArrayBlockingQueue<>(capacity);
    }
}
