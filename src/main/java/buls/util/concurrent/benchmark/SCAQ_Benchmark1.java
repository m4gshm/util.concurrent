package buls.util.concurrent.benchmark;

import buls.util.concurrent.SimpleConcurrentArrayQueue;
import org.jetbrains.annotations.NotNull;

import java.util.Queue;

public class SCAQ_Benchmark1 extends AbstractBenchmark {
    @Override @NotNull
    protected Queue<Runnable> createQueue() {
        return new SimpleConcurrentArrayQueue<>(capacity);
    }
}
