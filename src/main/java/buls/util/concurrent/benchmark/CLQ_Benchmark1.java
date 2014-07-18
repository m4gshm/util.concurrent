package buls.util.concurrent.benchmark;

import org.jetbrains.annotations.NotNull;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CLQ_Benchmark1 extends AbstractBenchmark {
    @Override @NotNull
    protected Queue<Runnable> createQueue() {
        return new ConcurrentLinkedQueue<>();
    }
}
