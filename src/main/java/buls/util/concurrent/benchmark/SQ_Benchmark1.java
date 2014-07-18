package buls.util.concurrent.benchmark;

import buls.util.concurrent.research.SimpleConcurrentArrayQueue;
import org.jetbrains.annotations.NotNull;

import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

public class SQ_Benchmark1 extends AbstractBenchmark {
    @Override @NotNull
    protected Queue<Runnable> createQueue() {
        return new SynchronousQueue<>();
    }
}
