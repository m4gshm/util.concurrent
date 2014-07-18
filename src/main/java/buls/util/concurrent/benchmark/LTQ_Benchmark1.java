package buls.util.concurrent.benchmark;

import org.jetbrains.annotations.NotNull;

import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;

public class LTQ_Benchmark1 extends AbstractBenchmark {
    @Override
    @NotNull
    protected Queue<Runnable> createQueue() {
        return new LinkedTransferQueue<>();
    }
}
