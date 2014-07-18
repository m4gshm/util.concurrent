package buls.util.concurrent.benchmark;

import buls.util.concurrent.benchmark.impl.AbstractExecutor;
import buls.util.concurrent.benchmark.impl.AbstractNoBlockingQueueService;
import buls.util.concurrent.benchmark.impl.EmptyExecutor;
import buls.util.concurrent.benchmark.impl.Service;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * Created by alexander on 14.07.14.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.Throughput)
@Warmup(time = 50, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(timeUnit = TimeUnit.MILLISECONDS, time = 50)
public abstract class AbstractBenchmark {
    @Param({"1", "2", "3"})
    public int readers;

    public Service service;

    @Setup(Level.Iteration)
    public void setup() {
        ServiceFactory factory = new ServiceFactory(readers, false);
        service = factory.createService(createQueue(), createExecutor());
        service.start();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        try {
            service.shutdownAndWait();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Threads(2)
    @Benchmark
    public void _2_threads() {
        sendMessage();
    }

    @Threads(3)
    @Benchmark
    public void _3_threads() {
        sendMessage();
    }

    @Threads(4)
    @Benchmark
    public void _4_threads() {
        sendMessage();
    }

    @Threads(5)
    @Benchmark
    public void _5_threads() {
        sendMessage();
    }

    @Threads(6)
    @Benchmark
    public void _6_threads() {
        sendMessage();
    }

    protected void sendMessage() {
        try {
            service.sendMessage();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    protected abstract Queue<Runnable> createQueue();

    @NotNull
    protected AbstractExecutor createExecutor() {
        return new EmptyExecutor();
    }

    public static class ServiceFactory extends AbstractNoBlockingQueueService {

        public ServiceFactory(int threads, boolean writeStatistic) {
            super(writeStatistic, threads);
        }


    }
}
