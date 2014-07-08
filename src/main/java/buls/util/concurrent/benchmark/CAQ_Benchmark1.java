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

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Warmup(time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(timeUnit = TimeUnit.MILLISECONDS, time = 100)
public class CAQ_Benchmark1 {

    @Param({"1", "2", "3"})
    public int readers;

    @Param({"100", "1000", "10000", "1000000"})
    public int capacity;
    public Service service;
    @Param({"false"})
    private boolean writeStatistic;

    @Setup(Level.Iteration)
    public void setup() {

        ServiceFactory factory = new ServiceFactory(readers, capacity, writeStatistic);
        factory.startup();
        service = factory.getService();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        try {
            service.shutdownAndWait();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

//    @Threads(1)
//    @Benchmark
//    public void _1_threads() {
//        sendMessage();
//    }

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

    protected void sendMessage() {
        try {
            service.sendMessage();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ServiceFactory extends AbstractNoBlockingQueueService {

        private final int capacity;

        public ServiceFactory(int threads, int capacity, boolean writeStatistic) {
            super(writeStatistic, threads);
            this.capacity = capacity;

        }


        @NotNull
        @Override
        protected AbstractExecutor createExecutor() {
            return new EmptyExecutor();
        }


        @Override
        @NotNull
        protected Queue<Runnable> createQueue() {
            return new ConcurrentArrayQueue<>(capacity, writeStatistic);
        }
    }

}
