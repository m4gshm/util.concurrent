package buls.util.concurrent.benchmark.impl;

import buls.util.concurrent.benchmark.impl.AbstractExecutor;
import buls.util.concurrent.benchmark.impl.ConcurrentService;

import java.util.Queue;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by AlSBulgakov on 26.05.2014.
 */
public class NoBlockingQueueConcurrentService extends ConcurrentService {

    private final Queue<Runnable> queue;

    public NoBlockingQueueConcurrentService(String name, int threadAmount,
                                            boolean writeClientStatistic, boolean writeServerStatistic,
                                            boolean loopJoin, AbstractExecutor executor, Queue<Runnable> queue) {
        super(name, threadAmount, writeClientStatistic, writeServerStatistic, loopJoin, executor);
        this.queue = queue;

    }

    @Override
    protected final Queue getQueue() {
        return queue;
    }

    @Override
    protected void putToQueue() throws InterruptedException {
        while (!queue.offer(executor))
            if (shutdown) break;
    }

    @Override
    protected Runnable getFromQueue() throws InterruptedException {
        Runnable runnable;

        while ((runnable = queue.poll()) == null)
            if (shutdown && queue.isEmpty()) break;

        return runnable;
    }

    @Override
    protected Runnable getFromQueueAfterInterruption() {
        return queue.poll();
    }

}
