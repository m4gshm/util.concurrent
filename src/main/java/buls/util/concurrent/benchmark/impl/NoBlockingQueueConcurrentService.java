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
        boolean offer;
        long iterations = 0;
        while (!(offer = queue.offer(executor))) {
            ++iterations;
            if (shutdown) {
                break;
            }
            //yield();
            //parkClientThread(100 * iterations);
        }
        if (offer) {
            //unparkServerThreads();
            //yield();
        }
    }

    private void parkClientThread(long iterations) {
        LockSupport.park();
    }


    protected void unparkServerThreads() {
        for (Thread t : threads) {
            LockSupport.unpark(t);
        }
    }

    private void parkServerThread() {
        LockSupport.park();
    }

    @Override
    protected Runnable getFromQueue() throws InterruptedException {
        Runnable runnable;
        long iterations = 0;
        while ((runnable = queue.poll()) == null) {
            ++iterations;
            if (shutdown && queue.isEmpty()) {
                break;
            }
            //parkServerThread();

        }
        //yield();
        return runnable;
    }

    private void yield() {
        Thread.yield();
    }

    @Override
    protected Runnable getFromQueueAfterInterruption() {
        return queue.poll();
    }

}
