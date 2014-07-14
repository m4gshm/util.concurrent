package buls.util.concurrent.benchmark.impl;

import org.jetbrains.annotations.NotNull;

import java.util.Queue;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractNoBlockingQueueService extends AbstractServiceState {

    protected final boolean writeStatistic;
    private final int threads;

    public AbstractNoBlockingQueueService(boolean writeStatistic, int threads) {
        this.writeStatistic = writeStatistic;
        this.threads = threads;
    }

    @NotNull
    @Override
    public Service createService(Queue<Runnable> queue, AbstractExecutor executor) {
        return new NoBlockingQueueConcurrentService(getClass().getSimpleName(), threads,
                writeStatistic, writeStatistic, false, executor, queue);
    }

}
