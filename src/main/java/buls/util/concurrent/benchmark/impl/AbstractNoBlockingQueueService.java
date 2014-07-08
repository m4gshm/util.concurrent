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
    protected Service createService() {
        @NotNull final AbstractExecutor executor = createExecutor();
        @NotNull final Queue<Runnable> queue = createQueue();
        return new NoBlockingQueueConcurrentService(getClass().getSimpleName(), threads,
                writeStatistic, writeStatistic, true, executor, queue);

    }

    @NotNull
    protected abstract Queue<Runnable> createQueue();

    @NotNull
    protected abstract AbstractExecutor createExecutor();

}
