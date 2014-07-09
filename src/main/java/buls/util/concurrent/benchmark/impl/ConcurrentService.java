package buls.util.concurrent.benchmark.impl;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Created by Bulgakov Alex on 29.05.2014.
 */
public abstract class ConcurrentService extends AbstractService {

    @NotNull
    protected final List<Thread> threads;

    protected final LongAdder badPuts = new LongAdder();
    protected final LongAdder badGet = new LongAdder();
    protected final ThreadLocal<LongAdder> serverWait = new ThreadLocal<>();
    protected final List<LongAdder> serverWaitTimers = new ArrayList<>();
    protected final ThreadLocal<LongAdder> serverTimer = new ThreadLocal<>();
    protected final List<LongAdder> serverTimers = new ArrayList<>();

    protected final ThreadLocal<LongAdder> localClientAttempts = new ThreadLocal<>();
    protected final List<LongAdder> clientAttempts = new ArrayList<>();
    protected final ThreadLocal<LongAdder> localClientTimer = new ThreadLocal<>();
    protected final List<LongAdder> clientTimers = new ArrayList<>();
    protected final boolean writeClientStatistic;
    protected final boolean writeServerStatistic;
    private final List<LongAdder> executionTimers = new ArrayList<>();
    private final ThreadLocal<LongAdder> localExecutionTimer = new ThreadLocal<>();
    private final List<LongAdder> executionSuccesses = new ArrayList<>();
    private final ThreadLocal<LongAdder> localExecutionSuccess = new ThreadLocal<>();
    private final List<LongAdder> interruptCheckTimers = new ArrayList<>();
    private final ThreadLocal<LongAdder> localInterruptCheckTimer = new ThreadLocal<>();
    private final boolean loopJoin;
    protected volatile boolean shutdown;
    private String name;

    public ConcurrentService(String name, int threadAmount,
                             boolean writeClientStatistic, boolean writeServerStatistic,
                             boolean loopJoin, AbstractExecutor executor) {
        super(executor);
        this.name = name;
        this.writeClientStatistic = writeClientStatistic;
        this.writeServerStatistic = writeServerStatistic;

        threads = new ArrayList<>();
        initThreads(threadAmount);
        this.loopJoin = loopJoin;
    }

    private static long cnv(@Nullable TimeUnit convertTo, long l) {
        l = convertTo != null ? convertTo.convert(l, NANOSECONDS) : l;
        return l;
    }

    protected void initThreads(int threadAmount) {
        if (threadAmount <= 0) {
            throw new IllegalArgumentException("threadAmount<=0");
        }
        CountDownLatch latch = new CountDownLatch(threadAmount);
        for (int i = 1; i <= threadAmount; ++i) {
            Thread thread = createThread(latch, i);
            threads.add(thread);
            latch.countDown();
        }
    }

    @Override
    public void start() {
        for (Thread t : threads) {
            t.start();
        }
    }

    @NotNull
    private Thread createThread(CountDownLatch startWaiter, int index) {
        Thread thread = new Thread(createRunner(startWaiter), getName() + "-server-thread-" + index);
        thread.setDaemon(false);
        return thread;
    }

    @NotNull
    protected CycleRunner<Runnable> createRunner(CountDownLatch startWaiter) {
        return new CycleRunner<>(startWaiter);
    }

    @Override
    public void shutdownAndWait() throws InterruptedException {
        shutdown();

        if (loopJoin) {
            loopJoin();
        } else {
            join();
        }

        executor.shutdown();
    }

    protected void loopJoin() {
        long iterations = 0;
        Queue queue = getQueue();
        while (!queue.isEmpty()) {
            ++iterations;
            int size = queue.size();
            checkLiveThreads();
            //Thread.sleep(10);

            parkClosingThread(iterations);
            //Thread.yield();
        }
    }

    private void checkLiveThreads() {
        int counter = 0;
        for (Thread t : threads) {
            if (t.isAlive()) counter++;
        }
        if (counter == 0) {
            throw new IllegalStateException("There are no alive server threads");
        }
    }

    private void parkClosingThread(long iterations) {
        LockSupport.parkNanos(100 * iterations);
    }

    protected void join() {
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
            }
        }
    }

    protected abstract Queue getQueue();


    public void shutdown() {
        shutdown = true;
        for (Thread t : threads) {
            t.interrupt();
        }
    }

    @Override
    protected final void doInvoke() throws InterruptedException {
        writeClientAttempt();

        long start = System.nanoTime();


        try {
            isInterrupted(System.nanoTime());
            putToQueue();
        } catch (InterruptedException e) {
            thrw(e);
        }

        writeClientTime(start);
    }

    protected abstract void putToQueue() throws InterruptedException;

    /**
     * Блокируещее чтение во время работы треда
     *
     * @return
     */
    protected abstract Runnable getFromQueue() throws InterruptedException;

    /**
     * Неблокируещее чтение после приостановки сервайсного треда
     *
     * @return
     */
    protected abstract Runnable getFromQueueAfterInterruption();

    @Override
    public void printStatistic(@NotNull PrintStream out) {
        long avgSuccess = printListStatistic(out, "execution successes", this.executionSuccesses, null);

        long avgClientAttempt = 0;
        long avgClientTime = 0;
        if (writeClientStatistic) {
            avgClientAttempt = printListStatistic(out, "client attempts", this.clientAttempts, null);
            avgClientTime = printListStatistic(out, "client time", this.clientTimers, MILLISECONDS);
        }
        long avgServerTime = printListStatistic(out, "server time", serverTimers, MILLISECONDS);

        long avgExecutionTime = printListStatistic(out, "server executor time", executionTimers, MILLISECONDS);
        printListStatistic(out, "server wait time", serverWaitTimers, MILLISECONDS);
        printListStatistic(out, "interrupt check time", interruptCheckTimers, MILLISECONDS);

        out.println();

        if (avgClientAttempt > 0) {
            out.println("avg client time nano     "
                            + NANOSECONDS.toNanos(avgClientTime / avgClientAttempt)
            );
        }
        if (avgSuccess != 0) {
//            out.println("avg execution time nano  "
//                            + NANOSECONDS.toNanos(avgExecutionTime / avgSuccess)
//            );

            out.println("avg server time nano     "
                            + NANOSECONDS.toNanos(avgServerTime / avgSuccess)
            );
        }

        long serverInSec = NANOSECONDS.toSeconds(avgServerTime);
        if (serverInSec != 0) {
            out.println("server op per sec        "
                            + (avgSuccess / serverInSec)
            );
        }
        long clientInSec = NANOSECONDS.toSeconds(avgClientTime);
        if (clientInSec != 0) {
            out.println("client op per sec        "
                            + (avgClientAttempt / clientInSec)
            );
        }

        out.println("bad client puts " + badPuts);
        out.println("bad server gets " + badGet);

    }

    final protected long printListStatistic(@NotNull PrintStream out, String prefix, @NotNull List<LongAdder> list, TimeUnit printUnit) {
        out.println();
        out.println(prefix + " per thread ");
        for (LongAdder a : list) {
            out.print(cnv(printUnit, a.sum()) + " ");
        }

        out.println();

        long sum = 0;
        for (LongAdder a : list) {
            sum += a.sum();
        }

        long max = 0;
        for (LongAdder a : list) {
            long l = a.sum();
            max = l > max ? l : max;
        }

        long avg = sum / list.size();
        out.println("average " + prefix + " " + cnv(printUnit, avg));
        out.println("max " + prefix + " " + cnv(printUnit, max));
        out.println("total " + prefix + " " + cnv(printUnit, sum));
        return avg;
    }

    private void thrw(InterruptedException e) {
        String message = Thread.currentThread().getName() + " is incorrectly interrupted";
        throw new RuntimeException(message, e);
    }

    protected boolean isInterrupted(long start) throws InterruptedException {
        boolean interrupted;
        interrupted = Thread.interrupted();
        writeCheckInterruptTime(start);
        if (interrupted) {
            throw new InterruptedException(Thread.currentThread().getName());
        }
        return interrupted;
    }

    private void writeCheckInterruptTime(long start) {
        if (writeServerStatistic || writeClientStatistic) {
            writeTime(start, System.nanoTime(), localInterruptCheckTimer, interruptCheckTimers);
        }
    }

    private void execute(@NotNull Runnable execution) {
        long startTime = System.nanoTime();
        execution.run();

        writeSuccessStatistic(startTime, System.nanoTime());
    }

    protected void writeSuccessStatistic(long startTime, long endTime) {
        if (writeServerStatistic) {
            writeTime(startTime, endTime, localExecutionTimer, executionTimers);
            writeAttempt(localExecutionSuccess, executionSuccesses);
        }
    }

    protected final void writeClientAttempt() {
        if (writeClientStatistic) {
            writeAttempt(localClientAttempts, clientAttempts);
        }
    }

    protected final void writeClientTime(long start) {
        if (writeClientStatistic) {
            long end = System.nanoTime() - start;
            LongAdder timer = getStatisticCounter(localClientTimer, clientTimers);
            timer.add(end);
        }
    }

    protected final void writeServerQueueWaitTime(long start) {
        if (writeServerStatistic) {
            writeTime(start, System.nanoTime(), serverWait, serverWaitTimers);
        }
    }

    protected final void writeServerTime(long start) {
        if (writeServerStatistic) {
            writeTime(start, System.nanoTime(), serverTimer, serverTimers);
        }
    }

    protected final void writeTime(long start, long end, @NotNull ThreadLocal<LongAdder> threadLocal, @NotNull List<LongAdder> list) {
        LongAdder atomic = getStatisticCounter(threadLocal, list);

        atomic.add(end - start);
    }

    private LongAdder getStatisticCounter(@NotNull ThreadLocal<LongAdder> threadLocal, @NotNull List<LongAdder> list) {
        LongAdder atomic = threadLocal.get();
        if (atomic == null) {
            atomic = createStatisticCounter(threadLocal, list);
        }
        return atomic;
    }

    private LongAdder createStatisticCounter(@NotNull ThreadLocal<LongAdder> threadLocal, @NotNull List<LongAdder> list) {
        LongAdder atomic;
        atomic = new LongAdder();
        threadLocal.set(atomic);

        synchronized (list) {
            list.add(atomic);
        }
        return atomic;
    }

    protected final void writeAttempt(@NotNull ThreadLocal<LongAdder> threadLocal, @NotNull List<LongAdder> list) {
        LongAdder attempts = getStatisticCounter(threadLocal, list);
        attempts.increment();
    }

    public String getName() {
        return name;
    }

    protected class CycleRunner<T extends Runnable> implements Runnable {
        private final CountDownLatch latch;

        public CycleRunner(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            Thread thread = Thread.currentThread();
            //PrintStream out = System.out;
            try {
                latch.await();
                //out.println(thread.getName() + " running");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            runInCycle();
            //out.println(thread.getName() + " finish " + thread.isInterrupted());
        }

        protected void runInCycle() {
            boolean interrupted = false;
            outer:
            while (!interrupted) try {
                long start = System.nanoTime();
                normalExecution();
                writeServerTime(start);

                interrupted = isInterrupted(start);
            } catch (InterruptedException e) {
                interrupted = true;
                break;
            }
            if (interrupted) {
                executionAfterInterruption();
            }
            Queue queue = getQueue();
            assert queue.isEmpty() : queue;
        }

        private void executionAfterInterruption() {
            Runnable execution;
            long start = System.nanoTime();
            while ((execution = getFromQueueAfterInterruption()) != null) {
                writeServerQueueWaitTime(start);
                start = System.nanoTime();
                execute(execution);
                writeServerTime(start);
            }
        }

        private void normalExecution() throws InterruptedException {
            long start = System.nanoTime();
            Runnable execution = getFromQueue();
            if (execution == null) {
                if (shutdown) {
                    throw new InterruptedException("shutdown time. queue size " + getQueue().size());
                }
                throw new NullPointerException("execution cannot be null");
            }
            writeServerQueueWaitTime(start);
            execute(execution);
        }
    }

}
