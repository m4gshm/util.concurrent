package buls.util.concurrent;

import buls.util.concurrent.research.QueueWithStatistic;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Alex on 17.06.2014.
 */
public abstract class BaseArrayQueueTest {

    public static final boolean WRITE_STATISTIC = true;
    public static final int THRESHOLD = 1_000_000;


    public static final int SINGLE_THREAD = 100;
    public static final int MULTI_THREADS = 95;
    public static final int OVERFLOW_SINGLE = 90;
    public static final int OVERFLOW_MULTI = 85;

    @Test(priority = SINGLE_THREAD)
    public void testOne() {
        int capacity = 2;
        Queue<String> queue = createQueue(capacity, WRITE_STATISTIC);

        //заполнили
        Assert.assertTrue(queue.offer("Раз"));
        Assert.assertTrue(queue.offer("Два"));
        Assert.assertFalse(queue.offer("Три"));

        //все забрали
        Assert.assertEquals("Раз", queue.poll());
        Assert.assertEquals("Два", queue.poll());
        Assert.assertTrue(queue.poll() == null);
    }

    @Test(priority = SINGLE_THREAD)
    public void testTwo() {
        Queue<String> queue = createQueue(2, false);
        queue.offer("Раз");
        queue.offer("Два");
        //взяли
        Assert.assertEquals("Раз", queue.poll());

        boolean offer = queue.offer("Три");
        Assert.assertTrue(offer);

        Assert.assertFalse(queue.offer("Четыре"));

        Assert.assertEquals("Два", queue.poll());
        Assert.assertEquals("Три", queue.poll());
        Assert.assertTrue(queue.poll() == null);
    }

    @Test(priority = SINGLE_THREAD)
    public void testThree() {
        Queue<String> queue = createQueue(0, false);
        Assert.assertFalse(queue.offer("Раз"));
        Assert.assertTrue(queue.poll() == null);
    }

    @Test(priority = SINGLE_THREAD)
    public void testFour() {
        Queue<String> queue = createQueue(2, false);
        Assert.assertTrue(queue.poll() == null);
        Assert.assertTrue(queue.poll() == null);
        Assert.assertTrue(queue.poll() == null);
    }

    @Test(priority = SINGLE_THREAD)
    public void testFive() {
        Queue<String> queue = createQueue(2, false);
        queue.offer("Раз");
        queue.offer("Два");
        //взяли
        Assert.assertEquals("Раз", queue.poll());

        boolean offer = queue.offer("Три");
        Assert.assertTrue(offer);

        Assert.assertEquals("Два", queue.poll());

        Assert.assertTrue(queue.offer("Четыре"));
    }

    @Test(priority = MULTI_THREADS)
    public void testInsertAnGetsInConcurrentMode0() {
        int inserts = 1;
        int attemptsPerInsert = 100;
        int capacity = 1;
        int getters = 1;
        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode0", THRESHOLD);
    }

    @Test(priority = MULTI_THREADS)
    public void testInsertAnGetsInConcurrentMode1() {
        int capacity = 1;
        int inserts = 10;
        int attemptsPerInsert = 2;
        int getters = 2;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode1", THRESHOLD);
    }

    @Test(priority = MULTI_THREADS)
    public void testInsertAnGetsInConcurrentMode2() {
        int inserts = 10;
        int attemptsPerInsert = 2;
        int capacity = inserts * attemptsPerInsert;
        int getters = 5;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode2", THRESHOLD);
    }

    @Test(priority = MULTI_THREADS)
    public void testInsertAnGetsInConcurrentMode3() {
        int inserts = 50;
        int attemptsPerInsert = 1000;
        int capacity = inserts * attemptsPerInsert;
        int getters = 100;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode3", THRESHOLD);
    }

    @Test(priority = MULTI_THREADS)
    public void testInsertAnGetsInConcurrentMode4() {
        int inserts = 50;
        int attemptsPerInsert = 100;
        int capacity = 100;
        int getters = 5;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode4", THRESHOLD);
    }

    @Test(priority = MULTI_THREADS)
    public void testInsertAnGetsInConcurrentMode5() {
        int inserts = 1;
        int attemptsPerInsert = 1_000_000;
        int capacity = 100;
        int getters = 1;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode5", THRESHOLD * 10);
    }

    @Test(priority = MULTI_THREADS)
    public void testInsertAnGetsInConcurrentMode6() {
        int inserts = 1;
        int attemptsPerInsert = 1_000_000;
        int capacity = 5;
        int getters = 1;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode6", THRESHOLD * 2);
    }

    @Test(priority = MULTI_THREADS)
    public void testInsertAnGetsInConcurrentMode7() {
        int inserts = 3;
        int attemptsPerInsert = 100_000;
        int capacity = 10;
        int getters = 2;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode7", THRESHOLD * 2);
    }

    @Test(priority = MULTI_THREADS)
    public void testInsertAnGetsInConcurrentMode8() {
        int inserts = 3;
        int attemptsPerInsert = 100_000;
        int capacity = inserts * attemptsPerInsert;
        int getters = 3;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode8", THRESHOLD * 2);
    }

    private void testQueueConcurrently(int capacity, int inserts, int attemptsPerInsert, int getters,
                                       String testName, int threshold) {
        final Queue<String> queue = createQueue(capacity, WRITE_STATISTIC);

        testQueueConcurrently(queue, inserts, attemptsPerInsert, getters, testName, threshold, threshold);
    }

    protected void testQueueConcurrently(Queue<String> queue, int inserts, int attemptsPerInsert, int getters,
                                         String testName, int getterThreshold, int inserterThreshold) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream printStream = System.out;//new PrintStream(out);
        printStream.println("START " + testName);
        int attemptsPerGet = inserts * attemptsPerInsert / getters;
        if (attemptsPerGet == 0) {
            attemptsPerGet = 1;
        }
        Assert.assertTrue(attemptsPerGet * getters >= inserts * attemptsPerInsert);

        final CountDownLatch endTrigger = new CountDownLatch(inserts + getters);
        final CountDownLatch startTrigger = new CountDownLatch(1);

        final AtomicLong offerFailCounter = new AtomicLong();
        final AtomicLong pollFailCounter = new AtomicLong();

        List<String> sourceValues = new ArrayList<>(inserts);

        List<Thread> threads = new ArrayList<>(inserts);
        for (int i = 0; i < inserts; ++i) {
            String name = "insert-thread-" + i;
            for (int attempt = 0; attempt < attemptsPerInsert; ++attempt) {
                sourceValues.add(name + "-" + attempt);
            }
            Runnable runner = createInserter(queue, attemptsPerInsert, startTrigger, endTrigger,
                    offerFailCounter, threads, inserterThreshold);
            Thread thread = new Thread(runner, name);
            threads.add(thread);
            thread.start();
        }

        List<String> results = Collections.synchronizedList(new ArrayList<String>(inserts));
        for (int i = 0; i < getters; ++i) {
            Runnable runner = createGetter(queue, attemptsPerGet, results, startTrigger, endTrigger,
                    pollFailCounter, threads, getterThreshold);
            Thread thread = new Thread(runner, "get-thread-" + i);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }

        startTrigger.countDown();

        //endingWait(endTrigger, 10);

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        printStream.println("queue size " + queue.size());
        printStream.println("queue: " + queue);
        printStream.println("offer fails " + offerFailCounter.get());
        printStream.println("poll fails " + pollFailCounter.get());

        printStream.println(queue.getClass().getName() + " statistic:");

        printStatistic(queue, printStream);
        try {
            Assert.assertEquals(sourceValues.size(), results.size(), queue.toString());
            Collections.sort(sourceValues);
            Collections.sort(results);
            Assert.assertEquals(sourceValues, results);
        } finally {
            printStream.println("END " + testName);
            try {
                System.out.write(out.toByteArray());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void printStatistic(Queue<String> queue, PrintStream printStream) {
        if (queue instanceof QueueWithStatistic<?>) {
            ((QueueWithStatistic) queue).printStatistic(printStream);
        }
    }

    protected void endingWait(CountDownLatch endTrigger, int timeout) {
        try {
            endTrigger.await(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Runnable createGetter(final Queue<String> queue, final int attemptsPerGet,
                                  final Collection<String> results,
                                  final CountDownLatch startTrigger, final CountDownLatch endTrigger,
                                  final AtomicLong pollFailCounter, List<Thread> threads, final int threshold) {
        return new GetterRunnable(startTrigger, attemptsPerGet, queue, pollFailCounter, endTrigger, results, threads, threshold);
    }

    protected Runnable createInserter(final Queue<String> queue, final int attemptsPerInsert, final CountDownLatch startTrigger,
                                      final CountDownLatch endTrigger, final AtomicLong offerFailCounter, List<Thread> threads, int threshold) {
        return new InserterRunnable(startTrigger, attemptsPerInsert, queue, offerFailCounter, endTrigger, threads, threshold);
    }

    protected abstract Queue<String> createQueue(int capacity, boolean writeStatistic);

    private void checkFail(long fails, int threshold, Queue queue, List<Thread> threads) {
        if (fails > threshold) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            printStatistic(queue, new PrintStream(stream));
            for (Thread thread : threads) {
                thread.interrupt();
            }
            throw new IllegalStateException(fails + " fails, " +threshold +" threshold, \n"
                    +"\n"+ new String(stream.toByteArray()));
        }
    }

    private class GetterRunnable implements Runnable {

        private final CountDownLatch startTrigger;
        private final int attemptsPerGet;
        private final Queue<String> queue;
        private final AtomicLong pollFailCounter;
        private final List<Thread> threads;
        private final int threshold;
        private final Collection<String> results;
        private final CountDownLatch endTrigger;

        public GetterRunnable(CountDownLatch startTrigger, int attemptsPerGet, Queue<String> queue, AtomicLong pollFailCounter,
                              CountDownLatch endTrigger, Collection<String> results, List<Thread> threads, int threshold) {
            this.startTrigger = startTrigger;
            this.attemptsPerGet = attemptsPerGet;
            this.queue = queue;
            this.pollFailCounter = pollFailCounter;
            this.threads = threads;
            this.threshold = threshold;
            this.results = results;
            this.endTrigger = endTrigger;
        }

        public void run() {
            try {
                startTrigger.await();
            } catch (InterruptedException ignored) {
            }

            try {
                int attempts = attemptsPerGet;
                long fails = 0;
                while (!Thread.interrupted() && attempts > 0) {
                    String poll = queue.poll();
                    if (poll == null) {
                        pollFailCounter.incrementAndGet();
                        Thread.yield();
                        fails++;
                        checkFail(fails, threshold, queue, threads);
                    } else {
                        results.add(poll);
                        --attempts;
                    }
                }
            } finally {
                endTrigger.countDown();
            }
        }


    }

    private class InserterRunnable implements Runnable {
        private final CountDownLatch startTrigger;
        private final int attemptsPerInsert;
        private final Queue<String> queue;
        private final AtomicLong offerFailCounter;
        private final CountDownLatch endTrigger;
        private final List<Thread> threads;
        private final int threshold;

        public InserterRunnable(CountDownLatch startTrigger, int attemptsPerInsert, Queue<String> queue,
                                AtomicLong offerFailCounter, CountDownLatch endTrigger, List<Thread> threads, int threshold) {
            this.startTrigger = startTrigger;
            this.attemptsPerInsert = attemptsPerInsert;
            this.queue = queue;
            this.offerFailCounter = offerFailCounter;
            this.endTrigger = endTrigger;
            this.threads = threads;
            this.threshold = threshold;
        }

        public void run() {
            try {
                startTrigger.await();
            } catch (InterruptedException ignored) {
            }
            try {
                int attempts = attemptsPerInsert;
                long fails = 0;
                while (!Thread.interrupted() && attempts > 0) {
                    String name = Thread.currentThread().getName() + "-" + (attempts - 1);
                    boolean offer = queue.offer(name);
                    if (!offer) {
                        offerFailCounter.incrementAndGet();
                        Thread.yield();
                        fails++;
                        checkFail(fails, threshold, queue, threads);
                    } else {
                        --attempts;
                    }
                }
            } finally {
                endTrigger.countDown();
            }
        }
    }

}
