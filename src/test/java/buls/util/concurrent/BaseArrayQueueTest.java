package buls.util.concurrent;

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

    @Test
    public void testInsertAnGetsInConcurrentMode0() {
        int inserts = 1;
        int attemptsPerInsert = 100;
        int capacity = 1;
        int getters = 1;
        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode0", THRESHOLD);
    }

    @Test
    public void testInsertAnGetsInConcurrentMode1() {
        int capacity = 1;
        int inserts = 10;
        int attemptsPerInsert = 2;
        int getters = 2;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode1", THRESHOLD);
    }

    @Test
    public void testInsertAnGetsInConcurrentMode2() {
        int inserts = 10;
        int attemptsPerInsert = 2;
        int capacity = inserts * attemptsPerInsert;
        int getters = 5;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode2", THRESHOLD);
    }

    @Test
    public void testInsertAnGetsInConcurrentMode3() {
        int inserts = 50;
        int attemptsPerInsert = 1000;
        int capacity = inserts * attemptsPerInsert;
        int getters = 100;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode3", THRESHOLD);
    }

    @Test
    public void testInsertAnGetsInConcurrentMode4() {
        int inserts = 50;
        int attemptsPerInsert = 100;
        int capacity = 100;
        int getters = 5;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode4", THRESHOLD);
    }

    @Test
    public void testInsertAnGetsInConcurrentMode5() {
        int inserts = 1;
        int attemptsPerInsert = 1_000_000;
        int capacity = 100;
        int getters = 1;

        testQueueConcurrently(capacity, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode4", THRESHOLD * 2);
    }

    protected void testQueueConcurrently(int capacity, int inserts, int attemptsPerInsert, int getters, String testName, int threshold) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream printStream = System.out;//new PrintStream(out);
        printStream.println("START " + testName);
        int attemptsPerGet = inserts * attemptsPerInsert / getters;
        if (attemptsPerGet == 0) {
            attemptsPerGet = 1;
        }
        Assert.assertTrue(attemptsPerGet * getters >= inserts * attemptsPerInsert);

        final QueueWithStatistic<String> queue = createQueue(capacity, WRITE_STATISTIC);

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
            Runnable runner = createInserter(queue, attemptsPerInsert, startTrigger, endTrigger, offerFailCounter);
            Thread thread = new Thread(runner, name);
            threads.add(thread);
            thread.start();
        }

        List<String> results = Collections.synchronizedList(new ArrayList<String>(inserts));
        for (int i = 0; i < getters; ++i) {
            Runnable runner = createGetter(queue, attemptsPerGet, results, startTrigger, endTrigger, pollFailCounter, threshold);
            Thread thread = new Thread(runner, "get-thread-" + i);
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

        queue.printStatistic(printStream);
        try {
            Assert.assertEquals(sourceValues.size(), results.size());
            Collections.sort(sourceValues);
            Collections.sort(results);
            //System.printStream.println(sourceValues);
            //System.printStream.println(results);
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
                                  final AtomicLong pollFailCounter, final int threshold) {
        return new Runnable() {

            public void run() {
                try {
                    startTrigger.await();
                } catch (InterruptedException ignored) {
                }

                try {
                    int attempts = attemptsPerGet;
                    long fails = 0;
                    while (attempts > 0) {
                        String poll = queue.poll();
                        if (poll == null) {
                            pollFailCounter.incrementAndGet();
                            Thread.yield();
                            fails++;
                            if (fails > threshold) {
                                throw new IllegalStateException(fails + " fails");
                            }
                        } else {
                            results.add(poll);
                            --attempts;
                        }
                    }
                } finally {
                    endTrigger.countDown();
                }
            }
        };
    }

    protected Runnable createInserter(final Queue<String> queue, final int attemptsPerInsert, final CountDownLatch startTrigger,
                                      final CountDownLatch endTrigger, final AtomicLong offerFailCounter) {
        return new Runnable() {
            public void run() {
                try {
                    startTrigger.await();
                } catch (InterruptedException ignored) {
                }
                try {
                    int attempts = attemptsPerInsert;
                    long fails = 0;
                    while (attempts > 0) {
                        String name = Thread.currentThread().getName() + "-" + (attempts - 1);
                        boolean offer = queue.offer(name);
                        if (!offer) {
                            offerFailCounter.incrementAndGet();
                            Thread.yield();
                            fails++;
                            if (fails > 100_000_000) {
                                throw new IllegalStateException(fails + " fails");
                            }
                        } else {
                            --attempts;
                        }
                    }
                } finally {
                    endTrigger.countDown();
                }
            }
        };
    }


    @Test
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

    protected abstract QueueWithStatistic<String> createQueue(int capacity, boolean writeStatistic);

    @Test
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

    @Test
    public void testThree() {
        Queue<String> queue = createQueue(0, false);
        Assert.assertFalse(queue.offer("Раз"));
        Assert.assertTrue(queue.poll() == null);
    }

    @Test
    public void testFour() {
        Queue<String> queue = createQueue(2, false);
        Assert.assertTrue(queue.poll() == null);
        Assert.assertTrue(queue.poll() == null);
        Assert.assertTrue(queue.poll() == null);
    }

    @Test
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
}
