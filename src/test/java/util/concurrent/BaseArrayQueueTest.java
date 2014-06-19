package util.concurrent;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Alex on 17.06.2014.
 */
public abstract class BaseArrayQueueTest {

    @Test
    public void testInsertAnGetsInConcurrentMode0() {
        int inserts = 1;
        int attemptsPerInsert = 100;
        int capacity = 1;
        int getters = 1;
        testQueueCuncurrently(capacity, inserts, attemptsPerInsert, getters);
    }

    @Test
    public void testInsertAnGetsInConcurrentMode1() {
        int capacity = 1;
        int inserts = 10;
        int attemptsPerInsert = 2;
        int getters = 2;

        testQueueCuncurrently(capacity, inserts, attemptsPerInsert, getters);
    }

    @Test
    public void testInsertAnGetsInConcurrentMode2() {
        int inserts = 10;
        int attemptsPerInsert = 2;
        int capacity = inserts * attemptsPerInsert;
        int getters = 5;

        testQueueCuncurrently(capacity, inserts, attemptsPerInsert, getters);
    }

    @Test
    public void testInsertAnGetsInConcurrentMode3() {
        int inserts = 30;
        int attemptsPerInsert = 1000;
        int capacity = inserts * attemptsPerInsert;
        int getters = 40;

        testQueueCuncurrently(capacity, inserts, attemptsPerInsert, getters);
    }


    protected void testQueueCuncurrently(int capacity, int inserts, int attemptsPerInsert, int getters) {
        int attemptsPerGet = inserts * attemptsPerInsert / getters;
        if (attemptsPerGet == 0) {
            attemptsPerGet = 1;
        }
        Assert.assertTrue(attemptsPerGet * getters >= inserts * attemptsPerInsert);

        final ConcurrentArrayQueue<String> queue = createQueue(capacity, true);

        final CountDownLatch endTrigger = new CountDownLatch(inserts + getters);
        final CountDownLatch startTrigger = new CountDownLatch(1);

        final AtomicLong offerFailCounter = new AtomicLong();
        final AtomicLong pollFailCounter = new AtomicLong();

        List<String> sourceValues = new ArrayList<>(inserts);


        for (int i = 0; i < inserts; ++i) {
            String name = "insert-thread-" + i;
            for (int attempt = 0; attempt < attemptsPerInsert; ++attempt) {
                sourceValues.add(name + "-" + attempt);
            }
            Runnable runner = createInserter(queue, attemptsPerInsert, startTrigger, endTrigger, offerFailCounter);
            new Thread(runner, name).start();
        }

        List<String> results = Collections.synchronizedList(new ArrayList<String>(inserts));
        for (int i = 0; i < getters; ++i) {
            Runnable runner = createGetter(queue, attemptsPerGet, results, startTrigger, endTrigger, pollFailCounter);
            new Thread(runner, "get-thread-" + i).start();
        }

        startTrigger.countDown();

        endingWait(endTrigger, 100);

        try {
            Assert.assertEquals(sourceValues.size(), results.size());
            Collections.sort(sourceValues);
            Collections.sort(results);
            Assert.assertEquals(sourceValues, results);
        } finally {

            System.out.println("queue size " + queue.size());
//            System.out.println("queue: " + queue);
            System.out.println("offer fails " + offerFailCounter.get());
            System.out.println("poll fails " + pollFailCounter.get());

            System.out.println(queue.getClass().getName() + " statistic:");

            queue.printStatistic(System.out);
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
                                  final AtomicLong pollFailCounter) {
        return new Runnable() {

            public void run() {
                try {
                    startTrigger.await();
                } catch (InterruptedException ignored) {
                }

                try {
                    int attempts = attemptsPerGet;
                    while (attempts > 0) {
                        String poll = queue.poll();
                        if (poll == null) {
                            pollFailCounter.incrementAndGet();
                            Thread.yield();
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
                    while (attempts > 0) {
                        String name = Thread.currentThread().getName() + "-" + (attempts - 1);
                        boolean offer = queue.offer(name);
                        if (!offer) {
                            offerFailCounter.incrementAndGet();
                            Thread.yield();
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
        Queue<String> queue = createQueue(capacity, false);

        //заполнили
        Assert.assertTrue(queue.offer("Раз"));
        Assert.assertTrue(queue.offer("Два"));
        Assert.assertFalse(queue.offer("Три"));

        //все забрали
        Assert.assertEquals("Раз", queue.poll());
        Assert.assertEquals("Два", queue.poll());
        Assert.assertTrue(queue.poll() == null);
    }

    protected abstract ConcurrentArrayQueue<String> createQueue(int capacity, boolean writeStatistic);

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
