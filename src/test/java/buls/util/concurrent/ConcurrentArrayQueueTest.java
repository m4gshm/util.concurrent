package buls.util.concurrent;

import org.junit.Assert;
import org.junit.Test;


/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueueTest extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueue<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueue<>(capacity, true);
    }

    @Test
    public void testTailOverflow() {
        ConcurrentArrayQueue<String> queue = createQueue(2, false);
        queue.offer("A");
        queue.offer("B");
        queue.poll();
        queue.poll();
        long tail = queue.max_tail() - queue.capacity() + 1;
        Assert.assertEquals(tail % queue.capacity(), 0);

        queue.tailSequence.set(tail);
        queue.headSequence.set(tail);
        long nextLevel = queue.afterGetLevel(tail - 1);
        queue.levels.set(0, nextLevel);
        queue.levels.set(1, nextLevel);

        Assert.assertTrue(queue.offer("C"));
        Assert.assertTrue(queue.offer("D"));

        Assert.assertEquals(queue.poll(), "C");
        Assert.assertEquals(queue.poll(), "D");


        Assert.assertTrue(queue.offer("E"));
        Assert.assertTrue(queue.offer("F"));

        Assert.assertFalse(queue.offer("G"));

        Assert.assertEquals(queue.poll(), "E");
        Assert.assertTrue(queue.offer("H"));
        Assert.assertEquals(queue.poll(), "F");

        Assert.assertEquals(queue.poll(), "H");

        System.out.println(queue);
        char c = 'A';
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(queue.offer(new String(a(c++))));
            Assert.assertTrue(queue.offer(new String(a(c++))));
            Assert.assertTrue(queue.poll() != null);
            Assert.assertTrue(queue.poll() != null);
        }


        System.out.println(queue);
    }

    @Test
    public void testTailOverflow1() {
        int capacity = 1;
        int iterations = 1000;
        testOverflow(capacity, iterations);
    }

    @Test
    public void testTailOverflow2() {
        int capacity = 2;
        int iterations = 1000;
        testOverflow(capacity, iterations);
    }

    @Test
    public void testTailOverflow3() {
        int capacity = 3;
        int iterations = 1000;
        testOverflow(capacity, iterations);
    }


    protected void testOverflow(int capacity, int iterations) {
        ConcurrentArrayQueue<String> queue = createQueue(capacity, false);
        int maxValue = queue.max_tail();
        long tail = maxValue - (maxValue % capacity);
        testOverflow(capacity, iterations, tail, queue);
    }

    private void testOverflow(int capacity, int iterations, long tail, ConcurrentArrayQueue<String> queue) {

        initQueueOverflow(queue, capacity, tail);
        char c = 'A';
        for (int i = 0; i < iterations; i++) {
            Assert.assertTrue(queue.offer(new String(a(c++))));
            Assert.assertTrue(queue.poll() != null);
        }

        System.out.println(queue);
    }

    private void initQueueOverflow(ConcurrentArrayQueue<String> queue, int capacity, long tail) {
        Assert.assertEquals(tail % queue.capacity(), 0);

        for (int i = 0; i < capacity; i++) {
            queue.offer("A");
            queue.poll();
        }

        queue.tailSequence.set(tail);
        queue.headSequence.set(tail);
        long nextLevel = queue.afterGetLevel(tail - 1);
        //Assert.assertEquals(nextLevel % queue.capacity(), 0);
        for (int i = 0; i < capacity; i++) {
            queue.levels.set(i, nextLevel);
        }

        System.out.println(queue);
    }

    private char[] a(char... c) {
        return c;
    }

    @Test
    public void testOverflowInConcurrentMode6() {
        int inserts = 2;
        int attemptsPerInsert = 1_000_000;
        int capacity = 5;
        int getters = 2;

        final ConcurrentArrayQueue<String> queue = createQueue(capacity, WRITE_STATISTIC);
        int maxValue = queue.max_tail();
        long tail = maxValue - (maxValue % capacity) - capacity;

        initQueueOverflow(queue, capacity, tail);

        testQueueConcurrently(queue, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode7",
                (int) (inserts * attemptsPerInsert * 1.5), (int) (getters * attemptsPerInsert * 1.5));
    }

    @Test
    public void testOverflowInConcurrentMode7() {
        int inserts = 2;
        int attemptsPerInsert = 1_000_000;
        int capacity = 1;
        int getters = 2;

        final ConcurrentArrayQueue<String> queue = createQueue(capacity, WRITE_STATISTIC);
        int maxValue = queue.max_tail();
        long tail = maxValue - (maxValue % capacity) - capacity;

        initQueueOverflow(queue, capacity, tail);

        testQueueConcurrently(queue, inserts, attemptsPerInsert, getters, "testInsertAnGetsInConcurrentMode7",
                (int) (inserts * attemptsPerInsert * 3), (int) (getters * attemptsPerInsert * 3));
    }
}
