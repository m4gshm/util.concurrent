package buls.util.concurrent;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
@Test
public class ConcurrentArrayQueueTest extends BaseArrayQueueTest {

    @Override
    protected ConcurrentArrayQueue<String> createQueue(int capacity, boolean writeStatistic) {
        return new ConcurrentArrayQueue<>(capacity, writeStatistic);
    }

    @Test
    public void testTailOverflow() {
        ConcurrentArrayQueue<String> queue = createQueue(2, false);
        queue.offer("A");
        queue.offer("B");
        queue.poll();
        queue.poll();
        long tail = Long.MAX_VALUE - 1;
        Assert.assertEquals(tail % queue.capacity(), 0);
        long head = tail;

        queue.tailSequence.set(tail);
        queue.headSequence.set(head);
        long nextLevel = queue.computeLevel(head);
        Assert.assertEquals(nextLevel % queue.capacity(), 0);
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
        for (int i = 0; i < capacity; i++) {
            queue.offer("A");
            queue.poll();
        }
        long tail = Long.MAX_VALUE;
        while (tail % queue.capacity() != 0) {
            tail--;
        }
        long head = tail;

        queue.tailSequence.set(tail);
        queue.headSequence.set(head);
        long nextLevel = queue.computeLevel(head);
        //Assert.assertEquals(nextLevel % queue.capacity(), 0);
        for (int i = 0; i < capacity; i++) {
            queue.levels.set(i, nextLevel);
        }

        System.out.println(queue);
        char c = 'A';
        for (int i = 0; i < iterations; i++) {
            Assert.assertTrue(queue.offer(new String(a(c++))));
            Assert.assertTrue(queue.poll() != null);
        }


        System.out.println(queue);
    }

    private char[] a(char... c) {
        return c;
    }

}
