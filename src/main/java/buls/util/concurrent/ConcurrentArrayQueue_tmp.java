package buls.util.concurrent;

/**
 * Created by Bulgakov Alex on 31.05.2014.
 */
public class ConcurrentArrayQueue_tmp<E> extends ConcurrentArrayQueue<E> {

    public ConcurrentArrayQueue_tmp(int capacity, boolean writeStatistic) {
        super(capacity, writeStatistic);
    }

    @Override
    protected final long computeTail(long tail) {
        long currentTail = getTail();
        if (tail < currentTail) {
            tail = currentTail;
        } else {
            tail++;
        }
        return tail;
    }

    @Override
    protected final long computeHead(long head) {
        long currentHead = getHead();
        if (head < currentHead) {
            head = currentHead;
        } else {
            head++;
        }
        return head;
    }

}
