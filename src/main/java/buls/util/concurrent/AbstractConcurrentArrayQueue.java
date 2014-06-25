package buls.util.concurrent;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractConcurrentArrayQueue<E> extends AbstractQueue<E> {
    public static final int EMPTY = 0;
    public static final int POOLING = -2;
    public static final int PUTTING = -1;
    public static final int NOT_EMPTY = 1;

    public final static int SUCCESS = 0;
    public final static int GO_NEXT = 1;
    public final static int RECALCULATE = 2;

    protected final AtomicReferenceArray<Object> elements;
    protected final AtomicLongArray levels;
    //protected final AtomicReferenceArray<Thread> threads;
    protected final AtomicLong tailSequence = new AtomicLong(0);
    protected final AtomicLong headSequence = new AtomicLong(0);

    public AbstractConcurrentArrayQueue(int capacity) {
        this.elements = new AtomicReferenceArray<>(capacity);
        levels = new AtomicLongArray(capacity);
        //threads = new AtomicReferenceArray<>(capacity);
    }

    @Override
    public String toString() {
        return "h: " + headSequence + ", t:" + tailSequence + ", c:" + capacity()
                + "\n" + elements.toString()
                + "\n" + levels.toString();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    protected final boolean isInterrupted() {
        return Thread.interrupted();
    }

    @Override
    public final int size() {
        long tail = tailSequence.get();
        long head = headSequence.get();
        return (int) (tail - head);
    }

    public final int capacity() {
        return elements.length();
    }

    @Override
    public final boolean offer(E e) {
        if (e == null) {
            throw new IllegalArgumentException("element cannot be null");
        }

        int capacity = capacity();
        if (capacity == 0) {
            return false;
        }

        long head = this.headSequence.get();
        long tail = this.tailSequence.get();

        boolean result;
        long amount = tail - head;
        if (amount < capacity) {
            result = setElement(e, tail, head);
        } else {

            result = false;
        }

        return result;
    }

    protected abstract boolean setElement(E e, long tail, long head);

    @Deprecated
    protected final boolean set(E e, int index) {
        return elements.compareAndSet(index, null, e);
    }

    protected final int set(final E e, final long tail, final long currentTail, final long head, final long attempt) {
        int index = calcIndex(currentTail);
        while (true) {
            //long l0 = levels.get(index);
            if (levels.compareAndSet(index, EMPTY, PUTTING)) {
                boolean success = false;
                try {
                    boolean set = elements.compareAndSet(index, null, e);
                    int result;
                    if (set) {
                        //threads.lazySet(index, Thread.currentThread());
                        //проверка вставки за хвост
                        long h = getHead();
                        boolean behindHead = checkBehindHead(currentTail, h);
                        if (behindHead) {
                            //int hI = calcIndex(h);
                            //int tI = calcIndex(getTail());
                            elements.compareAndSet(index, e, null);
                            result = RECALCULATE;
                        } else {
                            checkHeadTailConsistency(h, currentTail);
                            setNextTail(tail, currentTail);
                            success = true;
                            result = SUCCESS;
                        }

                    } else {
                        throw new IllegalStateException("bad set " + index);
                        //return false;
                    }
                    return result;
                } finally {
                    int result = success ? NOT_EMPTY : EMPTY;
                    levels.compareAndSet(index, PUTTING, result);
                }

            } else {
                final long l = levels.get(index);
                if (l == PUTTING || l == NOT_EMPTY) {
                    return GO_NEXT;
                } else if (l == POOLING) {
                    continue;
                } else {
                    assert l == EMPTY;
                    //поток опоздал во всем
                    return RECALCULATE;
                }
            }
        }
        //return false;
    }

    protected boolean checkBehindHead(long currentTail, long head) {
        if (head > currentTail) {
            //headSequence.set(currentTail);
            return true;
        } else {
            return false;
        }
    }

    protected final long getTail() {
        return tailSequence.get();
    }

    protected final void yield() {
        Thread.yield();
    }

    /**
     * @param oldTail      счетчик хвоста с которым поток вошел в режим вставки
     * @param insertedTail
     * @return
     */
    protected final boolean setNextTail(long oldTail, long insertedTail) {
        long newValue = insertedTail + 1;
        assert oldTail < newValue;
        AtomicLong sequence = tailSequence;
        boolean set = sequence.compareAndSet(oldTail, newValue);
        while (!set) {
            long currentValue = sequence.get();
            if (currentValue < newValue) {
                set = sequence.compareAndSet(currentValue, newValue);
            } else if (currentValue == newValue) {
                return false;
            } else {
                assert currentValue > newValue : oldTail + " " + currentValue + " " + newValue;
                return true;
            }
        }
        return set;
    }

    protected final int calcIndex(long counter) {
        return (int) (counter % capacity());
    }

    protected final void checkHeadTailConsistency(long head, long tail) {
        if (head > tail) {
            throw new IllegalStateException("head <= tail, " + " head: " + head + ", tail: " + tail);
        }
    }

    protected abstract E getElement(long head, long tail);

    @Deprecated
    @SuppressWarnings("unchecked")
    protected final E get(int index) {
        E e = (E) elements.get(index);
        if (e != null) {
            boolean set = elements.compareAndSet(index, e, null);
            if (set) {
                return e;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected final E get(long oldHead, long currentHead, long tail, long attempt) {
        int index = calcIndex(currentHead);
        while (true) {
            //long l0 = levels.get(index);
            if (levels.compareAndSet(index, NOT_EMPTY, POOLING)) {
                boolean success = false;
                try {
                    E e = (E) elements.get(index);
                    if (e == null) {
                        throw new IllegalStateException("NULL");
                        //return null;
                    }
                    boolean set = elements.compareAndSet(index, e, null);
                    if (set) {
                        //threads.lazySet(index, Thread.currentThread());
                        setNextHead(oldHead, currentHead);
                        success = true;
                        return e;
                    } else {
                        throw new RuntimeException();
                    }
                } finally {
                    int result = success ? EMPTY : NOT_EMPTY;
                    levels.compareAndSet(index, POOLING, result);
                }
            } else {
                final long l = levels.get(index);
                if (l == PUTTING) {
                    //завершается вставка в ячейку
                    continue;
                    //} else if (l == POOLING || l == EMPTY) {
                    //    //другой поток производит взятие
                    //    return null;
                } else {
                    return null;
                }
            }
        }
        //return null;
    }

    @Override
    public final E poll() {
        int capacity = capacity();
        if (capacity == 0) {
            return null;
        }

        long tail = tailSequence.get();
        long head = headSequence.get();

        E result;

        if (head < tail) {
            result = getElement(head, tail);
        } else {
            result = null;
        }
        return result;
    }

    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }

    protected long getHead() {
        return headSequence.get();
    }

    protected boolean setNextHead(long oldHead, long insertedHead) {
        if (insertedHead < oldHead) {
            throw new RuntimeException("insertedHead < oldHead " + insertedHead + " < " + oldHead);
        }
        long newValue = insertedHead + 1;
        assert oldHead < newValue;
        AtomicLong sequence = headSequence;
        boolean set = sequence.compareAndSet(oldHead, newValue);
        while (!set) {
            long currentValue = sequence.get();
            if (currentValue < newValue) {
                set = sequence.compareAndSet(currentValue, newValue);
            } else if (currentValue == newValue) {
                break;
            } else {
                assert currentValue > newValue : oldHead + " " + currentValue + " " + newValue;
                break;
            }
        }
        return set;
    }
}
