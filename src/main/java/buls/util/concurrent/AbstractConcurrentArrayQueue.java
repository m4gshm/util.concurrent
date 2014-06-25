package buls.util.concurrent;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractConcurrentArrayQueue<E> extends AbstractArrayQueue<E> {
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

    @Override
    public final int capacity() {
        return elements.length();
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

}
