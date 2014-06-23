package buls.util.concurrent;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by Alex on 20.06.2014.
 */
public class ConcurrentArrayQueue2<E> extends ConcurrentArrayQueue<E> {

    public static final int LOCK_ATTEMPTIONS = 100000;
    private final AtomicReferenceArray<Type> types;
    private final AtomicReferenceArray<Thread> threads;

    public ConcurrentArrayQueue2(int capacity, boolean writeStatistic) {
        super(capacity, writeStatistic);
        this.types = new AtomicReferenceArray<>(capacity);
        this.threads = new AtomicReferenceArray<>(capacity);
    }

    @Override
    protected boolean setElement(final E e, final long tail, long head) {

        long currentTail = tail;
        int capacity = capacity();
        int index = calcIndex(currentTail);

        while (true) {

            boolean locked = lockSet(currentTail, capacity, index);
            if (locked) {

                boolean set = set(e, index);
                assert set : "cannot set element";
                setNextTail(tail, currentTail);

                checkHeadTailConsistency();

                successSet();


                unlockSet(index);

                return true;
            } else {
                failSet();

                currentTail = computeTail(currentTail);
                if (checkTailOverflow(currentTail, capacity)) {
                    return false;
                }
                index = calcIndex(currentTail);
            }
        }
        //throw new IllegalStateException("setElement");
    }

    private void unlockSet(int index) {
        boolean unlocked = types.compareAndSet(index, Type.writing, Type.written);
        assert unlocked;
    }

    private boolean lockSet(long newTail, int capacity, int index) {
        Type prevType = newTail >= capacity ? Type.read : null;
        boolean set;
        int iterations=0;
        while (!(set = types.compareAndSet(index, prevType, Type.writing))) {
            Type type = types.get(index);
            if (type != Type.reading) {
                break;
            } else {
//                if(++iterations > LOCK_ATTEMPTIONS) {
//                    throw new IllegalStateException("Thread "+Thread.currentThread().getName()+" has looped on locking set");
//                }
            }
        }
        threads.set(index, Thread.currentThread());
        return set;
    }

    protected E getElement(final long head, long tail) {
        long currentHead = head;
        int index = calcIndex(currentHead);

        while (true) {
            boolean locked = lockGet(index);
            if (locked) {
                E e = get(index);
                assert e != null : "e == null";

                setNextHead(head, currentHead);

                checkHeadTailConsistency();

                successGet();

                unlockGet(index);
                return e;
            } else {
                failGet();

                currentHead = computeHead(currentHead);
                if (checkHeadOverflow(currentHead)) {
                    return null;
                }
                index = calcIndex(currentHead);
            }
        }

        //throw new IllegalStateException("getElement");
    }

    private void unlockGet(int index) {
        boolean unlocked = types.compareAndSet(index, Type.reading, Type.read);
        assert unlocked;
    }

    private boolean lockGet(int index) {
        boolean set;
        int iterations=0;
        while (!(set = types.compareAndSet(index, Type.written, Type.reading))) {
            Type type = types.get(index);
            if (type != Type.writing) {
                break;
            } else {
//                if(++iterations > LOCK_ATTEMPTIONS) {
//                    throw new IllegalStateException("Thread "+Thread.currentThread().getName()+" has looped on locking get");
//                }
            }
        }
        return set;
    }

    private static enum Type {reading, read, writing, written}
}

