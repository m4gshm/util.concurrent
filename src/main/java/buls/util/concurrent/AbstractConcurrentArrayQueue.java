package buls.util.concurrent;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractConcurrentArrayQueue<E> extends AbstractQueue<E> {
    protected final AtomicReferenceArray<Object> elements;
    protected final AtomicLong tailSequence = new AtomicLong(0);
    protected final AtomicLong headSequence = new AtomicLong(0);

    public AbstractConcurrentArrayQueue(int capacity) {
        this.elements = new AtomicReferenceArray<>(capacity);
    }

    @Override
    public String toString() {
        return "h: " + headSequence + ", t:" + tailSequence + " " + elements.toString();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    protected boolean isInterrupted() {
        return Thread.interrupted();
    }

    @Override
    public int size() {
        long tail = tailSequence.get();
        long head = headSequence.get();
        return (int) (tail - head);
    }

    public int capacity() {
        return elements.length();
    }

    @Override
    public boolean offer(E e) {
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
        if (amount <= capacity) {
            result = setElement(e, tail);
        } else {

            result = false;
        }

        return result;
    }

    protected abstract boolean setElement(E e, long tail);

    protected boolean set(E e, int index) {
        return elements.compareAndSet(index, null, e);
    }

    protected long getTail() {
        return tailSequence.get();
    }

    protected void yield() {
        Thread.yield();
    }

    /**
     * @param oldTail      счетчик хвоста с которым поток вошел в режим вставки
     * @param insertedTail
     * @return
     */
    protected boolean setNextTail(long oldTail, long insertedTail) {
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

    protected abstract E getElement(long head);

    @SuppressWarnings("unchecked")
    protected E get(int index) {
        E e = (E) elements.get(index);
        if (e != null) {
            boolean set = elements.compareAndSet(index, e, null);
            if (set) {
                return e;
            }
        }
        return null;
    }

    @Override
    public E poll() {
        int capacity = capacity();
        if (capacity == 0) {
            return null;
        }

        long tail = tailSequence.get();
        long head = headSequence.get();

        E result;

        if (head < tail) {
            result = getElement(head);
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
                //двойное взятие
                //head = 0, tail = 5, capacity = 5
                //t1    pool()          index = head % capacity = 0
                //t1    pool()          index = head % capacity = 0
                //t1    getElement(0)   true
                //t1    setNextHead 1   true
                //t3    offer()         index = tail % capacity = 0
                //t3    setElement(0)   true
                //t2    getElement(0)   true
                //t2    setNextHead 1   false

                //t1 и t2 успешно взяли объект из одной и той же ячейки, t1 первым выставил следующий индекс, t2 опоздал

                break;
            } else {
                assert currentValue > newValue : oldHead + " " + currentValue + " " + newValue;
                break;
            }
        }
        return set;
    }
}
