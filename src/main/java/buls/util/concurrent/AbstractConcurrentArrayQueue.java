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
        this.elements = new AtomicReferenceArray<Object>(capacity);
    }

    @Override
    public String toString() {
        return elements.toString();
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
            //если есть место для вставки, just do it

            result = setElement(e, tail);
        } else {
            //если предполагаемое кол-во элементов больше размера очереди, то нескольк потоков пытаются вставить,
            //нас обогнали и выдавили за пределы очереди
            //todo: по идее, тут можно быть оптимистом и проверить счетчик головы, вдруг она уехала вперед, освободив место
            result = false;
        }

        return result;
    }

    protected abstract boolean setElement(E e, long tail);

    protected boolean set(E e, int index) {
        assert e != null;
        boolean set = elements.compareAndSet(index, null, e);
        return set;

    }

    protected long getTail() {
        return tailSequence.get();
    }

    protected void yield() {
        Thread.yield();
    }

    /**
     *
     * @param oldTail счетчик хвоста скоторымпоток вошел в режим вставки
     * @param currentTail
     * @return
     */
    protected boolean setNextTail(long oldTail, long currentTail) {
        return tailSequence.compareAndSet(oldTail, currentTail + 1);
    }

    protected abstract E getElement(long head);

    protected E get(int index) {
        return (E) elements.getAndSet(index, null);
    }

    @Override
    public E poll() {
        int capacity = capacity();
        if (capacity == 0) {
            return null;
        }

        long tail = this.tailSequence.get();
        long head = this.headSequence.get();

        E result;

        if (head < tail) {
            //если новая голова все еще меньше хвоста, значит мы все еще в очереди и пробуем взять элемент
            result = getElement(head);

        } else {
            //если голова перевалила за хвост, то значит несколько потоков пытаются взть из головы,
            // нас обогонали и выдавили за пределы очереди
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

    protected boolean setNextHead(long oldHead, long currentHead) {
        return headSequence.compareAndSet(oldHead, currentHead + 1);
    }
}
