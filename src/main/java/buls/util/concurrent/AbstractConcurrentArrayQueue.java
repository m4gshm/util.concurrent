package buls.util.concurrent;

import java.io.Serializable;
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

    protected boolean set(E e, int tail, int currentTail) {
        int index = calcIndex(currentTail);
        Marker prevMarker = getPrevMarker(currentTail);
        boolean set = elements.compareAndSet(index, prevMarker, e);
        if(set) {
            setNextTail(tail, currentTail);
        }
        return set;
    }

    private Marker getPrevMarker(int value) {
        int level = value / capacity();
        return level == 0 ? null : new Marker(level);
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
                //todo: просчитать двойную вставку с обгоном хвостом головы
                //двойна вставка, случай 1: запоздавшая вставка.
                //t2 пытается вставить  в ячейку 0, в момент когда t1 уже вставил
                // в эту ячейку и передвинул хвость вперед,
                //а t3 прочитал её и передвинул голову вперед

                //head = 0, tail = 0, capacity = 5

                //t1    offer()                 index = tail % capacity = 0
                //t1    setElement(0, val1)     true
                //t2    offer()                 index = tail % capacity = 0
                //t1    setNextTail 1           true

                //t3    pool()                  index = head % capacity = 0,  head 0 < tail 1
                //t3    getElement(0)           true
                //t3    setNextHead 1           true

                //t2    setElement(0, val2)     true
                //t2    setNextTail 1           false  ; tail уже 1 , нас обогнал t1
                //t2    insertTail 0 < head 1   true , голова уведена вперед потоком t3
                //t2    replaceByNull(0, val1)  true
                //t2    return false

                //двойная вставка, случай 2: запоздавшая вставка.
                //t2 пытается вставить  в ячейку 0, в момент когда t1 уже вставил
                // в эту ячейку и передвинул хвость вперед,
                //а t3 прочитал её, но НЕ ПЕРЕДВИНУЛ ГОЛОВУ ВПЕРЕД

                //head = 0, tail = 0, capacity = 5

                //t1    offer()                 index = tail % capacity = 0
                //t1    setElement(0, val1)     true
                //t2    offer()                 index = tail % capacity = 0
                //t1    setNextTail 1           true

                //t3    pool()                  index = head % capacity = 0,  head 0 < tail 1
                //t3    getElement(0)           true


                //t2    setElement(0, val2)     true
                //t2    setNextTail 1           false  ; tail уже 1 , нас обогнал t1
                //t2    insertTail 0 == head 0   true , голова на месте
                //t2    return true

                //t3    setNextHead 1           true  - передвигаем голову и получаем val2 за пределами очереди


                //двойная вставка, случай 3: запоздавшая вставка, но взятие с маркером взятия
                //t2 пытается вставить  в ячейку 0, в момент когда t1 уже вставил
                // в эту ячейку и передвинул хвость вперед,
                //а t3 прочитал её, отметил маркером

                //head = 0, tail = 0, capacity = 5, prevMarker = null

                //t1    offer()                 index = tail % capacity = 0
                //t1    getPrevMarker           PM = null
                //t1    cas(0, PM, val1)        true
                //t2    offer()                 index = tail % capacity = 0
                //t1    setNextTail 1           true

                //t3    pool()                  index = head % capacity = 0,  head 0 < tail 1
                //t3    get(0)                  val1 взял значение
                //t3    getCurrentMarker        head 0 / capacity 5 = 0 MARKER(0 + 1) = MARKER(1)
                //t3    MARKER(1)
                //t3    cas(0, val1, MARKER(1))  true заменил его на MARKER_1

                //t2    getPrevMarker           null
                //t2    cas(0, null, val2)      false хвост обогнал, можно выходить
                //t2    return false

                //t3    setNextHead 1           true

                //произошло наполнение очереди, наичнаем вставку вначало массива
                //head = 2, tail = 5, capacity = 5, prevMarker = null, MARKER_1 != null
                //t1    offer()             index = tail 5 % capacity 5 = 0
                //t1    getPrevMarker()     tail / capacity = 1 = new MARKER(1)
                //t1    cas(0, MARKER(1), val3)        true

                //t2    offer()                 index = tail 5 % capacity 5 = 0
                //t2    getPrevMarker()     tail / capacity = 1 = new MARKER(1)
                //t1    cas(0, MARKER(1), val4)       false  - ячейка уже извлечена
                return false;
            } else {
                //todo: хвост может обогнать и в этом случае
                assert currentValue > newValue : oldTail + " " + currentValue + " " + newValue;
                return true;
            }
        }
        return set;
    }

    protected int calcIndex(long counter) {
        return (int) (counter % capacity());
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

    private class Marker implements Serializable {
        private final int level;

        public Marker(int level) {
            this.level = level;
        }

        @Override
        public boolean equals(Object o) {
            Marker marker = (Marker) o;
            if (level != marker.level) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return level;
        }
    }
}
