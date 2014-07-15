package buls.util.concurrent;

import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public class SimpleConcurrentArrayQueue<E> extends AbstractArrayQueue<E> {

    private static final Unsafe unsafe;

    static {
        Field field;
        try {
            field = Unsafe.class.getDeclaredField("theUnsafe");
        } catch (NoSuchFieldException e) {
            throw new InternalError("cannot retrieve unsafe field", e);
        }
        field.setAccessible(true);
        try {
            unsafe = (Unsafe) field.get(null);
        } catch (IllegalAccessException e) {
            throw new InternalError("cannot retrieve unsafe object", e);
        }
    }

    private static final int base = unsafe.arrayBaseOffset(Object[].class);
    private static final int shift;

    static {
        int scale = unsafe.arrayIndexScale(Object[].class);
        if ((scale & (scale - 1)) != 0)
            throw new Error("data type scale not a power of two");
        shift = 31 - Integer.numberOfLeadingZeros(scale);
    }

    private final int MAX_TAIL;


    AtomicInteger tail = new AtomicInteger();
    private AtomicInteger amount = new AtomicInteger();

    public SimpleConcurrentArrayQueue(int capacity) {
        super(capacity);
        MAX_TAIL = Integer.MAX_VALUE - (Integer.MAX_VALUE % capacity) - 1;
    }

    private static long byteOffset(int i) {
        return ((long) i << shift) + base;
    }

    @Override
    public boolean offer(@Nullable E e) {
        int amount = this.amount.getAndIncrement();
        boolean full = amount >= capacity();

        boolean success = false;
        if (!full) {
            int tail = getAndIncrement(this.tail);
            int index = _index(tail);
            success = _insert(e, index);
            if (!success) this.tail.decrementAndGet();
        }

        if (full || !success) this.amount.decrementAndGet();
        return success;
    }

    protected int getAndIncrement(AtomicInteger counter) {
        int result = counter.getAndIncrement();
        boolean overflow = result > max_tail() || result < 0;
        if (overflow) {
            int expect = result + 1;
            while (expect < 0 || expect > max_tail()) {
                boolean zero = counter.compareAndSet(expect, 0);
                if (zero) return 0;
                expect = counter.get();
            }
            result = counter.getAndIncrement();
        }
        return result;
    }

    private int _index(int counter) {
        return (counter < 0 ? -counter : counter) % capacity();
    }

    @Nullable
    @Override
    public E poll() {
        int amount = this.amount.get();
        boolean empty = amount <= 0;

        if (empty) return null;

        int head = this.tail.get() - amount;
        int index = _index(head);
        E e = _retrieve(index);
        if (e != null) this.amount.getAndDecrement();
        return e;

    }

    @Nullable
    protected final E _get(int index) {
        return (E) getRaw(checkedByteOffset(index));
    }

    @Override
    protected final boolean _insert(E e, int index) {
        return _cas(index, null, e);
    }

    @Override
    @Nullable
    protected final E _retrieve(int index) {
        E e = _get(index);
        if (_cas(index, e, null)) return e;
        else return null;
    }

    private long checkedByteOffset(int i) {
        if (i < 0 || i >= elements.length)
            throw new IndexOutOfBoundsException("index " + i);
        return byteOffset(i);
    }

    private Object getRaw(long offset) {
        return unsafe.getObjectVolatile(elements, offset);
    }

    private boolean compareAndSetRaw(long offset, Object expect, Object update) {
        return unsafe.compareAndSwapObject(elements, offset, expect, update);
    }

    public final boolean _cas(int i, Object expect, Object update) {
        return compareAndSetRaw(checkedByteOffset(i), expect, update);
    }

    @Override
    public int size() {
        return amount.get();
    }

    public int max_tail() {
        return MAX_TAIL;
    }
}
