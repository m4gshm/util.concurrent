package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
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
        MAX_TAIL = capacity <= 0 ? 0 : Integer.MAX_VALUE - (Integer.MAX_VALUE % capacity) - 1;
    }

    private static long byteOffset(int i) {
        return ((long) i << shift) + base;
    }

    @NotNull
    @Override
    public String toString() {
        final int a = amount.get();
        final int t = tail.get();
        return "t: " + t + " (" + _index(t) + ")"
                + ", a: " + a
                + ", c:" + capacity()
                + ", mT:" + max_tail()
                + "\n" + super.toString();
    }

    @Override
    public boolean offer(@Nullable E e) {
        int amount = this.amount.getAndIncrement();
        boolean full = amount >= capacity();

        boolean success;
        if (!full) {
            int tail = getAndIncrement(this.tail);
            int index = _index(tail);
            success = _insert(e, index);
            if (!success) getAndDecrement(this.tail);
        } else success = false;

        if (!success) {
            this.amount.decrementAndGet();
            failSet();
        } else successSet();

        return success;
    }

    @Nullable
    @Override
    public E poll() {
        int tail = this.tail.get();
        int amount = this.amount.getAndDecrement();
        boolean empty = amount <= 0;

        E e;
        if (empty) e = null;
        else {
            int index = _index(tail) - amount;
            if (index >= 0) {
                e = _retrieve(index);
                assert e != null : tail + " " + _index(tail) + " " + amount + " " + index + "\n" + this;
            } else e = null;
        }

        if (e == null) {
            this.amount.incrementAndGet();
            failGet();
        }

        successGet();
        return e;
    }

    protected int getAndDecrement(AtomicInteger counter) {
        int result = counter.getAndDecrement();
        boolean overflow = isOverflow(result);
        if (overflow) {
            int expect = result - (result < 0 ? -1 : 1);
            if (reset(counter, expect, max_tail())) return 0;
            result = counter.getAndIncrement();
        }
        return result;
    }

    protected int getAndIncrement(AtomicInteger counter) {
        int result = counter.getAndIncrement();
        boolean overflow = isOverflow(result);
        if (overflow) {
            int expect = result + (result < 0 ? -1 : 1);
            if (reset(counter, expect, 1)) return 0;
            result = counter.getAndIncrement();
        }
        return result;
    }

    private boolean reset(AtomicInteger counter, int expect, int next) {
        while (isOverflow(expect)) {
            boolean resetTail = counter.compareAndSet(expect, next);
            if (resetTail) return true;
            expect = counter.get();
        }
        return false;
    }

    private boolean isOverflow(int counter) {
        return counter < 0 || counter > max_tail();
    }

    private int _index(int counter) {
        return counter % capacity();
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

    protected void failGet() {

    }

    protected void successGet() {

    }

    protected void failSet() {

    }

    protected void successSet() {

    }
}
