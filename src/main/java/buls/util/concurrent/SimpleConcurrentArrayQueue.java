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
    private volatile boolean tailOverflow;

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
        //increment amount
        int amount;
        int nextAmount;
        do {
            amount = this.amount.get();
            boolean full = amount == capacity();
            assertAmount(amount);

            if (full) return false;
            nextAmount = amount + 1;
        } while (!this.amount.compareAndSet(amount, nextAmount));

        assertAmount(amount);

        boolean success;
        int tail = getAndIncrementTail();
        int index = _index(tail);
        success = _insert(e, index);
        //assert success : tail + " " + index + "\n" + this;
        if (!success) getAndDecrementTail();

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

        //decrement amount
        int amount;
        int nextAmount;
        do {
            amount = this.amount.get();
            assertAmount(amount);

            boolean empty = amount == 0;
            if (empty) return null;

            nextAmount = amount - 1;

        } while (!this.amount.compareAndSet(amount, nextAmount));

        assertAmount(amount);
        //assert amount <= tail : tail + " " + amount;

        int head = (tail > amount) ? tail - amount : 0;
        int index = _index(head);
        E e = _retrieve(index);

        //assert e != null : tail + " " + head + " " + amount + " " + index + "\n" + this;


        if (e == null) {
            int i = this.amount.incrementAndGet();
            failGet();
        } else successGet();
        return e;
    }

    private void assertAmount(int amount) {
        assert amount <= capacity() : amount + "\n" + this;
        assert amount >= 0 : amount + "\n" + this;
    }

    protected int getAndDecrementTail() {
        int result = tail.getAndDecrement();
        boolean overflow = isOverflow(result);
        if (overflow) {
            int expect = result - (result < 0 ? -1 : 1);
            if (reset(tail, expect, max_tail())) return 0;
            result = tail.getAndIncrement();
        }
        return result;
    }

    protected int getAndIncrementTail() {
        int result = tail.getAndIncrement();
        boolean overflow = isOverflow(result);
        if (overflow) {
            int expect = result + (result < 0 ? -1 : 1);
            if (reset(tail, expect, 1)) {
                tailOverflow = true;
                return 0;
            }
            result = tail.getAndIncrement();
        } else if (result < capacity()) tailOverflow = false;
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
