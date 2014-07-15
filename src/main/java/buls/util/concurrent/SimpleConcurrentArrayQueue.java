package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public class SimpleConcurrentArrayQueue<E> extends AbstractConcurrentArrayQueue<E> {

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

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    public SimpleConcurrentArrayQueue(int capacity) {
        this(capacity, true);
    }

    protected SimpleConcurrentArrayQueue(int capacity, boolean checkInterruption) {
        super(capacity, checkInterruption);
    }

    private static long byteOffset(int i) {
        return ((long) i << shift) + base;
    }

    @Override
    protected final int set(@NotNull final E e, final long tail, final long currentTail, long head) {
        final int index = computeIndex(currentTail);
        if (_insert(e, index)) {
            setNextTail(tail, currentTail);
            readUnlock();
            writeLock();
            try {
                long h = getHead();
                boolean tailOverflow = currentTail < tail;
                boolean headOverflow = h < head;
                long hLevel = h / capacity();
                long tLevel = currentTail / capacity();
                if (hLevel <= tLevel && (tailOverflow == headOverflow || !tailOverflow)) {
                    boolean behindHead = !(h <= currentTail);
                    if (behindHead) {
                        boolean b = _cas(index, e, null);
                        if(b) {
                            return GET_CURRENT_TAIL;
                        } else {
                            String.valueOf(b);
                        }
                    }
                }
            } finally {
                readLock();
                writeUnlock();
            }
            return SUCCESS;
        } else return GO_NEXT;
    }

    @Override
    @Nullable
    protected final E get(long head, long currentHead) {
        final int index = computeIndex(currentHead);
        E e = _retrieve(index);
        if (e != null) {
            setNextHead(head, currentHead);
            return e;
        }
        return null;
    }

    @Override
    public boolean offer(@Nullable E e) {
        try {
            readLock();
            return super.offer(e);
        } finally {
            readUnlock();
        }
    }

    @Nullable
    @Override
    public E poll() {
        try {
            readLock();
            return super.poll();
        } finally {
            readUnlock();
        }
    }

    private void readUnlock() {
        rwl.readLock().unlock();
    }

    private void readLock() {
        rwl.readLock().lock();
    }

    private void writeUnlock() {
        rwl.writeLock().unlock();
    }

    private void writeLock() {
        rwl.writeLock().lock();
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
        if (_cas(index, e, null)) {
            return e;
        } else {
            return null;
        }
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
    protected long computeHead(long head) {
        return _increment(head);
    }
}
