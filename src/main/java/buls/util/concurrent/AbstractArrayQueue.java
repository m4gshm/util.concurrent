package buls.util.concurrent;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * Created by alexander on 15.07.14.
 */
public abstract class AbstractArrayQueue<E> extends AbstractQueue<E> {
    @NotNull
    protected final Object[] elements;

    public AbstractArrayQueue(int capacity) {
        this.elements = new Object[capacity];
    }

    @NotNull
    @Override
    public String toString() {
        return _string/*Lines*/();
    }

    @NotNull
    protected String _stringLines() {
        int iMax = elements.length - 1;
        if (iMax == -1) return "[]";

        StringBuilder b = new StringBuilder();
        b.append("[\n");
        for (int i = 0; ; i++) {
            b.append(i).append(" ").append(_get(i));
            if (i == iMax) return b.append("\n]").toString();
            b.append("\n");
        }
    }

    @NotNull
    protected String _string() {
        int iMax = elements.length - 1;
        if (iMax == -1) return "[]";

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            b.append(_get(i));
            if (i == iMax)  return b.append(']').toString();
            b.append(',').append(' ');
        }
    }

    @NotNull
    @Override
    @SuppressWarnings("unchecked")
    public Iterator<E> iterator() {
        List list = Arrays.asList(elements);
        return Collections.unmodifiableCollection(list).iterator();
    }

    public final int capacity() {
        return elements.length;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    protected E _get(int index) {
        return (E) elements[index];
    }

    protected boolean _insert(E e, int index) {
        elements[index] = e;
        return true;
    }

    protected E _retrieve(int index) {
        E e = _get(index);
        _insert(null, index);
        return e;
    }

    @NotNull
    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }
}
