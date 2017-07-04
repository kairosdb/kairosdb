package org.kairosdb.eventbus;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class SortedCopyOnWriteArrayList<E> extends CopyOnWriteArrayList<E>
{
    private final Comparator<E> comparator;

    public SortedCopyOnWriteArrayList(Comparator<E> comparator)
    {
        this.comparator = comparator;
    }

    public boolean addAll(Collection<? extends E> c)
    {
        boolean response;
        final ReentrantLock lock = this.getLock();
        lock.lock();
        try {
            response = super.addAll(c);
            if (response) {
                Object[] array = getTheArray();
                setTheArray(castAndSortArray(array));
            }
        }
        finally {
            lock.unlock();
        }
        return response;
    }

    @SuppressWarnings("unchecked")
    private Object[] castAndSortArray(Object[] elements)
    {
        ArrayList<E> array = new ArrayList<>();
        for (Object element : elements) {
            array.add((E) element);
        }
        array.sort(comparator);
        return array.toArray();
    }

    private ReentrantLock getLock()
    {
        try {
            Field lock = this.getClass().getSuperclass().getDeclaredField("lock");
            lock.setAccessible(true);
            return (ReentrantLock) lock.get(this);
        }
        catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Could not acquire lock", e.getCause());
        }
    }

    private Object[] getTheArray()
    {
        try {
            Method method = this.getClass().getSuperclass().getDeclaredMethod("getArray");
            method.setAccessible(true);
            return (Object[]) method.invoke(this);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Could not access array", e.getCause());
        }
    }

    private void setTheArray(Object[] array)
    {
        try {
            Method method = this.getClass().getSuperclass().getDeclaredMethod("setArray", Object[].class);
            method.setAccessible(true);
            method.invoke(this, new Object[]{array});
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Could not change array value", e.getCause());
        }
    }
}
