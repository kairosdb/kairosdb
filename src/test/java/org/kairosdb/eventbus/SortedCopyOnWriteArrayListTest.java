package org.kairosdb.eventbus;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SortedCopyOnWriteArrayListTest
{
    @Test
    public void test()
    {
        ArrayList<MyClass> list = new ArrayList<>();
        list.add(new MyClass("1", 20));
        list.add(new MyClass("2", 0));
        list.add(new MyClass("3", 100));
        list.add(new MyClass("4", 30));
        list.add(new MyClass("5", 40));

        SortedCopyOnWriteArrayList<MyClass> arrayList = new SortedCopyOnWriteArrayList<>(new PriorityComparator());

        arrayList.addAll(list);

        assertThat(arrayList.get(0), equalTo(list.get(1)));
        assertThat(arrayList.get(1), equalTo(list.get(0)));
        assertThat(arrayList.get(2), equalTo(list.get(3)));
        assertThat(arrayList.get(3), equalTo(list.get(4)));
        assertThat(arrayList.get(4), equalTo(list.get(2)));
    }

    private static class PriorityComparator implements Comparator<MyClass>
    {
        @Override
        public int compare(MyClass o1, MyClass o2)
        {
            return (o1.getPriority() < o2.getPriority()) ? -1 : ((o1.getPriority() == o2.getPriority()) ? 1 : 1);
        }
    }

    private static class MyClass
    {
        private String name;
        private int priority;

        public MyClass(String name, int priority)
        {
            this.name = name;
            this.priority = priority;
        }

        public String getName()
        {
            return name;
        }

        public int getPriority()
        {
            return priority;
        }
    }
}