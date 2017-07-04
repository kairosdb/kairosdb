package org.kairosdb.eventbus;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterRegistryTest {

    private FilterRegistry registry;

    @Before
    public void setup()
    {
        registry = new FilterRegistry(new EventBusWithFilters(new EventBusConfiguration(new Properties())));
    }

    @Test
    public void testRegister_invalidMethod_noReturnType()
    {
        try {
            registry.register(new InvalidSubscriber());
            fail("expected an UncheckedExecutionException");
        }
        catch(UncheckedExecutionException e) {
            assertEquals(
                    "Method public void org.kairosdb.eventbus.FilterRegistryTest$InvalidSubscriber.handle(java.lang.String) must have return type of java.lang.String",
                    e.getCause().getMessage());
        }
    }

    @Test
    public void testRegister() {
        assertEquals(0, registry.getSubscribersForTesting(String.class).size());

        registry.register(new StringSubscriber());
        assertEquals(1, registry.getSubscribersForTesting(String.class).size());

        registry.register(new StringSubscriber());
        assertEquals(2, registry.getSubscribersForTesting(String.class).size());

        registry.register(new ObjectSubscriber());
        assertEquals(2, registry.getSubscribersForTesting(String.class).size());
        assertEquals(1, registry.getSubscribersForTesting(Object.class).size());
    }

    @Test
    public void testUnregister() {
        StringSubscriber s1 = new StringSubscriber();
        StringSubscriber s2 = new StringSubscriber();

        registry.register(s1);
        registry.register(s2);

        registry.unregister(s1);
        assertEquals(1, registry.getSubscribersForTesting(String.class).size());

        registry.unregister(s2);
        assertTrue(registry.getSubscribersForTesting(String.class).isEmpty());
    }

    @SuppressWarnings("EmptyCatchBlock")
    @Test
    public void testUnregister_notRegistered() {
        try {
            registry.unregister(new StringSubscriber());
            fail();
        } catch (IllegalArgumentException expected) {
        }

        StringSubscriber s1 = new StringSubscriber();
        registry.register(s1);
        try {
            registry.unregister(new StringSubscriber());
            fail();
        } catch (IllegalArgumentException expected) {
            // a StringSubscriber was registered, but not the same one we tried to unregister
        }

        registry.unregister(s1);

        try {
            registry.unregister(s1);
            fail();
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testGetSubscribers() {
        assertEquals(0, Iterators.size(registry.getSubscribers("")));

        registry.register(new StringSubscriber());
        assertEquals(1, Iterators.size(registry.getSubscribers("")));

        registry.register(new StringSubscriber());
        assertEquals(2, Iterators.size(registry.getSubscribers("")));

        registry.register(new ObjectSubscriber());
        assertEquals(3, Iterators.size(registry.getSubscribers("")));
        assertEquals(1, Iterators.size(registry.getSubscribers(new Object())));
        assertEquals(1, Iterators.size(registry.getSubscribers(1)));

        registry.register(new IntegerSubscriber());
        assertEquals(3, Iterators.size(registry.getSubscribers("")));
        assertEquals(1, Iterators.size(registry.getSubscribers(new Object())));
        assertEquals(2, Iterators.size(registry.getSubscribers(1)));
    }

    @Test
    public void testGetSubscribers_returnsImmutableSnapshot() {
        StringSubscriber s1 = new StringSubscriber();
        StringSubscriber s2 = new StringSubscriber();
        ObjectSubscriber o1 = new ObjectSubscriber();

        Iterator<FilterSubscriber> empty = registry.getSubscribers("");
        assertFalse(empty.hasNext());

        empty = registry.getSubscribers("");

        registry.register(s1);
        assertFalse(empty.hasNext());

        Iterator<FilterSubscriber> one = registry.getSubscribers("");
        assertEquals(s1, one.next().target);
        assertFalse(one.hasNext());

        one = registry.getSubscribers("");

        registry.register(s2);
        registry.register(o1);

        Iterator<FilterSubscriber> three = registry.getSubscribers("");
        assertEquals(s1, one.next().target);
        assertFalse(one.hasNext());

        assertEquals(s1, three.next().target);
        assertEquals(s2, three.next().target);
        assertEquals(o1, three.next().target);
        assertFalse(three.hasNext());

        three = registry.getSubscribers("");

        registry.unregister(s2);

        assertEquals(s1, three.next().target);
        assertEquals(s2, three.next().target);
        assertEquals(o1, three.next().target);
        assertFalse(three.hasNext());

        Iterator<FilterSubscriber> two = registry.getSubscribers("");
        assertEquals(s1, two.next().target);
        assertEquals(o1, two.next().target);
        assertFalse(two.hasNext());
    }

    @Test
    public void test_register_priority()
    {
        StringSubscriber s1 = new StringSubscriber();
        StringSubscriber s2 = new StringSubscriber();
        StringSubscriber s3 = new StringSubscriber();

        assertEquals(0, registry.getSubscribersForTesting(String.class).size());

        registry.register(s1, 80);
        registry.register(s2, 30);
        registry.register(s3, 10);

        List<FilterSubscriber> subscribers = registry.getSubscribersForTesting(String.class);
        assertEquals(3, subscribers.size());
        assertThat(subscribers.get(0).target, equalTo(s3));
        assertThat(subscribers.get(1).target, equalTo(s2));
        assertThat(subscribers.get(2).target, equalTo(s1));
    }

    public static class InvalidSubscriber {
        @SuppressWarnings("unused")
        @Filter
        public void handle(String s) {
        }
    }

    public static class StringSubscriber {

        @Filter
        @SuppressWarnings("unused")
        public String handle(String s) {
            return s;
        }
    }

    public static class IntegerSubscriber {

        @Filter
        @SuppressWarnings("unused")
        public Integer handle(Integer i) {
            return i;
        }
    }

    public static class ObjectSubscriber {

        @Filter
        @SuppressWarnings("unused")
        public Object handle(Object o) {
            return o;
        }
    }

    @Test
    public void testFlattenHierarchy() {
        assertEquals(
                ImmutableSet.of(
                        Object.class,
                        HierarchyFixtureInterface.class,
                        HierarchyFixtureSubinterface.class,
                        HierarchyFixtureParent.class,
                        HierarchyFixture.class),
                FilterRegistry.flattenHierarchy(HierarchyFixture.class));
    }

    private interface HierarchyFixtureInterface {
        // Exists only for hierarchy mapping; no members.
    }

    private interface HierarchyFixtureSubinterface
            extends HierarchyFixtureInterface {
        // Exists only for hierarchy mapping; no members.
    }

    private static class HierarchyFixtureParent
            implements HierarchyFixtureSubinterface {
        // Exists only for hierarchy mapping; no members.
    }

    private static class HierarchyFixture extends HierarchyFixtureParent {
        // Exists only for hierarchy mapping; no members.
    }
}