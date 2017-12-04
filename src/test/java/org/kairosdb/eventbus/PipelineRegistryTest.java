package org.kairosdb.eventbus;

import com.google.common.collect.ImmutableSet;
import org.kairosdb.eventbus.Subscribe;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Properties;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PipelineRegistryTest
{

	private PipelineRegistry registry;

	@Before
	public void setup()
	{
		registry = new PipelineRegistry(new FilterEventBus(new EventBusConfiguration(new Properties())));
	}

	@Test
	public void testRegister_noReturnType()
	{
		registry.register(new NoReturnSubscriber());
	}

	@Test
	public void testRegister()
	{
		assertEquals(0, registry.getPipeline(String.class).size());

		registry.register(new StringSubscriber());
		assertEquals(1, registry.getPipeline(String.class).size());

		registry.register(new StringSubscriber());
		assertEquals(2, registry.getPipeline(String.class).size());

		registry.register(new ObjectSubscriber());
		assertEquals(2, registry.getPipeline(String.class).size());
		assertEquals(1, registry.getPipeline(Object.class).size());
	}

	/*@Test
	public void testUnregister()
	{
		StringSubscriber s1 = new StringSubscriber();
		StringSubscriber s2 = new StringSubscriber();

		registry.register(s1);
		registry.register(s2);

		registry.unregister(s1);
		assertEquals(1, registry.getPipeline(String.class).size());

		registry.unregister(s2);
		assertTrue(registry.getPipeline(String.class).isEmpty());
	}

	@SuppressWarnings("EmptyCatchBlock")
	@Test
	public void testUnregister_notRegistered()
	{
		try
		{
			registry.unregister(new StringSubscriber());
			fail();
		}
		catch (IllegalArgumentException expected)
		{
		}

		StringSubscriber s1 = new StringSubscriber();
		registry.register(s1);
		try
		{
			registry.unregister(new StringSubscriber());
			fail();
		}
		catch (IllegalArgumentException expected)
		{
			// a StringSubscriber was registered, but not the same one we tried to unregister
		}

		registry.unregister(s1);

		try
		{
			registry.unregister(s1);
			fail();
		}
		catch (IllegalArgumentException expected)
		{
		}
	}*/

	@Test
	public void testGetSubscribers()
	{
		assertEquals(0, registry.getPipeline(String.class).size());

		registry.register(new StringSubscriber());
		assertEquals(1, registry.getPipeline(String.class).size());

		registry.register(new StringSubscriber());
		assertEquals(2, registry.getPipeline(String.class).size());

		registry.register(new ObjectSubscriber());
		assertEquals(2, registry.getPipeline(String.class).size());
		assertEquals(1, registry.getPipeline(Object.class).size());
		assertEquals(0, registry.getPipeline(Integer.class).size());

		registry.register(new IntegerSubscriber());
		assertEquals(2, registry.getPipeline(String.class).size());
		assertEquals(1, registry.getPipeline(Object.class).size());
		assertEquals(1, registry.getPipeline(Integer.class).size());
	}

	@Test
	public void testGetSubscribers_returnsImmutableSnapshot()
	{
		StringSubscriber s1 = new StringSubscriber();
		StringSubscriber s2 = new StringSubscriber();
		StringSubscriber o1 = new StringSubscriber();

		Iterator<FilterSubscriber> empty = registry.getPipeline(String.class).iterator();
		assertFalse(empty.hasNext());

		empty = registry.getPipeline(String.class).iterator();

		registry.register(s1, 1);
		assertFalse(empty.hasNext());

		Iterator<FilterSubscriber> one = registry.getPipeline(String.class).iterator();
		assertEquals(s1, one.next().target);
		assertFalse(one.hasNext());

		one = registry.getPipeline(String.class).iterator();

		registry.register(s2, 2);
		registry.register(o1, 3);

		Iterator<FilterSubscriber> three = registry.getPipeline(String.class).iterator();
		assertEquals(s1, one.next().target);
		assertFalse(one.hasNext());

		assertEquals(s1, three.next().target);
		assertEquals(s2, three.next().target);
		assertEquals(o1, three.next().target);
		assertFalse(three.hasNext());

		three = registry.getPipeline(String.class).iterator();

		/*registry.unregister(s2);

		assertEquals(s1, three.next().target);
		assertEquals(s2, three.next().target);
		assertEquals(o1, three.next().target);
		assertFalse(three.hasNext());

		Iterator<FilterSubscriber> two = registry.getPipeline(String.class).iterator();
		assertEquals(s1, two.next().target);
		assertEquals(o1, two.next().target);
		assertFalse(two.hasNext());*/
	}

	@Test
	public void test_register_priority()
	{
		StringSubscriber s1 = new StringSubscriber();
		StringSubscriber s2 = new StringSubscriber();
		StringSubscriber s3 = new StringSubscriber();

		assertEquals(0, registry.getPipeline(String.class).size());

		registry.register(s1, 80);
		registry.register(s2, 30);
		registry.register(s3, 10);

		Pipeline subscribers = registry.getPipeline(String.class);
		assertEquals(3, subscribers.size());
		Iterator<FilterSubscriber> iterator = subscribers.iterator();
		assertThat(iterator.next().target, equalTo(s3));
		assertThat(iterator.next().target, equalTo(s2));
		assertThat(iterator.next().target, equalTo(s1));
	}

	public static class NoReturnSubscriber
	{
		@SuppressWarnings("unused")
		@Subscribe
		public void handle(String s)
		{
		}
	}

	public static class StringSubscriber
	{

		@Subscribe
		@SuppressWarnings("unused")
		public String handle(String s)
		{
			return s;
		}
	}

	public static class IntegerSubscriber
	{

		@Subscribe
		@SuppressWarnings("unused")
		public Integer handle(Integer i)
		{
			return i;
		}
	}

	public static class ObjectSubscriber
	{

		@Subscribe
		@SuppressWarnings("unused")
		public Object handle(Object o)
		{
			return o;
		}
	}

	@Test
	public void testFlattenHierarchy()
	{
		assertEquals(
				ImmutableSet.of(
						Object.class,
						HierarchyFixtureInterface.class,
						HierarchyFixtureSubinterface.class,
						HierarchyFixtureParent.class,
						HierarchyFixture.class),
				PipelineRegistry.flattenHierarchy(HierarchyFixture.class));
	}

	private interface HierarchyFixtureInterface
	{
		// Exists only for hierarchy mapping; no members.
	}

	private interface HierarchyFixtureSubinterface
			extends HierarchyFixtureInterface
	{
		// Exists only for hierarchy mapping; no members.
	}

	private static class HierarchyFixtureParent
			implements HierarchyFixtureSubinterface
	{
		// Exists only for hierarchy mapping; no members.
	}

	private static class HierarchyFixture extends HierarchyFixtureParent
	{
		// Exists only for hierarchy mapping; no members.
	}
}