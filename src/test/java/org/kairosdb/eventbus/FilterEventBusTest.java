package org.kairosdb.eventbus;

import org.kairosdb.core.KairosRootConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterEventBusTest
{
	@Test(expected = NullPointerException.class)
	public void test_constructor_nullConfig_invalid()
	{
		new FilterEventBus(null);
	}

	@Test
	public void test_publishEvent()
	{
		Subscriber subscriber1 = new Subscriber();
		Subscriber subscriber2 = new Subscriber();
		Subscriber subscriber3 = new Subscriber();
		EventBusConfiguration config = new EventBusConfiguration(new KairosRootConfig());
		FilterEventBus eventBus = new FilterEventBus(config);

		eventBus.register(subscriber1);
		eventBus.register(subscriber2);
		eventBus.register(subscriber3);
		eventBus.createPublisher(String.class).post("Hi");

		assertEquals("Hi", subscriber1.what());
		assertEquals("Hi", subscriber2.what());
		assertEquals("Hi", subscriber3.what());
	}

	@Test
	public void test_modifyEvent()
	{
		Subscriber subscriber1 = new Subscriber();
		Subscriber subscriber2 = new Subscriber();
		Subscriber subscriber3 = new Subscriber();
		FilterSubscriber filter = new FilterSubscriber("Bye");
		EventBusConfiguration config = new EventBusConfiguration(new KairosRootConfig());
		FilterEventBus eventBus = new FilterEventBus(config);

		eventBus.register(subscriber1, 1);
		eventBus.register(subscriber2, 2);
		eventBus.register(subscriber3, 10);
		eventBus.register(filter, 5);

		eventBus.createPublisher(String.class).post("Hi");

		assertEquals("Hi", subscriber1.what());
		assertEquals("Hi", subscriber2.what());
		assertEquals("Hi", filter.what());
		assertEquals("Bye", subscriber3.what());
	}

	@Test
	public void test_filterEvent()
	{
		Subscriber subscriber1 = new Subscriber();
		Subscriber subscriber2 = new Subscriber();
		Subscriber subscriber3 = new Subscriber();
		FilterSubscriber filter = new FilterSubscriber(null);
		EventBusConfiguration config = new EventBusConfiguration(new KairosRootConfig());
		FilterEventBus eventBus = new FilterEventBus(config);

		eventBus.register(subscriber1, 1);
		eventBus.register(subscriber2, 2);
		eventBus.register(subscriber3, 10);
		eventBus.register(filter, 5);

		eventBus.createPublisher(String.class).post("Hi");

		assertEquals("Hi", subscriber1.what());
		assertEquals("Hi", subscriber2.what());
		assertEquals("Hi", filter.what());
		assertEquals(null, subscriber3.what());
		assertThat(subscriber3.isWasCalled()).isFalse();
	}

	public class Subscriber
	{
		private String m_what;
		private boolean m_wasCalled = false;

		@Subscribe
		public void consume(String data)
		{
			m_what = data;
			m_wasCalled = true;
		}

		public String what() { return m_what; }

		public boolean isWasCalled()
		{
			return m_wasCalled;
		}
	}

	public class FilterSubscriber
	{
		private String m_what;
		private final String m_change;

		public FilterSubscriber(String change)
		{
			m_change = change;
		}

		@Subscribe
		public String consume(String data)
		{
			m_what = data;
			return m_change;
		}

		public String what() { return m_what; }
	}
}