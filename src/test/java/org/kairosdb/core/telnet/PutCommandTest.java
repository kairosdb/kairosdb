/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.core.telnet;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.jboss.netty.channel.*;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.ValidationException;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class PutCommandTest
{
	private PutCommand m_command;
	private FakeDatastore m_datastore;

	@Before
	public void setup() throws DatastoreException
	{
		EventBus eventBus = new EventBus();
		m_datastore = new FakeDatastore();
		eventBus.register(m_datastore);

		m_command = new PutCommand(eventBus, "test", new LongDataPointFactoryImpl(),
				new DoubleDataPointFactoryImpl());
	}

	@Test
	public void test() throws DatastoreException, ValidationException
	{
		m_command.execute(new FakeChannel(), new String[]{"telnet", "MetricName", "12345678999", "789", "foo=bar", "fum=barfum"});

		assertThat(m_datastore.getSet().getName(), equalTo("MetricName"));
		assertThat(m_datastore.getSet().getTags().size(), equalTo(2));
		assertThat(m_datastore.getSet().getTags().get("foo"), equalTo("bar"));
		assertThat(m_datastore.getSet().getTags().get("fum"), equalTo("barfum"));
		assertThat(m_datastore.getSet().getDataPoints().get(0).getTimestamp(), equalTo(12345678999L));
		assertThat(m_datastore.getSet().getDataPoints().get(0).getLongValue(), equalTo(789L));
	}

	@Test
	public void test_metricName_empty_invalid() throws DatastoreException, ValidationException
	{
		try
		{
			m_command.execute(new FakeChannel(), new String[]{"telnet", "", "12345678999", "789", "foo=bar", "fum=barfum"});
			fail("ValidationException expected");
		}
		catch (DatastoreException e)
		{
			fail("ValidationException expected");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metricName may not be empty."));
		}
	}

	@Test
	public void test_metricName_characters_valid() throws DatastoreException, ValidationException
	{
		m_command.execute(new FakeChannel(), new String[]{"telnet", "你好", "12345678999", "789", "foo=bar", "fum=barfum"});
	}

	@Test
	public void test_tagName_empty_invalid() throws DatastoreException, ValidationException
	{
		try
		{
			m_command.execute(new FakeChannel(), new String[]{"telnet", "metricName", "12345678999", "789", "foo=bar", "=barfum"});
			fail("ValidationException expected");
		}
		catch (DatastoreException e)
		{
			fail("ValidationException expected");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("tag[1].name may not be empty."));
		}
	}

	@Test
	public void test_tagName_characters_validColonTagName() throws DatastoreException, ValidationException
	{
		m_command.execute(new FakeChannel(), new String[]{"telnet", "metricName", "12345678999", "789", "foo=bar", "fum:fi=barfum"});
	}

	@Test
	public void test_tagValue_empty_invalid() throws DatastoreException, ValidationException
	{
		try
		{
			m_command.execute(new FakeChannel(), new String[]{"telnet", "metricName", "12345678999", "789", "foo=bar", "fum="});
			fail("ValidationException expected");
		}
		catch (DatastoreException e)
		{
			fail("ValidationException expected");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("tag[1] must be in the format 'name=value'."));
		}
	}

	@Test
	public void test_tagValue_characters_validColonTagValue() throws DatastoreException, ValidationException
	{
		m_command.execute(new FakeChannel(), new String[]{"telnet", "metricName", "12345678999", "789", "foo=bar", "fum=bar:fum"});
	}

	@Test
	public void test_tag_invalid() throws DatastoreException, ValidationException
	{
		try
		{
			m_command.execute(new FakeChannel(), new String[]{"telnet", "metricName", "12345678999", "789", "foo=bar", "fum-barfum"});
			fail("ValidationException expected");
		}
		catch (DatastoreException e)
		{
			fail("ValidationException expected");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("tag[1] must be in the format 'name=value'."));
		}
	}

	public static class FakeChannel implements Channel
	{
		@Override
		public Integer getId()
		{
			return null;
		}

		@Override
		public ChannelFactory getFactory()
		{
			return null;
		}

		@Override
		public Channel getParent()
		{
			return null;
		}

		@Override
		public ChannelConfig getConfig()
		{
			return null;
		}

		@Override
		public ChannelPipeline getPipeline()
		{
			return null;
		}

		@Override
		public boolean isOpen()
		{
			return false;
		}

		@Override
		public boolean isBound()
		{
			return false;
		}

		@Override
		public boolean isConnected()
		{
			return false;
		}

		@Override
		public SocketAddress getLocalAddress()
		{
			return null;
		}

		@Override
		public SocketAddress getRemoteAddress()
		{
			return null;
		}

		@Override
		public ChannelFuture write(Object o)
		{
			return null;
		}

		@Override
		public ChannelFuture write(Object o, SocketAddress socketAddress)
		{
			return null;
		}

		@Override
		public ChannelFuture bind(SocketAddress socketAddress)
		{
			return null;
		}

		@Override
		public ChannelFuture connect(SocketAddress socketAddress)
		{
			return null;
		}

		@Override
		public ChannelFuture disconnect()
		{
			return null;
		}

		@Override
		public ChannelFuture unbind()
		{
			return null;
		}

		@Override
		public ChannelFuture close()
		{
			return null;
		}

		@Override
		public ChannelFuture getCloseFuture()
		{
			return null;
		}

		@Override
		public int getInterestOps()
		{
			return 0;
		}

		@Override
		public boolean isReadable()
		{
			return false;
		}

		@Override
		public boolean isWritable()
		{
			return false;
		}

		@Override
		public ChannelFuture setInterestOps(int i)
		{
			return null;
		}

		@Override
		public ChannelFuture setReadable(boolean b)
		{
			return null;
		}

		@Override
		public boolean getUserDefinedWritability(int index)
		{
			return false;
		}

		@Override
		public void setUserDefinedWritability(int index, boolean isWritable)
		{

		}

		@Override
		public Object getAttachment()
		{
			return null;
		}

		@Override
		public void setAttachment(Object o)
		{
		}

		@Override
		public int compareTo(@Nullable Channel o)
		{
			return 0;
		}
	}

	private static class FakeDatastore implements Datastore
	{
		private DataPointSet set;

		private DataPointSet getSet()
		{
			return set;
		}

		@Override
		public void close() throws InterruptedException, DatastoreException
		{
		}

		@Subscribe
		public void putDataPoint(DataPointEvent event) throws DatastoreException
		{
			if (set == null)
				set = new DataPointSet(event.getMetricName(), event.getTags(), Collections.<DataPoint>emptyList());

			set.addDataPoint(event.getDataPoint());
		}

		/*@Override
		public void putDataPoints(DataPointSet dps) throws DatastoreException
		{
			this.set = dps;
		}*/

		@Override
		public Iterable<String> getMetricNames() throws DatastoreException
		{
			return null;
		}

		@Override
		public Iterable<String> getTagNames() throws DatastoreException
		{
			return null;
		}

		@Override
		public Iterable<String> getTagValues() throws DatastoreException
		{
			return null;
		}

		@Override
		public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
		{
		}

		@Override
		public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
		{
		}

		@Override
		public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
		{
			return null;
		}
	}
}