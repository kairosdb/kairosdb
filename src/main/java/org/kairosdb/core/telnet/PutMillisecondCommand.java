/*
 * Copyright 2013 Proofpoint Inc.
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

import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.channel.Channel;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.util.Tags;
import org.kairosdb.util.Util;
import org.kairosdb.util.ValidationException;
import org.kairosdb.util.Validator;

import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class PutMillisecondCommand implements TelnetCommand, KairosMetricReporter
{
	private KairosDatastore m_datastore;
	private AtomicInteger m_counter = new AtomicInteger();
	private String m_hostName;
	private LongDataPointFactory m_longFactory;
	private DoubleDataPointFactory m_doubleFactory;

	@Inject
	public PutMillisecondCommand(KairosDatastore datastore, @Named("HOSTNAME") String hostname,
			LongDataPointFactory longFactory, DoubleDataPointFactory doubleFactory)
	{
		checkNotNullOrEmpty(hostname);
		m_hostName = hostname;
		m_datastore = datastore;
		m_longFactory = longFactory;
		m_doubleFactory = doubleFactory;
	}

	@Override
	public void execute(Channel chan, String[] command) throws DatastoreException, ValidationException
	{
		long timestamp = Util.parseLong(command[2]);
		execute(command, timestamp);
	}

	protected void execute(String[] command, long timestamp) throws ValidationException, DatastoreException
	{
		Validator.validateNotNullOrEmpty("metricName", command[1]);

		String metricName = command[1];


		DataPoint dp;
		try
		{
			if (command[3].contains("."))
				dp = m_doubleFactory.createDataPoint(timestamp, Double.parseDouble(command[3]));
			else
				dp = m_longFactory.createDataPoint(timestamp, Util.parseLong(command[3]));
		}
		catch (NumberFormatException e)
		{
			throw new ValidationException(e.getMessage());
		}

		ImmutableSortedMap.Builder<String, String> tags = Tags.create();

		int tagCount = 0;
		for (int i = 4; i < command.length; i++)
		{
			String[] tag = command[i].split("=");
			validateTag(tagCount, tag);

			tags.put(tag[0], tag[1]);
			tagCount++;
		}

		if (tagCount == 0)
			tags.put("add", "tag");

		m_counter.incrementAndGet();
		m_datastore.putDataPoint(metricName, tags.build(), dp);
	}

	private void validateTag(int tagCount, String[] tag) throws ValidationException
	{
		if (tag.length < 2)
			throw new ValidationException(String.format("tag[%d] must be in the format 'name=value'.", tagCount));

		Validator.validateNotNullOrEmpty(String.format("tag[%d].name", tagCount), tag[0]);
		Validator.validateCharacterSet(String.format("tag[%d].name", tagCount), tag[0]);

		Validator.validateNotNullOrEmpty(String.format("tag[%d].value", tagCount), tag[1]);
		Validator.validateCharacterSet(String.format("tag[%d].value", tagCount), tag[1]);
	}

	@Override
	public String getCommand()
	{
		return ("putm");
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		DataPointSet dps = new DataPointSet(REPORTING_METRIC_NAME);
		dps.addTag("host", m_hostName);
		dps.addTag("method", "putm");
		dps.addDataPoint(m_longFactory.createDataPoint(now, m_counter.getAndSet(0)));

		return (Collections.singletonList(dps));
	}
}
