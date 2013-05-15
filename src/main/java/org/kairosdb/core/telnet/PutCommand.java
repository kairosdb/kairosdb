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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.channel.Channel;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.util.Util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class PutCommand implements TelnetCommand, KairosMetricReporter
{
	private KairosDatastore m_datastore;
	private AtomicInteger m_counter = new AtomicInteger();
	private String m_hostName;

	@Inject
	public PutCommand(KairosDatastore datastore, @Named("HOSTNAME") String hostname)
	{
		checkNotNullOrEmpty(hostname);
		m_hostName = hostname;
		m_datastore = datastore;
	}

	@Override
	public void execute(Channel chan, String[] command) throws DatastoreException
	{
		m_counter.incrementAndGet();

		DataPointSet dps = new DataPointSet(command[1]);

		long timestamp = Util.parseLong(command[2]);
		//Backwards compatible hack for the next 30 years
		//This allows clients to send seconds to us
		if (timestamp < 3000000000L)
			timestamp *= 1000;

		DataPoint dp;
		if (command[3].contains("."))
			dp = new DataPoint(timestamp, Double.parseDouble(command[3]));
		else
			dp = new DataPoint(timestamp, Util.parseLong(command[3]));

		dps.addDataPoint(dp);

		for (int i = 4; i < command.length; i++)
		{
			String[] tag = command[i].split("=");
			dps.addTag(tag[0], tag[1]);
		}

		m_datastore.putDataPoints(dps);
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		DataPointSet dps = new DataPointSet(REPORTING_METRIC_NAME);
		dps.addTag("host", m_hostName);
		dps.addTag("method", "put");
		dps.addDataPoint(new DataPoint(now, m_counter.getAndSet(0)));

		return (Collections.singletonList(dps));
	}
}
