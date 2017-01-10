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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.channel.Channel;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.KairosMetricReporter;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class VersionCommand implements TelnetCommand, KairosMetricReporter
{
	private AtomicInteger m_counter = new AtomicInteger();
	private final LongDataPointFactory m_dataPointFactory;
	private String m_hostName;

	@Inject
	public VersionCommand(@Named("HOSTNAME") String hostname, LongDataPointFactory factory)
	{
		checkNotNullOrEmpty(hostname);
		m_hostName = hostname;
		m_dataPointFactory = factory;
	}

	@Override
	public void execute(Channel chan, String[] command) throws DatastoreException
	{
		m_counter.incrementAndGet();
		if (chan.isConnected())
		{
			Package thisPackage = getClass().getPackage();
			String versionString = thisPackage.getImplementationTitle()+" "+thisPackage.getImplementationVersion();
			chan.write(versionString+"\n");
		}
	}

	@Override
	public String getCommand()
	{
		return ("version");
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		DataPointSet dps = new DataPointSet(REPORTING_METRIC_NAME);
		dps.addTag("host", m_hostName);
		dps.addTag("method", "version");
		dps.addDataPoint(m_dataPointFactory.createDataPoint(now, m_counter.getAndSet(0)));

		return (Collections.singletonList(dps));
	}
}
