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
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import org.jboss.netty.channel.Channel;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.KairosMetricRegistry;
import org.kairosdb.util.Util;

import static org.kairosdb.core.reporting.KairosMetricRegistry.Tag;

public class PutCommand implements TelnetCommand
{
	private Datastore m_datastore;
	private Counter m_counter;

	@Inject
	public PutCommand(Datastore datastore, KairosMetricRegistry metricRegistry)
	{
		m_datastore = datastore;
		m_counter = metricRegistry.newCounter(new MetricName("kairosdb", "protocol", "telnet_request_count"),
				new Tag("host", "server"), new Tag("method", "put"));
	}

	@Override
	public void execute(Channel chan, String[] command) throws DatastoreException
	{
		m_counter.inc();
		/*for (String cmd : command)
			System.out.print(cmd + " ");
		System.out.println();*/

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
}
