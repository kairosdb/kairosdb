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
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.KairosMetricRegistry;

public class VersionCommand implements TelnetCommand
{
	private Counter counter;

	@Inject
	public VersionCommand(KairosMetricRegistry metricRegistry)
	{
		counter = metricRegistry.newCounter(new MetricName("kairosdb", "protocol", "telnet_request_count"),
				new KairosMetricRegistry.Tag("host", "server"), new KairosMetricRegistry.Tag("method", "version"));
	}

	@Override
	public void execute(Channel chan, String[] command) throws DatastoreException
	{
		counter.inc();
		if (chan.isConnected())
		{
			//TODO fix this to return a real version
			chan.write("KairosDB2\nBeta1\n");
		}
	}
}
