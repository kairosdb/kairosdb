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
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.metrics4j.MetricSourceManager;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;

public class VersionCommand implements TelnetCommand
{
	private static final TelnetStats stats = MetricSourceManager.getSource(TelnetStats.class);

	private AtomicInteger m_counter = new AtomicInteger();

	@Inject
	public VersionCommand()
	{
	}

	@Override
	public void execute(Channel chan, List<String> command) throws DatastoreException
	{
		stats.request(getCommand()).put(1);
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

}
