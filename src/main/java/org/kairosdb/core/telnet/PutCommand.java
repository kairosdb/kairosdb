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

public class PutCommand extends PutMillisecondCommand
{
	@Inject
	public PutCommand(KairosDatastore datastore, @Named("HOSTNAME") String hostname,
			LongDataPointFactory longFactory, DoubleDataPointFactory doubleFactory)
	{
		super(datastore, hostname, longFactory, doubleFactory);
	}

	@Override
	public void execute(Channel chan, String[] command) throws DatastoreException, ValidationException
	{
		long timestamp = Util.parseLong(command[2]);
		//Backwards compatible hack for the next 30 years
		//This allows clients to send seconds to us
		if (timestamp < 3000000000L)
			timestamp *= 1000;

		execute(command, timestamp);
	}

	@Override
	public String getCommand()
	{
		return ("put");
	}

}
