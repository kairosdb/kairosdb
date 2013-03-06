// KairosDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>

package org.kairosdb.core.telnet;

import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.Util;
import org.jboss.netty.channel.Channel;

public class PutCommand implements TelnetCommand
{
	private Datastore m_datastore;

	@Inject
	public PutCommand(Datastore datastore)
	{
		m_datastore = datastore;
	}

	@Override
	public void execute(Channel chan, String[] command) throws DatastoreException
	{
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
