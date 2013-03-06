package org.kairosdb.core.telnet;

import org.kairosdb.core.exception.DatastoreException;
import org.jboss.netty.channel.Channel;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 2/8/13
 Time: 3:45 PM
 To change this template use File | Settings | File Templates.
 */
public class VersionCommand implements TelnetCommand
{
	@Override
	public void execute(Channel chan, String[] command) throws DatastoreException
	{
		if (chan.isConnected())
		{
			//TODO fix this to return a real version
			chan.write("KairosDB2\nAlpha\n");
		}
	}
}
