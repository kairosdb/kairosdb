package org.kairosdb.testing;

import java.io.IOException;
import java.net.ServerSocket;

public class TestUtil
{
	private TestUtil()
	{
	}

	public static int findFreePort() throws IOException
	{
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}
}
