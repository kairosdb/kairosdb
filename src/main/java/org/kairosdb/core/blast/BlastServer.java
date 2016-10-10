package org.kairosdb.core.blast;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.events.DataPointEvent;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 Created by bhawkins on 5/16/14.
 */
public class BlastServer implements KairosDBService, Runnable
{
	private Thread m_serverThread;
	private final EventBus m_evenBus;
	private final LongDataPointFactory m_longDataPointFactory;
	private boolean m_keepRunning = true;
	private ServerSocket m_serverSocket;

	@Inject
	public BlastServer(EventBus evenBus, LongDataPointFactory longDataPointFactory)
	{
		m_evenBus = evenBus;
		m_longDataPointFactory = longDataPointFactory;
	}

	@Override
	public void start() throws KairosDBException
	{
		Thread t = new Thread(this);
		t.start();
	}

	@Override
	public void stop()
	{
		m_keepRunning = false;
		if (m_serverSocket != null)
		{
			try
			{
				m_serverSocket.close();
			}
			catch (IOException e)
			{
			}
		}
	}


	@Override
	public void run()
	{
		try
		{
			m_serverSocket = new ServerSocket(7777);

			while (m_keepRunning)
			{
				try
				{
					Socket socket = m_serverSocket.accept();

					DataInputStream reader = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

					for (; ; )
					{
						long value = reader.readLong();
						String metric = reader.readUTF();
						String host = reader.readUTF();

						DataPoint dp = m_longDataPointFactory.createDataPoint(System.currentTimeMillis(), value);

						m_evenBus.post(new DataPointEvent(metric, ImmutableSortedMap.of("host", host), dp, 0));
					}
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}

			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

	}
}
