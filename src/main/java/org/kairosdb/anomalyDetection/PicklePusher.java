//
//  PicklePusher.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import net.razorvine.pickle.Pickler;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class PicklePusher
{
	public static final Logger logger = LoggerFactory.getLogger(PicklePusher.class);

	private static final String SKYLINE_SERVER = "10.92.2.75";
//	private static final String SKYLINE_SERVER = "10.92.2.54";
	private static final int PORT = 2024;
//	private static final int PORT = 2004;

//	private TCPClient client;

	public PicklePusher()
	{
//		client = new TCPClient(SKYLINE_SERVER, PORT);
	}

//	public void pushDataPoints(DataPointSet dataPointSet)
//	{
//		client.sendData(dataPointSet);
//
//	}

	public void pushDataPoints(DataPointSet dataPointSet)
	{
		System.out.println("Forwarding " + dataPointSet.getName());

		try
		{
			Socket socket = new Socket(SKYLINE_SERVER, PORT);

			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			String metric = dataPointSet.getName();
			Pickler pickler = new Pickler();

			for (DataPoint dataPoint : dataPointSet.getDataPoints())
			{
				Object[] data = new Object[] { metric, new Object[] {
						(dataPoint.getTimestamp() / 1000), dataPoint.getLongValue() } };

				List objList = new ArrayList<Object>();
				objList.add(data);

				byte[] byteData = pickler.dumps(objList);

				out.writeInt(byteData.length);
				out.write(byteData);
			}

			out.flush();
			out.close();

			socket.close();

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

//	private class TCPClient
//	{
//		private String server;
//		private int port;
//
//		private TCPClient(String server, int port)
//		{
//			this.server = server;
//			this.port = port;
//		}
//
//		private String toPickle(DataPointSet dataPointSet)
//		{
//			StringBuilder builder = new StringBuilder();
//			builder.append('[');
//
//			boolean first = true;
//			for (DataPoint dataPoint : dataPointSet.getDataPoints())
//			{
//				if (!first)
//				{
//					builder.append(", ");
//				}
//				builder.append('(');
//				builder.append(dataPointSet.getName());
//				builder.append(", (");
//				builder.append(dataPoint.getTimestamp() / 1000);
//				builder.append(", ");
//				if (dataPoint.isInteger())
//					builder.append(dataPoint.getLongValue());
//				else
//					builder.append(dataPoint.getDoubleValue());
//				builder.append("))");
//
//				first = false;
//			}
//			builder.append(']');
//
//			return builder.toString();
//		}

}


