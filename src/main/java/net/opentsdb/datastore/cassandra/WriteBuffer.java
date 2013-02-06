// OpenTSDB2
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
package net.opentsdb.datastore.cassandra;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.mutation.Mutator;


/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 1/16/13
 Time: 4:47 PM
 To change this template use File | Settings | File Templates.
 */
public class WriteBuffer<RowKeyType, ColumnKeyType, ValueType>  implements Runnable
{
	private Keyspace m_keyspace;
	private String m_cfName;
	private Mutator<RowKeyType> m_mutator;
	private final Object m_mutatorLock = new Object();

	private Thread m_writeThread;
	private boolean m_exit = false;
	private int m_writeDelay;
	private Serializer<RowKeyType> m_rowKeySerializer;
	private Serializer<ColumnKeyType> m_columnKeySerializer;
	private Serializer<ValueType> m_valueSerializer;
	private WriteBufferStats m_writeStats;

	public WriteBuffer(Keyspace keyspace, String cfName,
			int writeDelay, Serializer<RowKeyType> keySerializer,
			Serializer<ColumnKeyType> columnKeySerializer,
			Serializer<ValueType> valueSerializer,
			WriteBufferStats stats)
	{
		m_keyspace = keyspace;
		m_cfName = cfName;
		m_writeDelay = writeDelay;
		m_rowKeySerializer = keySerializer;
		m_columnKeySerializer = columnKeySerializer;
		m_valueSerializer = valueSerializer;
		m_writeStats = stats;

		m_mutator = new MutatorImpl<RowKeyType>(keyspace, keySerializer);
		m_writeThread = new Thread(this);
		m_writeThread.start();
	}

	public void addData(RowKeyType rowKey, ColumnKeyType columnKey, ValueType value,
			long timestamp)
	{
		synchronized (m_mutatorLock)
		{
			m_mutator.addInsertion(rowKey, m_cfName,
					new HColumnImpl<ColumnKeyType, ValueType>(columnKey, value,
							timestamp, m_columnKeySerializer, m_valueSerializer));
		}
	}


	public void close() throws InterruptedException
	{
		m_exit = true;
		m_writeThread.interrupt();
		m_writeThread.join();
	}


	@Override
	public void run()
	{
		while (!m_exit)
		{
			try
			{
				Thread.sleep(m_writeDelay);
			}
			catch (InterruptedException e)
			{
				// todo add logging here
			}

			Mutator<RowKeyType> pendingMutations = null;

			int pending = m_mutator.getPendingMutationCount();
			if (pending != 0)
			{
				m_writeStats.saveWriteSize(pending);

				synchronized (m_mutatorLock)
				{
					pendingMutations = m_mutator;
					m_mutator = new MutatorImpl<RowKeyType>(m_keyspace, m_rowKeySerializer);
				}
			}

			try
			{
				if (pendingMutations != null)
					pendingMutations.execute();
			}
			catch (Exception e)
			{
				//TODO:
				e.printStackTrace();
			}
		}
	}
}
