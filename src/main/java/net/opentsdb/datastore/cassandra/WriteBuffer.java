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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 1/16/13
 Time: 4:47 PM
 To change this template use File | Settings | File Templates.
 */
public class WriteBuffer<RowKeyType, ColumnKeyType, ValueType>  implements Runnable
{
	public static final Logger logger = LoggerFactory.getLogger(WriteBuffer.class);

	private Keyspace m_keyspace;
	private String m_cfName;
	private Mutator<RowKeyType> m_mutator;
	private volatile int m_bufferCount = 0;
	private ReentrantLock m_mutatorLock;
	private Condition m_lockCondition;

	private Thread m_writeThread;
	private boolean m_exit = false;
	private int m_writeDelay;
	private Serializer<RowKeyType> m_rowKeySerializer;
	private Serializer<ColumnKeyType> m_columnKeySerializer;
	private Serializer<ValueType> m_valueSerializer;
	private WriteBufferStats m_writeStats;
	private int m_maxBufferSize = 500000;

	public WriteBuffer(Keyspace keyspace, String cfName,
			int writeDelay, Serializer<RowKeyType> keySerializer,
			Serializer<ColumnKeyType> columnKeySerializer,
			Serializer<ValueType> valueSerializer,
			WriteBufferStats stats,
			ReentrantLock mutatorLock,
			Condition lockCondition)
	{
		m_keyspace = keyspace;
		m_cfName = cfName;
		m_writeDelay = writeDelay;
		m_rowKeySerializer = keySerializer;
		m_columnKeySerializer = columnKeySerializer;
		m_valueSerializer = valueSerializer;
		m_writeStats = stats;
		m_mutatorLock = mutatorLock;
		m_lockCondition = lockCondition;

		m_mutator = new MutatorImpl<RowKeyType>(keyspace, keySerializer);
		m_writeThread = new Thread(this);
		m_writeThread.start();
	}

	public void addData(RowKeyType rowKey, ColumnKeyType columnKey, ValueType value,
			long timestamp)
	{
		m_mutatorLock.lock();
		try
		{
			if ((m_bufferCount > m_maxBufferSize) && (m_mutatorLock.getHoldCount() == 1))
			{
				try
				{
					m_lockCondition.await();
				}
				catch (InterruptedException e) {}
			}

			m_bufferCount ++;
			m_mutator.addInsertion(rowKey, m_cfName,
					new HColumnImpl<ColumnKeyType, ValueType>(columnKey, value,
							timestamp, m_columnKeySerializer, m_valueSerializer));
		}
		finally
		{
			m_mutatorLock.unlock();
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
			catch (InterruptedException e) {}

			Mutator<RowKeyType> pendingMutations = null;

			if (m_bufferCount != 0)
			{
				m_mutatorLock.lock();
				try
				{
					m_writeStats.saveWriteSize(m_bufferCount);

					pendingMutations = m_mutator;
					m_mutator = new MutatorImpl<RowKeyType>(m_keyspace, m_rowKeySerializer);
					m_bufferCount = 0;
					m_lockCondition.signalAll();
				}
				finally
				{
					m_mutatorLock.unlock();
				}
			}

			try
			{
				if (pendingMutations != null)
					pendingMutations.execute();

				pendingMutations = null;
			}
			catch (Exception e)
			{
				logger.error("Error sending data to Cassandra", e);

				m_maxBufferSize = m_maxBufferSize * 3 / 4;

				logger.error("Reducing write buffer size to "+m_maxBufferSize);
			}


			//If the batch failed we will retry it without changing the buffer size.
			while (pendingMutations != null)
			{
				try
				{
					Thread.sleep(100);
				}
				catch (InterruptedException e){ }

				try
				{
					pendingMutations.execute();
					pendingMutations = null;
				}
				catch (Exception e)
				{
					logger.error("Error resending data", e);
				}
			}
		}
	}
}
