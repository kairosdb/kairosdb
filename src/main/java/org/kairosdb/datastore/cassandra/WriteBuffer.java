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
package org.kairosdb.datastore.cassandra;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.mutation.Mutator;
import org.kairosdb.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


public class WriteBuffer<RowKeyType, ColumnKeyType, ValueType>  implements Runnable
{
	public static final Logger logger = LoggerFactory.getLogger(WriteBuffer.class);

	private Keyspace m_keyspace;
	private String m_cfName;
	private List<Triple<RowKeyType, ColumnKeyType, ValueType>> m_buffer;
	private Mutator<RowKeyType> m_mutator;
	private volatile int m_bufferCount = 0;
	private ReentrantLock m_mutatorLock;

	private Thread m_writeThread;
	private boolean m_exit = false;
	private int m_writeDelay;
	private Serializer<RowKeyType> m_rowKeySerializer;
	private Serializer<ColumnKeyType> m_columnKeySerializer;
	private Serializer<ValueType> m_valueSerializer;
	private WriteBufferStats m_writeStats;
	private int m_maxBufferSize;
	private int m_initialMaxBufferSize;
	private ExecutorService m_executorService;

	public WriteBuffer(Keyspace keyspace, String cfName,
			int writeDelay, int maxWriteSize, Serializer<RowKeyType> keySerializer,
			Serializer<ColumnKeyType> columnKeySerializer,
			Serializer<ValueType> valueSerializer,
			WriteBufferStats stats,
			ReentrantLock mutatorLock,
			int threadCount)
	{
		m_executorService = Executors.newFixedThreadPool(threadCount,
				new ThreadFactoryBuilder().setNameFormat("WriteBuffer-"+cfName+"-%d").build());

		m_keyspace = keyspace;
		m_cfName = cfName;
		m_writeDelay = writeDelay;
		m_initialMaxBufferSize = m_maxBufferSize = maxWriteSize;
		m_rowKeySerializer = keySerializer;
		m_columnKeySerializer = columnKeySerializer;
		m_valueSerializer = valueSerializer;
		m_writeStats = stats;
		m_mutatorLock = mutatorLock;

		m_buffer = new ArrayList<>();
		m_mutator = new MutatorImpl<>(keyspace, keySerializer);
		m_writeThread = new Thread(this, "WriteBuffer Scheduler for "+cfName);
		m_writeThread.start();
	}
	
	/**
	 * Add a datapoint without a TTL. 
	 * This datapoint will never be automatically deleted
	 */
	public void addData(
		RowKeyType rowKey, 
		ColumnKeyType columnKey, 
		ValueType value, 
		long timestamp)
	{
		addData(rowKey, columnKey, value, timestamp, 0);
	}

	/**
	 * Add a datapoint with a TTL.
	 * This datapoint will be removed after ttl seconds
	 */
	public void addData(
			RowKeyType rowKey,
			ColumnKeyType columnKey,
			ValueType value,
			long timestamp,
			int ttl)
	{
		m_mutatorLock.lock();
		try
		{
			waitOnBufferFull();
			m_bufferCount ++;

			if (columnKey.toString().length() > 0)
			{
				m_buffer.add(new Triple<>(rowKey, columnKey, value, timestamp, ttl));
			}
			else
			{
				logger.info("Discarded " + m_cfName + " row with empty column name. This should never happen.");
			}
		}
		finally
		{
			m_mutatorLock.unlock();
		}
	}

	public void deleteRow(RowKeyType rowKey, long timestamp)
	{
		m_mutatorLock.lock();
		try
		{
			waitOnBufferFull();

			m_bufferCount ++;
			m_mutator.addDeletion(rowKey, m_cfName, timestamp);
		}
		finally
		{
			m_mutatorLock.unlock();
		}
	}

	public void deleteColumn(RowKeyType rowKey, ColumnKeyType columnKey, long timestamp)
	{
		m_mutatorLock.lock();
		try
		{
			waitOnBufferFull();

			m_bufferCount ++;
			m_mutator.addDeletion(rowKey, m_cfName, columnKey, m_columnKeySerializer, timestamp);
		}
		finally
		{
			m_mutatorLock.unlock();
		}
	}

	private void waitOnBufferFull()
	{
		if ((m_bufferCount > m_maxBufferSize) && (m_mutatorLock.getHoldCount() == 1))
		{
			submitJob();
		}
	}

	public void close() throws InterruptedException
	{
		m_exit = true;
		m_writeThread.interrupt();
		m_writeThread.join();
		m_executorService.shutdown();
		m_executorService.awaitTermination(1, TimeUnit.MINUTES);
	}

	/**
	 This will slowly increase the max buffer size up to the initial size.
	 The design is that this method is called periodically to correct 3/4
	 throttling that occurs down below.
	 */
	public void increaseMaxBufferSize()
	{
		if (m_maxBufferSize < m_initialMaxBufferSize)
		{
			m_maxBufferSize += 1000;
			logger.info("Increasing write buffer " + m_cfName + " size to "+m_maxBufferSize);
		}
	}

	private void submitJob()
	{
		Mutator<RowKeyType> pendingMutations;
		List<Triple<RowKeyType, ColumnKeyType, ValueType>> buffer;

		m_writeStats.saveWriteSize(m_bufferCount);

		pendingMutations = m_mutator;
		buffer = m_buffer;
		m_mutator = new MutatorImpl<>(m_keyspace, m_rowKeySerializer);
		m_buffer = new ArrayList<>();
		m_bufferCount = 0;

		WriteDataJob writeDataJob = new WriteDataJob(pendingMutations, buffer);
		//submit job
		m_executorService.submit(writeDataJob);
		writeDataJob.waitTillStarted();
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
			catch (InterruptedException ignored) {}

			if (m_bufferCount != 0)
			{
				m_mutatorLock.lock();
				try
				{
					submitJob();
				}
				finally
				{
					m_mutatorLock.unlock();
				}
			}
		}
	}

	private class WriteDataJob implements Runnable
	{
		private final Object m_jobLock = new Object();
		private boolean m_started = false;
		private Mutator<RowKeyType> m_pendingMutations;
		private final List<Triple<RowKeyType, ColumnKeyType, ValueType>> m_buffer;

		public WriteDataJob(Mutator<RowKeyType> pendingMutations, List<Triple<RowKeyType, ColumnKeyType, ValueType>> buffer)
		{
			m_pendingMutations = pendingMutations;
			m_buffer = buffer;
		}

		public void waitTillStarted()
		{
			synchronized (m_jobLock)
			{
				while (!m_started)
				{
					try
					{
						m_jobLock.wait();
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
			}
		}

		@Override
		public void run()
		{
			synchronized (m_jobLock)
			{
				m_started = true;
				m_jobLock.notifyAll();
			}

			try
			{
				if (m_pendingMutations != null)
				{
					for (Triple<RowKeyType, ColumnKeyType, ValueType> data : m_buffer)
					{
						HColumnImpl<ColumnKeyType, ValueType> col =
								new HColumnImpl<>(data.getSecond(), data.getThird(), data.getTime(), m_columnKeySerializer, m_valueSerializer);

						//if a TTL is set apply it to the column. This will
						//cause it to be removed after this number of seconds
						if (data.getTtl() != 0)
						{
							col.setTtl(data.getTtl());
						}

						m_pendingMutations.addInsertion(
								data.getFirst(),
								m_cfName,
								col
						);
					}
					m_pendingMutations.execute();
				}

				m_pendingMutations = null;
			}
			catch (Exception e)
			{
				logger.error("Error sending data to Cassandra (" + m_cfName + ")", e);

				m_maxBufferSize = m_maxBufferSize * 3 / 4;

				logger.error("Reducing write buffer size to " + m_maxBufferSize +
						".  You need to increase your cassandra capacity or change the kairosdb.datastore.cassandra.write_buffer_max_size property.");
			}


			//If the batch failed we will retry it without changing the buffer size.
			while (m_pendingMutations != null)
			{
				try
				{
					Thread.sleep(100);
				}
				catch (InterruptedException ignored)
				{
				}

				try
				{
					m_pendingMutations.execute();
					m_pendingMutations = null;
				}
				catch (Exception e)
				{
					logger.error("Error resending data", e);
				}
			}
		}
	}
}
