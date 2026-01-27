package org.kairosdb.datastore.cassandra;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

public class AsyncResultCollector
{
	//This is to make sure the callback is only called by a single thread at a time.
	private final Object m_callbackLock = new Object();
	//This is to lock access to the futures array in case of an exception
	private final Object m_collectorLock = new Object();
	private Throwable m_thrownException = null;
	//Used to make sure join is called before checking for exception
	private volatile boolean m_finished = false;
	//Used to make sure no more CompletionStages are added after someone calls join
	private volatile boolean m_stopAdding = false;
	private final List<CompletableFuture<AsyncResultSet>> m_futures = new ArrayList<>();
	public interface RowProcessor
	{
		void processRow(Row row);
	}
	public interface PageProcessor
	{
		void processPage(Iterable<Row> page);
	}
	public void addResultSet(CompletionStage<AsyncResultSet> completionStage, RowProcessor rowProcessor)
	{
		synchronized (m_collectorLock) {
			if (m_stopAdding)
				throw new RuntimeException("Unable to add CompletionStage after join has been called");

			if (m_thrownException == null) {
				m_futures.add(completionStage.toCompletableFuture());
				completionStage.thenCompose(rs -> processRowResults(rs, rowProcessor)).exceptionally(this::exception);
			}
			else {
				completionStage.toCompletableFuture().cancel(true);
			}
		}
	}

	public CompletionStage<Void> addResultSetPage(CompletionStage<AsyncResultSet> completionStage, PageProcessor pageProcessor, Executor executor)
	{
		synchronized (m_collectorLock) {
			if (m_stopAdding)
				throw new RuntimeException("Unable to add CompletionStage after join has been called");

			if (m_thrownException == null) {
				m_futures.add(completionStage.toCompletableFuture());
				return completionStage.thenComposeAsync(rs -> processPageResults(rs, pageProcessor), executor).exceptionally(this::exception);
			}
			else {
				completionStage.toCompletableFuture().cancel(true);
				return CompletableFuture.completedFuture(null);
			}
		}
	}

	private Void exception(Throwable throwable)
	{
		synchronized (m_collectorLock) {
			m_thrownException = throwable;
			//Cancel all other completion stages
			for (CompletableFuture<AsyncResultSet> future : m_futures) {
				future.cancel(true);
			}
		}
		return null;
	}

	/**
	 * Waits for all completion stages to finish
	 */
	public void join()
	{
		synchronized (m_collectorLock) {
			m_stopAdding = true;
		}
		for (CompletableFuture<AsyncResultSet> future : m_futures) {
			future.join();
		}
		m_finished = true;
	}

	public boolean hasThrownException()
	{
		if (!m_finished)
			throw new RuntimeException("You must call join before checking for exceptions on collector");

		return m_thrownException != null;
	}

	public Throwable getThrownException()
	{
		if (!m_finished)
			throw new RuntimeException("You must call join before checking for exceptions on collector");

		return m_thrownException;
	}

	public List<CompletableFuture<AsyncResultSet>> getFutures()
	{
		return m_futures;
	}

	/*
	used to process an entire page of results
	 */
	private CompletionStage<Void> processPageResults(AsyncResultSet resultSet, PageProcessor pageProcessor)
	{
		Iterable<Row> rows = resultSet.currentPage();
		synchronized (m_callbackLock)
		{
			pageProcessor.processPage(rows);
		}
		if (resultSet.hasMorePages())
			return resultSet.fetchNextPage().thenCompose(rs -> processPageResults(rs, pageProcessor));
		else
			return CompletableFuture.completedFuture(null);
	}

	/*
	Used to process one row at a time
	 */
	private CompletionStage<Void> processRowResults(AsyncResultSet resultSet, RowProcessor rowProcessor)
	{
		synchronized (m_callbackLock) {
			for (Row record : resultSet.currentPage()) {
				rowProcessor.processRow(record);
			}
		}
		if (resultSet.hasMorePages())
			return resultSet.fetchNextPage().thenCompose(rs -> processRowResults(rs, rowProcessor));
		else
			return CompletableFuture.completedFuture(null);
	}
}
