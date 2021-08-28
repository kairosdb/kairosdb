package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

/**
 * Interface for creating statements for CRUD operations for the row_keys or tag_indexed_row_keys tables.
 */
interface RowKeyLookup
{
	/**
	 * Create the statements to add new entries for the given row key.
	 */
	List<Statement> createInsertStatements(DataPointsRowKey rowKey, int rowKeyTtl);

	/**
	 * Create the statements to index references to the given row key.
	 */
	default List<Statement> createIndexStatements(DataPointsRowKey rowKey, int rowKeyTtl) {
		return new ArrayList<>();
	}

	/**
	 * Create the statements to delete references to the given row key.
	 */
	List<Statement> createDeleteStatements(DataPointsRowKey rowKey);

	ListenableFuture<ResultSet> queryRowKeys(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags);


	/**
	 * Provides Statements for querying row keys for a given metric, timestamp, and tag filter, and a processor
	 * to convert the returned list of ResultSet futures into a single ResultSet future.
	 *
	 * Applying this processor as a function converts a list of ResultSets as returned from executing the Statements
	 * from {@link #getQueryStatements()} into a single ResultSet. Note that the List of ResultSets passed into
	 * the {@link #apply} method should be passed in in the same order as the Statements used to create them from
	 * the {@link #getQueryStatements()} method.
	 */
	interface RowKeyResultSetProcessor extends Function<List<ResultSet>, ResultSet>
	{

		/**
		 * Returns the statements to retrieve all row key entries for the given metric name, timestamp, and tags.
		 */
		List<Statement> getQueryStatements();

	}
}
