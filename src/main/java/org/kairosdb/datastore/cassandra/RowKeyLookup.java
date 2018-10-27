package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.Statement;
import com.google.common.collect.SetMultimap;

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
	 * Create the statements to delete references to the given row key.
	 */
	List<Statement> createDeleteStatements(DataPointsRowKey rowKey);

	/**
	 * Create the statements to retrieve all row key entries for the given metric name, timestamp, and tags.
	 */
	List<Statement> createQueryStatements(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags);
}
