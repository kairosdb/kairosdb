package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Wrapper around a ResultSet query on the tag_indexed_row_keys table that uses a probabilistic algorithm to estimate
 * the number of rows that will be included in the underlying ResultSet.
 * <p>
 * The tag_indexed_row_keys table contains a tag_collection_hash field that contains a 32-bit Murmur3 hash of the
 * full set of tags related to the row key pointed to be the row. Rows returned from a query on the tag_indexed_row_keys
 * table are ordered by this field.
 * <p>
 * As the tag hash is uniformly distributed over the 32-bit integer space, we can estimate the cardinality of entries
 * by taking a sample of the difference in this hash value between ordered consecutive entries. Dividing the
 * total 32-bit space by the mean step between consecutive entries gives us an estimate of the total number of
 * entries that will be returned in the ResultSet.
 * <p>
 * If we encounter the end of the ResultSet while building our estimate, then of course we already know what the
 * total ResultSet size is and we're no longer dealing with an estimate.
 * <p>
 * When querying with a collection of tag name/value pairs, we create a ResultSet per tag pair. Tag pairs are the
 * same as an AND equality clause in a query, so we only need to iterate over the most selective ResultSet to get
 * the full results for the query.
 */
class RowCountEstimatingRowKeyResultSet implements ResultSet
{

	private static final Logger LOG = LoggerFactory.getLogger(RowCountEstimatingRowKeyResultSet.class);

	// The full range (i.e. 2**32) for a 32-bit integer
	private static final long INTEGER_RANGE = UnsignedInteger.MAX_VALUE.longValue();

	static final int TAG_COLLECTION_HASH_COLUMN_INDEX = 3;

	// We use a sample size of 50. This is based on the case where we 100k entries in a result set, which gives us
	// a mean step of 43k and a standard deviation that is also 43k. Using the 90% confidence interval from the
	// z distribution (1.645), this allows us to estimate the mean step between entries with a 90% confidence interval
	// and a margin of error of 10k (((1,645 × 43k)/10k)**2) =~ 50)
	// Precision is not important here, while speed is, so as long as we're within an order of magnitude then it's
	// more than sufficient.
	static final int SAMPLE_SIZE = 50;

	/**
	 * Static factory method for creation of new instances.
	 *
	 * This method takes a list of ResultSets. The given ResultSets must all have a selection based on a single
	 * tag name. There can be multiple ResultsSets because a single tag name may be filtered using multiple values
	 * (in which case it is interpreted as an OR clause between the given tag values).
	 */
	public static RowCountEstimatingRowKeyResultSet create(List<ResultSet> delegateResultSets)
	{

		List<Row> rowBuffer = new ArrayList<>(SAMPLE_SIZE * delegateResultSets.size());
		double estimatedRowCount = 0;
		int seenRowCount = 0;
		boolean isEstimated = false;
		for (ResultSet delegateResultSet : delegateResultSets)
		{
			Iterator<Row> resultSetItr = delegateResultSet.iterator();

			long currentValue = Integer.MIN_VALUE;
			long diffSum = 0;
			int rowCount = 0;
			while (resultSetItr.hasNext() && rowCount++ < SAMPLE_SIZE)
			{
				Row row = resultSetItr.next();
				rowBuffer.add(row);
				int tagCollectionHash = row.getInt(TAG_COLLECTION_HASH_COLUMN_INDEX);
				long diff = tagCollectionHash - currentValue;
				diffSum += diff;
				currentValue = tagCollectionHash;
			}

			double averageDiff = diffSum / (double) rowCount;
			seenRowCount += rowCount;
			estimatedRowCount += INTEGER_RANGE / averageDiff;
			isEstimated = isEstimated || rowCount > SAMPLE_SIZE;
		}

		int resultSetRowCount = isEstimated ? (int) estimatedRowCount : seenRowCount;
		LOG.debug("Creating instance with isEstimated = {} and row count = {}", isEstimated, resultSetRowCount);
		return new RowCountEstimatingRowKeyResultSet(
				rowBuffer.iterator(),
				delegateResultSets,
				isEstimated,
				resultSetRowCount);
	}


	private final List<ResultSet> delegateResultSets;
	private final boolean estimated;
	private final int rowCount;
	private final Iterator<Row> rowIterator;

	private RowCountEstimatingRowKeyResultSet(Iterator<Row> buffer, List<ResultSet> delegateResultSets, boolean estimated, int rowCount)
	{
		this.delegateResultSets = delegateResultSets;
		this.estimated = estimated;
		this.rowCount = rowCount;
		List<Iterator<Row>> rowIterators = new ArrayList<>(delegateResultSets.size() + 1);
		rowIterators.add(buffer);
		for (ResultSet delegateResultSet : delegateResultSets)
		{
			rowIterators.add(delegateResultSet.iterator());
		}
		this.rowIterator = Iterators.concat(rowIterators.iterator());
	}

	/**
	 * Determine if the row count returned by {@link #getRowCount()} is an estimate or not.
	 */
	public boolean isEstimated()
	{
		return estimated;
	}

	/**
	 * Return the (possibly estimated) number of rows for this ResultSet.
	 *
	 * @return number of rows in this ResultSet (possibly estimated)
	 * @see #isEstimated()
	 */
	public int getRowCount()
	{
		return rowCount;
	}


	@Override
	public Row one()
	{
		return rowIterator.hasNext() ? rowIterator.next() : null;
	}

	@Override
	public ColumnDefinitions getColumnDefinitions()
	{
		// All the ResultSets are based on the same query
		return delegateResultSets.get(0).getColumnDefinitions();
	}

	@Override
	public boolean wasApplied()
	{
		// TODO If we want to use this as a real ResultSet, this will need to get implemented
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public boolean isExhausted()
	{
		return !rowIterator.hasNext();
	}

	@Override
	public boolean isFullyFetched()
	{
		// TODO If we want to use this as a real ResultSet, this will need to get implemented
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public int getAvailableWithoutFetching()
	{
		// TODO If we want to use this as a real ResultSet, this will need to get implemented
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public ListenableFuture<ResultSet> fetchMoreResults()
	{
		// TODO If we want to use this as a real ResultSet, this will need to get implemented
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public List<Row> all()
	{
		return ImmutableList.copyOf(rowIterator);
	}

	@Override
	public Iterator<Row> iterator()
	{
		return rowIterator;
	}

	@Override
	public ExecutionInfo getExecutionInfo()
	{
		// TODO If we want to use this as a real ResultSet, this will need to get implemented
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public List<ExecutionInfo> getAllExecutionInfo()
	{
		// TODO If we want to use this as a real ResultSet, this will need to get implemented
		throw new UnsupportedOperationException("Not yet implemented");
	}


}
