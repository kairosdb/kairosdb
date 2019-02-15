package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class RowCountEstimatingRowKeyResultSetTest
{

	@Test
	public void testCreate_NumberOfRowsLessThanSampleSize()
	{
		ResultSet delegateResultSet = singleEntryResultSet();

		RowCountEstimatingRowKeyResultSet rowCountResultSet = RowCountEstimatingRowKeyResultSet.create(ImmutableList.of(delegateResultSet));

		assertFalse(rowCountResultSet.isEstimated());
		assertEquals(1, rowCountResultSet.getRowCount());
	}

	@Test
	public void testCreate_NumberOfRowsLargerThanSampleSize()
	{
		ResultSet delegateResultSet = sampleSizePlusOneResultSet();

		RowCountEstimatingRowKeyResultSet rowCountResultSet = RowCountEstimatingRowKeyResultSet.create(ImmutableList.of(delegateResultSet));

		assertTrue(rowCountResultSet.isEstimated());
		// The 2**32 keyspace divided up with steps of 1000 works out to a total sample size of ~4470272
		assertEquals(4470272, rowCountResultSet.getRowCount());
	}

	@Test
	public void testCreate_MultipleDelegateResultSets()
	{
		ResultSet delegateResultSetA = resultSet(row(0));
		ResultSet delegateResultSetB = resultSet(row(1));
		ResultSet delegateResultSetC = resultSet(row(0));

		RowCountEstimatingRowKeyResultSet rowCountResultSet =
				RowCountEstimatingRowKeyResultSet.create(
						ImmutableList.of(delegateResultSetA, delegateResultSetB, delegateResultSetC));

		assertFalse(rowCountResultSet.isEstimated());
		assertEquals(3, rowCountResultSet.getRowCount());

		assertEquals(3, Iterators.size(rowCountResultSet.iterator()));
	}

	@Test
	public void testOne_FullContentsInBuffer()
	{
		ResultSet resultSet = singleEntryResultSet();
		assertFalse(resultSet.isExhausted());
		Row firstRow = resultSet.one();
		assertEquals(0, firstRow.getInt(3));
		assertNull(resultSet.one());
		assertTrue(resultSet.isExhausted());
	}

	@Test
	public void testOne_PartialContentsInBuffer()
	{
		ResultSet resultSet = sampleSizePlusOneResultSet();
		for (int i = 0; i < RowCountEstimatingRowKeyResultSet.SAMPLE_SIZE + 1; i++)
		{
			assertFalse(resultSet.isExhausted());
			assertNotNull(resultSet.one());
		}
		assertTrue(resultSet.isExhausted());
		assertNull(resultSet.one());
	}

	@Test
	public void testIterator_FullContentsInBuffer()
	{
		assertEquals(
				1,
				Iterators.size(singleEntryResultSet().iterator()));
	}

	@Test
	public void testIterator_PartialContentsInBuffer()
	{
		assertEquals(
				RowCountEstimatingRowKeyResultSet.SAMPLE_SIZE + 1,
				Iterators.size(sampleSizePlusOneResultSet().iterator()));
	}

	private ResultSet sampleSizePlusOneResultSet()
	{
		return resultSet(IntStream.iterate(Integer.MIN_VALUE, i -> i + 1000).limit(RowCountEstimatingRowKeyResultSet.SAMPLE_SIZE + 1)
				.boxed().map(tagCollectionHash -> row(tagCollectionHash))
				.collect(Collectors.toList()).iterator());
	}

	private ResultSet singleEntryResultSet()
	{
		return resultSet(row(0));
	}

	private static Row row(int tagCollectionHashValue)
	{
		Row row = Mockito.mock(Row.class);
		when(row.getInt(RowCountEstimatingRowKeyResultSet.TAG_COLLECTION_HASH_COLUMN_INDEX)).thenReturn(tagCollectionHashValue);
		return row;
	}

	private static ResultSet resultSet(Row... rows)
	{
		return resultSet(Arrays.asList(rows).iterator());
	}

	private static ResultSet resultSet(Iterator<Row> rowItr)
	{
		ResultSet resultSet = Mockito.mock(ResultSet.class);
		when(resultSet.iterator()).thenReturn(rowItr);
		when(resultSet.one()).then((Answer<Row>) invocationOnMock -> rowItr.hasNext() ? rowItr.next() : null);
		when(resultSet.isExhausted()).then((Answer<Boolean>) invocationMock -> !rowItr.hasNext());
		return resultSet;
	}
}