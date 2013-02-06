// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.datastore.hbase;

import net.opentsdb.core.datastore.CachedSearchResult;
import net.opentsdb.datastore.hbase.stats.Histogram;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import static org.hbase.async.Bytes.ByteMap;

/**
 * Non-synchronized implementation of {@link Query}.
 */
final class TsdbQuery implements Query {

	private static final Logger LOG = LoggerFactory.getLogger(TsdbQuery.class);

	/**
	 * Keep track of the latency we perceive when doing Scans on HBase.
	 * We want buckets up to 16s, with 2 ms interval between each bucket up to
	 * 100 ms after we which we switch to exponential buckets.
	 */
	static final Histogram scanlatency = new Histogram(16000, (short) 2, 100);

	/**
	 * Charset to use with our server-side row-filter.
	 * We use this one because it preserves every possible byte unchanged.
	 */
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	/** The TSDB we belong to. */
	private final TSDB tsdb;

	/** Value used for timestamps that are uninitialized.  */
	private static final int UNSET = -1;

	/** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
	private int start_time = UNSET;

	/** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
	private int end_time = UNSET;

	/** ID of the metric being looked up. */
	private byte[] metric;

	/**
	 * Tags of the metrics being looked up.
	 * Each tag is a byte array holding the ID of both the name and value
	 * of the tag.
	 * Invariant: an element cannot be both in this array and in group_bys.
	 */
	private ArrayList<byte[]> tags;

	/**
	 * Tags by which we must group the results.
	 * Each element is a tag ID.
	 * Invariant: an element cannot be both in this array and in {@code tags}.
	 */
	private ArrayList<byte[]> group_bys;

	/**
	 * Values we may be grouping on.
	 * For certain elements in {@code group_bys}, we may have a specific list of
	 * values IDs we're looking for.  Those IDs are stored in this map.  The key
	 * is an element of {@code group_bys} (so a tag name ID) and the values are
	 * tag value IDs (at least two).
	 */
	private ByteMap<byte[][]> group_by_values;

	/** If true, use rate of change instead of actual values. */
	private boolean rate;

	/**
	 * Downsampling function to use, if any (can be {@code null}).
	 * If this is non-null, {@code sample_interval} must be strictly positive.
	 */
	private Aggregator downsampler;

	/** Minimum time interval (in seconds) wanted between each data point. */
	private int sample_interval;

	/** Constructor. */
	public TsdbQuery(final TSDB tsdb, String metric, long startTime, long endTime, Map<String, String> tags) {
		this.tsdb = tsdb;
		setStartTime(startTime);
		setEndTime(endTime);
		setTimeSeries(metric, tags);
	}

	private void setStartTime(final long timestamp) {
		if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
			throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
		} else if (end_time != UNSET && timestamp >= getEndTime()) {
			throw new IllegalArgumentException("new start time (" + timestamp
					+ ") is greater than or equal to end time: " + getEndTime());
		}
		// Keep the 32 bits.
		start_time = (int) timestamp;
	}

	public long getStartTime() {
		if (start_time == UNSET) {
			throw new IllegalStateException("setStartTime was never called!");
		}
		return start_time & 0x00000000FFFFFFFFL;
	}

	private void setEndTime(final long timestamp) {
		if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
			throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
		} else if (start_time != UNSET && timestamp <= getStartTime()) {
			throw new IllegalArgumentException("new end time (" + timestamp
					+ ") is less than or equal to start time: " + getStartTime());
		}
		// Keep the 32 bits.
		end_time = (int) timestamp;
	}

	public long getEndTime() {
		if (end_time == UNSET) {
			setEndTime(System.currentTimeMillis() / 1000);
		}
		return end_time;
	}

	public void setTimeSeries(final String metric,
			final Map<String, String> tags) throws NoSuchUniqueName {
//		findGroupBys(tags);
		this.metric = tsdb.metrics.getId(metric);
		this.tags = Tags.resolveAll(tsdb, tags);
//		this.rate = rate;
	}

	public void downsample(final int interval, final Aggregator downsampler) {
		if (downsampler == null) {
			throw new NullPointerException("downsampler");
		} else if (interval <= 0) {
			throw new IllegalArgumentException("interval not > 0: " + interval);
		}
		this.downsampler = downsampler;
		this.sample_interval = interval;
	}

	/**
	 * Extracts all the tags we must use to group results.
	 * <ul>
	 * <li>If a tag has the form {@code name=*} then we'll create one
	 *     group per value we find for that tag.</li>
	 * <li>If a tag has the form {@code name={v1,v2,..,vN}} then we'll
	 *     create {@code N} groups.</li>
	 * </ul>
	 * In the both cases above, {@code name} will be stored in the
	 * {@code group_bys} attribute.  In the second case specifically,
	 * the {@code N} values would be stored in {@code group_by_values},
	 * the key in this map being {@code name}.
	 * @param tags The tags from which to extract the 'GROUP BY's.
	 * Each tag that represents a 'GROUP BY' will be removed from the map
	 * passed in argument.
	 */
	private void findGroupBys(final Map<String, String> tags) {
		final Iterator<Map.Entry<String, String>> i = tags.entrySet().iterator();
		while (i.hasNext()) {
			final Map.Entry<String, String> tag = i.next();
			final String tagvalue = tag.getValue();
			if (tagvalue.equals("*")  // 'GROUP BY' with any value.
					|| tagvalue.indexOf('|', 1) >= 0) {  // Multiple possible values.
				if (group_bys == null) {
					group_bys = new ArrayList<byte[]>();
				}
				group_bys.add(tsdb.tag_names.getId(tag.getKey()));
				i.remove();
				if (tagvalue.charAt(0) == '*') {
					continue;  // For a 'GROUP BY' with any value, we're done.
				}
				// 'GROUP BY' with specific values.  Need to split the values
				// to group on and store their IDs in group_by_values.
				final String[] values = Tags.splitString(tagvalue, '|');
				if (group_by_values == null) {
					group_by_values = new ByteMap<byte[][]>();
				}
				final short value_width = tsdb.tag_values.width();
				final byte[][] value_ids = new byte[values.length][value_width];
				group_by_values.put(tsdb.tag_names.getId(tag.getKey()),
						value_ids);
				for (int j = 0; j < values.length; j++) {
					final byte[] value_id = tsdb.tag_values.getId(values[j]);
					System.arraycopy(value_id, 0, value_ids[j], 0, value_width);
				}
			}
		}
	}

	public void run(CachedSearchResult cachedSearchResult) throws HBaseException, IOException
	{
		TreeMap<byte[], Span> spanMap = findSpans();

		for(Span span : spanMap.values())
		{
			cachedSearchResult.startDataPointSet(span.getInclusiveTags());
			for (HBDataPoint dataPoint : span)
			{
				if (dataPoint.timestamp() < start_time || dataPoint.timestamp() > end_time)
				{
					// Remove data points not in the time range
					continue;
				}

				if (dataPoint.isInteger())
				{
					cachedSearchResult.addDataPoint(dataPoint.timestamp(), dataPoint.longValue());
				}
				else
				{
					cachedSearchResult.addDataPoint(dataPoint.timestamp(), dataPoint.doubleValue());
				}
			}
		}

		cachedSearchResult.endDataPoints();
	}

	/**
	 * Finds all the {@link Span}s that match this query.
	 * This is what actually scans the HBase table and loads the data into
	 * {@link Span}s.
	 * @return A map from HBase row key to the {@link Span} for that row key.
	 * Since a {@link Span} actually contains multiple HBase rows, the row key
	 * stored in the map has its timestamp zero'ed out.
	 * @throws HBaseException if there was a problem communicating with HBase to
	 * perform the search.
	 * @throws IllegalArgumentException if bad data was retreived from HBase.
	 */
	private TreeMap<byte[], Span> findSpans() throws HBaseException {
		final short metric_width = tsdb.metrics.width();
		final TreeMap<byte[], Span> spans =  // The key is a row key from HBase.
				new TreeMap<byte[], Span>(new SpanCmp(metric_width));
		int nrows = 0;
		int hbase_time = 0;  // milliseconds.
		long starttime = System.nanoTime();
		final Scanner scanner = getScanner();
		try {
			ArrayList<ArrayList<KeyValue>> rows;
			while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
				hbase_time += (System.nanoTime() - starttime) / 1000000;
				for (final ArrayList<KeyValue> row : rows) {
					final byte[] key = row.get(0).key();
					if (Bytes.memcmp(metric, key, 0, metric_width) != 0) {
						throw new IllegalDataException("HBase returned a row that doesn't match"
								+ " our scanner (" + scanner + ")! " + row + " does not start"
								+ " with " + Arrays.toString(metric));
					}
					Span datapoints = spans.get(key);
					if (datapoints == null) {
						datapoints = new Span(tsdb);
						spans.put(key, datapoints);
					}
					datapoints.addRow(tsdb.compact(row));
					nrows++;
					starttime = System.nanoTime();
				}
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException("Should never be here", e);
		} finally {
			hbase_time += (System.nanoTime() - starttime) / 1000000;
			scanlatency.add(hbase_time);
		}
		LOG.info(this + " matched " + nrows + " rows in " + spans.size() + " spans");
		if (nrows == 0) {
			return null;
		}
		return spans;
	}

	/**
	 * Creates the {@link Scanner} to use for this query.
	 */
	Scanner getScanner() throws HBaseException {
		final short metric_width = tsdb.metrics.width();
		final byte[] start_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
		final byte[] end_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
		// We search at least one row before and one row after the start & end
		// time we've been given as it's quite likely that the exact timestamp
		// we're looking for is in the middle of a row.  Plus, a number of things
		// rely on having a few extra data points before & after the exact start
		// & end dates in order to do proper rate calculation or downsampling near
		// the "edges" of the graph.
		Bytes.setInt(start_row, (int) getScanStartTime(), metric_width);
		Bytes.setInt(end_row, (end_time == UNSET
				? -1  // Will scan until the end (0xFFF...).
				: (int) getScanEndTime()),
				metric_width);
		System.arraycopy(metric, 0, start_row, 0, metric_width);
		System.arraycopy(metric, 0, end_row, 0, metric_width);

		final Scanner scanner = tsdb.client.newScanner(tsdb.table);
		scanner.setMaxNumRows(1024 * 10);
		scanner.setStartKey(start_row);
		scanner.setStopKey(end_row);
		if (tags.size() > 0 || group_bys != null) {
			createAndSetFilter(scanner);
		}
		scanner.setFamily(TSDB.FAMILY);
		return scanner;
	}

	/** Returns the UNIX timestamp from which we must start scanning.  */
	private long getScanStartTime() {
		// The reason we look before by `MAX_TIMESPAN * 2' seconds is because of
		// the following.  Let's assume MAX_TIMESPAN = 600 (10 minutes) and the
		// start_time = ... 12:31:00.  If we initialize the scanner to look
		// only 10 minutes before, we'll start scanning at time=12:21, which will
		// give us the row that starts at 12:30 (remember: rows are always aligned
		// on MAX_TIMESPAN boundaries -- so in this example, on 10m boundaries).
		// But we need to start scanning at least 1 row before, so we actually
		// look back by twice MAX_TIMESPAN.  Only when start_time is aligned on a
		// MAX_TIMESPAN boundary then we'll mistakenly scan back by an extra row,
		// but this doesn't really matter.
		// Additionally, in case our sample_interval is large, we need to look
		// even further before/after, so use that too.
		final long ts = getStartTime() - Const.MAX_TIMESPAN * 2 - sample_interval;
		return ts > 0 ? ts : 0;
	}

	/** Returns the UNIX timestamp at which we must stop scanning.  */
	private long getScanEndTime() {
		// For the end_time, we have a different problem.  For instance if our
		// end_time = ... 12:30:00, we'll stop scanning when we get to 12:40, but
		// once again we wanna try to look ahead one more row, so to avoid this
		// problem we always add 1 second to the end_time.  Only when the end_time
		// is of the form HH:59:59 then we will scan ahead an extra row, but once
		// again that doesn't really matter.
		// Additionally, in case our sample_interval is large, we need to look
		// even further before/after, so use that too.
		return getEndTime() + Const.MAX_TIMESPAN + 1 + sample_interval;
	}

	/**
	 * Sets the server-side regexp filter on the scanner.
	 * In order to find the rows with the relevant tags, we use a
	 * server-side filter that matches a regular expression on the row key.
	 * @param scanner The scanner on which to add the filter.
	 */
	void createAndSetFilter(final Scanner scanner) {
		if (group_bys != null) {
			Collections.sort(group_bys, Bytes.MEMCMP);
		}
		final short name_width = tsdb.tag_names.width();
		final short value_width = tsdb.tag_values.width();
		final short tagsize = (short) (name_width + value_width);
		// Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
		// and { 4 5 6 9 8 7 }, the regexp will be:
		// "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
		final StringBuilder buf = new StringBuilder(
				15  // "^.{N}" + "(?:.{M})*" + "$"
						+ ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
						* (tags.size() + (group_bys == null ? 0 : group_bys.size() * 3))));
		// In order to avoid re-allocations, reserve a bit more w/ groups ^^^

		// Alright, let's build this regexp.  From the beginning...
		buf.append("(?s)"  // Ensure we use the DOTALL flag.
				+ "^.{")
				// ... start by skipping the metric ID and timestamp.
				.append(tsdb.metrics.width() + Const.TIMESTAMP_BYTES)
				.append("}");
		final Iterator<byte[]> tags = this.tags.iterator();
		final Iterator<byte[]> group_bys = (this.group_bys == null
				? new ArrayList<byte[]>(0).iterator()
				: this.group_bys.iterator());
		byte[] tag = tags.hasNext() ? tags.next() : null;
		byte[] group_by = group_bys.hasNext() ? group_bys.next() : null;
		// Tags and group_bys are already sorted.  We need to put them in the
		// regexp in order by ID, which means we just merge two sorted lists.
		do {
			// Skip any number of tags.
			buf.append("(?:.{").append(tagsize).append("})*\\Q");
			if (isTagNext(name_width, tag, group_by)) {
				addId(buf, tag);
				tag = tags.hasNext() ? tags.next() : null;
			} else {  // Add a group_by.
				addId(buf, group_by);
				final byte[][] value_ids = (group_by_values == null
						? null
						: group_by_values.get(group_by));
				if (value_ids == null) {  // We don't want any specific ID...
					buf.append(".{").append(value_width).append('}');  // Any value ID.
				} else {  // We want specific IDs.  List them: /(AAA|BBB|CCC|..)/
					buf.append("(?:");
					for (final byte[] value_id : value_ids) {
						buf.append("\\Q");
						addId(buf, value_id);
						buf.append('|');
					}
					// Replace the pipe of the last iteration.
					buf.setCharAt(buf.length() - 1, ')');
				}
				group_by = group_bys.hasNext() ? group_bys.next() : null;
			}
		} while (tag != group_by);  // Stop when they both become null.
		// Skip any number of tags before the end.
		buf.append("(?:.{").append(tagsize).append("})*$");
		scanner.setKeyRegexp(buf.toString(), CHARSET);
	}

	/**
	 * Helper comparison function to compare tag name IDs.
	 * @param name_width Number of bytes used by a tag name ID.
	 * @param tag A tag (array containing a tag name ID and a tag value ID).
	 * @param group_by A tag name ID.
	 * @return {@code true} number if {@code tag} should be used next (because
	 * it contains a smaller ID), {@code false} otherwise.
	 */
	private boolean isTagNext(final short name_width,
			final byte[] tag,
			final byte[] group_by) {
		if (tag == null) {
			return false;
		} else if (group_by == null) {
			return true;
		}
		final int cmp = Bytes.memcmp(tag, group_by, 0, name_width);
		if (cmp == 0) {
			throw new AssertionError("invariant violation: tag ID "
					+ Arrays.toString(group_by) + " is both in 'tags' and"
					+ " 'group_bys' in " + this);
		}
		return cmp < 0;
	}

	/**
	 * Appends the given ID to the given buffer, followed by "\\E".
	 */
	private static void addId(final StringBuilder buf, final byte[] id) {
		boolean backslash = false;
		for (final byte b : id) {
			buf.append((char) (b & 0xFF));
			if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
				// So we just terminated the quoted section because we just added \E
				// to `buf'.  So let's put a litteral \E now and start quoting again.
				buf.append("\\\\E\\Q");
			} else {
				backslash = b == '\\';
			}
		}
		buf.append("\\E");
	}

	public String toString() {
		final StringBuilder buf = new StringBuilder();
		buf.append("TsdbQuery(start_time=")
				.append(getStartTime())
				.append(", end_time=")
				.append(getEndTime())
				.append(", metric=").append(Arrays.toString(metric));
		try {
			buf.append(" (").append(tsdb.metrics.getName(metric));
		} catch (NoSuchUniqueId e) {
			buf.append(" (<").append(e.getMessage()).append('>');
		}
		try {
			buf.append("), tags=").append(Tags.resolveIds(tsdb, tags));
		} catch (NoSuchUniqueId e) {
			buf.append("), tags=<").append(e.getMessage()).append('>');
		}
		buf.append(", rate=").append(rate)
				.append(", group_bys=(");
		if (group_bys != null) {
			for (final byte[] tag_id : group_bys) {
				try {
					buf.append(tsdb.tag_names.getName(tag_id));
				} catch (NoSuchUniqueId e) {
					buf.append('<').append(e.getMessage()).append('>');
				}
				buf.append(' ')
						.append(Arrays.toString(tag_id));
				if (group_by_values != null) {
					final byte[][] value_ids = group_by_values.get(tag_id);
					if (value_ids == null) {
						continue;
					}
					buf.append("={");
					for (final byte[] value_id : value_ids) {
						try {
							buf.append(tsdb.tag_values.getName(value_id));
						} catch (NoSuchUniqueId e) {
							buf.append('<').append(e.getMessage()).append('>');
						}
						buf.append(' ')
								.append(Arrays.toString(value_id))
								.append(", ");
					}
					buf.append('}');
				}
				buf.append(", ");
			}
		}
		buf.append("))");
		return buf.toString();
	}

	/**
	 * Comparator that ignores timestamps in row keys.
	 */
	private static final class SpanCmp implements Comparator<byte[]> {

		private final short metric_width;

		public SpanCmp(final short metric_width) {
			this.metric_width = metric_width;
		}

		public int compare(final byte[] a, final byte[] b) {
			final int length = Math.min(a.length, b.length);
			if (a == b) {  // Do this after accessing a.length and b.length
				return 0;    // in order to NPE if either a or b is null.
			}
			int i;
			// First compare the metric ID.
			for (i = 0; i < metric_width; i++) {
				if (a[i] != b[i]) {
					return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
				}
			}
			// Then skip the timestamp and compare the rest.
			for (i += Const.TIMESTAMP_BYTES; i < length; i++) {
				if (a[i] != b[i]) {
					return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
				}
			}
			return a.length - b.length;
		}

	}
}