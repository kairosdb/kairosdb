/*
 * Copyright 2013 Proofpoint Inc.
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

import com.datastax.driver.core.*;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.cassandra.cache.RowKeyCache;
import org.kairosdb.datastore.cassandra.cache.StringKeyCache;
import org.kairosdb.util.KDataOutput;
import org.kairosdb.util.MemoryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.datastore.cassandra.cache.DefaultMetricNameCache.METRIC_NAME_CACHE;
import static org.kairosdb.datastore.cassandra.cache.DefaultTagNameCache.TAG_NAME_CACHE;
import static org.kairosdb.datastore.cassandra.cache.DefaultTagValueCache.TAG_VALUE_CACHE;

public class CassandraDatastore implements Datastore {
    public static final Logger logger = LoggerFactory.getLogger(CassandraDatastore.class);

    public static final String DATA_POINTS_INSERT = "INSERT INTO data_points " +
            "(key, column1, value) VALUES (?, ?, ?) USING TTL ?";

    public static final String ROW_KEY_INDEX_INSERT = "INSERT INTO row_key_index " +
            "(key, column1, value) VALUES (?, ?, 0x00) USING TTL ?";

    public static final String ROW_KEY_INDEX_SPLIT_INSERT = "INSERT INTO row_key_split_index " +
            "(metric_name, tag_name, tag_value, column1, value) VALUES (?, ?, ?, ?, 0x00) USING TTL ?";

    public static final String STRING_INDEX_INSERT = "INSERT INTO string_index " +
            "(key, column1, value) VALUES (?, ?, 0x00) USING TTL ?";

    public static final String QUERY_STRING_INDEX = "SELECT column1 FROM string_index WHERE key = ?";

    public static final String QUERY_ROW_KEY_INDEX = "SELECT column1 FROM row_key_index WHERE key = ? AND column1 >= ? and column1 <= ? ORDER BY column1 ASC LIMIT ?";

    public static final String QUERY_ROW_KEY_SPLIT_INDEX = "SELECT column1 FROM row_key_split_index WHERE metric_name = ? AND tag_name = ? and tag_value IN ? AND column1 >= ? and column1 <= ? LIMIT ?";

    // TODO: delete next 3 constants when we're done with switching tables and row_key_split_index_2 is deleted
    public static final String QUERY_ROW_KEY_SPLIT_INDEX_2 = "SELECT column1 FROM row_key_split_index_2 WHERE metric_name = ? AND tag_name = ? and tag_value IN ? AND column1 >= ? and column1 <= ? LIMIT ?";
    public static final String ROW_KEY_INDEX_SPLIT_2_INSERT = "INSERT INTO row_key_split_index_2 " +
            "(metric_name, tag_name, tag_value, column1, value) VALUES (?, ?, ?, ?, 0x00) USING TTL ?";

    public static final String QUERY_DATA_POINTS = "SELECT column1, value FROM data_points WHERE key = ? AND column1 >= ? and column1 < ? ORDER BY column1 ASC";

    public static final int LONG_FLAG = 0x0;
    public static final int FLOAT_FLAG = 0x1;

    public static final DataPointsRowKeySerializer DATA_POINTS_ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();

    public final long m_rowWidthRead;
    public final long m_rowWidthWrite;

    private static final Charset UTF_8 = Charset.forName("UTF-8");
    public static final String ROW_KEY_METRIC_NAMES = "metric_names";
    private static final ByteBuffer METRIC_NAME_BYTE_BUFFER = ByteBuffer.wrap(ROW_KEY_METRIC_NAMES.getBytes(UTF_8));

    public static final String ROW_KEY_TAG_NAMES = "tag_names";
    private static final ByteBuffer ROW_KEY_TAG_NAMES_BYTE_BUFFER = ByteBuffer.wrap(ROW_KEY_TAG_NAMES.getBytes(UTF_8));
    public static final String ROW_KEY_TAG_VALUES = "tag_values";
    private static final ByteBuffer ROW_KEY_TAG_VALUES_BYTE_BUFFER = ByteBuffer.wrap(ROW_KEY_TAG_VALUES.getBytes(UTF_8));

    private final CassandraClient m_cassandraClient;
    private final Session m_session;

    private final PreparedStatement m_psInsertData;
    private final PreparedStatement m_psInsertRowKey;
    private final PreparedStatement m_psInsertRowKeySplit;
    private final PreparedStatement m_psInsertString;
    private final PreparedStatement m_psQueryStringIndex;
    private final PreparedStatement m_psQueryRowKeyIndex;
    private final PreparedStatement m_psQueryRowKeySplitIndex;
    private final PreparedStatement m_psQueryDataPoints;

    // TODO: delete next 3 vars when we're done with switching tables and row_key_split_index_2 is deleted
    private final PreparedStatement m_psInsertRowKeySplit2;
    private final PreparedStatement m_psQueryRowKeySplitIndex2;

    private final RowKeyCache rowKeyCache;
    private final StringKeyCache metricNameCache;
    private final StringKeyCache tagNameCache;
    private final StringKeyCache tagValueCache;

    private final KairosDataPointFactory m_kairosDataPointFactory;

    private final Set<String> m_indexTagList;

    private CassandraConfiguration m_cassandraConfiguration;

    private final Random random = new Random();
    private final int PERCENTAGE_OF_SIMPLE_QUERIES_LOGGED = 10;

    @Inject
    public CassandraDatastore(CassandraClient cassandraClient, CassandraConfiguration cassandraConfiguration,
                              KairosDataPointFactory kairosDataPointFactory, RowKeyCache rowKeyCache,
                              @Named(METRIC_NAME_CACHE) StringKeyCache metricNameCache,
                              @Named(TAG_NAME_CACHE) StringKeyCache tagNameCache,
                              @Named(TAG_VALUE_CACHE) StringKeyCache tagValueCache
    ) throws DatastoreException {
        m_cassandraConfiguration = cassandraConfiguration;
        this.rowKeyCache = rowKeyCache;
        this.metricNameCache = metricNameCache;
        this.tagNameCache = tagNameCache;
        this.tagValueCache = tagValueCache;

        logger.warn("Setting tag index: {}", cassandraConfiguration.getIndexTagList());
        m_indexTagList = Arrays.stream(cassandraConfiguration.getIndexTagList().split(","))
                .map(String::trim)
                .filter(Objects::nonNull)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());

        m_cassandraClient = cassandraClient;
        m_kairosDataPointFactory = kairosDataPointFactory;

        setupSchema();

        m_session = m_cassandraClient.getKeyspaceSession();

        m_psInsertData = m_session.prepare(DATA_POINTS_INSERT).setConsistencyLevel(cassandraConfiguration.getDataWriteLevelDataPoint());
        m_psInsertRowKeySplit = m_session.prepare(ROW_KEY_INDEX_SPLIT_INSERT).setConsistencyLevel(cassandraConfiguration.getDataWriteLevelMeta());
        // TODO: Remove this once we finish with row_key_split_index_2
        m_psInsertRowKeySplit2 = m_session.prepare(ROW_KEY_INDEX_SPLIT_2_INSERT).setConsistencyLevel(cassandraConfiguration.getDataWriteLevelMeta());
        m_psInsertRowKey = m_session.prepare(ROW_KEY_INDEX_INSERT).setConsistencyLevel(cassandraConfiguration.getDataWriteLevelMeta());
        m_psInsertString = m_session.prepare(STRING_INDEX_INSERT).setConsistencyLevel(cassandraConfiguration.getDataWriteLevelMeta());
        m_psQueryStringIndex = m_session.prepare(QUERY_STRING_INDEX).setConsistencyLevel(cassandraConfiguration.getDataReadLevel());
        m_psQueryRowKeyIndex = m_session.prepare(QUERY_ROW_KEY_INDEX).setConsistencyLevel(cassandraConfiguration.getDataReadLevel());
        m_psQueryRowKeySplitIndex = m_session.prepare(QUERY_ROW_KEY_SPLIT_INDEX).setConsistencyLevel(cassandraConfiguration.getDataReadLevel());
        m_psQueryRowKeySplitIndex2 = m_session.prepare(QUERY_ROW_KEY_SPLIT_INDEX_2).setConsistencyLevel(cassandraConfiguration.getDataReadLevel());
        m_psQueryDataPoints = m_session.prepare(QUERY_DATA_POINTS).setConsistencyLevel(cassandraConfiguration.getDataReadLevel());

        m_rowWidthRead = cassandraConfiguration.getRowWidthRead();
        m_rowWidthWrite = cassandraConfiguration.getRowWidthWrite();
    }

    public long getRowWidthRead() {
        return m_rowWidthRead;
    }

    public long getRowWidthWrite() {
        return m_rowWidthWrite;
    }

    private void setupSchema() {
        CassandraSetup setup = null;

        try (Session session = m_cassandraClient.getSession()) {
            ResultSet rs = session.execute("SELECT release_version FROM system.local");
            final String version = rs.all().get(0).getString(0);
            if (version.startsWith("3")) {
                logger.info("Selecting V3 Cassandra Schema Setup");
                setup = new CassandraSetupV3(m_cassandraClient, m_cassandraConfiguration.getKeyspaceName(), m_cassandraConfiguration.getReplicationFactor());
            } else {
                logger.info("Selecting V2 Cassandra Schema Setup");
                setup = new CassandraSetupV2(m_cassandraClient, m_cassandraConfiguration.getKeyspaceName(), m_cassandraConfiguration.getReplicationFactor());
            }
        }

        if (null != setup) {
            setup.initSchema();
        } else {
            logger.info("Cassandra Setup not performed");
        }
    }

    @Override
    public void close() throws InterruptedException {
        m_session.close();
        m_cassandraClient.close();
    }

    @Override
    public void putDataPoint(String metricName,
                             ImmutableSortedMap<String, String> tags,
                             DataPoint dataPoint,
                             int ttl) throws DatastoreException {
        try {
            Span span = GlobalTracer.get().activeSpan();
            //time the data is written.
            long writeTime = System.currentTimeMillis();
            if (0 == ttl) {
                ttl = m_cassandraConfiguration.getDatapointTtl();
            }

            int rowKeyTtl = 0;
            //Row key will expire after configured ReadRowWidth
            if (ttl != 0) {
                rowKeyTtl = ttl + ((int) (m_rowWidthWrite / 1000));
            }

            final long rowTime = calculateRowTimeWrite(dataPoint.getTimestamp());
            final DataPointsRowKey dataPointsRowKey = new DataPointsRowKey(metricName, rowTime, dataPoint.getDataStoreDataType(), tags);
            final ByteBuffer serializedKey = DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(dataPointsRowKey);

            // Write out the row key if it is not cached
            final boolean rowKeyKnown = rowKeyCache.isKnown(serializedKey);
            if (!rowKeyKnown) {
                storeRowKeyReverseLookups(metricName, serializedKey, rowKeyTtl, tags);
                if (span != null) {
                    span.setTag("cache_miss_row_key", Boolean.TRUE);
                }

                rowKeyCache.put(serializedKey);

                //Write metric name if not in cache
                if (!metricNameCache.isKnown(metricName)) {
                    if (span != null) {
                        span.setTag("cache_miss_metric_name", Boolean.TRUE);
                    }
                    if (metricName.length() == 0) {
                        logger.warn("Attempted to add empty metric name to string index. Row looks like: {}", dataPoint);
                    }
                    storeStringIndex(metricName, m_psInsertString, METRIC_NAME_BYTE_BUFFER);
                    metricNameCache.put(metricName);
                }

                //Check tag names and values to write them out
                for (final String tagName : tags.keySet()) {
                    if (!tagNameCache.isKnown(tagName)) {
                        if (span != null) {
                            span.setTag("cache_miss_tag_name", Boolean.TRUE);
                        }
                        if (tagName.length() == 0) {
                            logger.warn("Attempted to add empty tagName to string cache for metric: {}", metricName);
                        }
                        storeStringIndex(tagName, m_psInsertString, ROW_KEY_TAG_NAMES_BYTE_BUFFER);
                        tagNameCache.put(tagName);
                    }

                    final String value = tags.get(tagName);
                    boolean isCachedValue = tagValueCache.isKnown(value);
                    if (m_cassandraConfiguration.getTagValueCacheSize() > 0 && !isCachedValue) {
                        if (span != null) {
                            span.setTag("cache_miss_tag_value", Boolean.TRUE);
                        }
                        if (value.length() == 0) {
                            logger.warn("Attempted to add empty tagValue (tag name {}) to string cache for metric: {}",
                                    tagName, metricName);
                        }
                        storeStringIndex(value, m_psInsertString, ROW_KEY_TAG_VALUES_BYTE_BUFFER);
                        tagValueCache.put(value);
                    }
                }
            }

            int columnTime = getColumnName(rowTime, dataPoint.getTimestamp());
            KDataOutput kDataOutput = new KDataOutput();
            dataPoint.writeValueToBuffer(kDataOutput);

            BoundStatement boundStatement = new BoundStatement(m_psInsertData);
            boundStatement.setBytes(0, serializedKey);
            ByteBuffer b = ByteBuffer.allocate(4);
            b.putInt(columnTime);
            b.rewind();
            boundStatement.setBytes(1, b);
            boundStatement.setBytes(2, ByteBuffer.wrap(kDataOutput.getBytes()));
            boundStatement.setInt(3, ttl);
            m_session.executeAsync(boundStatement);
        } catch (Exception e) {
            logger.error("Failed to put data point for metric={} tags={} ttl={}", metricName, tags, ttl);
            throw new DatastoreException(e);
        }
    }

    private void storeStringIndex(String value, PreparedStatement m_psInsertString, ByteBuffer key) {
        BoundStatement bs = new BoundStatement(m_psInsertString);
        bs.setBytes(0, key);
        bs.setString(1, value);
        bs.setInt(2, m_cassandraConfiguration.getDatapointTtl());
        m_session.executeAsync(bs);
    }

    private void storeRowKeyReverseLookups(final String metricName, final ByteBuffer serializedKey, final int rowKeyTtl,
                                           final Map<String, String> tags) {
        BoundStatement bs = new BoundStatement(m_psInsertRowKey);
        bs.setBytes(0, ByteBuffer.wrap(metricName.getBytes(UTF_8)));
        bs.setBytes(1, serializedKey);
        bs.setInt(2, rowKeyTtl);
        m_session.executeAsync(bs);

        for (String split : m_indexTagList) {
            String v = tags.get(split);
            if (null == v || "".equals(v)) {
                continue;
            }

            if (m_cassandraConfiguration.isUseNewSplitIndexWrite()) {
                storeRowKeySplit(m_psInsertRowKeySplit2, metricName, serializedKey, rowKeyTtl, split, v);
            } else {
                storeRowKeySplit(m_psInsertRowKeySplit, metricName, serializedKey, rowKeyTtl, split, v);
            }
        }
    }

    private void storeRowKeySplit(final PreparedStatement insertStatement, final String metricName,
                                  final ByteBuffer serializedKey, final int rowKeyTtl,
                                  final String splitTagName, final String splitTagValue) {
        final BoundStatement bs = new BoundStatement(insertStatement);
        bs.setString(0, metricName);
        bs.setString(1, splitTagName);
        bs.setString(2, splitTagValue);
        bs.setBytes(3, serializedKey);
        bs.setInt(4, rowKeyTtl);

        m_session.executeAsync(bs);
    }

    private Iterable<String> queryStringIndex(final String key) {

        BoundStatement bs = m_psQueryStringIndex.bind();
        try {
            bs.setBytes(0, ByteBuffer.wrap(key.getBytes("UTF-8")));
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }

        ResultSet rs = m_session.execute(bs);

        List<String> ret = new ArrayList<>();
        for (Row r : rs) {
            ret.add(r.getString("column1"));
        }

        return ret;
    }

    @Override
    public Iterable<String> getMetricNames() {
        return queryStringIndex(ROW_KEY_METRIC_NAMES);
    }

    @Override
    public Iterable<String> getTagNames() {
        return queryStringIndex(ROW_KEY_TAG_NAMES);
    }

    @Override
    public Iterable<String> getTagValues() {
        return queryStringIndex(ROW_KEY_TAG_VALUES);
    }

    @Override
    public TagSet queryMetricTags(DatastoreMetricQuery query) {
        TagSetImpl tagSet = new TagSetImpl();
        Collection<DataPointsRowKey> rowKeys =
                getKeysForQueryIterator(query, m_cassandraConfiguration.getMaxRowsForKeysQuery() + 1);

        MemoryMonitor mm = new MemoryMonitor(20);
        for (DataPointsRowKey key : rowKeys) {
            for (Map.Entry<String, String> tag : key.getTags().entrySet()) {
                tagSet.addTag(tag.getKey(), tag.getValue());
                mm.checkMemoryAndThrowException();
            }
        }

        return (tagSet);
    }

    @Override
    public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) {
        queryWithRowKeys(query, queryCallback, getKeysForQueryIterator(query));
    }

    private void queryWithRowKeys(DatastoreMetricQuery query,
                                  QueryCallback queryCallback, Collection<DataPointsRowKey> rowKeys) {
        long startTime = System.currentTimeMillis();
        long currentTimeTier = 0L;
        String currentType = null;

        List<CQLQueryRunner> runners = new ArrayList<>();
        List<DataPointsRowKey> queryKeys = new ArrayList<>();

        MemoryMonitor mm = new MemoryMonitor(20);

        List<DataPointsRowKey> sorted = new ArrayList<>();
        sorted.addAll(rowKeys);
        sorted.sort(Comparator.comparingLong(DataPointsRowKey::getTimestamp));
        rowKeys = sorted;

        if (rowKeys.size() < 64) {
            for (DataPointsRowKey k : rowKeys) {
                // logger.info("<64: delta={}", k.getTimestamp() - currentTimeTier);
                currentTimeTier = k.getTimestamp();
            }
            queryKeys.addAll(rowKeys);
        } else {
            for (DataPointsRowKey rowKey : rowKeys) {
                if (currentTimeTier == 0L)
                    currentTimeTier = rowKey.getTimestamp();

                if (currentType == null)
                    currentType = rowKey.getDataType();

                if ((rowKey.getTimestamp() == currentTimeTier) &&
                        (currentType.equals(rowKey.getDataType()))) {
                    queryKeys.add(rowKey);
                } else {
                    // logger.info("Creating new query runner: metric={} size={} ts-delta={}", queryKeys.get(0).getMetricName(), queryKeys.size(), currentTimeTier - rowKey.getTimestamp());
                    runners.add(new CQLQueryRunner(m_session, m_psQueryDataPoints, m_kairosDataPointFactory,
                            queryKeys,
                            query.getStartTime(), query.getEndTime(), m_rowWidthRead, queryCallback, query.getLimit(), query.getOrder()));

                    queryKeys = new ArrayList<>();
                    queryKeys.add(rowKey);
                    currentTimeTier = rowKey.getTimestamp();
                    currentType = rowKey.getDataType();
                }
                mm.checkMemoryAndThrowException();
            }

            if (random.nextInt(100) == 42) {
                m_cassandraClient.logConnectionStats(m_session);
            }
        }

        //There may be stragglers that are not ran
        if (!queryKeys.isEmpty()) {
            // logger.info("Creating new runner for remaining keys: metric={} size={}", queryKeys.get(0).getMetricName(), queryKeys.size());
            runners.add(new CQLQueryRunner(m_session, m_psQueryDataPoints, m_kairosDataPointFactory,
                    queryKeys,
                    query.getStartTime(), query.getEndTime(), m_rowWidthRead, queryCallback, query.getLimit(), query.getOrder()));
        }

        //Changing the check rate
        mm.setCheckRate(1);
        try {
            // TODO: Run this with multiple threads - not easily possible with how QueryCallback behaves
            for (CQLQueryRunner runner : runners) {
                runner.runQuery();

                mm.checkMemoryAndThrowException();
            }

            queryCallback.endDataPoints();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException {
        checkNotNull(deleteQuery);

        boolean deleteAll = false;
        if (deleteQuery.getStartTime() == Long.MIN_VALUE && deleteQuery.getEndTime() == Long.MAX_VALUE)
            deleteAll = true;

        Iterator<DataPointsRowKey> rowKeyIterator = getKeysForQueryIterator(deleteQuery).iterator();
        List<DataPointsRowKey> partialRows = new ArrayList<>();

        while (rowKeyIterator.hasNext()) {
            DataPointsRowKey rowKey = rowKeyIterator.next();
            long rowKeyTimestamp = rowKey.getTimestamp();
            // TODO check which width to use
            if (deleteQuery.getStartTime() <= rowKeyTimestamp && (deleteQuery.getEndTime() >= rowKeyTimestamp + m_rowWidthRead - 1)) {
                // TODO fix me
                //m_dataPointWriteBuffer.deleteRow(rowKey, now);  // delete the whole row
                //m_rowKeyWriteBuffer.deleteColumn(rowKey.getMetricName(), rowKey, now); // Delete the index
                // m_rowKeyCache.clear();
            } else {
                partialRows.add(rowKey);
            }
        }

        queryWithRowKeys(deleteQuery, new DeletingCallback(deleteQuery.getName()), partialRows);

        // If index is gone, delete metric name from Strings column family
        if (deleteAll) {
            //m_rowKeyWriteBuffer.deleteRow(deleteQuery.getName(), now);
            //todo fix me
            //m_stringIndexWriteBuffer.deleteColumn(ROW_KEY_METRIC_NAMES, deleteQuery.getName(), now);
            // m_rowKeyCache.clear();
            // m_metricNameCache.clear();
        }
    }

    private SortedMap<String, String> getTags(DataPointRow row) {
        TreeMap<String, String> map = new TreeMap<>();
        for (String name : row.getTagNames()) {
            map.put(name, row.getTagValue(name));
        }

        return map;
    }

    public Collection<DataPointsRowKey> getKeysForQueryIterator(DatastoreMetricQuery query) {
        return getKeysForQueryIterator(query, m_cassandraConfiguration.getMaxRowsForKeysQuery() + 1);
    }


    /**
     * Returns the row keys for the query in tiers ie grouped by row key timestamp
     *
     * @param query query
     * @return row keys for the query
     */
    public Collection<DataPointsRowKey> getKeysForQueryIterator(DatastoreMetricQuery query, int limit) {
        Span span = GlobalTracer.get().activeSpan();
        Collection<DataPointsRowKey> ret = null;

        List<QueryPlugin> plugins = query.getPlugins();

        //First plugin that works gets it.
        for (QueryPlugin plugin : plugins) {
            if (plugin instanceof CassandraRowKeyPlugin) {
                ret = ((CassandraRowKeyPlugin) plugin).getKeysForQueryIterator(query);
                break;
            }
        }

        //Default to old behavior if no plugin was provided
        if (ret == null) {
            ret = getMatchingRowKeys(query, limit);
        }

        return ret;
    }

    public long calculateRowTimeRead(long timestamp) {
        return (timestamp - (Math.abs(timestamp) % m_rowWidthRead));
    }

    public long calculateRowTimeWrite(long timestamp) {
        return (timestamp - (Math.abs(timestamp) % m_rowWidthWrite));
    }


    /**
     * This is just for the delete operation of old data points.
     *
     * @param rowTime
     * @param timestamp
     * @param isInteger
     * @return
     */
    @SuppressWarnings("PointlessBitwiseExpression")
    private static int getColumnName(long rowTime, long timestamp, boolean isInteger) {
        int ret = (int) (timestamp - rowTime);

        if (isInteger)
            return ((ret << 1) | LONG_FLAG);
        else
            return ((ret << 1) | FLOAT_FLAG);

    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public static int getColumnName(long rowTime, long timestamp) {
        int ret = (int) (timestamp - rowTime);

		/*
            The timestamp is shifted to support legacy datapoints that
			used the extra bit to determine if the value was long or double
		 */
        return (ret << 1);
    }

    public static long getColumnTimestamp(long rowTime, int columnName) {
        return (rowTime + (long) (columnName >>> 1));
    }

    public static boolean isLongValue(int columnName) {
        return ((columnName & 0x1) == LONG_FLAG);
    }

    private static final Pattern GLOB_PATTERN = Pattern.compile("\\?|\\*");

    /**
     * Convert a GLOB style pattern ("*" for any number of any char, "?" for exactly one char) to a regex pattern.
     * <p>
     * Code borrowed from Spring's AntPathMatcher.java (Apache 2 license)
     */
    public static Pattern convertGlobToPattern(String pattern) {
        StringBuilder patternBuilder = new StringBuilder();
        Matcher matcher = GLOB_PATTERN.matcher(pattern);
        int end = 0;
        while (matcher.find()) {
            patternBuilder.append(quote(pattern, end, matcher.start()));
            String match = matcher.group();
            if ("?".equals(match)) {
                patternBuilder.append('.');
            } else if ("*".equals(match)) {
                patternBuilder.append(".*");
            }
            end = matcher.end();
        }
        patternBuilder.append(quote(pattern, end, pattern.length()));
        return Pattern.compile(patternBuilder.toString());
    }

    private static String quote(String s, int start, int end) {
        if (start == end) {
            return "";
        }
        return Pattern.quote(s.substring(start, end));
    }

    /**
     * Return whether the given input value matches any of the given GLOB-style patterns.
     */
    public static boolean matchesAny(final String value, final Collection<Pattern> patterns) {
        for (Pattern pattern : patterns) {
            if (pattern.matcher(value).matches()) {
                return true;
            }
        }
        return false;
    }

    private void filterAndAddKeys(DatastoreMetricQuery query, ResultSet rs, List<DataPointsRowKey> filteredRowKeys, int readRowsLimit, String index) {
        filterAndAddKeys(query, rs, filteredRowKeys, readRowsLimit, index, false);
    }

    private void filterAndAddKeys(DatastoreMetricQuery query, ResultSet rs, List<DataPointsRowKey> filteredRowKeys, int readRowsLimit, String index, boolean last) {
        final DataPointsRowKeySerializer keySerializer = new DataPointsRowKeySerializer();
        final SetMultimap<String, String> filterTags = query.getTags();
        final SetMultimap<String, Pattern> tagPatterns = MultimapBuilder.hashKeys(filterTags.size()).hashSetValues().build();
        for (Map.Entry<String, String> entry : filterTags.entries()) {
            tagPatterns.put(entry.getKey(), convertGlobToPattern(entry.getValue()));
        }
        int rowReadCount = 0;
        for (Row r : rs) {
            rowReadCount++;

            checkMaxRowKeyLimit(rowReadCount, readRowsLimit, query, filteredRowKeys, rowReadCount, index,
                    String.format("Exceeded limit: %d key rows read by KDB. Metric: %s", readRowsLimit, query.getName()));

            DataPointsRowKey key = keySerializer.fromByteBuffer(r.getBytes("column1"));
            Map<String, String> tags = key.getTags();

            boolean skipKey = false;
            for (String tag : filterTags.keySet()) {
                String value = tags.get(tag);
                if (value == null || !matchesAny(value, tagPatterns.get(tag))) {
                    skipKey = true;
                    break;
                }
            }
            if (!skipKey) {
                filteredRowKeys.add(key);
            }
        }

        final boolean isCriticalQuery = rowReadCount > 5000 || filteredRowKeys.size() > 100;
        query.setQueryUUID(UUID.randomUUID());
        query.setQueryLoggingType(isCriticalQuery ? "critical" : "simple");
        query.setLoggable(isCriticalQuery || random.nextInt(100) < PERCENTAGE_OF_SIMPLE_QUERIES_LOGGED);

        if (last) {
            final int filteredRowsLimit = m_cassandraConfiguration.getMaxRowKeysForQuery();
            checkMaxRowKeyLimit(filteredRowKeys.size(), filteredRowsLimit, query, filteredRowKeys, rowReadCount, index,
                    String.format("Exceeded limit: %d data point partitions read by KDB. Metric: %s",
                            filteredRowsLimit, query.getName()));
        }

        if (query.isLoggable()) {
            logQuery(query, filteredRowKeys, rowReadCount, false, readRowsLimit, index);
        }
    }

    private void checkMaxRowKeyLimit(int size, int limit, DatastoreMetricQuery query,
                                     List<DataPointsRowKey> filteredRowKeys, int rowReadCount, String index, String errorMessage) {
        if (size > limit) {
            Span span = GlobalTracer.get().activeSpan();
            if (span != null) {
                span.setTag("row_count", size);
                span.setTag("max_row_keys", Boolean.TRUE);
            }

            logQuery(query, filteredRowKeys, rowReadCount, true, limit, index);
            throw new MaxRowKeysForQueryExceededException(errorMessage);
        }
    }

    private static void logQuery(DatastoreMetricQuery query,
                                 Collection<DataPointsRowKey> filteredRowKeys,
                                 int rowReadCount,
                                 boolean limitExceeded,
                                 int limit,
                                 String index) {
        final long endTime = Long.MAX_VALUE == query.getEndTime() ? System.currentTimeMillis() : query.getEndTime();
        logger.warn("{}_query: uuid={} metric={} query={} read={} filtered={} start_time={} end_time={} duration={} exceeded={} limit={} index={}",
                query.getQueryLoggingType(), query.getQueryUUID(), query.getName(), query.getTags(), rowReadCount, filteredRowKeys.size(),
                query.getStartTime(), endTime, endTime - query.getStartTime(), limitExceeded, limit, index);
    }

    // TODO remove when old getMatchingRowKeys is uncommented
    private List<DataPointsRowKey> getMatchingRowKeys(DatastoreMetricQuery query, int limit) {
        // determine whether to use split index or not
        String useSplitField = null;
        Set<String> useSplitSet = new HashSet<>();

        SetMultimap<String, String> filterTags = query.getTags();
        for (String split : m_indexTagList) {
            if (filterTags.containsKey(split)) {
                Set<String> currentSet = filterTags.get(split);
                final boolean currentSetIsSmaller = currentSet.size() < useSplitSet.size();
                final boolean currentSetIsNotEmpty = currentSet.size() > 0 && useSplitSet.isEmpty();
                final boolean currentSetHasNoWildcards = currentSet.stream().noneMatch(x -> x.contains("*") || x.contains("?"));
                if ((currentSetIsSmaller || currentSetIsNotEmpty) && currentSetHasNoWildcards) {
                    useSplitSet = currentSet;
                    useSplitField = split;
                }
            }
        }
        List<String> useSplit = new ArrayList<>(useSplitSet);

        if (useSplitField != null && !"".equals(useSplitField) && useSplitSet.size() > 0) {
            return getMatchingRowKeysFromSplitIndex(query, useSplitField, useSplit, limit);
        } else {
            return getMatchingRowKeysFromRegularIndex(query, limit);
        }
    }

    private List<DataPointsRowKey> getMatchingRowKeysFromRegularIndex(DatastoreMetricQuery query,
                                                                      int limit) {
        final List<DataPointsRowKey> rowKeys = new LinkedList<>();
        collectFromRowKeyIndex(rowKeys, m_psQueryRowKeyIndex, query, limit);
        return rowKeys;
    }

    private List<DataPointsRowKey> getMatchingRowKeysFromSplitIndex(DatastoreMetricQuery query,
                                                                    String useSplitField,
                                                                    List<String> useSplit,
                                                                    int limit) {
        final List<DataPointsRowKey> rowKeys = new LinkedList<>();
        if (m_cassandraConfiguration.isUseNewSplitIndexRead() && query.getEndTime() >= m_cassandraConfiguration.getNewSplitIndexStartTimeMs()) {
            // Allowed cases for this branch (* - a point defined by m_cassandraConfiguration.getNewSplitIndexStartTimeMs())
            //  --|-------*------|--
            // startTime     endTime
            //
            //  --|--------------*--
            // startTime     endTime
            //
            //  *--------|--------------|--
            //        startTime     endTime
            //
            //  --*--------------|--
            //  startTime     endTime

            if (query.getStartTime() < m_cassandraConfiguration.getNewSplitIndexStartTimeMs()) {
                // cases
                //  --|-------*------|--
                // startTime     endTime
                //  --|--------------*--
                // startTime     endTime

                // in case m_cassandraConfiguration.getNewSplitIndexStartTimeMs() == endTime
                // with first round it will fetch data from new table for a single time point == 'endTime'
                // the second round will fetch all the data until 'endTime' (exclusively)
                collectFromRowKeySplitIndex(rowKeys,
                        m_psQueryRowKeySplitIndex2,
                        query,
                        useSplitField,
                        useSplit,
                        m_cassandraConfiguration.getNewSplitIndexStartTimeMs(),
                        query.getEndTime(),
                        limit);

                limit -= rowKeys.size();
                if (limit > 0) {
                    collectFromRowKeySplitIndex(rowKeys,
                            m_psQueryRowKeySplitIndex,
                            query,
                            useSplitField,
                            useSplit,
                            query.getStartTime(),
                            m_cassandraConfiguration.getNewSplitIndexStartTimeMs() - m_rowWidthRead,
                            limit);
                }
            } else {
                // cases
                //  *--------|--------------|--
                //        startTime     endTime
                //
                //  --*--------------|--
                //  startTime     endTime
                collectFromRowKeySplitIndex(rowKeys,
                        m_psQueryRowKeySplitIndex2,
                        query,
                        useSplitField,
                        useSplit,
                        query.getStartTime(),
                        query.getEndTime(),
                        limit);
            }
        } else {
            // Allowed cases for this branch (* - a point defined by m_cassandraConfiguration.getNewSplitIndexStartTimeMs())
            //  --|--------------|-------*
            // startTime     endTime

            collectFromRowKeySplitIndex(rowKeys,
                    m_psQueryRowKeySplitIndex,
                    query,
                    useSplitField,
                    useSplit,
                    query.getStartTime(),
                    query.getEndTime(),
                    limit);
        }

        return rowKeys;
    }

    // TODO remove when old getMatchingRowKeys is uncommented
    private void collectFromRowKeySplitIndex(List<DataPointsRowKey> collector,
                                             final PreparedStatement statement,
                                             final DatastoreMetricQuery query,
                                             final String useSplitField,
                                             final List<String> useSplit,
                                             final long startTime,
                                             final long endTime,
                                             final int limit) {
        Span span = GlobalTracer.get().activeSpan();
        final String indexName = "row_key_split_index";
        if (span != null) {
            span.setTag("index_name", indexName);
        }

        final DataPointsRowKeySerializer keySerializer = new DataPointsRowKeySerializer();

        final String metricName = query.getName();
        final SetMultimap<String, String> filterTags = query.getTags();

        final BoundStatement bs = statement.bind();
        bs.setString(0, metricName);
        bs.setString(1, useSplitField);
        bs.setList(2, useSplit);
        bs.setInt(5, limit);


        if ((startTime < 0) && (endTime >= 0)) {
            DataPointsRowKey startKey = new DataPointsRowKey(metricName, calculateRowTimeRead(startTime), "");
            DataPointsRowKey endKey = new DataPointsRowKey(metricName, calculateRowTimeRead(endTime), "");
            endKey.setEndSearchKey(true);

            bs.setBytes(3, keySerializer.toByteBuffer(startKey));
            bs.setBytes(4, keySerializer.toByteBuffer(endKey));

            ResultSet rs = m_session.execute(bs);

            filterAndAddKeys(query, rs, collector, m_cassandraConfiguration.getMaxRowsForKeysQuery(), indexName);

            startKey = new DataPointsRowKey(metricName, calculateRowTimeRead(0), "");
            endKey = new DataPointsRowKey(metricName, calculateRowTimeRead(endTime), "");

            bs.setBytes(3, keySerializer.toByteBuffer(startKey));
            bs.setBytes(4, keySerializer.toByteBuffer(endKey));

            rs = m_session.execute(bs);
            filterAndAddKeys(query, rs, collector, m_cassandraConfiguration.getMaxRowsForKeysQuery(), indexName, true);
        } else {
            long calculatedStartTime = calculateRowTimeRead(startTime);
            // Use write width here, as END time is upper bound for query and end with produces the bigger timestamp
            long calculatedEndTime = calculateRowTimeWrite(endTime);
            // logger.info("calculated: s={} cs={} e={} ce={}", startTime, calculatedStarTime, endTime, calculatedEndTime);

            DataPointsRowKey startKey = new DataPointsRowKey(metricName, calculatedStartTime, "");
            DataPointsRowKey endKey = new DataPointsRowKey(metricName, calculatedEndTime, "");
            endKey.setEndSearchKey(true);

            bs.setBytes(3, keySerializer.toByteBuffer(startKey));
            bs.setBytes(4, keySerializer.toByteBuffer(endKey));

            ResultSet rs = m_session.execute(bs);
            filterAndAddKeys(query, rs, collector, m_cassandraConfiguration.getMaxRowsForKeysQuery(), indexName, true);
        }
    }

    // TODO remove when old getMatchingRowKeys is uncommented
    private void collectFromRowKeyIndex(List<DataPointsRowKey> collector,
                                        final PreparedStatement statement,
                                        DatastoreMetricQuery query,
                                        final int limit) {
        Span span = GlobalTracer.get().activeSpan();
        final String indexName = "row_key_index";
        if (span != null) {
            span.setTag("index_name", indexName);
        }

        final DataPointsRowKeySerializer keySerializer = new DataPointsRowKeySerializer();

        final BoundStatement bs = statement.bind();
        final String metricName = query.getName();
        ByteBuffer bMetricName;
        try {
            bMetricName = ByteBuffer.wrap(metricName.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }

        bs.setBytes(0, bMetricName);
        bs.setInt(3, limit);

        final long startTime = query.getStartTime();
        final long endTime = query.getEndTime();
        if ((startTime < 0) && (endTime >= 0)) {
            DataPointsRowKey startKey = new DataPointsRowKey(metricName, calculateRowTimeRead(startTime), "");
            DataPointsRowKey endKey = new DataPointsRowKey(metricName, calculateRowTimeRead(endTime), "");
            endKey.setEndSearchKey(true);

            bs.setBytes(1, keySerializer.toByteBuffer(startKey));
            bs.setBytes(2, keySerializer.toByteBuffer(endKey));

            ResultSet rs = m_session.execute(bs);

            filterAndAddKeys(query, rs, collector, m_cassandraConfiguration.getMaxRowsForKeysQuery(), indexName);

            startKey = new DataPointsRowKey(metricName, calculateRowTimeRead(0), "");
            endKey = new DataPointsRowKey(metricName, calculateRowTimeRead(endTime), "");

            bs.setBytes(1, keySerializer.toByteBuffer(startKey));
            bs.setBytes(2, keySerializer.toByteBuffer(endKey));

            rs = m_session.execute(bs);
            filterAndAddKeys(query, rs, collector, m_cassandraConfiguration.getMaxRowsForKeysQuery(), indexName, true);
        } else {
            long calculatedStartTime = calculateRowTimeRead(startTime);
            // Use write width here, as END time is upper bound for query and end with produces the bigger timestamp
            long calculatedEndTime = calculateRowTimeWrite(endTime);
            // logger.info("calculated: s={} cs={} e={} ce={}", startTime, calculatedStarTime, endTime, calculatedEndTime);

            DataPointsRowKey startKey = new DataPointsRowKey(metricName, calculatedStartTime, "");
            DataPointsRowKey endKey = new DataPointsRowKey(metricName, calculatedEndTime, "");
            endKey.setEndSearchKey(true);

            bs.setBytes(1, keySerializer.toByteBuffer(startKey));
            bs.setBytes(2, keySerializer.toByteBuffer(endKey));

            ResultSet rs = m_session.execute(bs);
            filterAndAddKeys(query, rs, collector, m_cassandraConfiguration.getMaxRowsForKeysQuery(), indexName, true);
        }
    }


// TODO: to be uncommented when we finish our exercise with m_psQueryRowKeySplitIndex2
//    private List<DataPointsRowKey> getMatchingRowKeys(String metricName, long startTime, long endTime, SetMultimap<String, String> filterTags, int limit) {
//        final List<DataPointsRowKey> rowKeys = new ArrayList<>();
//        final DataPointsRowKeySerializer keySerializer = new DataPointsRowKeySerializer();
//
//        String useSplitField = null;
//        Set<String> useSplit = new HashSet<>();
//
//        for (String split : m_indexTagList) {
//            if (filterTags.containsKey(split)) {
//                Set<String> vs = filterTags.get(split);
//                if (((vs.size() < useSplit.size() && useSplit.size() > 0) || (vs.size() > 0 && useSplit.size() == 0))
//                        && vs.stream().noneMatch(x -> x.contains("*") || x.contains("?"))) {
//                    useSplit = vs;
//                    useSplitField = split;
//                }
//            }
//        }
//
//        int bsShift = 0;
//        BoundStatement bs;
//
//        if (useSplitField != null && !"".equals(useSplitField) && useSplit.size() > 0) {
//            // logger.warn("using split lookup: name={} fields={}", useSplitField, useSplit);
//            bsShift = 2;
//            bs = m_psQueryRowKeySplitIndex.bind();
//            bs.setString(0, metricName);
//            bs.setString(1, useSplitField);
//            bs.setList(2, Arrays.asList(useSplit.toArray()));
//        } else {
//            bs = m_psQueryRowKeyIndex.bind();
//
//            ByteBuffer bMetricName;
//            try {
//                bMetricName = ByteBuffer.wrap(metricName.getBytes("UTF-8"));
//            } catch (UnsupportedEncodingException ex) {
//                throw new RuntimeException(ex);
//            }
//
//            bs.setBytes(0, bMetricName);
//        }
//
//        bs.setInt(3 + bsShift, limit);
//
//        if ((startTime < 0) && (endTime >= 0)) {
//            DataPointsRowKey startKey = new DataPointsRowKey(metricName, calculateRowTimeRead(startTime), "");
//            DataPointsRowKey endKey = new DataPointsRowKey(metricName, calculateRowTimeRead(endTime), "");
//            endKey.setEndSearchKey(true);
//
//            bs.setBytes(1 + bsShift, keySerializer.toByteBuffer(startKey));
//            bs.setBytes(2 + bsShift, keySerializer.toByteBuffer(endKey));
//
//            ResultSet rs = m_session.execute(bs);
//
//            filterAndAddKeys(metricName, filterTags, rs, rowKeys, m_cassandraConfiguration.getMaxRowsForKeysQuery());
//
//            startKey = new DataPointsRowKey(metricName, calculateRowTimeRead(0), "");
//            endKey = new DataPointsRowKey(metricName, calculateRowTimeRead(endTime), "");
//
//            bs.setBytes(1, keySerializer.toByteBuffer(startKey));
//            bs.setBytes(2, keySerializer.toByteBuffer(endKey));
//
//            rs = m_session.execute(bs);
//            filterAndAddKeys(metricName, filterTags, rs, rowKeys, m_cassandraConfiguration.getMaxRowsForKeysQuery());
//        } else {
//            long calculatedStarTime = calculateRowTimeRead(startTime);
//            // Use write width here, as END time is upper bound for query and end with produces the bigger timestamp
//            long calculatedEndTime = calculateRowTimeWrite(endTime);
//            // logger.info("calculated: s={} cs={} e={} ce={}", startTime, calculatedStarTime, endTime, calculatedEndTime);
//
//            DataPointsRowKey startKey = new DataPointsRowKey(metricName, calculatedStarTime, "");
//            DataPointsRowKey endKey = new DataPointsRowKey(metricName, calculatedEndTime, "");
//            endKey.setEndSearchKey(true);
//
//            bs.setBytes(1 + bsShift, keySerializer.toByteBuffer(startKey));
//            bs.setBytes(2 + bsShift, keySerializer.toByteBuffer(endKey));
//
//            ResultSet rs = m_session.execute(bs);
//            filterAndAddKeys(metricName, filterTags, rs, rowKeys, m_cassandraConfiguration.getMaxRowsForKeysQuery());
//        }
//
//        return rowKeys;
//    }


    private class DeletingCallback implements QueryCallback {
        private SortedMap<String, String> m_currentTags;
        private DataPointsRowKey m_currentRow;
        private long m_now = System.currentTimeMillis();
        private final String m_metric;
        private String m_currentType;

        public DeletingCallback(String metric) {
            m_metric = metric;
        }


        @Override
        public void addDataPoint(DataPoint datapoint) throws IOException {
            long time = datapoint.getTimestamp();
            long rowTime = calculateRowTimeWrite(time);
            if (m_currentRow == null) {
                m_currentRow = new DataPointsRowKey(m_metric, rowTime, m_currentType, m_currentTags);
            }

            int columnName;
            //Handle old column name format.
            //We get the type after it has been translated from "" to kairos_legacy
            if (m_currentType.equals(LegacyDataPointFactory.DATASTORE_TYPE)) {
                columnName = getColumnName(rowTime, time, datapoint.isLong());
            } else
                columnName = getColumnName(rowTime, time);

            //todo fix me
            //m_dataPointWriteBuffer.deleteColumn(m_currentRow, columnName, m_now);
        }

        @Override
        public void startDataPointSet(String dataType, Map<String, String> tags) throws IOException {
            m_currentType = dataType;
            m_currentTags = new TreeMap<String, String>(tags);
            //This causes the row key to get reset with the first data point
            m_currentRow = null;
        }

        @Override
        public void endDataPoints() {
        }
    }
}
