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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.util.KDataOutput;
import org.kairosdb.util.MemoryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraDatastore implements Datastore {
    public static final Logger logger = LoggerFactory.getLogger(CassandraDatastore.class);

    public static final String DATA_POINTS_INSERT = "INSERT INTO data_points " +
            "(key, column1, value) VALUES (?, ?, ?) USING TTL ?";

    public static final String ROW_KEY_INDEX_INSERT = "INSERT INTO row_key_index " +
            "(key, column1, value) VALUES (?, ?, 0x00) USING TTL ?";

    public static final String ROW_KEY_INDEX_SPLIT_INSERT = "INSERT INTO row_key_split_index " +
            "(metric_name, tag_name, tag_value, column1, value) VALUES (?, ?, ?, ?, 0x00) USING TTL ?";

    public static final String STRING_INDEX_INSERT = "INSERT INTO string_index " +
            "(key, column1, value) VALUES (?, ?, 0x00)";

    public static final String QUERY_STRING_INDEX = "SELECT column1 FROM string_index WHERE key = ?";

    public static final String QUERY_ROW_KEY_INDEX = "SELECT column1 FROM row_key_index WHERE key = ? AND column1 >= ? and column1 <= ? LIMIT ?";

    public static final String QUERY_ROW_KEY_SPLIT_INDEX = "SELECT column1 FROM row_key_split_index WHERE metric_name = ? AND tag_name = ? and tag_value IN ? AND column1 >= ? and column1 <= ? ORDER BY column1 ASC LIMIT ?";

    public static final String QUERY_DATA_POINTS = "SELECT column1, value FROM data_points WHERE key IN ( ? ) AND column1 >= ? and column1 < ? ORDER BY column1 ASC";

    public static final int LONG_FLAG = 0x0;
    public static final int FLOAT_FLAG = 0x1;

    public static final DataPointsRowKeySerializer DATA_POINTS_ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();

    public final long m_rowWidthRead;
    public final long m_rowWidthWrite;

    public static final String KEY_QUERY_TIME = "kairosdb.datastore.cassandra.key_query_time";

    public static final String ROW_KEY_METRIC_NAMES = "metric_names";
    public static final String ROW_KEY_TAG_NAMES = "tag_names";
    public static final String ROW_KEY_TAG_VALUES = "tag_values";
    private static final Charset UTF_8 = Charset.forName("UTF-8");

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

    private final Cache<DataPointsRowKey, Boolean> m_rowKeyCache;

    private final Cache<String, Boolean> m_metricNameCache;

    private final Cache<String, Boolean> m_tagNameCache;

    private final Cache<String, Boolean> m_tagValueCache;

    private final KairosDataPointFactory m_kairosDataPointFactory;

    private final List<String> m_indexTagList;

    private CassandraConfiguration m_cassandraConfiguration;

    @Inject
    private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

    @Inject
    private List<RowKeyListener> m_rowKeyListeners = Collections.EMPTY_LIST;

    @Inject
    public CassandraDatastore(@Named("HOSTNAME") final String hostname,
                              CassandraClient cassandraClient,
                              CassandraConfiguration cassandraConfiguration,
                              KairosDataPointFactory kairosDataPointFactory) throws DatastoreException {

        m_cassandraConfiguration = cassandraConfiguration;

        logger.warn("Setting tag index: {}", cassandraConfiguration.getIndexTagList());
        logger.warn("Setting metric name cache size: {}", cassandraConfiguration.getMetricNameCacheSize());
        logger.warn("Setting row key cache size: {}", cassandraConfiguration.getRowKeyCacheSize());
        logger.warn("Setting tag name cache size: {}", cassandraConfiguration.getTagNameCacheSize());
        logger.warn("Setting tag value cache size: {}", cassandraConfiguration.getTagValueCacheSize());

        m_rowKeyCache = Caffeine.newBuilder()
                .initialCapacity(cassandraConfiguration.getRowKeyCacheSize()/3 + 1)
                .maximumSize(cassandraConfiguration.getRowKeyCacheSize())
                .expireAfterWrite(24, TimeUnit.HOURS).build();

        m_metricNameCache = Caffeine.newBuilder()
                .initialCapacity(cassandraConfiguration.getMetricNameCacheSize()/3 + 1)
                .maximumSize(cassandraConfiguration.getMetricNameCacheSize())
                .expireAfterWrite(24, TimeUnit.HOURS).build();

        m_tagNameCache = Caffeine.newBuilder()
                .initialCapacity(cassandraConfiguration.getTagNameCacheSize()/3 + 1)
                .maximumSize(cassandraConfiguration.getTagNameCacheSize())
                .expireAfterWrite(24, TimeUnit.HOURS).build();

        m_tagValueCache = Caffeine.newBuilder()
                .initialCapacity(cassandraConfiguration.getTagValueCacheSize()/3 + 1)
                .maximumSize(cassandraConfiguration.getTagValueCacheSize())
                .expireAfterWrite(24, TimeUnit.HOURS).build();

        m_indexTagList = Arrays.asList(cassandraConfiguration.getIndexTagList().split(",")).stream().map(String::trim).collect(Collectors.toList());

        m_cassandraClient = cassandraClient;
        m_kairosDataPointFactory = kairosDataPointFactory;

        setupSchema();

        m_session = m_cassandraClient.getKeyspaceSession();

        m_psInsertData = m_session.prepare(DATA_POINTS_INSERT).setConsistencyLevel(cassandraConfiguration.getDataWriteLevelDataPoint());
        m_psInsertRowKeySplit = m_session.prepare(ROW_KEY_INDEX_SPLIT_INSERT).setConsistencyLevel(cassandraConfiguration.getDataWriteLevelMeta());
        m_psInsertRowKey = m_session.prepare(ROW_KEY_INDEX_INSERT).setConsistencyLevel(cassandraConfiguration.getDataWriteLevelMeta());
        m_psInsertString = m_session.prepare(STRING_INDEX_INSERT).setConsistencyLevel(cassandraConfiguration.getDataWriteLevelMeta());
        m_psQueryStringIndex = m_session.prepare(QUERY_STRING_INDEX).setConsistencyLevel(cassandraConfiguration.getDataReadLevel());
        m_psQueryRowKeyIndex = m_session.prepare(QUERY_ROW_KEY_INDEX).setConsistencyLevel(cassandraConfiguration.getDataReadLevel());
        m_psQueryRowKeySplitIndex = m_session.prepare(QUERY_ROW_KEY_SPLIT_INDEX).setConsistencyLevel(cassandraConfiguration.getDataReadLevel());
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
        }
        else {
            logger.info("Not Cassandra Setup being performced");
        }

        return;
    }

    public void increaseMaxBufferSizes() {
        /*m_dataPointWriteBuffer.increaseMaxBufferSize();
		m_rowKeyWriteBuffer.increaseMaxBufferSize();
		m_stringIndexWriteBuffer.increaseMaxBufferSize();*/
    }

    public void cleanRowKeyCache() {
    }

    @Override
    public void close() throws InterruptedException {
        m_session.close();
        m_cassandraClient.close();
    }

    private final Integer LOCK_ROW_KEY = new Integer(1);
    private final Boolean CACHE_BOOLEAN = new Boolean(true);

    private volatile int keyInsertCount = 0;
    private volatile long keyInsertTimer = System.currentTimeMillis();

    private final Map<String, Integer> insertCountByMetric = new HashMap<>();

    @Override
    public void putDataPoint(String metricName,
                             ImmutableSortedMap<String, String> tags,
                             DataPoint dataPoint,
                             int ttl) throws DatastoreException {
        try {

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
            final DataPointsRowKey rowKey = new DataPointsRowKey(metricName, rowTime, dataPoint.getDataStoreDataType(), tags);
            List<Map.Entry<String, Integer>> countsByMetricName = null;

            // Write out the row key if it is not cached
            boolean writeRowKey = false;
            Boolean isCachedKey = m_rowKeyCache.getIfPresent(rowKey);
            if (isCachedKey == null) {
                synchronized (LOCK_ROW_KEY) {
                    isCachedKey = m_rowKeyCache.getIfPresent(rowKey);
                    if (null == isCachedKey) {
                        // leave lock
                        writeRowKey = true;
                        m_rowKeyCache.put(rowKey, CACHE_BOOLEAN);

                        int count = ++keyInsertCount;
                        if(insertCountByMetric.containsKey(metricName)) {
                            insertCountByMetric.put(metricName, insertCountByMetric.get(metricName) + 1);
                        }
                        else {
                            insertCountByMetric.put(metricName, 1);
                        }

                        if ((writeTime - keyInsertTimer) > 30_000) {
                            keyInsertTimer = writeTime;
                            keyInsertCount = 0;
                            countsByMetricName = new ArrayList<>();
                            countsByMetricName.addAll(insertCountByMetric.entrySet());
                            insertCountByMetric.clear();
                            logger.warn("RowKeys inserted: count={}", count);
                        }
                    }
                }
            }

            if (writeRowKey) {

                BoundStatement bs = new BoundStatement(m_psInsertRowKey);
                bs.setBytes(0, ByteBuffer.wrap(metricName.getBytes(UTF_8)));

                ByteBuffer serializedKey = DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey);

                bs.setBytes(1, serializedKey);
                bs.setInt(2, rowKeyTtl);
                m_session.executeAsync(bs);

                for (String split : m_indexTagList) {
                    String v = tags.get(split);
                    if (null == v || "".equals(v)) {
                        continue;
                    }

                    bs = new BoundStatement(m_psInsertRowKeySplit);
                    bs.setString(0, metricName);
                    bs.setString(1, split);
                    bs.setString(2, v);
                    bs.setBytes(3, serializedKey);
                    bs.setInt(4, rowKeyTtl);

                    m_session.executeAsync(bs);
                }

                for (RowKeyListener rowKeyListener : m_rowKeyListeners) {
                    rowKeyListener.addRowKey(metricName, rowKey, rowKeyTtl);
                }

                if (countsByMetricName != null) {
                    Collections.sort(countsByMetricName, (x,y) -> y.getValue() - x.getValue());
                    logger.warn("Keys inserted: {}", countsByMetricName.subList(0, Math.min(10, countsByMetricName.size())));
                }
            }

            //Write metric name if not in cache
            Boolean isCachedName  = m_metricNameCache.getIfPresent(metricName);
            if (isCachedName == null) {
                m_metricNameCache.put(metricName, CACHE_BOOLEAN);
                if (metricName.length() == 0) {
                    logger.warn(
                            "Attempted to add empty metric name to string index. Row looks like: " + dataPoint
                    );
                }
                BoundStatement bs = new BoundStatement(m_psInsertString);
                bs.setBytes(0, ByteBuffer.wrap(ROW_KEY_METRIC_NAMES.getBytes(UTF_8)));
                bs.setString(1, metricName);
                m_session.executeAsync(bs);
            }

            //Check tag names and values to write them out
            for (String tagName : tags.keySet()) {
                Boolean isCachedTagName = m_tagNameCache.getIfPresent(tagName);
                if (isCachedTagName == null) {
                    m_tagNameCache.put(tagName, CACHE_BOOLEAN);
                    if (tagName.length() == 0) {
                        logger.warn(
                                "Attempted to add empty tagName to string cache for metric: " + metricName
                        );
                    }
                    BoundStatement bs = new BoundStatement(m_psInsertString);
                    bs.setBytes(0, ByteBuffer.wrap(ROW_KEY_TAG_NAMES.getBytes(UTF_8)));
                    bs.setString(1, tagName);
                    m_session.executeAsync(bs);
                }

                String value = tags.get(tagName);
                Boolean isCachedValue = m_tagValueCache.getIfPresent(value);
                if (m_cassandraConfiguration.getTagValueCacheSize() > 0 && isCachedValue == null) {
                    m_tagValueCache.put(value, CACHE_BOOLEAN);
                    if (value.toString().length() == 0) {
                        logger.warn(
                                "Attempted to add empty tagValue (tag name " + tagName + ") to string cache for metric: " + metricName
                        );
                    }
                    BoundStatement bs = new BoundStatement(m_psInsertString);
                    bs.setBytes(0, ByteBuffer.wrap(ROW_KEY_TAG_VALUES.getBytes(UTF_8)));
                    bs.setString(1, value);
                    m_session.executeAsync(bs);
                }
            }

            int columnTime = getColumnName(rowTime, dataPoint.getTimestamp());
            KDataOutput kDataOutput = new KDataOutput();
            dataPoint.writeValueToBuffer(kDataOutput);

            BoundStatement boundStatement = new BoundStatement(m_psInsertData);
            boundStatement.setBytes(0, DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(rowKey));
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
        Collection<DataPointsRowKey> rowKeys = getKeysForQueryIterator(query, 150);

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

        if (rowKeys.size() < 64) {
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
                    // logger.info("Creating new query runner: metric={} size={}", queryKeys.get(0).getMetricName(), queryKeys.size());
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
        }

        //There may be stragglers that are not ran
        if (!queryKeys.isEmpty()) {
            // logger.info("Creating new runner for remaining keys: metric={} size={}", queryKeys.get(0).getMetricName(), queryKeys.size());
            runners.add(new CQLQueryRunner(m_session, m_psQueryDataPoints, m_kairosDataPointFactory,
                    queryKeys,
                    query.getStartTime(), query.getEndTime(), m_rowWidthRead, queryCallback, query.getLimit(), query.getOrder()));
        }

        ThreadReporter.addDataPoint(KEY_QUERY_TIME, System.currentTimeMillis() - startTime);

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

        long now = System.currentTimeMillis();

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
            ret = getMatchingRowKeys(query.getName(), query.getStartTime(),
                    query.getEndTime(), query.getTags(), limit);
        }

        if (ret.size() > m_cassandraConfiguration.getMaxRowKeysForQuery()) {
            throw new MaxRowKeysForQueryExceededException(String.format("Query for metric %s matches %d row keys, but only %d are allowed",
                    query.getName(), ret.size(), m_cassandraConfiguration.getMaxRowKeysForQuery()));
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
     *
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

    private static void filterAndAddKeys(String metricName, SetMultimap<String, String> filterTags, ResultSet rs, List<DataPointsRowKey> targetList, int limit) {
        long startTime = System.currentTimeMillis();
        final DataPointsRowKeySerializer keySerializer = new DataPointsRowKeySerializer();
        final SetMultimap<String, Pattern> tagPatterns = MultimapBuilder.hashKeys(filterTags.size()).hashSetValues().build();
        for (Map.Entry<String, String> entry : filterTags.entries()) {
            tagPatterns.put(entry.getKey(), convertGlobToPattern(entry.getValue()));
        }
        int i = 0;
        for (Row r : rs) {
            i++;

            if (i > limit) {
                throw new MaxRowKeysForQueryExceededException(String.format("Too many rows too scan: metric=%s limit=%d", metricName, limit));
            }

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
                targetList.add(key);
            }
        }
        if (i > 5000 || targetList.size() > 100) {
            final long endTime = System.currentTimeMillis();
            logger.warn("metric={} query={}", metricName, filterTags);
            logger.warn("filterAndAddKeys: metric={} read={} filtered={} time={}", metricName, i, targetList.size(), (endTime - startTime));
        }
    }

    private List<DataPointsRowKey> getMatchingRowKeys(String metricName, long startTime, long endTime, SetMultimap<String, String> filterTags, int limit) {
        final List<DataPointsRowKey> rowKeys = new ArrayList<>();
        final DataPointsRowKeySerializer keySerializer = new DataPointsRowKeySerializer();

        String useSplitField = null;
        Set<String> useSplit = new HashSet<>();

        for(String split : m_indexTagList) {
            if (filterTags.containsKey(split)) {
                Set<String> vs = filterTags.get(split);
                if(((vs.size() < useSplit.size() && useSplit.size() > 0) || (vs.size() > 0 && useSplit.size() == 0))
                        && vs.stream().noneMatch(x->x.contains("*") || x.contains("?"))) {
                    useSplit = vs;
                    useSplitField = split;
                }
            }
        }

        int bsShift = 0;
        BoundStatement bs;

        if (useSplitField != null && !"".equals(useSplitField) && useSplit.size() > 0) {
            // logger.warn("using split lookup: name={} fields={}", useSplitField, useSplit);
            bsShift = 2;
            bs = m_psQueryRowKeySplitIndex.bind();
            bs.setString(0, metricName);
            bs.setString(1, useSplitField);
            bs.setList(2, Arrays.asList(useSplit.toArray()));
        }
        else {
            bs = m_psQueryRowKeyIndex.bind();

            ByteBuffer bMetricName;
            try {
                bMetricName = ByteBuffer.wrap(metricName.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex);
            }

            bs.setBytes(0, bMetricName);
        }

        bs.setInt(3 + bsShift, limit);

        if ((startTime < 0) && (endTime >= 0)) {
            DataPointsRowKey startKey = new DataPointsRowKey(metricName, calculateRowTimeRead(startTime), "");
            DataPointsRowKey endKey = new DataPointsRowKey(metricName, calculateRowTimeRead(endTime), "");
            endKey.setEndSearchKey(true);

            bs.setBytes(1 + bsShift, keySerializer.toByteBuffer(startKey));
            bs.setBytes(2 + bsShift, keySerializer.toByteBuffer(endKey));

            ResultSet rs = m_session.execute(bs);

            filterAndAddKeys(metricName, filterTags, rs, rowKeys, m_cassandraConfiguration.getMaxRowsForKeysQuery());

            startKey = new DataPointsRowKey(metricName, calculateRowTimeRead(0), "");
            endKey = new DataPointsRowKey(metricName, calculateRowTimeRead(endTime), "");

            bs.setBytes(1, keySerializer.toByteBuffer(startKey));
            bs.setBytes(2, keySerializer.toByteBuffer(endKey));

            rs = m_session.execute(bs);
            filterAndAddKeys(metricName, filterTags, rs, rowKeys, m_cassandraConfiguration.getMaxRowsForKeysQuery());
        } else {
            long calculatedStarTime = calculateRowTimeRead(startTime);
            // Use write width here, as END time is upper bound for query and end with produces the bigger timestamp
            long calculatedEndTime = calculateRowTimeWrite(endTime);
            // logger.info("calculated: s={} cs={} e={} ce={}", startTime, calculatedStarTime, endTime, calculatedEndTime);

            DataPointsRowKey startKey = new DataPointsRowKey(metricName, calculatedStarTime, "");
            DataPointsRowKey endKey = new DataPointsRowKey(metricName, calculatedEndTime, "");
            endKey.setEndSearchKey(true);

            bs.setBytes(1 + bsShift, keySerializer.toByteBuffer(startKey));
            bs.setBytes(2 + bsShift, keySerializer.toByteBuffer(endKey));

            ResultSet rs = m_session.execute(bs);
            filterAndAddKeys(metricName, filterTags, rs, rowKeys, m_cassandraConfiguration.getMaxRowsForKeysQuery());
        }

        return rowKeys;
    }

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
