package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jmussler on 19.10.16.
 */
public abstract class CassandraSetup {

    protected final Logger logger = LoggerFactory.getLogger(CassandraSetup.class);

    public static final String CREATE_KEYSPACE = "" +
            "CREATE KEYSPACE IF NOT EXISTS %s" +
            "  WITH REPLICATION = {'class': 'SimpleStrategy'," +
            "  'replication_factor' : %d }";

    public static final String DATA_POINTS_TABLE = "" +
            "CREATE TABLE IF NOT EXISTS data_points (\n" +
            "  key blob,\n" +
            "  column1 blob,\n" +
            "  value blob,\n" +
            "  PRIMARY KEY ((key), column1)\n" +
            ") WITH CLUSTERING ORDER BY (column1 DESC) " + // This should fit the default use case better
            "   AND compaction = {'compaction_window_unit': 'MINUTES'," +
            "                     'min_threshold': '4'," +
            "                     'max_threshold': '32'," +
            "                     'compaction_window_size': '1440'," +
            "                     'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}";

    public static final String ROW_KEY_SPLIT_INDEX_TABLE = "" +
            "CREATE TABLE IF NOT EXISTS row_key_split_index (" +
            "  metric_name text," +
            "  tag_name text," +
            "  tag_value text, " +
            "  column1 blob," +
            "  value blob," +
            "  PRIMARY KEY ((metric_name, tag_name, tag_value), column1)" +
            ") WITH CLUSTERING ORDER BY (column1 DESC);";

    public static final String ROW_KEY_INDEX_TABLE = "" +
            "CREATE TABLE IF NOT EXISTS row_key_index (\n" +
            "  key blob,\n" +
            "  column1 blob,\n" +
            "  value blob,\n" +
            "  PRIMARY KEY ((key), column1)\n" +
            ") WITH CLUSTERING ORDER BY (column1 DESC);";

    public static final String STRING_INDEX_TABLE = "" +
            "CREATE TABLE IF NOT EXISTS string_index (\n" +
            "  key blob,\n" +
            "  column1 text,\n" +
            "  value blob,\n" +
            "  PRIMARY KEY ((key), column1)\n" +
            ");";

    final CassandraClient client;
    final String keySpace;
    final int replicationFactor;

    public CassandraSetup(CassandraClient client, String keySpace, int replicationFactor) {
        this.client = client;
        this.keySpace = keySpace;
        this.replicationFactor = replicationFactor;
    }

    protected abstract boolean tableExists(Session session, String tableName);

    protected abstract boolean keyspaceExists(Session session, String keySpace);

    public void initSchema() {
        try(Session session = client.getSession()) {
            if (!keyspaceExists(session, keySpace)) {
                logger.info("Creating keyspace ... {}", keySpace);
                session.execute(String.format(CREATE_KEYSPACE, keySpace, replicationFactor));
            }
        }

        try(Session session = client.getKeyspaceSession()) {
            logger.info("Checking and or creating column families ...");
            if(!tableExists(session, "data_points")) {
                logger.info("Creating table 'data_pints' ...");
                session.execute(DATA_POINTS_TABLE);
            }

            if(!tableExists(session, "row_key_index")) {
                logger.info("Creating table 'row_key_index' ...");
                session.execute(ROW_KEY_INDEX_TABLE);
            }

            if(!tableExists(session, "row_key_split_index")) {
                logger.info("Creating table 'row_key_split_index' ...");
                session.execute(ROW_KEY_SPLIT_INDEX_TABLE);
            }

            if(!tableExists(session, "string_index")) {
                logger.info("Creating table 'string_index' ...");
                session.execute(STRING_INDEX_TABLE);
            }
        }
    }
}
