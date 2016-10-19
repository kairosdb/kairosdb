package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;

/**
 * Created by jmussler on 19.10.16.
 */
public class CassandraSetupV2 extends  CassandraSetup {
    public CassandraSetupV2(CassandraClient client, String keySpace, int replicationFactor) {
        super(client, keySpace, replicationFactor);
    }

    @Override
    protected boolean tableExists(Session session, String tableName) {
        PreparedStatement ps = session.prepare("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = ? and columnfamily_name = ?");
        List<Row> rows = session.execute(ps.bind(keySpace, tableName)).all();
        return rows.size() == 1;
    }
}
