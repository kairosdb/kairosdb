package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;

/**
 * Created by jmussler on 19.10.16.
 */
public class CassandraSetupV3 extends CassandraSetup {
    public CassandraSetupV3(CassandraClient client, String keySpace, int replicationFactor) {
        super(client, keySpace, replicationFactor);
    }

    @Override
    protected boolean tableExists(Session session, String tableName) {
        PreparedStatement ps = session.prepare("SELECT * FROM system_schema.tables WHERE keyspace_name = ? and table_name = ?");
        List<Row> rows = session.execute(ps.bind(keySpace, tableName)).all();
        return rows.size() == 1;
    }

}
