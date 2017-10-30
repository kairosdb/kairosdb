package org.kairosdb.datastore.cassandra;

import org.junit.Test;

import static org.junit.Assert.*;

public class CassandraSetupTest {

    @Test
    public void name() {
        System.out.println(CassandraSetup.CREATE_KEYSPACE);
    }
}