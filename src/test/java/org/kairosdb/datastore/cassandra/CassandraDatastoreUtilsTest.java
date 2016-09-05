package org.kairosdb.datastore.cassandra;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CassandraDatastoreUtilsTest {

    @Test
    public void matchesAny() {
        assertFalse(CassandraDatastore.matchesAny("a", Lists.newArrayList("b")));
        assertFalse(CassandraDatastore.matchesAny("a", Lists.newArrayList("ba*b")));
        assertTrue(CassandraDatastore.matchesAny("a", Lists.newArrayList("a")));
        assertTrue(CassandraDatastore.matchesAny("a", Lists.newArrayList("*")));
        assertTrue(CassandraDatastore.matchesAny("a", Lists.newArrayList("a*")));
        assertTrue(CassandraDatastore.matchesAny("a", Lists.newArrayList("*a")));
        assertTrue(CassandraDatastore.matchesAny("foo", Lists.newArrayList("f*o")));
        assertFalse(CassandraDatastore.matchesAny("foo", Lists.newArrayList("x*o")));
        assertTrue(CassandraDatastore.matchesAny("foo", Lists.newArrayList("x*o", "foo")));

        assertFalse(CassandraDatastore.matchesAny("a", Lists.newArrayList("b?")));
        assertTrue(CassandraDatastore.matchesAny("ba", Lists.newArrayList("b?")));
        assertFalse(CassandraDatastore.matchesAny("bab", Lists.newArrayList("b?")));
    }
}
