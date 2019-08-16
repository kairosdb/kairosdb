package org.kairosdb.datastore.cassandra;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.convertGlobToPattern;

public class CassandraDatastoreUtilsTest {

    @Test
    public void matchesAny() {
        assertFalse(CassandraDatastore.matchesAny("a", Sets.newHashSet(convertGlobToPattern("b"))));
        assertFalse(CassandraDatastore.matchesAny("a", Sets.newHashSet(convertGlobToPattern("ba*b"))));
        assertTrue(CassandraDatastore.matchesAny("a", Sets.newHashSet(convertGlobToPattern("a"))));
        assertTrue(CassandraDatastore.matchesAny("a", Sets.newHashSet(convertGlobToPattern("*"))));
        assertTrue(CassandraDatastore.matchesAny("a", Sets.newHashSet(convertGlobToPattern("a*"))));
        assertTrue(CassandraDatastore.matchesAny("a", Sets.newHashSet(convertGlobToPattern("*a"))));
        assertTrue(CassandraDatastore.matchesAny("foo", Sets.newHashSet(convertGlobToPattern("f*o"))));
        assertFalse(CassandraDatastore.matchesAny("foo", Sets.newHashSet(convertGlobToPattern("x*o"))));
        assertTrue(CassandraDatastore.matchesAny("foo", Sets.newHashSet(convertGlobToPattern("x*o"), convertGlobToPattern("foo"))));

        assertFalse(CassandraDatastore.matchesAny("a", Sets.newHashSet(convertGlobToPattern("b?"))));
        assertTrue(CassandraDatastore.matchesAny("ba", Sets.newHashSet(convertGlobToPattern("b?"))));
        assertFalse(CassandraDatastore.matchesAny("bab", Sets.newHashSet(convertGlobToPattern("b?"))));
    }

    @Test
    public void benchmark() {
        int matches = 0;
        Set<Pattern> patterns = Sets.newHashSet(convertGlobToPattern("foo-*-xy"));
        for (int i = 0; i < 10000; i++) {
            if (CassandraDatastore.matchesAny("foo-" + i + "-xy", patterns)) {
                matches++;
            }
        }
        matches = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            if (CassandraDatastore.matchesAny("foo-" + i + "-xy", patterns)) {
                matches++;
            }
        }
        System.out.println("1: " + matches + " matches in " + (System.currentTimeMillis() - start) + "ms");
    }

    @Test
    public void benchmark2() {
        int matches = 0;
        Set<Pattern> patterns = Sets.newHashSet(convertGlobToPattern("foo-bar-xy"));
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            if (patterns.contains("foo-bar-xy")) {
                matches++;
            }
        }
        System.out.println("2: " + matches + " matches in " + (System.currentTimeMillis() - start) + "ms");
    }


    @Test
    public void test_parseMetricIndexTagMap() {
        final String metricIndexTagList = "zmon.check.1=entity,key,application_id,stack_name;zmon.check.2=stack_name,application_id,key";
        final ListMultimap<String, String> metricIndexTagMap = CassandraDatastore.parseMetricIndexTagMap(metricIndexTagList);
        assertThat(metricIndexTagMap.get("zmon.check.1"), hasItems("entity", "key", "application_id", "stack_name"));
        assertThat(metricIndexTagMap.get("zmon.check.2"), hasItems("stack_name", "application_id", "key"));
    }

}
