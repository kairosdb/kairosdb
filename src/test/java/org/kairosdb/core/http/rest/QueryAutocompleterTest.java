package org.kairosdb.core.http.rest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datastore.QueryMetric;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class QueryAutocompleterTest {
    private QueryAutocompleter service;

    @Before
    public void setUp() {
        this.service = new QueryAutocompleter();
    }

    @After
    public void tearDown() {
        this.service = null;
    }

    @Test
    public void testAddOneMetricWhichIsCommonForSeveralKeysWithWildcard() {
        Set<String> expected = setOf("bbb");
        QueryMetric queryMetric = createWithKeys("aaa.*.bbb", "ccc.bbb", "111.bbb");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testNoMetricWhichIsCommonForSeveralKeysWithOutWildcard() {
        Set<String> expected = emptySet();
        QueryMetric queryMetric = createWithKeys("aaa.123.bbb", "ccc.bbb", "111.bbb");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testNoMetricWhenThereIsNoWildcardInKey() {
        Set<String> expected = emptySet();
        QueryMetric queryMetric = createWithKeys("aaa.bbb", "ccc.ddd");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testAddTwoMetricsForTwoKeysWithFirstSeeWildCardKey() {
        Set<String> expected = setOf("bbb", "ddd");
        QueryMetric queryMetric = createWithKeys("aaa.*.bbb", "ccc.ddd");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testAddTwoMetricsForTwoKeysWithFirstSeeNonWildCardKey() {
        Set<String> expected = setOf("bbb", "ddd");
        QueryMetric queryMetric = createWithKeys("ccc.ddd", "aaa.*.bbb");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testAddTwoMetricsForThreeKeys() {
        Set<String> expected = setOf("bbb", "123");
        QueryMetric queryMetric = createWithKeys("aaa.*.bbb", "ccc.123", "eee.bbb");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testTreatKeyWithoutDotsAsMetric() {
        Set<String> expected = setOf("bbb", "ccc");
        QueryMetric queryMetric = createWithKeys("aaa.*.bbb", "ccc");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testWorkNormallyWhenDotAtTheEndOfTheKey() {
        Set<String> expected = setOf("bbb", "ddd");
        QueryMetric queryMetric = createWithKeys("aaa.*.bbb", "ccc.ddd.");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testAddMetricWhenThereIsOtherTagsInPlace() {
        Set<String> expected = setOf("bbb");
        QueryMetric queryMetric = createWithKeys("aaa.*.bbb", "ccc.*.bbb").addTag("entity", "aws123");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testDoNotAddMetricWhenWildcardAtTheEndOfKey() {
        Set<String> expected = emptySet();
        QueryMetric queryMetric = createWithKeys("aaa.b*b", "aaa.ccc");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testDoNotAddMetricWhenQuestionMarkAtTheEndOfKey() {
        Set<String> expected = emptySet();
        QueryMetric queryMetric = createWithKeys("aaa.b?b", "aaa.ccc");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testNotAutoCompleteIfMetricIsThere() {
        Set<String> expected = setOf("existing");
        QueryMetric queryMetric = createWithKeys("aaa.*.bbb").addTag("metric", "existing");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testNotAutoCompleteIfTheStartTimeIsTooEarly() {
        Set<String> expected = emptySet();
        long startTime = 1573344000000L;
        QueryMetric queryMetric = new QueryMetric(startTime, 60, "zmon.check.123");
        queryMetric.addTag("key", "aaa.*.bbb");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testDoNotFailIfKeyIsNull() {
        Set<String> expected = emptySet();
        QueryMetric queryMetric = createWithNormalStartTime().addTag("key", null);

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    @Test
    public void testDoNotFailIfKeyIsOnlyDots() {
        Set<String> expected = emptySet();
        QueryMetric queryMetric = createWithKeys("..");

        service.complete(queryMetric);

        assertEquals(expected, queryMetric.getTags().get("metric"));
    }

    private QueryMetric createWithKeys(String... keys) {
        QueryMetric queryMetric = createWithNormalStartTime();
        for (String keyValue : keys) {
            queryMetric = queryMetric.addTag("key", keyValue);
        }
        return queryMetric;
    }

    private QueryMetric createWithNormalStartTime() {
        long now = System.currentTimeMillis();
        return new QueryMetric(now - 3600_000, 60, "zmon.check.123");
    }

    private Set<String> setOf(String... strings) {
        Set<String> set = new HashSet<>(Arrays.asList(strings));
        return Collections.unmodifiableSet(set);
    }
}
