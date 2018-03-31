package org.kairosdb.rollup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ScoreBalancingAlgorithmTest
{
    private ScoreBalancingAlgorithm algorithm = new ScoreBalancingAlgorithm();

    @Test
    public void test_balance()
    {
        Map<String, String> result = algorithm.rebalance(ImmutableSet.of("host1", "host2", "host3"),
                ImmutableMap.of("1", 1L, "2", 10L, "3", 1L, "4", 1L, "5", 3L));

        assertThat(result.get("1"), equalTo("host1"));
        assertThat(result.get("2"), equalTo("host2"));
        assertThat(result.get("3"), equalTo("host3"));
        assertThat(result.get("4"), equalTo("host3"));
        assertThat(result.get("5"), equalTo("host1"));
    }

    @Test
    public void test_balance_singleServer()
    {
        Map<String, String> result = algorithm.rebalance(ImmutableSet.of("host1"),
                ImmutableMap.of("1", 1L, "2", 10L, "3", 1L, "4", 1L, "5", 3L));

        assertThat(result.get("1"), equalTo("host1"));
        assertThat(result.get("2"), equalTo("host1"));
        assertThat(result.get("3"), equalTo("host1"));
        assertThat(result.get("4"), equalTo("host1"));
        assertThat(result.get("5"), equalTo("host1"));
    }

    @Test
    public void test_balance_equalScores()
    {
        Map<String, String> result = algorithm.rebalance(ImmutableSet.of("host1", "host2", "host3"),
                ImmutableMap.of("1", 1L, "2", 1L, "3", 1L, "4", 1L, "5", 1L));

        assertThat(result.get("1"), equalTo("host1"));
        assertThat(result.get("2"), equalTo("host2"));
        assertThat(result.get("3"), equalTo("host3"));
        assertThat(result.get("4"), equalTo("host3"));
        assertThat(result.get("5"), equalTo("host2"));
    }

    @Test
    public void test__balance_moreHostsThanIds()
    {
        Map<String, String> result = algorithm.rebalance(ImmutableSet.of("host1", "host2", "host3"),
                ImmutableMap.of("1", 1L, "2", 1L));

        assertThat(result.get("1"), equalTo("host1"));
        assertThat(result.get("2"), equalTo("host2"));
    }

    @Test
    public void test_assign_equalsScores()
    {
        Map<String, String> currentAssignments = ImmutableMap.of("1", "host1", "2", "host2");
        Map<String, Long> scores = ImmutableMap.of("1", 1L, "2", 1L, "3", 1L, "4", 1L, "5", 1L);

        Map<String, String> newAssignments = algorithm.balance(ImmutableSet.of(), currentAssignments, scores);

        assertThat(newAssignments.size(), equalTo(3));
        assertThat(newAssignments.get("3"), equalTo("host1"));
        assertThat(newAssignments.get("4"), equalTo("host2"));
        assertThat(newAssignments.get("5"), equalTo("host1"));
    }

    @Test
    public void test_assign()
    {
        Map<String, String> currentAssignments = ImmutableMap.of("1", "host1", "2", "host2");
        Map<String, Long> scores = ImmutableMap.of("1", 5L, "2", 1L, "3", 5L, "4", 2L, "5", 1L);

        Map<String, String> newAssignments = algorithm.balance(ImmutableSet.of(), currentAssignments, scores);

        assertThat(newAssignments.size(), equalTo(3));
        assertThat(newAssignments.get("3"), equalTo("host2"));
        assertThat(newAssignments.get("4"), equalTo("host1"));
        assertThat(newAssignments.get("5"), equalTo("host2"));
    }
}
