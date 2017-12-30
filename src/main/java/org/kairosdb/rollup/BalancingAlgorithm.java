package org.kairosdb.rollup;

import java.util.Map;
import java.util.Set;

public interface BalancingAlgorithm
{

    /**
     * Returns balanced server assignments.
     * @param hosts list of hosts
     * @param scores mapping between task id and scores
     * @return map of task id to host assigned
     */
    Map<String, String> rebalance(Set<String> hosts, Map<String, Long> scores);

    /**
     * Returns an assignment mapping of task id to hostname for unassigned tasks.
     */
    Map<String, String> balance(Set<String> hosts, Map<String, String> currentAssignments, Map<String, Long> scores);

}
