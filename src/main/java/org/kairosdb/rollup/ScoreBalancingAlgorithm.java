package org.kairosdb.rollup;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.kairosdb.util.SummingMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class ScoreBalancingAlgorithm implements BalancingAlgorithm
{
    @Inject
    public ScoreBalancingAlgorithm()
    {
    }

    @Override
    public Map<String, String> rebalance(Set<String> hosts, Map<String, Long> scores)
    {
        Map<String, String> balancedAssignments = new HashMap<>();
        List<ServerAssignment> hostScores = new ArrayList<>();

        // Add all hosts
        for (String host : hosts) {
            hostScores.add(new ServerAssignment(host));
        }

        // Make assignments. The host on top of the set has the smallest total score
        for (String id : scores.keySet()) {
            ServerAssignment leastLoaded = hostScores.get(0);
            leastLoaded.score += scores.get(id);
            balancedAssignments.put(id, leastLoaded.host);
            hostScores.sort(Comparator.comparingLong(o -> o.score));
        }

        return balancedAssignments;
    }

    @Override
    public Map<String, String> balance(Set<String> hosts, Map<String, String> currentAssignments, Map<String, Long> scores)
    {
        Map<String, String> assignments = new HashMap<>();

        SummingMap totals = new SummingMap();

        for (String id : currentAssignments.keySet()) {
            String hostname = currentAssignments.get(id);
            Long score = scores.get(id);
            if (score != null) {
                totals.put(hostname, score);
            }
        }

        // Add any hosts not in the currentAssignments
        for (String host : hosts) {
            if (!totals.containsKey(host))
            {
                totals.put(host, 0L);
            }
        }

        Set<String> unassignedIds = Sets.difference(scores.keySet(), currentAssignments.keySet());
        for (String unassignedId : unassignedIds) {
            String host = totals.getKeyForSmallestValue();
            assignments.put(unassignedId, host);
            totals.put(host, scores.get(unassignedId));
        }

        return assignments;
    }

    public class ServerAssignment
    {
        public  long score;
        public String host;

        ServerAssignment(String host)
        {
            this.host = checkNotNullOrEmpty(host, "host cannot be null or empty");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ServerAssignment that = (ServerAssignment) o;

            return host.equals(that.host);
        }

        @Override
        public int hashCode()
        {
            return host.hashCode();
        }
    }
}
