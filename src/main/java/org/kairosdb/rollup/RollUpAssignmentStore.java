package org.kairosdb.rollup;

import java.util.Map;
import java.util.Set;

/**
 * Maintains which Kairos servers are executing which roll-up configuration.
 *
 * Uses 2 service keys.
 *
 * Assignment Service key is a mapping from roll-up ID to Kairos server assignment
 * Score Service Key is a mapping from roll-up ID to roll-up score.
 *
 * The score is an indication of how much processing is needed by the roll-up service.
 * Any roll-up that runs every hour or longer is given a score of 1. Anything that runs more often
 * receives a higher score. Assignments are made based on the score to try to balance the work
 * done by each roll-up server.
 *
 * Assignments are made by the first roll-up server that detects a configuration is not assigned.
 */
public interface RollUpAssignmentStore
{
    /**
     * Returns the last time assignments were modified.
     *
     * @return assignment modification time
     */
    long getLastModifiedTime() throws RollUpException;

    /**
     * Returns a list of all assigned ids.
     *
     * @return list of assigned roll-up ids
     */
    Set<String> getAssignmentIds() throws RollUpException;

    Map<String, String> getAssignments() throws RollUpException;

    /**
     * Returns the list of ids assigned to the given host
     * @param host hostname
     * @return list of ids assigned to the host or an empty list
     */
    Set<String> getAssignedIds(String host) throws RollUpException;

    /**
     * Assigns hostName to the task id.
     * @param unassignedId roll-up task id
     * @param hostName host to assign
     */
    void setAssignment(String unassignedId, String hostName) throws RollUpException;

    /**
     * Removes assignments for the specified ids.
     *
     * @param ids ids to remove
     */
    void removeAssignments(Set<String> ids)
            throws RollUpException;
}
