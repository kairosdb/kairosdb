package org.kairosdb.rollup;

/**
 * Stores the status of Rollup tasks. The key is the rollup task id and the value is the status specified as JSON.
 */
public interface RollupTaskStatusStore
{
    /**
     * Writes the status of a rollup task to the store.
     *
     * @param  id task id
     * @param status status to write to the store.
     */
    void write(String id, RollupTaskStatus status) throws RollUpException;

    /**
     * Reads the task associated with a given rollup task.
     *
     * @param id task id to get status for
     * @return status associated with the task id or null if not found
     */
    RollupTaskStatus read(String id) throws RollUpException;

     /**
     * Removes the status specified by the task id.
     *
     * @param id task id of the status to remove
     * @throws RollUpException if the task could not be removed
     */
    void remove(String id) throws RollUpException;

}
