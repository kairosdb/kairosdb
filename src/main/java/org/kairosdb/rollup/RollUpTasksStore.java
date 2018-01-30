package org.kairosdb.rollup;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages access to the roll up store. These are stored as Roll-id to Roll-up task JSON.
 */
public interface RollUpTasksStore
{
	/**
	 * Writes all tasks to the store. Note this overwrites existing tasks in the store.
	 *
	 * @param tasks tasks to write to the store.
	 */
	void write(List<RollupTask> tasks) throws RollUpException;

	/**
	 * Reads all tasks from the store
	 *
	 * @return all roll up tasks
	 */
	Map<String, RollupTask> read() throws RollUpException;

	/**
	 Removes the task specified by the id.
	 @throws RollUpException if the task could not be removed
	 @param id id of the task to remove
	 */
	void remove(String id) throws RollUpException;

	/**
	 * Returns the task associated with the id.
	 *
	 * @param id task id
	 * @return task or null
	 */
	RollupTask read(String id) throws RollUpException;

	/**
	 * Returns a list of all task ids
	 * @return list of task ids or an empty list
	 */
	Set<String> listIds()
			throws RollUpException;

	/**
	 * Returns the last time the store was modified.
	 * @return when the store was last modified
	 */
    long getLastModifiedTime()
			throws RollUpException;
}
