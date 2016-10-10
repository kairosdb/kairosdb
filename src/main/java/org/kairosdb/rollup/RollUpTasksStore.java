package org.kairosdb.rollup;

import java.util.List;

/**
 * Manages access to the roll up task store.
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
	List<RollupTask> read() throws RollUpException;
	/**
	 * Returns the time the store was last modified.

	/**
	 Removes the task specified by the id.
	 @throws RollUpException if the task could not be removed
	 @param id id of the task to remove
	 */
	void remove(String id) throws RollUpException;

	/**
	 Adds the listener to be notified when a task is added, changed, or removed.
	 @param listener listener to notify
	 */
	void addListener(RollupTaskChangeListener listener);

	}
