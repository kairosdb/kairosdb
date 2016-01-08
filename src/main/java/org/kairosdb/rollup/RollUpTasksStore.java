package org.kairosdb.rollup;

import java.io.IOException;
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
	 @param id id of the task to remove
	 @throws IOException if the task could not be removed
	 */
	void remove(String id) throws IOException;

	/**
	 Adds the listener to be notified when a task is added, changed, or removed.
	 @param listener listener to notify
	 */
	void addListener(RollupTaskChangeListener listener);

	}
