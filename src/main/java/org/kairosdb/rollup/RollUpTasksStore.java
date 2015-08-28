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

	 *
	 * @return last modified time
	 */
	long lastModifiedTime() throws RollUpException;

}
