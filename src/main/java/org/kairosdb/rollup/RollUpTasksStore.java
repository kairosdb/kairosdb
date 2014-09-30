package org.kairosdb.rollup;

import java.util.List;
import java.util.Set;

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
	public void write(List<RollUpTask> tasks) throws RollUpException;

	/**
	 * Reads all tasks from the store
	 *
	 * @return all roll up tasks
	 */
	public Set<RollUpTask> read() throws RollUpException;

	/**
	 * Returns the time the store was last modified.
	 *
	 * @return last modified time
	 */
	public long lastModifiedTime() throws RollUpException;

}
