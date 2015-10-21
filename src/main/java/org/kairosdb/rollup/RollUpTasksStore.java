package org.kairosdb.rollup;

import org.kairosdb.core.http.rest.QueryException;

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
	void write(List<RollupTask> tasks) throws RollUpException, QueryException;

	/**
	 * Reads all tasks from the store
	 *
	 * @return all roll up tasks
	 */
	List<RollupTask> read() throws RollUpException, QueryException;
	/**
	 * Returns the time the store was last modified.

	 *
	 * @return last modified time
	 */
	long lastModifiedTime() throws RollUpException;

	void remove(String id) throws IOException, RollUpException, QueryException;
}
