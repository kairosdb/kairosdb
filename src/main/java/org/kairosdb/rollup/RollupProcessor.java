package org.kairosdb.rollup;

import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;

public interface RollupProcessor extends Interruptable
{
	/*
			Rollup algorithm

			Sampling size is calculated from the last sampling aggregator for the rollup

			1 - Query for last rollup
				2a - No rollup and no status (First time) - set start time to now - run interval - sampling size
				2b - Rollup found - set start time to be the last rollup time (this will recreate the last rollup)
			4 - Set start and end times on sampling period
			5 - Create a rollup for each sampling interval until you reach now.
		 */
	long process(RollupTaskStatusStore statusStore, RollupTask task, Rollup rollup, QueryMetric rollupQueryMetric)
			throws RollUpException, DatastoreException, InterruptedException;

	long process(RollupTask task, Rollup rollup, QueryMetric rollupQueryMetric, long startTime, long endTime)
			throws DatastoreException, InterruptedException, RollUpException;
}
