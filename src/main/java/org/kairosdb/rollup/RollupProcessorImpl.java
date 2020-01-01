package org.kairosdb.rollup;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;
import java.util.List;
import org.joda.time.DateTimeZone;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.Order;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.plugin.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollupProcessorImpl implements RollupProcessor
{
	private static final Logger log = LoggerFactory.getLogger(RollupProcessorImpl.class);

	private final KairosDatastore datastore;
	private boolean interrupted;

	public RollupProcessorImpl(KairosDatastore datastore)
	{
		this.datastore = checkNotNull(datastore, "datastore must not be null");
	}

	/*
		Rollup algorithm

		Sampling size is calculated from the last sampling aggregator for the rollup

		1 - Query for last rollup
			2a - No rollup and no status (First time) - set start time to run interval + sampling size
			2b - Rollup found - set start time to be the last rollup time (this will recreate the last rollup)
		3 - Set all sampling aggregators to have align_sampling=true (default?)
		4 - Set start and end times on sampling period
		5 - Create a rollup for each sampling interval until you reach now.
	 */
	@Override
	public long process(RollupTaskStatusStore statusStore, RollupTask task, Rollup rollup, QueryMetric rollupQueryMetric)
			throws RollUpException, DatastoreException, InterruptedException
	{
		long now = now();
		Sampling samplingSize = getSamplingSize(rollupQueryMetric.getAggregators());
		long lastExecutionTime = getLastExecutionTime(statusStore, task, now);
		if (log.isDebugEnabled())
			log.debug("LastExecutionTime = " + new Date(lastExecutionTime));


		long startTime = calculateStartTime(task.getExecutionInterval(), samplingSize, lastExecutionTime, now);
		if (log.isDebugEnabled())
			log.debug("startTime = " + new Date(startTime));

		return process(task, rollup, rollupQueryMetric, startTime, now);
	}

	@Override
	public long process(RollupTask task, Rollup rollup, QueryMetric rollupQueryMetric, long startTime, long endTime)
			throws DatastoreException, InterruptedException
	{
		DateTimeZone timeZone = rollup.getTimeZone();
		if (timeZone == null) {
			timeZone = DateTimeZone.UTC;
		}
		Sampling samplingSize = getSamplingSize(rollupQueryMetric.getAggregators());
		List<SamplingPeriod> samplingPeriods = RollupUtil.getSamplingPeriodsAlignedToUnit(samplingSize, startTime, endTime, timeZone.toTimeZone());

		if (log.isDebugEnabled())
		{
			for (SamplingPeriod samplingPeriod : samplingPeriods)
			{
				log.debug("Sampling period " + samplingPeriod);
			}
		}

		long dpCount = 0;
		// Note: there will always be at least 2 sampling periods (start and end)
		for (SamplingPeriod samplingPeriod : samplingPeriods)
		{
			if (interrupted)
			{
				break;
			}
			rollupQueryMetric.setStartTime(samplingPeriod.getStartTime());
			rollupQueryMetric.setEndTime(samplingPeriod.getEndTime());
			dpCount += executeRollup(datastore, rollupQueryMetric);

			log.debug("Rollup Task: " + task.getName() + " for Rollup " + task.getName() + " data point count of " + dpCount);
			Thread.sleep(50);
		}
		return dpCount;
	}

	/**
	 Returns the sampling from the last RangeAggregator in the aggregators list
	 or null if no sampling is found
	 */
	private static Sampling getSamplingSize(List<Aggregator> aggregators)
	{
		for (int i = aggregators.size() - 1; i >= 0; i--)
		{
			Aggregator aggregator = aggregators.get(i);
			if (aggregator instanceof RangeAggregator)
			{
				return ((RangeAggregator) aggregator).getSampling();
			}
		}
		return null;
	}

	private long executeRollup(KairosDatastore datastore, QueryMetric query) throws DatastoreException
	{
		log.debug("Execute Rollup: " + query.getName() + " Start time: " + new Date(query.getStartTime()) + " End time: " + new Date(query.getEndTime()));

		int dpCount = 0;
		try (DatastoreQuery dq = datastore.createQuery(query))
		{
			List<DataPointGroup> result = dq.execute();

			for (DataPointGroup dataPointGroup : result)
			{
				while (dataPointGroup.hasNext())
				{
					dataPointGroup.next();
					dpCount++;
				}
			}
		}

		return dpCount;
	}

	private static long now()
	{
		return System.currentTimeMillis();
	}

	private static DataPoint performQuery(KairosDatastore datastore, QueryMetric rollupQuery) throws DatastoreException
	{
		try (DatastoreQuery query = datastore.createQuery(rollupQuery))
		{
			List<DataPointGroup> rollupResult = query.execute();

			DataPoint dataPoint = null;
			for (DataPointGroup dataPointGroup : rollupResult)
			{
				while (dataPointGroup.hasNext())
				{
					DataPoint next = dataPointGroup.next();
					if (next.getApiDataType().equals(DataPoint.API_DOUBLE) ||
							next.getApiDataType().equals(DataPoint.API_LONG))
					{
						dataPoint = next;
					}
				}
			}
			return dataPoint;
		}
	}

	private long getLastExecutionTime(RollupTaskStatusStore statusStore, RollupTask task, long now)
			throws RollUpException, DatastoreException
	{
		long lastExecutionTime = 0L;
		// get last status
		RollupTaskStatus status = statusStore.read(task.getId());
		if (status != null)
			return geStatusExecutionTime(status);
		else
		{
			// get last rollup
			DataPoint lastRollup = getLastRollup(datastore, task.getName(), now);
			if (lastRollup != null)
				lastExecutionTime = lastRollup.getTimestamp();
		}
		return lastExecutionTime;
	}

	// Find the last execution time
	private static long geStatusExecutionTime(RollupTaskStatus status)
	{
		if (status == null)
		{
			return 0L;
		}
		long lastExecutionTime = 0L;
		for (RollupQueryMetricStatus metricStatus : status.getStatuses())
		{
			lastExecutionTime = Math.max(lastExecutionTime, metricStatus.getLastExecutionTime());
		}
		return lastExecutionTime;
	}

	/**
	 Returns the last data point the rollup created
	 */
	private static DataPoint getLastRollup(KairosDatastore datastore, String rollupName, long now)
			throws DatastoreException
	{
		QueryMetric rollupQuery = new QueryMetric(0, now, 0, rollupName);
		rollupQuery.setLimit(1);
		rollupQuery.setOrder(Order.DESC);

		return performQuery(datastore, rollupQuery);
	}

	/**
	 Returns the last execution time if there was one or the (run interval + sampling size) from now
	 */
	private static long calculateStartTime(Duration executionInterval, Sampling samplingSize, long lastExecutionTime, long now)
	{
		return lastExecutionTime > 0 ? lastExecutionTime : RollupUtil.subtract(RollupUtil.subtract(now, executionInterval), samplingSize);
	}

	@Override
	public void interrupt()
	{
		interrupted = true;
	}
}
