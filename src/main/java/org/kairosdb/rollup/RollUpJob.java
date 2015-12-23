package org.kairosdb.rollup;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.http.rest.json.RelativeTime;
import org.kairosdb.core.scheduler.KairosDBSchedulerImpl;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.inject.internal.util.$Preconditions.checkState;

public class RollUpJob implements InterruptableJob
{
	private static final Logger log = LoggerFactory.getLogger(KairosDBSchedulerImpl.class);

	protected static final int TOO_OLD_MULTIPLIER = 4;

	private boolean interrupted;

	public RollUpJob()
	{
	}

	@SuppressWarnings("ConstantConditions")
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		try
		{
			JobDataMap dataMap = jobExecutionContext.getMergedJobDataMap();
			RollupTask task = (RollupTask) dataMap.get("task");
			KairosDatastore datastore = (KairosDatastore) dataMap.get("datastore");
			checkState(task != null, "Task was null");
			checkState(datastore != null, "Datastore was null");

			for (Rollup rollup : task.getRollups())
			{
				log.info("Executing Rollup Task: " + task.getName() + " for Rollup  " + rollup.getSaveAs());

				if (interrupted)
					break;

				for (QueryMetric queryMetric : rollup.getQueryMetrics())
				{
					try
					{
						if (interrupted)
							break;

						long now = System.currentTimeMillis();
						DataPoint rollupDataPoint = getLastRollupDataPoint(datastore, rollup.getSaveAs(), now);
						queryMetric.setStartTime(calculateStartTime(rollupDataPoint, getLastSampling(queryMetric.getAggregators()), now));
						queryMetric.setEndTime(calculateEndTime(rollupDataPoint, task.getExecutionInterval(), now));
						long dpCount = executeRollup(datastore, queryMetric);
						log.info("Rollup Task: " + task.getName() + " for Rollup " + rollup.getSaveAs() + " data point count of " + dpCount);

						if (dpCount == 0)
						{
							// Advance forward if a data point exists for the query metric
							DataPoint dataPoint = getFutureDataPoint(datastore, queryMetric.getName(), now, rollupDataPoint);
							queryMetric.setStartTime(calculateStartTime(dataPoint, getLastSampling(queryMetric.getAggregators()), now));
							queryMetric.setEndTime(calculateEndTime(dataPoint, task.getExecutionInterval(), now));
							dpCount = executeRollup(datastore, queryMetric);
							log.info("Tried again Rollup Task: " + task.getName() + " for Rollup " + rollup.getSaveAs() + " data point count of " + dpCount);
						}

						// todo add metrics about query include failure cases
						//			ThreadReporter.addDataPoint(QUERY_TIME, System.currentTimeMillis() - startQuery);
					}
					catch (DatastoreException e)
					{
						log.error("Failed to execute query for roll-up task: " + task.getName() + " roll-up: " + rollup.getSaveAs(), e);
					}
					catch (Exception e)
					{
						log.error("Failed to roll-up task: " + task.getName() + " roll-up: " + rollup.getSaveAs(), e);
					}
				}
			}
		}
		catch (Throwable t)
		{
			log.error("Failed to execute job " + jobExecutionContext.toString(), t);
		}
	}


	private long executeRollup(KairosDatastore datastore, QueryMetric query) throws DatastoreException
	{
		log.info("Execute Rollup: Start time: " + new Date(query.getStartTime()) + " End time: " + new Date(query.getEndTime()));

		int dpCount = 0;
		DatastoreQuery dq = datastore.createQuery(query);
		try
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
		finally
		{
			if (dq != null)
				dq.close();
		}

		return dpCount;
	}

	/**
	 Returns the last data point the rollup created
	 */
	protected static DataPoint getLastRollupDataPoint(KairosDatastore datastore, String rollupName, long now) throws DatastoreException
	{
		QueryMetric rollupQuery = new QueryMetric(0, now, 0, rollupName);
		rollupQuery.setLimit(1);
		rollupQuery.setOrder(Order.DESC);

		return performQuery(datastore, rollupQuery);
	}

	/**
	 Returns the next data point for the metric given a starting data point
	 */
	protected static DataPoint getFutureDataPoint(KairosDatastore datastore, String metricName, long now, DataPoint startPoint) throws DatastoreException
	{
		QueryMetric rollupQuery = new QueryMetric(startPoint.getTimestamp() + 1, now, 0, metricName);
		rollupQuery.setLimit(1);
		rollupQuery.setOrder(Order.ASC);

		return performQuery(datastore, rollupQuery);
	}

	private static DataPoint performQuery(KairosDatastore datastore, QueryMetric rollupQuery) throws DatastoreException
	{
		DatastoreQuery query = null;
		try
		{
			query = datastore.createQuery(rollupQuery);
			List<DataPointGroup> rollupResult = query.execute();

			DataPoint dataPoint = null;
			for (DataPointGroup dataPointGroup : rollupResult)
			{
				while (dataPointGroup.hasNext())
				{
					dataPoint = dataPointGroup.next();
				}
			}

			return dataPoint;
		}
		finally
		{
			if (query != null)
				query.close();
		}
	}

	/**
	 Returns the time stamp of the specified data point. If the data point is
	 null then it returns the start time for one sampling period.
	 */
	protected static long calculateStartTime(DataPoint dataPoint, Sampling lastSampling, long now)
	{
		checkNotNull(lastSampling, "At least one aggregators in the query must be a RangeAggregator.");

		if (dataPoint == null)
		{
			// go back one unit of time
			RelativeTime samplingTime = new RelativeTime((int) lastSampling.getValue(), lastSampling.getUnit());
			return samplingTime.getTimeRelativeTo(now);
		}
		else
		{
			return dataPoint.getTimestamp();
		}
	}

	/**
	 Returns now if the data point is null. If the data point is not null
	 and its time stamp is too old, return a time that is 4 intervals from
	 the data point time.
	 */
	protected static long calculateEndTime(DataPoint datapoint, Duration executionInterval, long now)
	{
		long endTime = now;

		RelativeTime relativeTime = new RelativeTime((int) (TOO_OLD_MULTIPLIER * executionInterval.getValue()), executionInterval.getUnit());
		if (datapoint != null && datapoint.getTimestamp() < relativeTime.getTimeRelativeTo(now))
		{
			// last time was too old. Only do part of the rollup
			endTime = relativeTime.getFutureTimeRelativeTo(datapoint.getTimestamp());
		}
		return endTime;
	}

	/**
	 Returns the sampling from the last RangeAggregator in the aggregators list
	 or null if no sampling is found
	 */
	protected static Sampling getLastSampling(List<Aggregator> aggregators)
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

	@Override
	public void interrupt()
	{
		interrupted = true;
	}
}
