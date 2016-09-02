package org.kairosdb.rollup;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.http.rest.json.RelativeTime;
import org.kairosdb.core.reporting.ThreadReporter;
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

	private static final String ROLLUP_TIME = "kairosdb.rollup.execution-time";

	protected static final int TOO_OLD_MULTIPLIER = 4;
	private boolean interrupted;
	private LongDataPointFactory longDataPointFactory = new LongDataPointFactoryImpl();

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
			String hostName = (String) dataMap.get("hostName");
			checkState(task != null, "Task was null");
			checkState(datastore != null, "Datastore was null");
			checkState(hostName != null, "hostname was null");

			for (Rollup rollup : task.getRollups())
			{
				log.info("Executing Rollup Task: " + task.getName() + " for Rollup  " + rollup.getSaveAs());

				if (interrupted)
					break;

				for (QueryMetric queryMetric : rollup.getQueryMetrics())
				{
					boolean success = true;
					long startQueryTime = System.currentTimeMillis();
					try
					{
						if (interrupted)
							break;

						DataPoint rollupDataPoint = getLastRollupDataPoint(datastore, rollup.getSaveAs(), startQueryTime);
						queryMetric.setStartTime(calculateStartTime(rollupDataPoint, getLastSampling(queryMetric.getAggregators()), startQueryTime));
						queryMetric.setEndTime(calculateEndTime(rollupDataPoint, task.getExecutionInterval(), startQueryTime));
						long dpCount = executeRollup(datastore, queryMetric);
						log.info("Rollup Task: " + task.getName() + " for Rollup " + rollup.getSaveAs() + " data point count of " + dpCount);

						if (dpCount == 0 && rollupDataPoint != null)
						{
							// Advance forward if a data point exists for the query metric
							DataPoint dataPoint = getFutureDataPoint(datastore, queryMetric.getName(), startQueryTime, rollupDataPoint);
							queryMetric.setStartTime(calculateStartTime(dataPoint, getLastSampling(queryMetric.getAggregators()), startQueryTime));
							queryMetric.setEndTime(calculateEndTime(dataPoint, task.getExecutionInterval(), startQueryTime));
							dpCount = executeRollup(datastore, queryMetric);
							log.info("Tried again Rollup Task: " + task.getName() + " for Rollup " + rollup.getSaveAs() + " data point count of " + dpCount);
						}
					}
					catch (DatastoreException e)
					{
						success = false;
						log.error("Failed to execute query for roll-up task: " + task.getName() + " roll-up: " + rollup.getSaveAs(), e);
					}
					catch (Exception e)
					{
						success = false;
						log.error("Failed to roll-up task: " + task.getName() + " roll-up: " + rollup.getSaveAs(), e);
					}
					finally
					{
						ThreadReporter.setReportTime(System.currentTimeMillis());
						ThreadReporter.clearTags();
						ThreadReporter.addTag("host", hostName);
						ThreadReporter.addTag("rollup", rollup.getSaveAs());
						ThreadReporter.addTag("rollup-task", task.getName());
						ThreadReporter.addTag("status", success ? "success" : "failure");
						ThreadReporter.addDataPoint(ROLLUP_TIME, System.currentTimeMillis() - ThreadReporter.getReportTime());
						ThreadReporter.submitData(longDataPointFactory, datastore);
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
	 null then it returns the start time for one sampling period before now.
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
