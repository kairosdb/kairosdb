package org.kairosdb.rollup;

import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.SaveAsAggregator;
import org.kairosdb.core.aggregator.TrimAggregator;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.scheduler.KairosDBSchedulerImpl;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.inject.internal.util.$Preconditions.checkState;

public class RollUpJob implements InterruptableJob
{
	private static final Logger log = LoggerFactory.getLogger(KairosDBSchedulerImpl.class);

	private boolean interrupted;

	public RollUpJob()
	{
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		JobDataMap dataMap = jobExecutionContext.getMergedJobDataMap();
		RollupTask task = (RollupTask) dataMap.get("task");
		KairosDatastore datastore = (KairosDatastore) dataMap.get("datastore");
		checkState(task != null, "Task was null");
		checkState(datastore != null, "Datastore was null");


		DatastoreQuery dq = null;
		try
		{
			//noinspection ConstantConditions
			for (Rollup rollup : task.getRollups())
			{
				if (interrupted)
					break;

				for (QueryMetric queryMetric : rollup.getQueryMetrics())
				{
					if (interrupted)
						break;

					if (!hasAggregator(queryMetric, TrimAggregator.class))
						queryMetric.addAggregator(new TrimAggregator(TrimAggregator.Trim.LAST));

					if (!hasAggregator(queryMetric, SaveAsAggregator.class))
					{

						//noinspection ConstantConditions
						queryMetric.addAggregator(new SaveAsAggregator(datastore.getDatastore()));
					}

					//noinspection ConstantConditions
					dq = datastore.createQuery(queryMetric);
					dq.execute();
					// todo add metrics about query
					//			ThreadReporter.addDataPoint(QUERY_TIME, System.currentTimeMillis() - startQuery);
				}
			}
		}
		catch (DatastoreException e)
		{
			log.error("Failed to execute query", e);
		}
		finally
		{
			if (dq != null)
				dq.close();
		}
	}

	private boolean hasAggregator(QueryMetric queryMetric, Class aggregatorClass)
	{
		for (Aggregator aggregator : queryMetric.getAggregators())
		{
			if (aggregator.getClass().getName().equals(aggregatorClass.getName()))
				return true;
		}
		return false;
	}

	@Override
	public void interrupt()
	{
		interrupted = true;
	}
}
