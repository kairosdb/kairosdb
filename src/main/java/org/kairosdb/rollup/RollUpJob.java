package org.kairosdb.rollup;

import org.kairosdb.core.datastore.DataPointGroup;
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

import java.util.List;

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

					//noinspection ConstantConditions
					dq = datastore.createQuery(queryMetric);
					List<DataPointGroup> result = dq.execute();

					for (DataPointGroup dataPointGroup : result)
					{
						while (dataPointGroup.hasNext())
						{
							dataPointGroup.next();
						}
					}
				}

				// todo add metrics about query
				//			ThreadReporter.addDataPoint(QUERY_TIME, System.currentTimeMillis() - startQuery);
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

	@Override
	public void interrupt()
	{
		interrupted = true;
	}
}
