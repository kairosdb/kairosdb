package org.kairosdb.core.scheduler;

import org.kairosdb.core.exception.KairosDBException;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Trigger;

import java.util.Set;

public interface KairosDBScheduler
{
	void start() throws KairosDBException;

	void stop();

	/**
	 Schedules a job with the specified id and trigger

	 @param jobDetail job id
	 @param trigger   job trigger
	 @throws KairosDBException if the job could not be schedule
	 */
	void schedule(JobDetail jobDetail, Trigger trigger) throws KairosDBException;

	/**
	 Cancels a scheduled job.

	 @param jobKey key of the job to cancel
	 @throws KairosDBException if the job could not be canceled
	 */
	void cancel(JobKey jobKey) throws KairosDBException;

	/**
	 Returns a list of schedule job ids

	 @return list of scheduled job ids
	 @throws KairosDBException if could not get the list
	 */
	Set<String> getScheduledJobIds() throws KairosDBException;
}
