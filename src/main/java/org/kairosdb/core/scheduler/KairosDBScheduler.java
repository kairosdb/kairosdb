package org.kairosdb.core.scheduler;

import org.kairosdb.core.exception.KairosDBException;

import java.util.Set;

public interface KairosDBScheduler
{
	void start() throws KairosDBException;

	void stop();

	/**
	 Schedules a job with an id of the class name

	 @param job job to schedule
	 @throws KairosDBException if the job could not be scheduled
	 */
	void schedule(KairosDBJob job) throws KairosDBException;

	/**
	 Schedules a job with the specified id

	 @param id job id
	 @param job job to schedule
	 @throws KairosDBException if the job could not be schedule
	 */
	void schedule(String id, KairosDBJob job) throws KairosDBException;

	/**
	 Cancels a scheduled job.

	 @param id id of the job to cancel
	 @throws KairosDBException if the job could not be canceled
	 */
	void cancel(String id) throws KairosDBException;

	/**
	 Returns a list of schedule job ids
	 @return list of scheduled job ids
	 @throws KairosDBException if could not get the list
	 */
	Set<String> getScheduledJobIds() throws KairosDBException;
}
