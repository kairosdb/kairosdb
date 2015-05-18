package org.kairosdb.core.scheduler;

import com.google.inject.Guice;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.rollup.RollUpJob;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;

import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.quartz.TriggerBuilder.newTrigger;

public class KairosDBSchedulerImplTest
{
	private KairosDBScheduler scheduler;

	@Before
	public void setup() throws SchedulerException
	{
		scheduler = new KairosDBSchedulerImpl(Guice.createInjector());
	}

	@Test(expected = NullPointerException.class)
	public void testScheduleNullJobDetailInvalid() throws KairosDBException
	{
		scheduler.schedule(null, newTrigger().build());
	}

	@Test(expected = NullPointerException.class)
	public void testScheduleNullTriggerInvalid() throws KairosDBException
	{
		scheduler.schedule(new JobDetailImpl(), null);
	}

	@Test(expected = NullPointerException.class)
	public void testCancelNullIdInvalid() throws KairosDBException
	{
		scheduler.cancel(null);
	}

	@Test
	public void test() throws KairosDBException
	{

		scheduler.schedule(createJobDetail("1"), createTrigger("1"));
		scheduler.schedule(createJobDetail("2"), createTrigger("2"));

		Set<String> scheduledJobIds = scheduler.getScheduledJobIds();
		assertThat(scheduledJobIds.size(), equalTo(2));
		assertThat(scheduledJobIds, hasItem("1"));
		assertThat(scheduledJobIds, hasItem("2"));

		scheduler.cancel("1");

		scheduledJobIds = scheduler.getScheduledJobIds();
		assertThat(scheduledJobIds.size(), equalTo(1));
		assertThat(scheduledJobIds, hasItem("2"));

		scheduler.cancel("2");

		assertThat(scheduler.getScheduledJobIds().size(), equalTo(0));
	}

	private JobDetail createJobDetail(String key)
	{
		JobDetailImpl jobDetail = new JobDetailImpl();
		jobDetail.setJobClass(RollUpJob.class);
		jobDetail.setKey(new JobKey(key));
		return jobDetail;
	}

	private Trigger createTrigger(String key)
	{
		return newTrigger()
				.withIdentity(key + "-" + this.getClass().getSimpleName())
				.withSchedule(CronScheduleBuilder.cronSchedule("0 */1 * * * ?"))
				.build();
	}
}