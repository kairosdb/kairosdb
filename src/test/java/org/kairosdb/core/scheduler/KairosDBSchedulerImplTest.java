package org.kairosdb.core.scheduler;

import com.google.inject.Guice;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.exception.KairosDBException;
import org.quartz.*;

import java.util.Set;
import java.util.UUID;

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
	public void testScheduleNullJobInvalid() throws KairosDBException
	{
		scheduler.schedule(null);
	}

	@Test(expected = NullPointerException.class)
	public void testScheduleNullIdInvalid() throws KairosDBException
	{
		scheduler.schedule(null, new TestJob());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testScheduleEmptyIdInvalid() throws KairosDBException
	{
		scheduler.schedule("", new TestJob());
	}

	@Test(expected = NullPointerException.class)
	public void testCancelNullIdInvalid() throws KairosDBException
	{
		scheduler.cancel(null);
	}

	@Test
	public void test() throws KairosDBException
	{
		scheduler.schedule("1",new TestJob());
		scheduler.schedule("2",new TestJob());

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

	private class TestJob implements KairosDBJob
	{
		@Override
		public Trigger getTrigger()
		{
			return newTrigger()
					.withIdentity(UUID.randomUUID() + "-" + this.getClass().getSimpleName())
					.withSchedule(CronScheduleBuilder.cronSchedule("0 */1 * * * ?"))
					.build();
		}

		@Override
		public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
		{
		}

		@Override
		public void interrupt()
		{
		}
	}
}