package org.kairosdb.rollup;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import org.kairosdb.core.http.rest.RollUpResource;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class RollUpModule extends AbstractModule
{

	static final String ROLLUP_EXECUTOR = "RollupExecutor";

	@Override
	protected void configure()
	{
		bind(RollUpResource.class).in(Scopes.SINGLETON);
		bind(SchedulingManager.class).in(Scopes.SINGLETON);
		bind(AssignmentManager.class).in(Scopes.SINGLETON);
		bind(RollUpTasksStore.class).to(RollUpTasksStoreImpl.class).in(Scopes.SINGLETON);
		bind(RollUpAssignmentStore.class).to(RollUpAssignmentStoreImpl.class).in(Scopes.SINGLETON);
		bind(BalancingAlgorithm.class).to(ScoreBalancingAlgorithm.class).in(Scopes.SINGLETON);
		bind(RollupTaskStatusStore.class).to(RollupTaskStatusStoreImpl.class).in(Scopes.SINGLETON);
		bind(RollUpJob.class);
	}

	@Provides
	@Named(ROLLUP_EXECUTOR)
	public ScheduledExecutorService getExecutorService()
	{
		return Executors.newSingleThreadScheduledExecutor(
				new ThreadFactoryBuilder().setNameFormat("Roll-up-Modification-Checker-%s").build());

	}
}
