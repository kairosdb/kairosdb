package org.kairosdb.rollup;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.kairosdb.core.http.rest.RollUpResource;

public class RollUpModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(RollUpResource.class).in(Scopes.SINGLETON);
		bind(RollUpManager.class).in(Scopes.SINGLETON);
		bind(RollUpTasksStore.class).to(RollUpTasksFileStore.class).in(Scopes.SINGLETON);
		bindConstant().annotatedWith(Names.named("STORE_DIRECTORY")).to("/tmp");
	}
}
