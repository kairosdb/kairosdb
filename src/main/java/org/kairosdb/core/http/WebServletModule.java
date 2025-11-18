/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.core.http;

import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.servlet.ServletModule;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import jakarta.ws.rs.Path;
import org.eclipse.jetty.ee10.servlets.QoSFilter;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;
import org.kairosdb.core.KairosRootConfig;
import org.kairosdb.core.http.exceptionmapper.InvalidServerTypeExceptionMapper;
import org.kairosdb.core.http.rest.AdminResource;
import org.kairosdb.core.http.rest.FeaturesResource;
import org.kairosdb.core.http.rest.MetadataResource;
import org.kairosdb.core.http.rest.MetricsResource;

import jakarta.inject.Inject;
import jakarta.ws.rs.core.Feature;
import jakarta.ws.rs.core.FeatureContext;
import org.glassfish.jersey.internal.inject.InjectionManager;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WebServletModule extends ServletModule
{
	private Set<Class<?>> m_resourceClasses = new HashSet<>();

	public WebServletModule(KairosRootConfig props)
	{
	}


	@Override
	protected void configureServlets()
	{
		binder().requireExplicitBindings();

		//Bind web server
		bind(WebServer.class);
		bind(ServletContainer.class).in(Scopes.SINGLETON);

		//Bind resource classes here - these will be injected by Guice and bridged to HK2
		bind(MetricsResource.class).in(Scopes.SINGLETON);
		bind(MetadataResource.class).in(Scopes.SINGLETON);
		bind(FeaturesResource.class).in(Scopes.SINGLETON);
		bind(AdminResource.class).in(Scopes.SINGLETON);

		//Bind filters
		bind(LoggingFilter.class).in(Scopes.SINGLETON);
		filter("/*").through(LoggingFilter.class);

		// Bind providers and exception mappers
		bind(JacksonJsonProvider.class).in(Scopes.SINGLETON);
		bind(InvalidServerTypeExceptionMapper.class).in(Scopes.SINGLETON);

		// Configure Jersey 3 ServletContainer with HK2-Guice bridge
		/*Map<String, String> jerseyParams = new HashMap<>();
		jerseyParams.put("jakarta.ws.rs.Application", KairosResourceConfig.class.getName());
		serve("/*").with(ServletContainer.class, jerseyParams);*/

		bindListener(Matchers.any(), new TypeListener()
		{
			@Override
			public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter)
			{
				Class<?> clazz = type.getRawType();

				if (clazz.isAnnotationPresent(Path.class))
					m_resourceClasses.add(clazz);
			}
		});
	}

	@Provides
	@ResourceClasses
	public Set<Class<?>> getResources()
	{
		return m_resourceClasses;
	}

}
