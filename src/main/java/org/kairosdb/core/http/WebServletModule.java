/*
 * Copyright 2013 Proofpoint Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceFilter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.eclipse.jetty.servlets.GzipFilter;
import org.kairosdb.core.http.rest.MetricsResource;
import org.kairosdb.core.http.rest.QueryAutocompleter;
import org.kairosdb.core.http.rest.metrics.DefaultQueryMeasurementProvider;
import org.kairosdb.core.http.rest.metrics.QueryMeasurementProvider;

import javax.ws.rs.core.MediaType;
import java.util.Properties;

public class WebServletModule extends JerseyServletModule
{
	public WebServletModule(Properties props)
	{
	}

	@Override
	protected void configureServlets()
	{
		binder().requireExplicitBindings();
		bind(GuiceFilter.class);

		//Bind web server
		bind(WebServer.class);

		bind(QueryMeasurementProvider.class).to(DefaultQueryMeasurementProvider.class).in(Singleton.class);
		bind(QueryAutocompleter.class).in(Singleton.class);

		//Bind resource classes here
		bind(MetricsResource.class).in(Scopes.SINGLETON);

		bind(GuiceContainer.class);

		ImmutableMap<String, String> params = new ImmutableMap.Builder<String, String>()
				.put("mimeTypes", MediaType.APPLICATION_JSON)
				.put("methods", "GET,POST")
				.build();
		bind(GzipFilter.class).in(Scopes.SINGLETON);
		filter("/*").through(GzipFilter.class, params);

		// hook Jackson into Jersey as the POJO <-> JSON mapper
		bind(JacksonJsonProvider.class).in(Scopes.SINGLETON);
		serve("/*").with(GuiceContainer.class);


	}
}