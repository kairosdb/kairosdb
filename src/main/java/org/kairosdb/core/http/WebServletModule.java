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

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Scopes;
import com.google.inject.servlet.GuiceFilter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.kairosdb.core.KairosRootConfig;
import org.kairosdb.core.http.exceptionmapper.InvalidServerTypeExceptionMapper;
import org.kairosdb.core.http.rest.AdminResource;
import org.kairosdb.core.http.rest.FeaturesResource;
import org.kairosdb.core.http.rest.MetadataResource;
import org.kairosdb.core.http.rest.MetricsResource;

public class WebServletModule extends JerseyServletModule
{
	public WebServletModule(KairosRootConfig props)
	{
	}

	@Override
	protected void configureServlets()
	{
		binder().requireExplicitBindings();
		bind(GuiceFilter.class);

		//Bind web server
		bind(WebServer.class);

		//Bind resource classes here
		bind(MetricsResource.class).in(Scopes.SINGLETON);
		bind(MetadataResource.class).in(Scopes.SINGLETON);
		bind(FeaturesResource.class).in(Scopes.SINGLETON);
		bind(AdminResource.class).in(Scopes.SINGLETON);

		bind(GuiceContainer.class);

		bind(LoggingFilter.class).in(Scopes.SINGLETON);
		filter("/*").through(LoggingFilter.class);

		// hook Jackson into Jersey as the POJO <-> JSON mapper
		bind(JacksonJsonProvider.class).in(Scopes.SINGLETON);
		serve("/*").with(GuiceContainer.class);

		//
		bind(InvalidServerTypeExceptionMapper.class).in(Scopes.SINGLETON);
	}
}
