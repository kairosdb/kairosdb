// KairosDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>
package org.kairosdb.core.http;

import com.google.inject.Scopes;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.kairosdb.core.http.rest.MetricsResource;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import java.util.Properties;


/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 1/13/13
 Time: 9:03 PM
 To change this template use File | Settings | File Templates.
 */
public class WebServletModule extends ServletModule
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

		//Bind resource classes here
		bind(MetricsResource.class).in(Scopes.SINGLETON);

		bind(GuiceContainer.class);
		// hook Jackson into Jersey as the POJO <-> JSON mapper
		bind(JacksonJsonProvider.class).in(Scopes.SINGLETON);
		serve("/*").with(GuiceContainer.class);
	}
}