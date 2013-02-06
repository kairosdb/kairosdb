// OpenTSDB2
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
package net.opentsdb.core.http;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.google.inject.servlet.GuiceFilter;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 1/13/13
 Time: 8:52 PM
 To change this template use File | Settings | File Templates.
 */
public class JettyServerProvider implements Provider<Server>
{
	public static final String JETTY_PORT_PROPERTY = "opentsdb.jetty.port";
	public static final String JETTY_WEB_ROOT_PROPERTY = "opentsdb.jetty.static_web_root";

	private int m_port;
	private String m_webRoot;
	private GuiceFilter m_guiceFilter;


	@Inject
	public JettyServerProvider(@Named(JETTY_PORT_PROPERTY)int port,
			@Named(JETTY_WEB_ROOT_PROPERTY)String webRoot,
			GuiceFilter guiceFilter)
	{
		m_port = port;
		m_webRoot = webRoot;
		m_guiceFilter = guiceFilter;
	}

	@Override
	public Server get()
	{
		Server server = new Server(m_port);
		ServletContextHandler servletContextHandler =
				new ServletContextHandler();

		servletContextHandler.addFilter(GuiceFilter.class, "/api/*", null);
		servletContextHandler.addServlet(DefaultServlet.class, "/api/*");

		ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setDirectoriesListed(true);
		resourceHandler.setWelcomeFiles(new String[]{"index.html"});
		resourceHandler.setResourceBase(m_webRoot);

		HandlerList handlers = new HandlerList();
		handlers.setHandlers(new Handler[] { servletContextHandler, resourceHandler, new DefaultHandler() });
		server.setHandler(handlers);

		return (server);
	}
}