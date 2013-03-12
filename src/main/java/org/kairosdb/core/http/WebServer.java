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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.inject.servlet.GuiceFilter;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WebServer implements KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(WebServer.class);

	public static final String JETTY_PORT_PROPERTY = "kairosdb.jetty.port";
	public static final String JETTY_WEB_ROOT_PROPERTY = "kairosdb.jetty.static_web_root";

	private int m_port;
	private String m_webRoot;
	private Server m_server;

	@Inject
	public WebServer(@Named(JETTY_PORT_PROPERTY)int port,
			@Named(JETTY_WEB_ROOT_PROPERTY)String webRoot)
	{
		m_port = port;
		m_webRoot = webRoot;
	}

	@Override
	public void start() throws TsdbException
	{
		try
		{
			m_server = new Server(m_port);
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
			m_server.setHandler(handlers);

			m_server.start();
		}
		catch (Exception e)
		{
			throw new TsdbException(e);
		}
	}

	@Override
	public void stop()
	{
		try
		{
			m_server.stop();
			m_server.join();
		}
		catch (Exception e)
		{
			logger.error("Error stopping web server", e);
		}
	}
}
