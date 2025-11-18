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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.typesafe.config.Config;
import org.eclipse.jetty.ee10.servlet.DefaultServlet;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.security.ConstraintMapping;
import org.eclipse.jetty.ee10.servlet.security.ConstraintSecurityHandler;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.security.Constraint;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.jaas.JAASLoginService;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.QoSHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.eclipse.jetty.util.ssl.KeyStoreScanner;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.glassfish.jersey.servlet.ServletContainer;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.EnumSet;

import jakarta.servlet.DispatcherType;

import static java.util.Objects.requireNonNull;
import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;


public class WebServer implements KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(WebServer.class);
	public static final int LOG_RETAIN_DAYS = 30;

	public static final String QOS_CONFIG = "kairosdb.qos";

	public static final String JETTY_ADDRESS_PROPERTY = "kairosdb.jetty.address";
	public static final String JETTY_PORT_PROPERTY = "kairosdb.jetty.port";
	public static final String JETTY_WEB_ROOT_PROPERTY = "kairosdb.jetty.static_web_root";
	public static final String JETTY_SOCKET_IDLE_TIMEOUT = "kairosdb.jetty.socket_idle_timeout";
	public static final String JETTY_SSL_PORT = "kairosdb.jetty.ssl.port";
	public static final String JETTY_SSL_PROTOCOLS = "kairosdb.jetty.ssl.protocols";
	public static final String JETTY_SSL_CIPHER_SUITES = "kairosdb.jetty.ssl.cipherSuites";
	public static final String JETTY_SSL_KEYSTORE_PATH = "kairosdb.jetty.ssl.keystore.path";
	public static final String JETTY_SSL_KEYSTORE_PASSWORD = "kairosdb.jetty.ssl.keystore.password";
	public static final String JETTY_SSL_KEYSTORE_SCANNER_INTERVAL = "kairosdb.jetty.ssl.keystore.scanner_interval";
	public static final String JETTY_SSL_TRUSTSTORE_PATH = "kairosdb.jetty.ssl.truststore.path";
	public static final String JETTY_THREADS_QUEUE_SIZE_PROPERTY = "kairosdb.jetty.threads.queue_size";
	public static final String JETTY_THREADS_MIN_PROPERTY = "kairosdb.jetty.threads.min";
	public static final String JETTY_THREADS_MAX_PROPERTY = "kairosdb.jetty.threads.max";
	public static final String JETTY_THREADS_KEEP_ALIVE_MS_PROPERTY = "kairosdb.jetty.threads.keep_alive_ms";
	public static final String JETTY_SHOW_STACKTRACE = "kairosdb.jetty.show_stacktrace";
	public static final String JETTY_AUTH_MODULE_NAME = "kairosdb.jetty.auth_module_name";
	public static final String JETTY_REQUEST_LOGGING_ENABLED = "kairosdb.jetty.request_logging.enabled";
	public static final String JETTY_REQUEST_LOGGING_RETAIN_DAYS = "kairosdb.jetty.request_logging.retain_days";
	public static final String JETTY_REQUEST_LOGGING_IGNORE_PATHS = "kairosdb.jetty.request_logging.ignore_paths";


	private InetAddress m_address;
	private int m_port;
	private String m_webRoot;
	private Server m_server;
	private final int m_idleTimeout;
	private int m_sslPort;
	private String[] m_cipherSuites;
	private String[] m_protocols;
	private String m_keyStorePath;
	private String m_keyStorePassword;
	private int m_keyStoreScannerInterval = 3600; //Defaults to 1 hour
	private String m_trustStorePath = null;
	private QueuedThreadPool m_pool;
	private boolean m_showStacktrace;
	private String m_authModuleName = null;
	private int m_requestLoggingRetainDays = LOG_RETAIN_DAYS;
	private boolean m_requestLoggingEnabled;
	private String[] m_loggingIgnorePaths;
	private Config m_qosConfig;

	@Inject
	private Injector m_injector;

	public WebServer(int port, String webRoot)
			throws UnknownHostException
	{
		this(null, port, webRoot, 120000);
	}

	@Inject
	public WebServer(@Named(JETTY_ADDRESS_PROPERTY) String address,
			@Named(JETTY_PORT_PROPERTY) int port,
			@Named(JETTY_WEB_ROOT_PROPERTY) String webRoot,
			@Named(JETTY_SOCKET_IDLE_TIMEOUT) int idleTimeout)
			throws UnknownHostException
	{
		requireNonNull(webRoot);

		m_port = port;
		m_webRoot = webRoot;
		m_address = InetAddress.getByName(address);
		m_idleTimeout = idleTimeout;
	}

	@Inject(optional = true)
	public void setQosConfig(@Named(QOS_CONFIG) Config qosConfig)
	{
		m_qosConfig = qosConfig;
	}

	@Inject(optional = true)
	public void setSSLSettings(@Named(JETTY_SSL_PORT) int sslPort,
	                           @Named(JETTY_SSL_KEYSTORE_PATH) String keyStorePath,
	                           @Named(JETTY_SSL_KEYSTORE_PASSWORD) String keyStorePassword)
	{
		m_sslPort = sslPort;
		m_keyStorePath = requireNonNullOrEmpty(keyStorePath);
		m_keyStorePassword = requireNonNullOrEmpty(keyStorePassword);
	}

	@Inject(optional = true)
	public void setKeyStoreScannerInterval(@Named(JETTY_SSL_KEYSTORE_SCANNER_INTERVAL) int scannerInterval)
	{
		m_keyStoreScannerInterval = scannerInterval;
	}

	@Inject(optional = true)
	public void setSSLSettings(@Named(JETTY_SSL_TRUSTSTORE_PATH) String truststorePath)
	{
		m_trustStorePath = requireNonNullOrEmpty(truststorePath);
	}

	@Inject(optional = true)
	public void setSSLCipherSuites(@Named(JETTY_SSL_CIPHER_SUITES) String cipherSuites)
	{
		requireNonNull(cipherSuites);
		m_cipherSuites = cipherSuites.split("\\s*,\\s*");
	}

	@Inject(optional = true)
	public void setSSLProtocols(@Named(JETTY_SSL_PROTOCOLS) String protocols)
	{
		m_protocols = protocols.split("\\s*,\\s*");
	}

	@Inject(optional = true)
	public void setThreadPool(@Named(JETTY_THREADS_QUEUE_SIZE_PROPERTY) int maxQueueSize,
	                            @Named(JETTY_THREADS_MIN_PROPERTY) int minThreads,
	                            @Named(JETTY_THREADS_MAX_PROPERTY) int maxThreads,
	                            @Named(JETTY_THREADS_KEEP_ALIVE_MS_PROPERTY) long keepAliveMs)
	{
		m_pool = new QueuedThreadPool(maxThreads, minThreads, (int)keepAliveMs);
		// Note: Jetty 12's QueuedThreadPool manages queue size automatically
	}

	@Inject
	public void setJettyShowStacktrace(@Named(JETTY_SHOW_STACKTRACE) boolean showStacktrace) {
		m_showStacktrace = showStacktrace;
	}

	@Inject(optional = true)
	public void setJettyAuthModuleName(@Named(JETTY_AUTH_MODULE_NAME) String moduleName)
	{
		m_authModuleName = moduleName;
	}

	@Inject(optional = true)
	public void setJettyRequestLoggingEnabled(@Named(JETTY_REQUEST_LOGGING_ENABLED) String loggingEnabled)
	{
		m_requestLoggingEnabled = Boolean.parseBoolean(loggingEnabled);
	}

	@Inject(optional = true)
	public void setJettyRequestLoggingRetainDays(@Named(JETTY_REQUEST_LOGGING_RETAIN_DAYS) String retainDays)
	{
		m_requestLoggingRetainDays = Integer.parseInt(retainDays);
	}

	@Inject(optional = true)
	void setJettyRequestLoggingIgnorePaths(@Named(JETTY_REQUEST_LOGGING_IGNORE_PATHS) List<String> ignorePaths)
	{
		m_loggingIgnorePaths = ignorePaths.toArray(new String[ignorePaths.size()]);
	}

	@Override
	public void start() throws KairosDBException
	{
		try
		{
			if (m_pool != null)
				m_server = new Server(m_pool);
			else
				m_server = new Server();

			//Error handler
			ErrorHandler errorHandler = new ErrorHandler();
			errorHandler.setShowStacks(m_showStacktrace);
			m_server.addBean(errorHandler);

			if (m_port > 0)
			{
				ServerConnector http = new ServerConnector(m_server);
				http.setHost(m_address.getHostName());
				http.setPort(m_port);
				http.setIdleTimeout(m_idleTimeout);
				m_server.addConnector(http);
			}

			if (m_keyStorePath != null && !m_keyStorePath.isEmpty())
				initializeSSL();

			ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
			servletContextHandler.setContextPath("/");

			if (m_authModuleName != null)
			{
				servletContextHandler.setSecurityHandler(initializeAuth());
			}

			// Add GuiceFilter - note: Guice 4.x+ requires manual filter registration
			//FilterHolder guiceFilter = new FilterHolder();
			//guiceFilter.setName("guice");
			//guiceFilter.setClassName(GuiceFilter.class.getName());

			servletContextHandler.addFilter(GuiceFilter.class, "/api/*", EnumSet.of(DispatcherType.REQUEST, DispatcherType.ASYNC));
			//servletContextHandler.addServlet(DefaultServlet.class, "/api/*");
			ServletHolder holder = new ServletHolder(ServletContainer.class);
			holder.setInitParameter("jakarta.ws.rs.Application", GuiceJerseyResourceConfig.class.getName());
			servletContextHandler.addServlet(holder, "/api/*");
			servletContextHandler.setBaseResourceAsString("/");

			//Add this listener so we can get guice injector from within GuiceJerseyResourceConfig
			servletContextHandler.addEventListener(new GuiceServletContextListener() {
				@Override
				protected Injector getInjector() {
					return m_injector;
				}
			});

			ResourceFactory resourceFactory = ResourceFactory.of(m_server);
			ResourceHandler resourceHandler = new ResourceHandler();
			resourceHandler.setBaseResource(resourceFactory.newResource(m_webRoot));
			resourceHandler.setDirAllowed(true);
			resourceHandler.setWelcomeFiles("index.html");

			/*ServletHolder servletHolder = new ServletHolder("static", DefaultServlet.class);
			servletHolder.setInitParameter("baseResource", m_webRoot);
			servletHolder.setInitParameter("dirAllowed","true");
			servletContextHandler.addServlet(servletHolder,"/");
			servletContextHandler.setWelcomeFiles(new String[]{"index.html"});*/

			//adding gzip handler
			GzipHandler gzipHandler = new GzipHandler();
			gzipHandler.addIncludedMimeTypes("application/json");
			gzipHandler.addIncludedMethods("GET","POST");
			gzipHandler.addIncludedPaths("/*");

			//chain handlers
			gzipHandler.setHandler(servletContextHandler);

			Handler currentHandler = gzipHandler;


			if (m_qosConfig != null)
			{
				QoSHandler qoSHandler = new QoSHandler();
				qoSHandler.setHandler(gzipHandler);

				//todo do some config stuff

				currentHandler = qoSHandler;
			}

			resourceHandler.setHandler(currentHandler);
			//m_server.setHandler(resourceHandler);

			m_server.setHandler(servletContextHandler);

			m_server.setDefaultHandler(new DefaultHandler());



			//some code for logging
			if(m_requestLoggingEnabled)
				initializeJettyRequestLogging();

			m_server.start();
		}
		catch (Exception e)
		{
			throw new KairosDBException(e);
		}
	}

	@Override
	public void stop()
	{
		try
		{
			if (m_server != null)
			{
				m_server.stop();
				m_server.join();
			}
		}
		catch (Exception e)
		{
			logger.error("Error stopping web server", e);
		}
	}

	public InetAddress getAddress()
    {
        return m_address;
    }

	private void initializeSSL()
	{
		logger.info("Using SSL");
		HttpConfiguration httpConfig = new HttpConfiguration();
		httpConfig.setSecureScheme("https");
		httpConfig.setSecurePort(m_sslPort);
		httpConfig.addCustomizer(new SecureRequestCustomizer());

		SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
		sslContextFactory.setKeyStorePath(m_keyStorePath);
		sslContextFactory.setKeyStorePassword(m_keyStorePassword);
		if (m_trustStorePath != null && !m_trustStorePath.isEmpty())
			sslContextFactory.setTrustStorePath(m_trustStorePath);

		if (m_cipherSuites != null && m_cipherSuites.length > 0)
			sslContextFactory.setIncludeCipherSuites(m_cipherSuites);

		if (m_protocols != null && m_protocols.length > 0)
			sslContextFactory.setIncludeProtocols(m_protocols);

		//Add dynamic reload of certificates.
		KeyStoreScanner keyStoreScanner = new KeyStoreScanner(sslContextFactory);
		keyStoreScanner.setScanInterval(m_keyStoreScannerInterval);
		m_server.addBean(keyStoreScanner);

		SslConnectionFactory sslFactory = new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString());
		HttpConnectionFactory httpFactory = new HttpConnectionFactory(httpConfig);
		ServerConnector https = new ServerConnector(m_server, sslFactory, httpFactory);
		https.setPort(m_sslPort);
		https.setIdleTimeout(m_idleTimeout);
		m_server.addConnector(https);
	}

	private SecurityHandler initializeAuth() throws Exception
	{
		Constraint constraint = new Constraint.Builder()
				.name("BASIC")
				.roles("*") //authentication is all that's supported so this allows any role.
				.authorization(Constraint.Authorization.ANY_USER)
				.build();

		Constraint noConstraint = new Constraint.Builder()
				.build();

		ConstraintMapping healthcheckConstraintMapping = new ConstraintMapping();
		healthcheckConstraintMapping.setConstraint(noConstraint);
		healthcheckConstraintMapping.setPathSpec("/api/v1/health/*");

		ConstraintMapping cm = new ConstraintMapping();
		cm.setConstraint(constraint);
		cm.setPathSpec("/*");

		ConstraintSecurityHandler csh = new ConstraintSecurityHandler();
		JAASLoginService l = new JAASLoginService();
		l.setName(m_authModuleName);
		l.setLoginModuleName(m_authModuleName);
		csh.addConstraintMapping(healthcheckConstraintMapping);
		csh.addConstraintMapping(cm);
		csh.setLoginService(l);
		l.start();
		return csh;
    }

    private void initializeJettyRequestLogging()
	{
		RequestLogWriter logWriter = new RequestLogWriter("log/jetty-yyyy_mm_dd.request.log");
		CustomRequestLog requestLog = new CustomRequestLog(logWriter, CustomRequestLog.NCSA_FORMAT);
		//NCSARequestLog requestLog = new NCSARequestLog("log/jetty-yyyy_mm_dd.request.log");
		logWriter.setAppend(true);
		logWriter.setTimeZone("UTC");
		logWriter.setRetainDays(m_requestLoggingRetainDays);
		if(m_loggingIgnorePaths != null)
			requestLog.setIgnorePaths(m_loggingIgnorePaths);
		m_server.setRequestLog(requestLog);
	}
}
