package org.kairosdb.core.http;

import com.google.inject.Injector;
import jakarta.inject.Inject;
import jakarta.servlet.ServletContext;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.server.ResourceConfig;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;

public class GuiceJerseyResourceConfig extends ResourceConfig
{
	@Inject
	public GuiceJerseyResourceConfig(ServiceLocator serviceLocator, ServletContext servletContext)
	{
		super(getResourceConfigFromGuice(servletContext));

		// We access the injector that was attached to the context by GuiceServletContextListener
		Injector injector = (Injector) servletContext.getAttribute(Injector.class.getName());
		GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);
		GuiceIntoHK2Bridge guiceBridge = serviceLocator.getService(GuiceIntoHK2Bridge.class);
		guiceBridge.bridgeGuiceInjector(injector);
	}

	private static ResourceConfig getResourceConfigFromGuice(ServletContext servletContext)
	{
		Injector injector = (Injector) servletContext.getAttribute(Injector.class.getName());
		JerseyConfiguration jerseyConfiguration = injector.getInstance(JerseyConfiguration.class);
		return jerseyConfiguration.getResourceConfig();
	}
}
