package org.kairosdb.core.http;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import jakarta.inject.Inject;
import jakarta.servlet.ServletContext;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.server.ResourceConfig;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;

import java.util.Set;

public class GuiceJerseyResourceConfig extends ResourceConfig
{
	@Inject
	public GuiceJerseyResourceConfig(ServiceLocator serviceLocator, ServletContext servletContext)
	{
		super();

		Injector injector = (Injector) servletContext.getAttribute(Injector.class.getName());

		Set<Class<?>> resources = injector.getInstance(Key.get(new TypeLiteral<>() {}, ResourceClasses.class));

		System.out.println("Registering resource classes "+resources);
		registerClasses(resources);

		// We access the injector that was attached to the context by GuiceServletContextListener
		GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);
		GuiceIntoHK2Bridge guiceBridge = serviceLocator.getService(GuiceIntoHK2Bridge.class);
		guiceBridge.bridgeGuiceInjector(injector);
	}
}
