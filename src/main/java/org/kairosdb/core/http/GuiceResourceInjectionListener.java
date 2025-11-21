package org.kairosdb.core.http;

import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.ws.rs.ext.Provider;
import org.jboss.resteasy.spi.Registry;
import org.jboss.resteasy.spi.ResourceFactory;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.jboss.resteasy.util.GetRestful;

import java.util.ArrayList;
import java.util.List;

public class GuiceResourceInjectionListener implements ServletContextListener
{

	private final Injector m_injector;

	@Inject
	public GuiceResourceInjectionListener(Injector injector)
	{
		m_injector = injector;
	}


	@Override
	public void contextInitialized(ServletContextEvent sce)
	{
		final ServletContext context = sce.getServletContext();
		final ResteasyDeployment deployment = (ResteasyDeployment) context.getAttribute(ResteasyDeployment.class.getName());
		final Registry registry = deployment.getRegistry();
		final ResteasyProviderFactory providerFactory = deployment.getProviderFactory();


		List<Binding<?>> rootResourceBindings = new ArrayList<Binding<?>>();
		for (final Binding<?> binding : m_injector.getBindings().values()) {
			final Class<?> type = binding.getKey().getTypeLiteral().getRawType();
			if (type != null) {
				if (GetRestful.isRootResource(type)) {
					// deferred registration
					rootResourceBindings.add(binding);
				}
				if (type.isAnnotationPresent(Provider.class)) {
					//LogMessages.LOGGER.info(Messages.MESSAGES.registeringProviderInstance(type.getName()));
					providerFactory.registerProviderInstance(binding.getProvider().get());
				}
			}
		}

		for (Binding<?> binding : rootResourceBindings)
		{
			Class<?> beanClass = (Class<?>) binding.getKey().getTypeLiteral().getType();
			final ResourceFactory resourceFactory = new GuiceResourceFactory(binding.getProvider(), beanClass);
			//LogMessages.LOGGER.info(Messages.MESSAGES.registeringFactory(beanClass.getName()));
			registry.addResourceFactory(resourceFactory);
		}
	}
}

