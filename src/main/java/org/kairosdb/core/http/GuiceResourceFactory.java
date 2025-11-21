package org.kairosdb.core.http;

import com.google.inject.Provider;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponse;
import org.jboss.resteasy.spi.PropertyInjector;
import org.jboss.resteasy.spi.ResourceFactory;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import java.util.concurrent.CompletionStage;

public class GuiceResourceFactory implements ResourceFactory
{

	private final Provider<?> provider;
	private final Class<?> scannableClass;
	private PropertyInjector propertyInjector;

	public GuiceResourceFactory(final Provider<?> provider, final Class<?> scannableClass) {
		this.provider = provider;
		this.scannableClass = scannableClass;
	}

	public Class<?> getScannableClass() {
		return scannableClass;
	}

	public void registered(ResteasyProviderFactory factory) {
		propertyInjector = factory.getInjectorFactory().createPropertyInjector(scannableClass, factory);
	}

	@Override
	public Object createResource(final HttpRequest request, final HttpResponse response,
			final ResteasyProviderFactory factory) {
		final Object resource = provider.get();
		CompletionStage<Void> propertyStage = propertyInjector.inject(request, response, resource, true);
		return propertyStage == null ? resource
				: propertyStage
				.thenApply(v -> resource);
	}

	public void requestFinished(final HttpRequest request, final HttpResponse response, final Object resource) {
	}

	public void unregistered() {
	}
}
