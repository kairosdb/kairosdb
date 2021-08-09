package org.kairosdb.eventbus;

import com.google.common.eventbus.EventBus;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

/**
 EventBus enhanced to include filters that modify or filter out events.
 Events are passed through the filters first using a sequential dispatcher.
 */
public class FilterEventBus
{
	public static final org.slf4j.Logger logger = LoggerFactory.getLogger(FilterEventBus.class);

	private final LoggingHandler exceptionHandler = new LoggingHandler();
	private final PipelineRegistry filters = new PipelineRegistry(this);
	private final EventBusConfiguration config;

	@Inject
	public FilterEventBus(EventBusConfiguration config)
	{
		super();
		this.config = requireNonNull(config);
	}

	public void register(Object listener)
	{
		filters.register(listener, config.getFilterPriority(listener.getClass().getName()));
	}

	public void register(Object listener, int priority)
	{
		filters.register(listener, priority);
	}

	public <T> Publisher<T> createPublisher(Class<T> tClass)
	{
		return new Publisher<>(filters.getPipeline(tClass));
	}


	/**
	 Handles the given exception thrown by a subscriber with the given context.
	 */
	//@Override
	void handleSubscriberException(Throwable e, SubscriberExceptionContext context)
	{
		requireNonNull(e);
		requireNonNull(context);
		try
		{
			exceptionHandler.handleException(e, context);
		}
		catch (Throwable e2)
		{
			// if the handler threw an exception... well, just log it
			logger.error(String.format(Locale.ROOT, "Exception %s thrown while handling exception: %s", e2, e),
					e2);
		}
	}

	/**
	 Simple logging handler for subscriber exceptions.
	 */
	static final class LoggingHandler
	{
		public void handleException(Throwable exception, SubscriberExceptionContext context)
		{
			Logger logger = logger(context);
			if (logger.isLoggable(Level.SEVERE))
			{
				logger.log(Level.SEVERE, message(context), exception);
			}
		}

		private static Logger logger(SubscriberExceptionContext context)
		{
			return Logger.getLogger(EventBus.class.getName());
		}

		private static String message(SubscriberExceptionContext context)
		{
			Method method = context.getSubscriberMethod();
			return "Exception thrown by subscriber method "
					+ method.getName() + '(' + method.getParameterTypes()[0].getName() + ')'
					+ " on subscriber " + context.getSubscriber()
					+ " when dispatching event: " + context.getEvent();
		}
	}
}
