package org.kairosdb.eventbus;

import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.EventBus;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EventBus enhanced to include filters that modify or filter out events.
 * Events are passed through the filters first using a sequential dispatcher.
 */
public class EventBusWithFilters extends EventBus
{
    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(EventBusWithFilters.class);

    private final LoggingHandler exceptionHandler = new LoggingHandler();
    private final FilterRegistry filters = new FilterRegistry(this);
    private final EventBusConfiguration config;

    @Inject
    public EventBusWithFilters(EventBusConfiguration config)
    {
        super();
        this.config = checkNotNull(config);
    }

    @Override
    public void register(Object listener)
    {
        filters.register(listener, config.getFilterPriority(listener.getClass().getName()));
        super.register(listener);
    }

    @Override
    public void unregister(Object listener)
    {
        filters.unregister(listener);
        super.unregister(listener);
    }

    @Override
    public void post(Object event)
    {
        Iterator<FilterSubscriber> subscribers = filters.getSubscribers(event);
        if (subscribers.hasNext()) {
            checkNotNull(event);

            Object previousEvent = event;
            while (subscribers.hasNext()) {
                event = subscribers.next().dispatchEvent(event);
                if (event ==  null)
                {
                    event = previousEvent;
                    break;
                }
                else
                {
                    previousEvent = event;
                }
            }
        } else if (!(event instanceof DeadEvent)) {
            // the event had no subscribers and was not itself a DeadEvent
            post(new DeadEvent(this, event));
        }

        // Now post to regular listeners
        super.post(event);
    }

    /**
     * Handles the given exception thrown by a subscriber with the given context.
     */
    void handleSubscriberException(Throwable e, SubscriberExceptionContext context) {
        checkNotNull(e);
        checkNotNull(context);
        try {
            exceptionHandler.handleException(e, context);
        } catch (Throwable e2) {
            // if the handler threw an exception... well, just log it
            logger.error(String.format(Locale.ROOT, "Exception %s thrown while handling exception: %s", e2, e),
                    e2);
        }
    }

    /**
     * Simple logging handler for subscriber exceptions.
     */
    static final class LoggingHandler
    {
        public void handleException(Throwable exception, SubscriberExceptionContext context) {
            Logger logger = logger(context);
            if (logger.isLoggable(Level.SEVERE)) {
                logger.log(Level.SEVERE, message(context), exception);
            }
        }

        private static Logger logger(SubscriberExceptionContext context) {
            return Logger.getLogger(EventBus.class.getName() + "." + context.getEventBus().identifier());
        }

        private static String message(SubscriberExceptionContext context) {
            Method method = context.getSubscriberMethod();
            return "Exception thrown by subscriber method "
                    + method.getName() + '(' + method.getParameterTypes()[0].getName() + ')'
                    + " on subscriber " + context.getSubscriber()
                    + " when dispatching event: " + context.getEvent();
        }
    }
}
