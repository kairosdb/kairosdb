/*
 * Copyright (C) 2014 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.kairosdb.eventbus;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.util.concurrent.MoreExecutors;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A subscriber method on a filter object. Uses an executor that executes in the same thread.
 *
 * <p>Two subscribers are equivalent when they refer to the same method on the same object (not
 * class). This property is used to ensure that no subscriber method is registered more than once.
 *
 */
class FilterSubscriber
{
    /**
     * Creates a {@code FilterSubscriber} for {@code method} on {@code listener}.
     */
    static FilterSubscriber create(EventBusWithFilters bus, Object listener, Method method, int priority)
    {
        return isDeclaredThreadSafe(method)
                ? new FilterSubscriber(bus, listener, method, priority)
                : new SynchronizedFilterSubscriber(bus, listener, method, priority);
    }

    /**
     * The event bus this subscriber belongs to.
     */
    private EventBusWithFilters bus;

    /**
     * The object with the subscriber method.
     */
    @VisibleForTesting
    final Object target;

    /**
     * FilterSubscriber method.
     */
    private final Method method;

    private final int priority;

    /**
     * Executor to use for dispatching events to this subscriber.
     */
    private final Executor executor = MoreExecutors.directExecutor();

    private FilterSubscriber(EventBusWithFilters bus, Object target, Method method, int priority)
    {
        this.bus = bus;
        this.target = checkNotNull(target);
        this.method = method;
        this.priority = priority;
        method.setAccessible(true);
        checkArgument(priority >=0 && priority <=100, "Priority must be between 0 and 100 inclusive");
    }

    public int getPriority()
    {
        return priority;
    }

    final Object dispatchEvent(final Object event)
    {
        try {
            return invokeSubscriberMethod(event);
        }
        catch (InvocationTargetException e) {
            bus.handleSubscriberException(e.getCause(), context(event));
            return null;
        }
    }

    /**
     * Invokes the subscriber method. This method can be overridden to make the invocation
     * synchronized.
     */
    @VisibleForTesting
    Object invokeSubscriberMethod(Object event)
            throws InvocationTargetException
    {
        try {
            return method.invoke(target, checkNotNull(event));
        }
        catch (IllegalArgumentException e) {
            throw new Error("Method rejected target/argument: " + event, e);
        }
        catch (IllegalAccessException e) {
            throw new Error("Method became inaccessible: " + event, e);
        }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            }
            throw e;
        }
    }

    /**
     * Gets the context for the given event.
     */
    private SubscriberExceptionContext context(Object event)
    {
        return new SubscriberExceptionContext(bus, event, target, method);
    }

    @Override
    public final int hashCode()
    {
        return (31 + method.hashCode()) * 31 + System.identityHashCode(target);
    }

    @Override
    public final boolean equals(@Nullable Object obj)
    {
        if (obj instanceof FilterSubscriber) {
            FilterSubscriber that = (FilterSubscriber) obj;
            // Use == so that different equal instances will still receive events.
            // We only guard against the case that the same object is registered
            // multiple times
            return target == that.target && method.equals(that.method);
        }
        return false;
    }

    /**
     * Checks whether {@code method} is thread-safe, as indicated by the presence of the
     * {@link AllowConcurrentEvents} annotation.
     */
    private static boolean isDeclaredThreadSafe(Method method)
    {
        return method.getAnnotation(AllowConcurrentEvents.class) != null;
    }

    /**
     * FilterSubscriber that synchronizes invocations of a method to ensure that only one thread may enter
     * the method at a time.
     */
    @VisibleForTesting
    static final class SynchronizedFilterSubscriber extends FilterSubscriber
    {

        private SynchronizedFilterSubscriber(EventBusWithFilters bus, Object target, Method method, int priority)
        {
            super(bus, target, method, priority);
        }
    }
}
