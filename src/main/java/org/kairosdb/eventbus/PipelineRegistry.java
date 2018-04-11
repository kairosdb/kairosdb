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
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.kairosdb.eventbus.Subscribe;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 Registry of filters to a single event bus. Would have extended SubscriberRegistry but it is final.
 */
public class PipelineRegistry
{
	public static final int DEFAULT_PRIORITY = 50;
	//private static final PriorityComparator COMPARATOR = new PriorityComparator();

	/**
	 All registered subscribers, indexed by event type.
	 <p>
	 <p>The SortedCopyOnWriteArrayList values make it easy and relatively lightweight to get an
	 immutable snapshot of all current subscribers to an event without any locking.
	 */
	//private final ConcurrentMap<Class<?>, SortedCopyOnWriteArrayList<FilterSubscriber>> subscribers =
	//		Maps.newConcurrentMap();

	private final ConcurrentMap<Class<?>, Pipeline> m_piplines = Maps.newConcurrentMap();

	/**
	 The event bus this registry belongs to.
	 */
	private final FilterEventBus bus;

	public PipelineRegistry(FilterEventBus bus)
	{
		this.bus = checkNotNull(bus);
	}

	/**
	 Registers all filter methods on the given listener object with the default priority of 50.
	 */
	public void register(Object filter)
	{
		register(filter, DEFAULT_PRIORITY);
	}

	/**
	 Registers all filter methods on the given listener object.

	 @param filter   filter instance
	 @param priority priority a value between 0 and 100 inclusive. Zero is the highest priority
	 */
	void register(Object filter, int priority)
	{
		checkArgument(priority >= 0 && priority <= 100);
		Multimap<Class<?>, FilterSubscriber> listenerMethods = findAllSubscribers(filter, priority);

		for (Map.Entry<Class<?>, Collection<FilterSubscriber>> entry : listenerMethods.asMap().entrySet())
		{
			Class<?> eventType = entry.getKey();
			Collection<FilterSubscriber> eventMethodsInFilter = entry.getValue();

			Pipeline eventPipeline = m_piplines.computeIfAbsent(eventType, k -> new Pipeline());
			eventPipeline.addAll(eventMethodsInFilter);
		}
	}

	/**
	 Unregisters all filters on the given listener object.
	 */
	/*public void unregister(Object listener)
	{
		Multimap<Class<?>, FilterSubscriber> listenerMethods = findAllSubscribers(listener);

		for (Map.Entry<Class<?>, Collection<FilterSubscriber>> entry : listenerMethods.asMap().entrySet())
		{
			Class<?> eventType = entry.getKey();
			Collection<FilterSubscriber> listenerMethodsForType = entry.getValue();

			CopyOnWriteArrayList<FilterSubscriber> currentFilterSubscribers = subscribers.get(eventType);
			if (currentFilterSubscribers == null || !currentFilterSubscribers.removeAll(listenerMethodsForType))
			{
				// if removeAll returns true, all we really know is that at least one subscriber was
				// removed... however, barring something very strange we can assume that if at least one
				// subscriber was removed, all subscribers on listener for that event type were... after
				// all, the definition of subscribers on a particular class is totally static
				throw new IllegalArgumentException(
						"missing event subscriber for an annotated method. Is " + listener + " registered?");
			}

			// don't try to remove the set if it's empty; that can't be done safely without a lock
			// anyway, if the set is empty it'll just be wrapping an array of length 0
		}
	}*/

	/*@VisibleForTesting
	List<FilterSubscriber> getSubscribersForTesting(Class<?> eventType)
	{
		return MoreObjects.firstNonNull(subscribers.get(eventType), ImmutableList.<FilterSubscriber>of());
	}*/

	public Pipeline getPipeline(Class<?> eventType)
	{
		return m_piplines.computeIfAbsent(eventType, k -> new Pipeline());
	}

	/**
	 Gets an iterator representing an immutable snapshot of all subscribers to the given event at
	 the time this method is called.
	 */
	/*public Iterator<FilterSubscriber> getSubscribers(Object event)
	{
		ImmutableSet<Class<?>> eventTypes = flattenHierarchy(event.getClass());

		List<Iterator<FilterSubscriber>> subscriberIterators =
				Lists.newArrayListWithCapacity(eventTypes.size());

		for (Class<?> eventType : eventTypes)
		{
			CopyOnWriteArrayList<FilterSubscriber> eventSubscribers = subscribers.get(eventType);
			if (eventSubscribers != null)
			{
				// eager no-copy snapshot
				subscriberIterators.add(eventSubscribers.iterator());
			}
		}

		return Iterators.concat(subscriberIterators.iterator());
	}*/

	/**
	 A thread-safe cache that contains the mapping from each class to all methods in that class and
	 all super-classes, that are annotated with {@code @Subscribe}. The cache is shared across all
	 instances of this class; this greatly improves performance if multiple EventBus instances are
	 created and objects of the same class are registered on all of them.
	 */
	private static final LoadingCache<Class<?>, ImmutableList<Method>> subscriberMethodsCache =
			CacheBuilder.newBuilder()
					.weakKeys()
					.build(
							new CacheLoader<Class<?>, ImmutableList<Method>>()
							{
								@SuppressWarnings("NullableProblems")
								@Override
								public ImmutableList<Method> load(Class<?> concreteClass)
										throws Exception
								{
									return getAnnotatedMethodsNotCached(concreteClass);
								}
							});

	private Multimap<Class<?>, FilterSubscriber> findAllSubscribers(Object listener)
	{
		return findAllSubscribers(listener, 0);
	}

	/**
	 Returns all subscribers for the given listener grouped by the type of event they subscribe to.
	 */
	private Multimap<Class<?>, FilterSubscriber> findAllSubscribers(Object listener, int priority)
	{
		Multimap<Class<?>, FilterSubscriber> methodsInListener = HashMultimap.create();
		Class<?> clazz = listener.getClass();
		for (Method method : getAnnotatedMethods(clazz))
		{
			Class<?>[] parameterTypes = method.getParameterTypes();
			Class<?> eventType = parameterTypes[0];
			Class<?> returnType = method.getReturnType();
			methodsInListener.put(eventType, FilterSubscriber.create(bus, listener, method, priority));
		}
		return methodsInListener;
	}

	private static ImmutableList<Method> getAnnotatedMethods(Class<?> clazz)
	{
		return subscriberMethodsCache.getUnchecked(clazz);
	}

	private static ImmutableList<Method> getAnnotatedMethodsNotCached(Class<?> clazz)
	{
		Set<? extends Class<?>> supertypes = TypeToken.of(clazz).getTypes().rawTypes();
		Map<MethodIdentifier, Method> identifiers = Maps.newHashMap();
		for (Class<?> supertype : supertypes)
		{
			for (Method method : supertype.getDeclaredMethods())
			{
				if (method.isAnnotationPresent(Subscribe.class) && !method.isSynthetic())
				{
					Class<?>[] parameterTypes = method.getParameterTypes();
					checkArgument(
							parameterTypes.length == 1,
							"Method %s has @Filter annotation but has %s parameters."
									+ "Filter methods must have exactly 1 parameter.",
							method,
							parameterTypes.length);

                    Class<?> returnType = method.getReturnType();
                    checkArgument((returnType.equals(parameterTypes[0]) || returnType.getName().equals("void")),
                            "Method %s must have return type of %s or void",
                            method, parameterTypes[0].getName());

					MethodIdentifier ident = new MethodIdentifier(method);
					if (!identifiers.containsKey(ident))
					{
						identifiers.put(ident, method);
					}
				}
			}
		}
		return ImmutableList.copyOf(identifiers.values());
	}

	/**
	 Global cache of classes to their flattened hierarchy of supertypes.
	 */
	private static final LoadingCache<Class<?>, ImmutableSet<Class<?>>> flattenHierarchyCache =
			CacheBuilder.newBuilder()
					.weakKeys()
					.build(
							new CacheLoader<Class<?>, ImmutableSet<Class<?>>>()
							{
								// <Class<?>> is actually needed to compile
								@SuppressWarnings({"RedundantTypeArguments", "NullableProblems"})
								@Override
								public ImmutableSet<Class<?>> load(Class<?> concreteClass)
								{
									return ImmutableSet.<Class<?>>copyOf(
											TypeToken.of(concreteClass).getTypes().rawTypes());
								}
							});

	/**
	 Flattens a class's type hierarchy into a set of {@code Class} objects including all
	 superclasses (transitively) and all interfaces implemented by these superclasses.
	 */
	@VisibleForTesting
	static ImmutableSet<Class<?>> flattenHierarchy(Class<?> concreteClass)
	{
		try
		{
			return flattenHierarchyCache.getUnchecked(concreteClass);
		}
		catch (UncheckedExecutionException e)
		{
			throw new RuntimeException(e.getCause());
		}
	}

	private static final class MethodIdentifier
	{

		private final String name;
		private final List<Class<?>> parameterTypes;

		MethodIdentifier(Method method)
		{
			this.name = method.getName();
			this.parameterTypes = Arrays.asList(method.getParameterTypes());
		}

		@Override
		public int hashCode()
		{
			return Objects.hashCode(name, parameterTypes);
		}

		@Override
		public boolean equals(@Nullable Object o)
		{
			if (o instanceof MethodIdentifier)
			{
				MethodIdentifier ident = (MethodIdentifier) o;
				return name.equals(ident.name) && parameterTypes.equals(ident.parameterTypes);
			}
			return false;
		}
	}

	/*private static class PriorityComparator implements Comparator<FilterSubscriber>
	{
		@Override
		public int compare(FilterSubscriber o1, FilterSubscriber o2)
		{
			return (o1.getPriority() < o2.getPriority()) ? -1 : ((o1.getPriority() == o2.getPriority()) ? 1 : 1);
		}
	}*/
}
