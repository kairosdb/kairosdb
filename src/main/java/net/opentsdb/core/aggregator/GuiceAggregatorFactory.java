package net.opentsdb.core.aggregator;

import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import net.opentsdb.core.OpenTsdbService;
import net.opentsdb.core.aggregator.annotation.AggregatorName;
import net.opentsdb.core.exception.TsdbException;

import java.util.*;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 3/1/13
 Time: 1:14 PM
 To change this template use File | Settings | File Templates.
 */
public class GuiceAggregatorFactory implements AggregatorFactory
{
	private Map<String, Class<Aggregator>> m_aggregators = new HashMap<String, Class<Aggregator>>();
	private Injector m_injector;


	@Inject
	public GuiceAggregatorFactory(Injector injector)
	{
		m_injector = injector;
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class bindingClass = key.getTypeLiteral().getRawType();
			if (Aggregator.class.isAssignableFrom(bindingClass))
			{
				AggregatorName ann = (AggregatorName)bindingClass.getAnnotation(AggregatorName.class);
				if (ann == null)
					continue;

				m_aggregators.put(ann.name(), bindingClass);
			}
		}
	}

	public Aggregator createAggregator(String name)
	{
		Class<Aggregator> aggClass = m_aggregators.get(name);

		if (aggClass == null)
			return (null);

		Aggregator agg = m_injector.getInstance(aggClass);
		return (agg);
	}

}
