package org.kairosdb.core.aggregator.json;


import com.google.common.collect.ImmutableList;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

import java.util.Comparator;
import java.util.List;

public class AggregatorMetadata
{
	private final String name;
	private final String description;
	private final ImmutableList<AggregatorPropertyMetadata> properties;

	public AggregatorMetadata(AggregatorName name, List<AggregatorPropertyMetadata> properties)
	{
		this.name = name.name();
		this.description = name.description();

        properties.sort(new Comparator<AggregatorPropertyMetadata>()
        {
            @Override
            public int compare(AggregatorPropertyMetadata o1, AggregatorPropertyMetadata o2)
            {
                return o1.getLabel().compareTo(o2.getLabel());
            }
        });
        this.properties = ImmutableList.copyOf(properties);
	}

	public String getName()
	{
		return name;
	}

	public String getDescription()
	{
		return description;
	}

	public ImmutableList<AggregatorPropertyMetadata> getProperties()
	{
		return properties;
	}
}
