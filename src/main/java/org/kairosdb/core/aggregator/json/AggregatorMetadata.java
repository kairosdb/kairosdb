package org.kairosdb.core.aggregator.json;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

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
        Builder<AggregatorPropertyMetadata> builder = ImmutableList.builder();
        this.properties = builder.addAll(properties).build();
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
