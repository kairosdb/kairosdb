package org.kairosdb.core.aggregator;


import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.aggregator.annotation.AggregatorProperty;

public class AggregatorMetadata
{
	private final String name;
	private final String description;
	private final ImmutableList<AggregatorPropertyMetadata> properties;

	@Inject
	public AggregatorMetadata(AggregatorName name, AggregatorProperty[] propertyArray)
	{
		this.name = name.name();
		this.description = name.description();

		ImmutableList.Builder<AggregatorPropertyMetadata> builder = ImmutableList.builder();
		for (AggregatorProperty property : propertyArray)
		{
			builder.add(new AggregatorPropertyMetadata(property));
		}
		properties = builder.build();
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
