package org.kairosdb.core.aggregator.json;


import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;

public class QueryMetadata
{
	private final String name;
	private final String description;
	private final ImmutableList<QueryPropertyMetadata> properties;

	public QueryMetadata(String name, String description, List<QueryPropertyMetadata> properties)
	{
		this.name = name;
		this.description = description;

        //noinspection Convert2Lambda
        properties.sort(new Comparator<QueryPropertyMetadata>()
        {
            @Override
            public int compare(QueryPropertyMetadata o1, QueryPropertyMetadata o2)
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

	public ImmutableList<QueryPropertyMetadata> getProperties()
	{
		return properties;
	}
}
