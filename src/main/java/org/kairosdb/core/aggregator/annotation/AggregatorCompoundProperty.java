package org.kairosdb.core.aggregator.annotation;

/**
 * Aggregators cannot be recursive meaning that AggregatorProperty cannot contain an AggregatorProperty.
 * Thus this class exists to allow for properties within properties.
 */
public @interface AggregatorCompoundProperty
{
	String name();
}
