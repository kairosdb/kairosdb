package org.kairosdb.core.aggregator.annotation;

public @interface AggregatorProperty
{
    public String name();
    public String type();
    public String[] values() default {};
}
