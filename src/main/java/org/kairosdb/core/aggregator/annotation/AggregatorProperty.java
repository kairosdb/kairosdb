package org.kairosdb.core.aggregator.annotation;

public @interface AggregatorProperty
{
	String name();

	String label();

	String description();

	boolean optional();

	String type();

	String[] options() default {};

	String default_value();

	String validation() default "";
}
