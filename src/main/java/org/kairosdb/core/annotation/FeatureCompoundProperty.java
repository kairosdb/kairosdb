package org.kairosdb.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Aggregators cannot be recursive meaning that FeatureProperty cannot contain a FeatureProperty.
 * Thus this class exists to allow for properties within properties.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FeatureCompoundProperty
{
    String name() default "";
	String label();

	FeatureProperty[] properties() default {};
	String[] order() default {};
}
