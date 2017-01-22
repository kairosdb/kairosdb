package org.kairosdb.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Aggregators cannot be recursive meaning that QueryProperty cannot contain an QueryProperty.
 * Thus this class exists to allow for properties within properties.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface QueryCompoundProperty
{
    String name() default "";
	String label();

	QueryProperty[] properties() default {};
	String[] order() default {};
}
