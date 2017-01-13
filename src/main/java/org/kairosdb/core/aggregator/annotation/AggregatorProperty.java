package org.kairosdb.core.aggregator.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AggregatorProperty
{
    String name() default "";

    String label() default "";

    String description();

    boolean optional() default false;

    String type() default "";

    String[] options() default {};

    String default_value() default "";

    String validation() default "";
}
