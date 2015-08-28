package org.kairosdb.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 Created by bhawkins on 3/11/15.
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface PluginName
{
	String name();
	String description();
}
